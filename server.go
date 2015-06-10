package pangolin

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
)

const (
	DefaultDialTimeout = time.Second * 3
	DefaultMaxIdleTime = time.Second * 35
)

type Authenticator interface {
	Auth(id, token string) error
	Token() string
}

type Command struct {
	ConnId string
	Cmd    string
	Args   []interface{}
}

type newAgentCallback func(agentId string)

type Hub struct {
	idGen IdGenerator

	// The connections that issued by agent and are used for transporting instructions.
	// When an agent joins to the controller, it issues a persistent connection to
	// the controller to wait for instructions such as "new_connection".
	// The key of the map is the ID of the request.
	onlineAgents map[string]net.Conn
	agentsLock   sync.RWMutex

	// The network connections issued by agent.
	// A new connection is created by the agent once it receives the "new_connection" instruction.
	// The connection is closed when the RPC call is finished.
	// The key of the map is the ID that the "new_connection" command had specified.
	workerConnections map[string]net.Conn
	workerLock        sync.RWMutex

	// A guard to ensure only connections issued from the Dial method are accepted.
	pendingWorkers map[string]struct{}
	pendingLock    sync.RWMutex

	auth             Authenticator
	newAgentCallback newAgentCallback
}

func NewHub(auth Authenticator) *Hub {
	return &Hub{
		idGen:             newIdGenerator("cmd"),
		onlineAgents:      make(map[string]net.Conn),
		workerConnections: make(map[string]net.Conn),
		pendingWorkers:    make(map[string]struct{}),
		auth:              auth,
	}
}

func (hub *Hub) ListenAndServe(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	defer l.Close()
	var tempDelay time.Duration // how long to sleep on accept failure

	for {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				log.Errorf("pangolin: Accept error: %v; retrying in %v", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return err
		}
		tempDelay = 0
		go hub.Handle(conn)
	}
	return nil
}

func (hub *Hub) Handle(conn net.Conn) {
	dec := json.NewDecoder(conn)
	if hub.auth != nil {
		credentials := make(map[string]string)
		err := dec.Decode(&credentials)
		if err != nil {
			log.Error("pangolin: json ", err)
			conn.Close()
			return
		}

		err = hub.auth.Auth(credentials["id"], credentials["token"])
		if err != nil {
			log.Error("pangolin: auth failed ", err)
			conn.Close()
			return
		}
	}

	msg := make(map[string]string)
	err := dec.Decode(&msg)
	if err != nil {
		conn.Close()
		log.Error("pangolin: ", err)
		return
	}
	log.Debug("pangolin: got message ", msg)
	id, ok := msg["id"]
	if !ok {
		log.Error("pangolin: malformed frame. id is missing")
		return
	}

	switch msg["cmd"] {
	case "join":
		hub.AddAgentConn(id, conn)
	case "worker":
		hub.AddWorkerConn(id, conn)
	case "error":
		log.Error("pangolin: error occured, ", msg["message"])
	default:
		log.Error("pangolin: malformed frame. cmd is missing")
		return
	}
}

// Let the server reuse the HTTP port.
func (hub *Hub) HijackHTTP(w http.ResponseWriter, r *http.Request) {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		w.WriteHeader(500)
		return
	}
	conn, _, err := hijacker.Hijack()
	if err != nil {
		w.WriteHeader(400)
		return
	}

	fmt.Fprintf(conn, "HTTP/1.1 101 UPGRADED\r\nContent-Type: text/raw-stream\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n")
	hub.Handle(conn)
}

func (hub *Hub) SetNewAgentCallback(callback newAgentCallback) {
	hub.newAgentCallback = callback
}

func (hub *Hub) AddAgentConn(id string, conn net.Conn) {
	log.Debug("pangolin: add agent ", id)
	hub.agentsLock.Lock()
	defer hub.agentsLock.Unlock()
	oldConn, ok := hub.onlineAgents[id]
	if ok {
		// close the stale connection
		oldConn.Close()
	}
	hub.onlineAgents[id] = conn
	if hub.newAgentCallback != nil {
		hub.newAgentCallback(id)
	}
}

func (hub *Hub) CloseAgent(id string) error {
	hub.agentsLock.Lock()
	defer hub.agentsLock.Unlock()
	conn, ok := hub.onlineAgents[id]
	if ok {
		delete(hub.onlineAgents, id)
		return conn.Close()
	}
	return nil
}

func (hub *Hub) AddWorkerConn(id string, conn net.Conn) {
	log.Debug("pangolin: AddWorkerConn ", id)

	// Close unexpected connection.
	if !hub.hasPendingWorker(id) {
		conn.Close()
		return
	}

	hub.workerLock.Lock()
	defer hub.workerLock.Unlock()
	_, ok := hub.workerConnections[id]
	if ok {
		// There is already a worker connection with the same ID.
		// The new worker can't be added.
		conn.Close()
		return
	}
	hub.workerConnections[id] = conn
}

func (hub *Hub) CloseWorker(id string) error {
	hub.workerLock.Lock()
	defer hub.workerLock.Unlock()
	conn, ok := hub.workerConnections[id]
	if ok {
		delete(hub.workerConnections, id)
		return conn.Close()
	}
	return nil
}

func (hub *Hub) GetWorkerConn(id string) net.Conn {
	hub.workerLock.RLock()
	defer hub.workerLock.RUnlock()
	return hub.workerConnections[id]
}

func (hub *Hub) GetAgentConn(id string) net.Conn {
	hub.agentsLock.RLock()
	defer hub.agentsLock.RUnlock()
	return hub.onlineAgents[id]
}

func (hub *Hub) addPendingWorker(id string) {
	hub.pendingLock.Lock()
	hub.pendingWorkers[id] = struct{}{}
	hub.pendingLock.Unlock()
}

func (hub *Hub) removePendingWorker(id string) {
	hub.pendingLock.Lock()
	delete(hub.pendingWorkers, id)
	hub.pendingLock.Unlock()
}

func (hub *Hub) hasPendingWorker(id string) bool {
	hub.pendingLock.RLock()
	defer hub.pendingLock.RUnlock()
	_, ok := hub.pendingWorkers[id]
	return ok
}

func (hub *Hub) NewWorkerConn(agentConn net.Conn, connId string, timeout time.Duration) (net.Conn, error) {
	cmd := Command{
		ConnId: connId,
		Cmd:    "new_conn",
	}
	hub.addPendingWorker(connId)
	defer hub.removePendingWorker(connId)
	err := json.NewEncoder(agentConn).Encode(cmd)
	if err != nil {
		return nil, err
	}

	var (
		conn net.Conn
		ch   = time.After(timeout)
	)

	for {
		select {
		case <-ch:
			break
		default:
			conn = hub.GetWorkerConn(connId)
			if conn != nil {
				hub.workerLock.Lock()
				delete(hub.workerConnections, connId)
				hub.workerLock.Unlock()
				return conn, nil
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	return nil, errors.New("pangolin: timeout occured")
}

// addr must be the ID of the agent.
func (hub *Hub) Dial(_, addr string) (net.Conn, error) {
	agentConn := hub.GetAgentConn(strings.Split(addr, ":")[0])
	if agentConn == nil {
		return nil, &net.OpError{
			Op:   "dial",
			Net:  "tcp",
			Addr: Addr{Id: addr},
			Err:  errors.New("agent is not connected to the cluster")}
	}

	id := hub.idGen.Generate()
	netConn, err := hub.NewWorkerConn(agentConn, id, DefaultDialTimeout)
	if err != nil {
		return nil, err
	}
	return netConn, nil
}

func (hub *Hub) DialTimeout(network, addr string, _ time.Duration) (net.Conn, error) {
	return hub.Dial(network, addr)
}

// Implement net.Addr interface.
type Addr struct {
	Id string
}

func (addr Addr) Network() string {
	return "tcp"
}

func (addr Addr) String() string {
	return addr.Id
}
