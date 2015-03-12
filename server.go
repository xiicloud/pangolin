package pangolin

import (
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
)

const (
	DefaultDialTimeout = time.Second * 3
)

type Command struct {
	ConnId string
	Cmd    string
	Args   []interface{}
}

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
	workerConnectsions map[string]net.Conn
	workerLock         sync.RWMutex
}

func NewHub() *Hub {
	return &Hub{
		idGen:              newIdGenerator("cmd"),
		onlineAgents:       make(map[string]net.Conn),
		workerConnectsions: make(map[string]net.Conn),
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

		go func(hub *Hub, conn net.Conn) {
			msg := make(map[string]string)
			err := json.NewDecoder(conn).Decode(&msg)
			if err != nil {
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
				hub.addAgentConn(id, conn)
			case "worker":
				hub.addWorkerConn(id, conn)
			case "ping":
				log.Debug("pangolin: ping from ", id)
			case "error":
				log.Error("pangolin: error occured, ", msg["message"])
			default:
				log.Error("pangolin: malformed frame. cmd is missing")
				return
			}

			// Don't close the conn!!!
		}(hub, conn)
	}
	return nil
}

func (hub *Hub) addAgentConn(id string, conn net.Conn) {
	log.Debug("pangolin: add agent ", id)
	hub.agentsLock.Lock()
	defer hub.agentsLock.Unlock()
	oldConn, ok := hub.onlineAgents[id]
	if ok {
		// close the stale connection
		oldConn.Close()
	}
	hub.onlineAgents[id] = conn
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

func (hub *Hub) addWorkerConn(id string, conn net.Conn) {
	hub.workerLock.Lock()
	defer hub.workerLock.Unlock()
	_, ok := hub.workerConnectsions[id]
	if ok {
		// There is already a worker connection with the same ID.
		// The new worker can't be added.
		conn.Close()
		return
	}
	hub.workerConnectsions[id] = conn
}

func (hub *Hub) CloseWorker(id string) error {
	hub.workerLock.Lock()
	defer hub.workerLock.Unlock()
	conn, ok := hub.workerConnectsions[id]
	if ok {
		delete(hub.workerConnectsions, id)
		return conn.Close()
	}
	return nil
}

func (hub *Hub) GetWorkerConn(id string) net.Conn {
	hub.workerLock.RLock()
	defer hub.workerLock.RUnlock()
	return hub.workerConnectsions[id]
}

func (hub *Hub) GetAgentConn(id string) net.Conn {
	hub.agentsLock.RLock()
	defer hub.agentsLock.RUnlock()
	return hub.onlineAgents[id]
}

func (hub *Hub) NewWorkerConn(agentConn net.Conn, connId string, timeout time.Duration) (net.Conn, error) {
	cmd := Command{
		ConnId: connId,
		Cmd:    "new_conn",
	}
	err := json.NewEncoder(agentConn).Encode(cmd)
	if err != nil {
		return nil, err
	}

	var (
		timedOut int32
		conn     net.Conn
		timer    = time.NewTimer(timeout)
	)
	defer timer.Stop()
	go func() {
		<-timer.C
		atomic.StoreInt32(&timedOut, 1)
	}()

	for atomic.LoadInt32(&timedOut) == 0 {
		conn = hub.GetWorkerConn(connId)
		if conn != nil {
			return conn, nil
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

	conn := &Conn{
		id:  hub.idGen.Generate(),
		hub: hub,
	}
	netConn, err := hub.NewWorkerConn(agentConn, conn.id, DefaultDialTimeout)
	if err != nil {
		return nil, err
	}
	conn.Conn = netConn
	return conn, nil
}

func (hub *Hub) DialTimeout(network, addr string, _ time.Duration) (net.Conn, error) {
	return hub.Dial(network, addr)
}

// Implement net.Conn interface.
type Conn struct {
	hub *Hub
	id  string
	net.Conn
}

func (conn *Conn) Close() error {
	return conn.hub.CloseWorker(conn.id)
}

func (conn *Conn) LocalAddr() net.Addr {
	return Addr{Id: conn.id}
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
