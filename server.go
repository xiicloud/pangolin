package pangolin

import (
	"errors"
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

type newAgentCallback func(agentId string)

type workerConn struct {
	net.Conn
	expires time.Time
}

type agentConn struct {
	net.Conn
	p    Protocol
	lock sync.Mutex
}

func (ac *agentConn) requestConn(id uint32) error {
	ac.lock.Lock()
	defer ac.lock.Unlock()
	return ac.p.NewWorker(ac, id, false)
}

type Hub struct {
	p     Protocol
	idGen IdGenerator

	// The connections that issued by agent and are used for transporting instructions.
	// When an agent joins to the controller, it issues a persistent connection to
	// the controller to wait for instructions such as "new_connection".
	// The key of the map is the ID of the request.
	onlineAgents map[string]*agentConn
	agentsLock   sync.RWMutex

	// The network connections issued by agent.
	// A new connection is created by the agent once it receives the "new_connection" instruction.
	// The connection is closed when the RPC call is finished.
	// The key of the map is the ID that the "new_connection" command had specified.
	workerConnections map[uint32]*workerConn
	workerLock        sync.RWMutex

	// A guard to ensure only connections issued from the Dial method are accepted.
	pendingWorkers map[uint32]struct{}
	pendingLock    sync.RWMutex

	auth             Authenticator
	newAgentCallback newAgentCallback
}

func NewHub(auth Authenticator) *Hub {
	hub := &Hub{
		idGen:             newIdGenerator(),
		onlineAgents:      make(map[string]*agentConn),
		workerConnections: make(map[uint32]*workerConn),
		pendingWorkers:    make(map[uint32]struct{}),
		auth:              auth,
		p:                 Protocol{auth: auth},
	}
	go hub.gc()
	return hub
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
	cmd, err := hub.p.GetAgentCmd(conn)
	if err != nil {
		log.Error("pangolin: ", err)
		conn.Close()
		return
	}
	log.Debugf("pangolin: got command %d", cmd)

	if cmd == CmdJoin {
		agentId, err := hub.p.GetAgentId(conn)
		if err != nil {
			log.Error("pangolin: CmdJoin ", err)
			conn.Close()
		}
		hub.AddAgentConn(agentId, conn)
		return
	}

	if cmd >= MinRequestId {
		hub.AddWorkerConn(uint32(cmd), conn)
	} else {
		conn.Close()
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
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(false)
	}

	conn.Write([]byte("HTTP/1.1 101 UPGRADED\r\nContent-Type: text/raw-stream\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n"))
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
	hub.onlineAgents[id] = &agentConn{Conn: conn, p: hub.p}
	if hub.newAgentCallback != nil {
		hub.newAgentCallback(id)
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
		tcpConn.SetKeepAlive(true)
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

func (hub *Hub) OnlineAgents() map[string]string {
	agents := make(map[string]string)
	hub.agentsLock.RLock()
	defer hub.agentsLock.RUnlock()
	for id, conn := range hub.onlineAgents {
		agents[id] = conn.RemoteAddr().String()
	}
	return agents
}

func (hub *Hub) AddWorkerConn(id uint32, conn net.Conn) {
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
	hub.workerConnections[id] = &workerConn{Conn: conn, expires: time.Now().Add(DefaultDialTimeout)}
}

// Ensure the connection is closed if no consumer pick it up.
func (hub *Hub) gc() {
	for range time.Tick(DefaultDialTimeout) {
		hub.workerLock.Lock()
		for id, worker := range hub.workerConnections {
			if worker.expires.Before(time.Now()) {
				delete(hub.workerConnections, id)
				worker.Close()
			}
		}
		hub.workerLock.Unlock()
	}
}

func (hub *Hub) CloseWorker(id uint32) error {
	hub.workerLock.Lock()
	defer hub.workerLock.Unlock()
	conn, ok := hub.workerConnections[id]
	if ok {
		delete(hub.workerConnections, id)
		return conn.Close()
	}
	return nil
}

func (hub *Hub) GetWorkerConn(id uint32) *workerConn {
	hub.workerLock.Lock()
	defer hub.workerLock.Unlock()
	if conn, ok := hub.workerConnections[id]; ok {
		delete(hub.workerConnections, id)
		return conn
	}
	return nil
}

func (hub *Hub) GetAgentConn(id string) *agentConn {
	hub.agentsLock.RLock()
	defer hub.agentsLock.RUnlock()
	return hub.onlineAgents[id]
}

func (hub *Hub) addPendingWorker(id uint32) {
	hub.pendingLock.Lock()
	hub.pendingWorkers[id] = struct{}{}
	hub.pendingLock.Unlock()
}

func (hub *Hub) removePendingWorker(id uint32) {
	hub.pendingLock.Lock()
	delete(hub.pendingWorkers, id)
	hub.pendingLock.Unlock()
}

func (hub *Hub) hasPendingWorker(id uint32) bool {
	hub.pendingLock.RLock()
	defer hub.pendingLock.RUnlock()
	_, ok := hub.pendingWorkers[id]
	return ok
}

func (hub *Hub) NewWorkerConn(ac *agentConn, connId uint32, timeout time.Duration) (net.Conn, error) {
	hub.addPendingWorker(connId)
	defer hub.removePendingWorker(connId)
	if err := ac.requestConn(connId); err != nil {
		return nil, err
	}

	var (
		conn *workerConn
		ch   = time.After(timeout)
	)

	for {
		select {
		case <-ch:
			break
		default:
			conn = hub.GetWorkerConn(connId)
			if conn != nil {
				return conn.Conn, nil
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	return nil, errors.New("pangolin: timeout occured")
}

// addr must be the ID of the agent.
func (hub *Hub) Dial(_, addr string) (net.Conn, error) {
	ac := hub.GetAgentConn(strings.Split(addr, ":")[0])
	if ac == nil {
		return nil, &net.OpError{
			Op:   "dial",
			Net:  "tcp",
			Addr: Addr{Id: addr},
			Err:  errors.New("agent is not connected to the cluster")}
	}

	id := hub.idGen.Generate()
	netConn, err := hub.NewWorkerConn(ac, id, DefaultDialTimeout)
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
