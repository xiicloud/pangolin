package rrpc

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

type Bus struct {
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

func NewBus() *Bus {
	return &Bus{
		idGen:              newIdGenerator("cmd"),
		onlineAgents:       make(map[string]net.Conn),
		workerConnectsions: make(map[string]net.Conn),
	}
}

func (bus *Bus) ListenAndServe(addr string) error {
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
				log.Errorf("rrpc: Accept error: %v; retrying in %v", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return err
		}
		tempDelay = 0

		go func(bus *Bus, conn net.Conn) {
			msg := make(map[string]string)
			err := json.NewDecoder(conn).Decode(&msg)
			if err != nil {
				log.Error("rrpc: ", err)
				return
			}
			log.Debug("rrpc: got message ", msg)

			id, ok := msg["id"]
			if !ok {
				log.Error("rrpc: malformed frame. id is missing")
				return
			}

			switch msg["cmd"] {
			case "join":
				bus.addAgentConn(id, conn)
			case "worker":
				bus.addWorkerConn(id, conn)
			case "ping":
				log.Debug("rrpc: ping from ", id)
			case "error":
				log.Error("rrpc: error occured, ", msg["message"])
			default:
				log.Error("rrpc: malformed frame. cmd is missing")
				return
			}

			// Don't close the conn!!!
		}(bus, conn)
	}
	return nil
}

func (bus *Bus) addAgentConn(id string, conn net.Conn) {
	log.Debug("rrpc: add agent ", id)
	bus.agentsLock.Lock()
	defer bus.agentsLock.Unlock()
	oldConn, ok := bus.onlineAgents[id]
	if ok {
		// close the stale connection
		oldConn.Close()
	}
	bus.onlineAgents[id] = conn
}

func (bus *Bus) CloseAgent(id string) error {
	bus.agentsLock.Lock()
	defer bus.agentsLock.Unlock()
	conn, ok := bus.onlineAgents[id]
	if ok {
		delete(bus.onlineAgents, id)
		return conn.Close()
	}
	return nil
}

func (bus *Bus) addWorkerConn(id string, conn net.Conn) {
	bus.workerLock.Lock()
	defer bus.workerLock.Unlock()
	_, ok := bus.workerConnectsions[id]
	if ok {
		// There is already a worker connection with the same ID.
		// The new worker can't be added.
		conn.Close()
		return
	}
	bus.workerConnectsions[id] = conn
}

func (bus *Bus) CloseWorker(id string) error {
	bus.workerLock.Lock()
	defer bus.workerLock.Unlock()
	conn, ok := bus.workerConnectsions[id]
	if ok {
		delete(bus.workerConnectsions, id)
		return conn.Close()
	}
	return nil
}

func (bus *Bus) GetWorkerConn(id string) net.Conn {
	bus.workerLock.RLock()
	defer bus.workerLock.RUnlock()
	return bus.workerConnectsions[id]
}

func (bus *Bus) GetAgentConn(id string) net.Conn {
	bus.agentsLock.RLock()
	defer bus.agentsLock.RUnlock()
	return bus.onlineAgents[id]
}

func (bus *Bus) NewWorkerConn(agentConn net.Conn, connId string, timeout time.Duration) (net.Conn, error) {
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
		conn = bus.GetWorkerConn(connId)
		if conn != nil {
			return conn, nil
		}
	}
	return nil, errors.New("rrpc: timeout occured")
}

// addr must be the ID of the agent.
func (bus *Bus) Dial(_, addr string) (net.Conn, error) {
	agentConn := bus.GetAgentConn(strings.Split(addr, ":")[0])
	if agentConn == nil {
		return nil, &net.OpError{
			Op:   "dial",
			Net:  "tcp",
			Addr: Addr{Id: addr},
			Err:  errors.New("agent is not connected to the cluster")}
	}

	conn := &Conn{
		id:  bus.idGen.Generate(),
		bus: bus,
	}
	netConn, err := bus.NewWorkerConn(agentConn, conn.id, DefaultDialTimeout)
	if err != nil {
		return nil, err
	}
	conn.Conn = netConn
	return conn, nil
}

func (bus *Bus) DialTimeout(network, addr string, _ time.Duration) (net.Conn, error) {
	return bus.Dial(network, addr)
}

// Implement net.Conn interface.
type Conn struct {
	bus *Bus
	id  string
	net.Conn
}

func (conn *Conn) Close() error {
	return conn.bus.CloseWorker(conn.id)
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
