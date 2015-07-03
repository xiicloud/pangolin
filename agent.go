package pangolin

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/url"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
)

type ConnectionHandler interface {
	Handle(net.Conn) error
}

type Agent struct {
	peerUrl    *url.URL
	serviceUrl *url.URL
	id         string
	conn       net.Conn
	tlsConfig  *tls.Config
	auth       Authenticator
	handler    ConnectionHandler
	cmdLock    sync.Mutex
	p          Protocol
}

var (
	ErrUnsupportedProtocol = errors.New("protocol not supported")
	ErrClosed              = errors.New("connection closed")
)

func NewAgent(id, peerAddr, serviceAddr string, tlsConfig *tls.Config, auth Authenticator) (*Agent, error) {
	peerUrl, err := url.Parse(peerAddr)
	if err != nil {
		return nil, err
	}
	serviceUrl, err := url.Parse(serviceAddr)
	if err != nil {
		return nil, err
	}

	if serviceUrl.Scheme == "http" || serviceUrl.Scheme == "https" {
		serviceUrl.Scheme = "tcp"
	}

	if peerUrl.Scheme != "tcp" && peerUrl.Scheme != "unix" &&
		peerUrl.Scheme != "http" && peerUrl.Scheme != "https" {
		return nil, ErrUnsupportedProtocol
	}
	if serviceUrl.Scheme != "tcp" && serviceUrl.Scheme != "unix" {
		return nil, ErrUnsupportedProtocol
	}

	agent := &Agent{
		peerUrl:    peerUrl,
		serviceUrl: serviceUrl,
		id:         id,
		tlsConfig:  tlsConfig,
		auth:       auth,
		p:          Protocol{auth: auth},
	}
	agent.handler = agent
	return agent, nil
}

func (self *Agent) dial(addr *url.URL) (net.Conn, error) {
	if self.tlsConfig != nil && addr.Scheme != "unix" {
		return tlsDial(addr.Scheme, addr.Host, self.tlsConfig)
	}

	switch addr.Scheme {
	case "unix":
		return net.Dial(addr.Scheme, addr.Path)
	case "tcp", "http", "https":
		return net.Dial("tcp", addr.Host)
	default:
		return nil, ErrUnsupportedProtocol
	}
}

func (self *Agent) Join() error {
	conn, err := self.newConn(true)

	if err != nil {
		return err
	}

	err = self.p.Join(conn, []byte(self.id))
	if err != nil {
		conn.Close()
		return err
	}
	self.conn = conn
	conn.SetReadDeadline(time.Time{})
	return nil
}

func (self *Agent) Serve() error {
	defer self.conn.Close()
	for {
		reqId, err := self.p.GetCmd(self.conn)
		if err == io.EOF {
			log.Error("pangolin-agent: command tunnel closed")
			return err
		}

		if err != nil {
			log.Error("pangolin-agent: ", err)
			return err
		}

		workerConn, err := self.createWorker(uint32(reqId))
		if err == nil {
			go self.handler.Handle(workerConn)
		} else {
			log.Error("pangolin-agent: ", err)
		}
	}
	return nil
}

func (self *Agent) newConn(keepalive bool) (conn net.Conn, err error) {
	if self.peerUrl.Scheme == "http" || self.peerUrl.Scheme == "https" {
		conn, err = self.HijackHTTP(keepalive)
	} else {
		conn, err = self.dial(self.peerUrl)
	}

	return
}

func (self *Agent) createWorker(connId uint32) (net.Conn, error) {
	conn, err := self.newConn(false)
	if err != nil {
		log.Error("pangolin-agent: connection to controller failed: ", err)
		return nil, err
	}

	err = self.p.NewWorker(conn, connId)
	if err != nil {
		log.Error("pangolin-agent: report connection failed, ", err)
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func (self *Agent) Handle(conn net.Conn) error {
	defer conn.Close()
	backend, err := self.dial(self.serviceUrl)
	if err != nil {
		log.Error("pangolin-agent: backend connection failed: ", err)
		return err
	}
	defer backend.Close()

	log.Debug("pangolin-agent: new connection establisthed")

	ch := make(chan error)
	cp := func(dst io.Writer, src io.Reader) {
		_, err := io.Copy(dst, src)
		ch <- err
	}
	go cp(backend, conn)
	go cp(conn, backend)

	<-ch
	return <-ch
}

func (self *Agent) NewListener() (net.Listener, error) {
	listener := &AgentListener{
		agent:   self,
		workers: make(chan net.Conn, 100),
	}
	self.handler = listener
	return listener, nil
}

type AgentListener struct {
	agent   *Agent
	workers chan net.Conn
	closed  bool
	lock    sync.RWMutex
	once    sync.Once
}

func (self *AgentListener) Accept() (net.Conn, error) {
	conn, ok := <-self.workers
	if ok {
		return conn, nil
	}
	return nil, ErrClosed
}

func (self *AgentListener) Close() error {
	self.once.Do(func() {
		self.lock.Lock()
		defer self.lock.Unlock()
		close(self.workers)
		self.closed = true
		self.agent.conn.Close()
	})
	return nil
}

func (self *AgentListener) Addr() net.Addr {
	return self.agent.conn.LocalAddr()
}

func (self *AgentListener) Handle(conn net.Conn) error {
	self.lock.RLock()
	if self.closed {
		self.lock.RUnlock()
		conn.Close()
		return ErrClosed
	}
	self.lock.RUnlock()

	self.workers <- conn
	return nil
}
