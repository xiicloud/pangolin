package pangolin

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
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
	dec        *json.Decoder
	enc        *json.Encoder
	tlsConfig  *tls.Config
	auth       Authenticator
	handler    ConnectionHandler
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
	conn, err := self.newConn()

	if err != nil {
		return err
	}
	self.conn = conn

	msg := map[string]string{
		"id":  self.id,
		"cmd": "join",
	}
	self.enc = json.NewEncoder(conn)
	self.dec = json.NewDecoder(conn)
	err = self.enc.Encode(msg)
	conn.SetReadDeadline(time.Time{})
	return err
}

func (self *Agent) Serve() error {
	defer self.conn.Close()
	for {
		msg := make(map[string]string)
		err := self.dec.Decode(&msg)
		if err != nil {
			log.Error("pangolin-agent: json error: ", err)
			return err
		}
		log.Debug("pangolin-agent: got command ", msg)
		switch msg["Cmd"] {
		case "new_conn":
			workerConn, err := self.createWorker(msg["ConnId"])
			if err == nil {
				go self.handler.Handle(workerConn)
			}
		}
	}
}

func (self *Agent) reportError(msg string) {
	self.enc.Encode(map[string]string{
		"id":      self.id,
		"message": msg,
		"cmd":     "error",
	})
}

func (self *Agent) newConn() (conn net.Conn, err error) {
	if self.peerUrl.Scheme == "http" || self.peerUrl.Scheme == "https" {
		conn, err = self.HijackHTTP()
	} else {
		conn, err = self.dial(self.peerUrl)
	}

	if err == nil && self.auth != nil {
		fmt.Fprintf(conn, `{"id":%q,"token":%q}`, self.id, self.auth.Token())
	}
	return
}

func (self *Agent) createWorker(connId string) (net.Conn, error) {
	conn, err := self.newConn()
	if err != nil {
		log.Error("pangolin-agent: connection to controller failed: ", err)
		return nil, err
	}

	_, err = fmt.Fprintf(conn, `{"id":%q,"cmd":"worker"}`, connId)
	if err != nil {
		log.Error("pangolin-agent: report connection failed, ", err)
		return nil, err
	}
	return conn, nil
}

func (self *Agent) Handle(conn net.Conn) error {
	defer conn.Close()
	backend, err := self.dial(self.serviceUrl)
	if err != nil {
		log.Error("pangolin-agent: backend connection failed: ", err)
		self.reportError(err.Error())
		return err
	}
	defer backend.Close()

	log.Debug("pangolin-agent: new connection establisthed")

	go io.Copy(backend, conn)
	_, err = io.Copy(conn, backend)
	return err
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
	defer self.lock.RUnlock()
	if self.closed {
		return ErrClosed
	} else {
		self.workers <- conn
		return nil
	}
}
