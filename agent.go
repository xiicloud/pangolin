package pangolin

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"time"

	log "github.com/Sirupsen/logrus"
)

type Agent struct {
	peerUrl    *url.URL
	serviceUrl *url.URL
	id         string
	conn       net.Conn
	dec        *json.Decoder
	enc        *json.Encoder
	tlsConfig  *tls.Config
	auth       Authenticator
}

var (
	ErrUnsupportedProtocol = errors.New("protocol not supported")
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

	return &Agent{
		peerUrl:    peerUrl,
		serviceUrl: serviceUrl,
		id:         id,
		tlsConfig:  tlsConfig,
		auth:       auth,
	}, nil
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
			backend, err := self.dial(self.serviceUrl)
			if err != nil {
				log.Error("pangolin-agent: backend connection failed: ", err)
				self.reportError(err.Error())
				continue
			}
			go self.proxy(backend, msg["ConnId"])
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

func (self *Agent) proxy(backend net.Conn, connId string) error {
	defer backend.Close()
	conn, err := self.newConn()
	if err != nil {
		log.Error("pangolin-agent: connection to controller failed: ", err)
		return err
	}
	defer conn.Close()

	_, err = fmt.Fprintf(conn, `{"id":%q,"cmd":"worker"}`, connId)
	if err != nil {
		log.Error("pangolin-agent: report connection failed, ", err)
		return err
	}

	log.Debug("pangolin-agent: new connection establisthed")

	go io.Copy(backend, conn)
	_, err = io.Copy(conn, backend)
	return err
}
