package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"

	log "github.com/Sirupsen/logrus"
)

func main() {
	log.SetLevel(log.DebugLevel)
	agent := &Agent{id: "node1", saddr: "127.0.0.1:99"}
	err := agent.Join()
	if err != nil {
		log.Fatal(err)
	}
	agent.Serve()
}

type Agent struct {
	saddr string
	id    string
	conn  net.Conn
	dec   *json.Decoder
	enc   *json.Encoder
}

func (self *Agent) Join() error {
	conn, err := net.Dial("tcp", self.saddr)
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
			log.Error("agent: json error: ", err)
			return err
		}
		log.Debug("agent: got command ", msg)
		switch msg["Cmd"] {
		case "new_conn":
			backend, err := net.Dial("tcp", "127.0.0.1:90")
			if err != nil {
				log.Error("agent: backend connection failed: ", err)
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

func (self *Agent) proxy(backend net.Conn, connId string) error {
	conn, err := net.Dial("tcp", self.saddr)
	if err != nil {
		log.Error("agent: connection to controller failed: ", err)
		return err
	}
	defer backend.Close()
	defer conn.Close()
	_, err = fmt.Fprintf(conn, `{"id":%q,"cmd":"worker"}`, connId)
	if err != nil {
		log.Error("agent: report connection failed, ", err)
		return err
	} else {
		log.Debug("agent: new connection establisthed")
	}
	go io.Copy(backend, conn)
	_, err = io.Copy(conn, backend)
	log.Debug(err)
	return nil
}
