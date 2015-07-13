package main

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/nicescale/pangolin"
	"net/http"
)

func main() {
	log.SetLevel(log.DebugLevel)

	// mock a http service.
	// Imagine this is the service behind the firewall.
	go func() {
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "Hi, this is the service behind the firewall.\n")
		})
		http.ListenAndServe(":5123", nil)
	}()
	agent, err := pangolin.NewAgent("node1", "tcp://localhost:9000", "http://localhost:5123", nil, nil)
	if err != nil {
		log.Fatal(err)
	}
	err = agent.Join()
	if err != nil {
		log.Fatal(err)
	}
	agent.Serve()
}
