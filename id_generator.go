package rrpc

import (
	"fmt"
	"math"
	"os"
)

type IdGenerator interface {
	Generate() string
}

type idGenerator struct {
	prefix   string
	pid      int
	hostname string
	seq      chan uint32
}

func newIdGenerator(prefix string) IdGenerator {
	var err error
	g := &idGenerator{
		pid:    os.Getpid(),
		prefix: prefix,
		seq:    make(chan uint32),
	}
	g.hostname, err = os.Hostname()
	if err != nil {
		g.hostname = "csphere-controller"
	}

	go func(g *idGenerator) {
		var seq uint32 = 1
		for {
			g.seq <- seq
			seq++
			if seq >= math.MaxUint32 {
				seq = 1
			}
		}
	}(g)

	return g
}

func (self *idGenerator) Generate() string {
	return fmt.Sprintf("%s:%d:%d", self.hostname, self.pid, <-self.seq)
}
