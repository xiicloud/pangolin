package pangolin

import (
	"math"
)

type IdGenerator interface {
	Generate() uint32
}

type idGenerator struct {
	seq chan uint32
}

func newIdGenerator() IdGenerator {
	g := &idGenerator{
		seq: make(chan uint32),
	}

	go func(g *idGenerator) {
		var seq uint32 = MinRequestId
		for {
			g.seq <- seq
			seq++
			if seq >= math.MaxUint32 {
				seq = MinRequestId
			}
		}
	}(g)

	return g
}

func (self *idGenerator) Generate() uint32 {
	return <-self.seq
}
