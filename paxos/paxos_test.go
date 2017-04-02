package paxos

import (
	"testing"

	pb "github.com/nvanbenschoten/paxos/paxos/paxospb"
)

func assertState(t *testing.T, p *paxos, exp NodeState) {
	if p.state != exp {
		t.Fatalf("expected node state %s, found state %s", exp, p.state)
	}
}

func assertGlobalHistorySize(t *testing.T, p *paxos, exp int) {
	if a := p.globalHistory.Len(); a != exp {
		t.Fatalf("expected global history size %d, found %d", exp, a)
	}
}

func TestConfig(t *testing.T) {
	l := NewDefaultLogger()
	c := &Config{
		ID:        12,
		NodeCount: 23,
		Logger:    l,
	}
	p := newPaxos(c)

	if p.id != c.ID {
		t.Errorf("expected Paxos node ID %d, found %d", c.ID, p.id)
	}
	if p.nodeCount != c.NodeCount {
		t.Errorf("expected Paxos node count %d, found %d", c.NodeCount, p.nodeCount)
	}
	if p.logger != l {
		t.Errorf("expected Paxos logger %p, found %p", l, p.logger)
	}
}

type stateMachine interface {
	Step(m pb.Message)
	ReadMessages() []pb.Message
}

func (p *paxos) ReadMessages() []pb.Message {
	msgs := p.msgs
	p.clearMsgs()
	return msgs
}

type network struct {
	peers []stateMachine
}

func newNetwork(NodeCount int) network {
	peers := make([]stateMachine, NodeCount)
	for i := range peers {
		peers[i] = newPaxos(&Config{ID: uint64(i), NodeCount: uint64(NodeCount)})
	}
	return network{peers: peers}
}
