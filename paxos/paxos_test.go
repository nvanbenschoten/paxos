package paxos

import (
	"math/rand"
	"testing"

	"reflect"

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

func (p *paxos) ReadMessages() []pb.Message {
	msgs := p.msgs
	p.clearMsgs()
	return msgs
}

type conn struct {
	from, to uint64
}

type network struct {
	peers    map[uint64]*paxos
	failures map[*paxos]struct{}
	dropm    map[conn]float64
}

func newNetwork(nodeCount uint64) network {
	peers := make(map[uint64]*paxos, nodeCount)
	for i := uint64(0); i < nodeCount; i++ {
		peers[i] = newPaxos(&Config{
			ID:        i,
			NodeCount: nodeCount,
			RandSeed:  int64(i),
		})
	}
	return network{
		peers:    peers,
		failures: make(map[*paxos]struct{}, nodeCount),
		dropm:    make(map[conn]float64),
	}
}

func (n *network) crash(id uint64) *paxos {
	p := n.peers[id]
	n.failures[p] = struct{}{}
	return p
}

func (n *network) alive(p *paxos) bool {
	_, failed := n.failures[p]
	return !failed
}

func (n *network) drop(from, to uint64, perc float64) {
	n.dropm[conn{from: from, to: to}] = perc
}

func (n *network) dropForAll(perc float64) {
	for from := range n.peers {
		for to := range n.peers {
			if from != to {
				n.drop(from, to, perc)
			}
		}
	}
}

func (n *network) cut(one, other uint64) {
	n.drop(one, other, 1.0)
	n.drop(other, one, 1.0)
}

func (n *network) isolate(id uint64) {
	for other := range n.peers {
		if other != id {
			n.cut(id, other)
		}
	}
}

func (n *network) tickAll() {
	for _, p := range n.peers {
		if n.alive(p) {
			p.Tick()
		}
	}
}

func (n *network) deliverAllMessages() {
	var msgs []pb.Message
	for _, p := range n.peers {
		if n.alive(p) {
			newMsgs := p.ReadMessages()
			for _, msg := range newMsgs {
				msgConn := conn{from: p.id, to: msg.To}
				perc := n.dropm[msgConn]
				if perc > 0 {
					if n := rand.Float64(); n < perc {
						continue
					}
				}
				msgs = append(msgs, msg)
			}
		}
	}
	for _, msg := range msgs {
		dest := n.peers[msg.To]
		if n.alive(dest) {
			dest.Step(msg)
		}
	}
}

func (n *network) quorumHas(pred func(*paxos) bool) bool {
	count := 0
	for _, p := range n.peers {
		if pred(p) {
			count++
		}
	}
	return count > int(len(n.peers)/2)
}

func (n *network) waitInstallView(view uint64, maxTicks int) bool {
	for ticksLeft := maxTicks; ticksLeft > 0; ticksLeft-- {
		n.tickAll()
		n.deliverAllMessages()
		if n.quorumHas(func(p *paxos) bool {
			return p.lastInstalled == view
		}) {
			return true
		}
	}
	return false
}

const maxTicksPerElection = 2000

func TestLeaderElectionNoFailures(t *testing.T) {
	n := newNetwork(5)

	const observeElectionCount = 5
	for election := uint64(1); election <= observeElectionCount; election++ {
		if !n.waitInstallView(election, maxTicksPerElection) {
			t.Fatalf("leader election failed, view %d never installed", election)
		}
	}
}

func TestLeaderElectionOneFailure(t *testing.T) {
	n := newNetwork(5)
	crashed := n.crash(3)

	const observeElectionCount = 5
	for election := uint64(1); election <= observeElectionCount; election++ {
		if crashed.amILeaderOfView(election) {
			continue
		}

		if !n.waitInstallView(election, maxTicksPerElection) {
			t.Fatalf("leader election failed, view %d never installed", election)
		}
	}
}

func TestLeaderElectionTwoFailure(t *testing.T) {
	n := newNetwork(5)
	crashed1 := n.crash(0)
	crashed2 := n.crash(3)

	const observeElectionCount = 5
	for election := uint64(1); election <= observeElectionCount; election++ {
		if crashed1.amILeaderOfView(election) || crashed2.amILeaderOfView(election) {
			continue
		}

		if !n.waitInstallView(election, maxTicksPerElection) {
			t.Fatalf("leader election failed, view %d never installed", election)
		}
	}
}

func TestLeaderElectionThreeFailure(t *testing.T) {
	n := newNetwork(5)
	n.crash(0)
	n.crash(1)
	n.crash(3)

	if n.waitInstallView(1, maxTicksPerElection) {
		t.Fatal("leader election succeeded when it should have blocked because quorum impossible")
	}
}

func TestLeaderElectionOrderedFailures(t *testing.T) {
	n := newNetwork(7)
	n.crash(1)
	n.crash(2)
	n.crash(3)

	// Instrument the progress timer so that we can make sure the
	// first three elections time out.
	electionTimeouts := make([]bool, 7)
	p0 := n.peers[0]
	p0.progressTimer.instrumentTimeout(func() {
		if p0.state == StateLeaderElection {
			electionTimeouts[p0.lastAttempted] = true
		}
	})

	if !n.waitInstallView(4, maxTicksPerElection) {
		t.Fatalf("leader election failed, view 4 never installed")
	}

	expTimeouts := []bool{false, true, true, true, false, false, false}
	if !reflect.DeepEqual(electionTimeouts, expTimeouts) {
		t.Fatalf("expected election timeouts %v, found timeouts %v", expTimeouts, electionTimeouts)
	}
}

func TestLeaderElectionSomeDroppedMessages(t *testing.T) {
	n := newNetwork(5)

	const dropPerc = 0.50
	n.dropForAll(dropPerc)

	const observeElectionCount = 5
	for election := uint64(1); election <= observeElectionCount; election++ {
		if !n.waitInstallView(election, maxTicksPerElection) {
			t.Fatalf("leader election failed, view %d never installed", election)
		}
	}
}

func TestLeaderElectionAllDroppedMessages(t *testing.T) {
	n := newNetwork(5)

	const dropPerc = 1.0
	n.dropForAll(dropPerc)

	if n.waitInstallView(1, maxTicksPerElection) {
		t.Fatal("leader election succeeded when it should have blocked because all messages dropped")
	}
}

func TestLeaderElectionOneIsolated(t *testing.T) {
	n := newNetwork(5)

	isolated := n.peers[3]
	n.isolate(isolated.id)

	const observeElectionCount = 5
	for election := uint64(1); election <= observeElectionCount; election++ {
		if isolated.amILeaderOfView(election) {
			continue
		}

		if !n.waitInstallView(election, maxTicksPerElection) {
			t.Fatalf("leader election failed, view %d never installed", election)
		}
	}
}

func TestLeaderElectionTwoIsolated(t *testing.T) {
	n := newNetwork(5)

	isolated1 := n.peers[0]
	isolated2 := n.peers[3]
	n.isolate(isolated1.id)
	n.isolate(isolated2.id)

	const observeElectionCount = 5
	for election := uint64(1); election <= observeElectionCount; election++ {
		if isolated1.amILeaderOfView(election) || isolated2.amILeaderOfView(election) {
			continue
		}

		if !n.waitInstallView(election, maxTicksPerElection) {
			t.Fatalf("leader election failed, view %d never installed", election)
		}
	}
}

func TestLeaderElectionThreeIsolated(t *testing.T) {
	n := newNetwork(5)
	n.isolate(0)
	n.isolate(1)
	n.isolate(3)

	if n.waitInstallView(1, maxTicksPerElection) {
		t.Fatal("leader election succeeded when it should have blocked because quorum impossible")
	}
}
