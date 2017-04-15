package paxos

import (
	"container/list"
	"math/rand"
	"time"

	"github.com/google/btree"
	"github.com/pkg/errors"

	pb "github.com/nvanbenschoten/paxos/paxos/paxospb"
)

// NodeState represents the state of the Paxos instance.
type NodeState int

//go:generate stringer -type=NodeState
const (
	// StateLeaderElection is the state occupied during leadership
	// elections, where client updates can be buffered, but will not
	// be executed.
	StateLeaderElection NodeState = iota
	// StateRegLeader is the state occupied by the leader of a view.
	StateRegLeader
	// StateRegNonLeader is the state occupied by all nodes except
	// the leader of a view.
	StateRegNonLeader
)

// Config contains the parameters to start paxos.
type Config struct {
	// ID is the identity of the local paxos.
	ID uint64
	// NodeCount is the count of all nodes in the Paxos network.
	NodeCount uint64
	// Logger is the logger that the Paxos state machine will use
	// to log events. If not set, a default logger will be used.
	Logger Logger
	// RandSeed allows the seed used by paxos's rand.Source to be
	// injected, to allow for fully deterministic execution.
	RandSeed int64
}

func (c *Config) validate() error {
	if c.ID >= c.NodeCount {
		return errors.Errorf("node ID %d outside range [0, %d)", c.ID, c.NodeCount)
	}
	if c.Logger == nil {
		c.Logger = NewDefaultLogger()
	}
	if c.RandSeed == 0 {
		c.RandSeed = time.Now().UnixNano()
	}
	return nil
}

type globalUpdate struct {
	// seqNum is the sequence number of this update.
	seqNum uint64
	// proposal is the latest Proposal accepted for this sequence number.
	proposal *pb.Proposal
	// accepts is a map of corresponding Accept messages, indexed by server id.
	accepts map[uint64]*pb.Accept
	// globallyOrderedUpdate is an ordered update for this sequence number.
	globallyOrderedUpdate *pb.GloballyOrderedUpdate
}

// Len implements the btree.Item interface.
func (gu *globalUpdate) Less(than btree.Item) bool {
	return gu.seqNum < than.(*globalUpdate).seqNum
}

// globalUpdateKey creates a key to index into the globalHistory btree.
func globalUpdateKey(seqNum uint64) btree.Item {
	return &globalUpdate{seqNum: seqNum}
}

// paxos is a state machine that implements the Paxos replication protocol.
//
// State Transition Methods:
// - shiftToLeaderElection
// - shiftToPreparePhase
// - shiftToRegLeader
// - shiftToRegNonLeader
//
// paxos is not safe to use from multiple goroutines.
type paxos struct {
	// Node State Variables:
	//
	// id is a unique identifier for this node.
	id uint64
	// nodeCount is the count of all nodes in the Paxos network.
	nodeCount uint64
	// state is the current NodeState of the node.
	state NodeState

	// Disk Update Variables:
	// TODO this is never used.
	//
	// persistentState contains state that must persist across crashes. Contains:
	// - id
	// - nodeCount
	// - lastInstalled
	// - prepare
	// - proposal
	persistentState pb.PersistentState
	// mustSync is set when the persistentState is updated and must be synced
	// to disk.
	mustSync bool

	// View State Variables:
	//
	// lastAttempted is the last view this node attempted to install.
	lastAttempted uint64
	// lastInstalled is the last view this node installed.
	lastInstalled uint64
	// map of ViewChange messages, indexed by node id.
	viewChanges map[uint64]*pb.ViewChange

	// Prepare Phase Variables:
	//
	// prepare is the Prepare message from the last preinstalled view, if received.
	prepare *pb.Prepare
	// prepareOKs is a map of PrepareOK messages received, indexed by node id.
	prepareOKs map[uint64]*pb.PrepareOK

	// Global Ordering Variables:
	//
	// localAru is the local ARU (all received up to) value of this node.
	localAru uint64
	// lastProposed is the last sequence number proposed by the leader.
	lastProposed uint64
	// globalHistory is a btree of global slots, indexed by sequence number.
	// TODO this never gets garbage collected at the moment.
	globalHistory *btree.BTree

	// Timer Variables:
	//
	// progressTimer is the timeout on making global progress.
	progressTimer tickingTimer
	// updateTimer is the timeout on globally ordering a specific update.
	updateTimer tickingTimer
	// viewChangeProofTimer is the timeout on broadcasting a ViewChangeProof.
	viewChangeProofTimer tickingTimer
	// viewChangeRetransTimer is the timeout on retransmitting a ViewChange.
	viewChangeRetransTimer tickingTimer

	// Client Handling Variables:
	//
	// updateQueue is a queue of ClientUpdate messages. Contains *pb.ClientUpdate elements.
	updateQueue list.List
	// lastExecuted is a map of timestamps, indexed by client id.
	lastExecuted map[uint64]uint64
	// lastEnqueued is a map of timestamps, indexed by client id.
	lastEnqueued map[uint64]uint64
	// pendingUpdates is a map of ClientUpdate messages, indexed by client id.
	pendingUpdates map[uint64]*pb.ClientUpdate

	// Outbox Variables:
	//
	// orderedUpdates is the outbox for updates successfully globally ordered.
	orderedUpdates []pb.GloballyOrderedUpdate
	// msgs is the outbox for the paxos node, containing all messages that need
	// to be delivered.
	msgs []pb.Message

	// Utility Variables:
	//
	// logger is used by paxos to log event.
	logger Logger
	// rand holds the paxos instance's local Rand object. This allows us to avoid
	// using the synchronized global Rand object.
	rand *rand.Rand
}

func newPaxos(c *Config) *paxos {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	p := &paxos{
		id:             c.ID,
		nodeCount:      c.NodeCount,
		logger:         c.Logger,
		state:          StateLeaderElection,
		globalHistory:  btree.New(32 /* degree */),
		rand:           rand.New(rand.NewSource(c.RandSeed)),
		lastExecuted:   make(map[uint64]uint64),
		lastEnqueued:   make(map[uint64]uint64),
		pendingUpdates: make(map[uint64]*pb.ClientUpdate),
	}
	p.initTimers()
	p.shiftToLeaderElection(1)
	return p
}

// initTimers initializes all timers for the paxos state machine.
// TODO allow injecting timeouts.
func (p *paxos) initTimers() {
	// The progressTimer assures that the paxos state machine is making progress
	// and helps detect when a new leader election should begin.
	const progressTimerTimeout = 500
	p.progressTimer = makeTickingTimer(progressTimerTimeout, func() {
		p.shiftToLeaderElection(p.lastAttempted + 1)
	})
	p.progressTimer.stop()

	// The updateTimer is used to resend client updates when they have not made
	// progress after a period of time.
	const updateTimerTimeout = 300
	p.updateTimer = makeTickingTimer(updateTimerTimeout, func() {
		// TODO
	})

	// The viewChangeProofTimer broadcasts a ViewChangeProof message periodically
	// to help assure that all nodes stay up to date with the current majority's
	// installed view.
	const viewChangeProofTimeout = 100
	p.viewChangeProofTimer = makeTickingTimer(viewChangeProofTimeout, func() {
		p.broadcastViewChangeProof()

		jitter := p.rand.Intn(viewChangeProofTimeout / 5)
		p.viewChangeProofTimer.resetWithJitter(jitter)
	})

	// The viewChangeRetransTimer broadcasts the current ViewChange message
	// periodically during a leader election.
	const viewChangeRetransTimeout = 100
	p.viewChangeRetransTimer = makeTickingTimer(viewChangeRetransTimeout, func() {
		if p.state == StateLeaderElection {
			vc := p.viewChanges[p.id]
			p.broadcast(pb.WrapMessage(vc))
		}
		p.viewChangeRetransTimer.reset()
	})
}

func (p *paxos) Tick() {
	p.progressTimer.tick()
	p.updateTimer.tick()
	p.viewChangeProofTimer.tick()
	p.viewChangeRetransTimer.tick()
}

func (p *paxos) Step(m pb.Message) {
	switch t := m.Type.(type) {
	case *pb.Message_ViewChange:
		p.onViewChange(t.ViewChange)
	case *pb.Message_ViewChangeProof:
		p.onViewChangeProof(t.ViewChangeProof)
	case *pb.Message_Prepare:
		p.onPrepare(t.Prepare)
	case *pb.Message_PrepareOk:
		p.onPrepareOK(t.PrepareOk)
	case *pb.Message_Proposal:
		p.onProposal(t.Proposal)
	case *pb.Message_Accept:
		p.onAccept(t.Accept)
	case *pb.Message_ClientUpdate:
		p.onClientUpdate(t.ClientUpdate)
	default:
		p.logger.Panicf("unexpected Message type: %T", t)
	}
}

func (p *paxos) leaderOfView(view uint64) uint64 {
	return view % p.nodeCount
}

func (p *paxos) amILeaderOfView(view uint64) bool {
	return p.id == p.leaderOfView(view)
}

func (p *paxos) sendTo(m pb.Message, to uint64) {
	m.To = to
	p.msgs = append(p.msgs, m)
}

func (p *paxos) sendToLeader(m pb.Message) {
	p.sendTo(m, p.leaderOfView(p.lastInstalled))
}

func (p *paxos) broadcast(m pb.Message) {
	for i := uint64(0); i < p.nodeCount; i++ {
		if i != p.id {
			p.sendTo(m, i)
		}
	}
}

func (p *paxos) clearMsgs() {
	p.msgs = nil
}

func (p *paxos) deliverOrderedUpdate(up pb.GloballyOrderedUpdate) {
	p.orderedUpdates = append(p.orderedUpdates, up)
}

func (p *paxos) clearOrderedUpdates() {
	p.orderedUpdates = nil
}

func (p *paxos) quorum(val int) bool {
	return val > int(p.nodeCount/2)
}
