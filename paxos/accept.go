package paxos

import (
	"github.com/google/btree"
	pb "github.com/nvanbenschoten/paxos/paxos/paxospb"
)

func (p *paxos) shiftToRegNonLeader() {
	p.state = StateRegNonLeader
	p.lastInstalled = p.lastAttempted
	p.updateQueue.Init()
	// Sync to disk
	p.logger.Debugf("installed new view: %d (%s)", p.lastInstalled, p.state)
}

func (p *paxos) onAccept(a *pb.Accept) {
	if a.View != p.lastInstalled {
		p.logger.Debugf("ignoring Accept, wrong attempt: %v", a)
		return
	}

	guItem := p.globalHistory.Get(globalUpdateKey(a.Seq))
	if guItem == nil {
		p.logger.Debugf("ignoring Accept, never received Proposal: %v", a)
		return
	}

	gu := guItem.(*globalUpdate)
	prop := gu.proposal
	if prop == nil || prop.View != a.View {
		// If there was not a proposal for this sequence number from
		// this view, ignore.
		return
	}
	if gu.globallyOrderedUpdate != nil {
		// If we've already ordered an update for this sequence, ignore.
		return
	}
	if p.globallyOrderedReady(gu) {
		// If we already have a quorum on accepts, we can ignore this one.
		return
	}

	gu.accepts[a.NodeId] = a
	if p.globallyOrderedReady(gu) {
		gou := &pb.GloballyOrderedUpdate{
			NodeId: prop.NodeId,
			Seq:    prop.Seq,
			Update: prop.Update,
		}
		gu.globallyOrderedUpdate = gou
		p.advanceARU()
	}
}

// globallyOrderedReady checks whether the globalUpdate is ready to be globally
// ordered, based on the number of accept messages we have received for the update.
func (p *paxos) globallyOrderedReady(gu *globalUpdate) bool {
	// We start with 1 because the leader's proposal counts as an implicit
	// accept message.
	acceptCount := 1
	for _, a := range gu.accepts {
		if p.lastInstalled == a.View {
			acceptCount++
		}
	}
	return p.quorum(acceptCount)
}

func (p *paxos) advanceARU() {
	key := globalUpdateKey(p.localAru + 1)
	p.globalHistory.AscendGreaterOrEqual(key, func(i btree.Item) bool {
		gou := i.(*globalUpdate).globallyOrderedUpdate
		if gou != nil {
			p.localAru++
			p.deliverOrderedUpdate(*gou)
			if gou.Update.NodeId == p.id {
				delete(p.pendingUpdates, gou.Update.ClientId)
			}
			p.lastExecuted[gou.Update.ClientId] = gou.Update.Timestamp
			if p.state != StateLeaderElection {
				p.progressTimer.reset()
			}
			if p.state == StateRegLeader {
				p.sendProposal()
			}
			return true
		}
		return false
	})
}
