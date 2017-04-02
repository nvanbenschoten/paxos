package paxos

import (
	"github.com/google/btree"
	pb "github.com/nvanbenschoten/paxos/paxos/paxospb"
)

func (p *paxos) shiftToRegLeader() {
	p.state = StateRegLeader
	p.updateQueue.Init()
	boundUpdates := p.boundUpdates()
	p.enqueueUnboundPendingUpdates(boundUpdates)
	p.removeBoundUpdatesFromQueue(boundUpdates)
	p.lastProposed = p.localAru
	p.sendProposal()
	p.logger.Debugf("installed new view: %d (%s)", p.lastInstalled, p.state)
}

func (p *paxos) enqueueUnboundPendingUpdates(boundUpdates map[uint64]*pb.ClientUpdate) {
	for clientID, pendingUpdate := range p.pendingUpdates {
		if _, bound := boundUpdates[clientID]; !bound {
			p.enqueueUpdate(pendingUpdate)
		}
	}
}

func (p *paxos) removeBoundUpdatesFromQueue(boundUpdates map[uint64]*pb.ClientUpdate) {
	for e := p.updateQueue.Front(); e != nil; {
		eCur := e
		e = e.Next()

		up := eCur.Value.(*pb.ClientUpdate)
		upTs := up.Timestamp
		_, bound := boundUpdates[up.ClientId]
		if bound || upTs <= p.lastExecuted[up.ClientId] ||
			(upTs <= p.lastEnqueued[up.ClientId] && up.NodeId != p.id) {
			p.updateQueue.Remove(eCur)
			if upTs > p.lastEnqueued[up.ClientId] {
				p.lastEnqueued[up.ClientId] = upTs
			}
		}
	}
}

func (p *paxos) boundUpdates() map[uint64]*pb.ClientUpdate {
	updates := make(map[uint64]*pb.ClientUpdate)
	key := globalUpdateKey(p.localAru + 1)
	p.globalHistory.AscendGreaterOrEqual(key, func(i btree.Item) bool {
		gu := i.(*globalUpdate)
		if gou := gu.globallyOrderedUpdate; gou != nil {
			updates[gou.Update.ClientId] = gou.Update
		} else if prop := gu.proposal; prop != nil {
			updates[prop.Update.ClientId] = prop.Update
		} else {
			p.logger.Panicf("globalUpdate without proposal or ordered update: %v", gu)
		}
		return true
	})
	return updates
}

func (p *paxos) onProposal(prop *pb.Proposal) {
	if prop.NodeId == p.id {
		p.logger.Debugf("ignoring Proposal, sent by ourselves: %v", prop)
		return
	}
	if p.state != StateRegNonLeader {
		p.logger.Debugf("ignoring Proposal, wrong state: %v", p.state)
		return
	}
	if prop.View != p.lastInstalled {
		p.logger.Debugf("ignoring Proposal, wrong attempt: %v", prop)
		return
	}

	p.updateProposalState(prop)

	// TODO Sync to disk

	a := &pb.Accept{
		NodeId: p.id,
		View:   prop.View,
		Seq:    prop.Seq,
	}
	p.broadcast(pb.WrapMessage(a))
	p.onAccept(a)
}

func (p *paxos) sendProposal() {
	seq := p.lastProposed + 1
	guItem := p.globalHistory.Get(globalUpdateKey(seq))
	var up *pb.ClientUpdate
	if guItem != nil {
		gu := guItem.(*globalUpdate)
		if gu.globallyOrderedUpdate != nil {
			p.lastProposed++
			p.sendProposal()
			return
		}
		if gu.proposal != nil {
			up = gu.proposal.Update
		}
	}
	if up == nil {
		if p.updateQueue.Len() == 0 {
			return
		}
		e := p.updateQueue.Front()
		up = e.Value.(*pb.ClientUpdate)
		p.updateQueue.Remove(e)
	}

	prop := &pb.Proposal{
		NodeId: p.id,
		View:   p.lastInstalled,
		Seq:    seq,
		Update: up,
	}
	p.updateProposalState(prop)
	p.lastProposed = seq

	// TODO Sync

	p.broadcast(pb.WrapMessage(prop))
}

func (p *paxos) updateProposalState(prop *pb.Proposal) {
	guItem := p.globalHistory.Get(globalUpdateKey(prop.Seq))
	if guItem != nil {
		gu := guItem.(*globalUpdate)
		if gu.globallyOrderedUpdate != nil {
			// Ignore the proposal if we have already ordered that sequence num.
			return
		}
		if oldProp := gu.proposal; oldProp != nil {
			// We have already seen a proposal for this sequence number. Only
			// replace that proposal if this new proposal has a larger sequence
			// number.
			if prop.View > oldProp.View {
				gu.proposal = prop
				gu.accepts = make(map[uint64]*pb.Accept)
			}
		}
	} else {
		gu := &globalUpdate{
			seqNum:   prop.Seq,
			proposal: prop,
			accepts:  make(map[uint64]*pb.Accept),
		}
		p.globalHistory.ReplaceOrInsert(gu)
	}
}
