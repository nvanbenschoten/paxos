package paxos

import (
	pb "github.com/nvanbenschoten/paxos/paxos/paxospb"
)

func (p *paxos) onClientUpdate(up *pb.ClientUpdate) {
	switch p.state {
	case StateLeaderElection:
		if up.NodeId == p.id {
			// Only take responsibility for updates from my clients.
			if p.enqueueUpdate(up) {
				p.addToPendingUpdates(up)
			}
		}
	case StateRegNonLeader:
		if up.NodeId == p.id {
			// Only take responsibility for updates from my clients.
			p.addToPendingUpdates(up)
			p.sendToLeader(pb.WrapMessage(up))
		}
	case StateRegLeader:
		if p.enqueueUpdate(up) {
			if up.NodeId == p.id {
				p.addToPendingUpdates(up)
			}
			p.sendProposal()
		}
	}
}

func (p *paxos) enqueueUpdate(up *pb.ClientUpdate) bool {
	if up.Timestamp <= p.lastExecuted[up.ClientId] {
		// Never enqueue an update that we've already executed.
		p.logger.Debugf("Not enqueueing update, already executed: %v", up)
		return false
	}
	if up.Timestamp <= p.lastEnqueued[up.ClientId] {
		// lastEnqueued is reset for each view, so if the timestamp is less
		// than or equal to another enqueued update in this view, we dont
		// want to enqueue this one.
		p.logger.Debugf("Not enqueueing update, already enqueued: %v", up)
		return false
	}
	p.updateQueue.PushBack(up)
	p.lastEnqueued[up.ClientId] = up.Timestamp
	return true
}

func (p *paxos) addToPendingUpdates(up *pb.ClientUpdate) {
	p.pendingUpdates[up.ClientId] = up
	// Set updateTimer(U.client_id)
	// Sync to disk.
}
