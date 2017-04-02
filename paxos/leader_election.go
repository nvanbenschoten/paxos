package paxos

import (
	pb "github.com/nvanbenschoten/paxos/paxos/paxospb"
)

func (p *paxos) shiftToLeaderElection(view uint64) {
	// Clear out data structures used in the previous view.
	p.state = StateLeaderElection
	p.viewChanges = make(map[uint64]*pb.ViewChange)
	p.prepare = nil
	p.prepareOKs = make(map[uint64]*pb.PrepareOK)
	p.lastEnqueued = make(map[uint64]uint64)
	p.lastAttempted = view
	p.logger.Debugf("beginning election for view: %d", view)

	// Create and send ViewChange to all other nodes, then update
	// internal state based on this ViewChange.
	vc := &pb.ViewChange{
		NodeId:        p.id,
		AttemptedView: view,
	}
	p.updateViewChangeState(vc)
	p.broadcast(pb.WrapMessage(vc))
}

func (p *paxos) onViewChange(vc *pb.ViewChange) {
	// Only process the ViewChange if we're already in a LeaderElection state.
	switch p.state {
	case StateLeaderElection:
		if vc.NodeId == p.id {
			p.logger.Debugf("ignoring ViewChange, sent by ourselves: %v", vc)
			return
		}
		if p.progressTimer.isSet() {
			p.logger.Debugf("ignoring ViewChange, still have active progress timer: %v", vc)
			return
		}
		if vc.AttemptedView <= p.lastInstalled {
			p.logger.Debugf("ignoring ViewChange, not greater than last installed: %v", vc)
			return
		}
		p.applyViewChange(vc)
	case StateRegLeader, StateRegNonLeader:
		p.logger.Debugf("ignoring ViewChange, not in leader election (%s): %v", p.state, vc)
	default:
		panic("unreachable")
	}
}

func (p *paxos) applyViewChange(vc *pb.ViewChange) {
	switch {
	case vc.AttemptedView > p.lastAttempted:
		p.logger.Debugf("found ViewChange with greater view: %v", vc)
		p.shiftToLeaderElection(vc.AttemptedView)
		p.updateViewChangeState(vc)
	case vc.AttemptedView == p.lastAttempted:
		p.updateViewChangeState(vc)
		if p.preinstallReady(vc.AttemptedView) {
			p.progressTimer.reset()
			if p.amILeaderOfView(vc.AttemptedView) {
				p.shiftToPreparePhase()
			}
		}
	case vc.AttemptedView < p.lastAttempted:
		// Ignore ViewChanges from previous views.
	default:
		panic("unreachable")
	}
}

// preinstallReady checks whether the view is ready to be preinstalled, based
// on the number of ViewChange messages for that view observered. A quorum of
// ViewChange messages is required to preinstall the view.
func (p *paxos) preinstallReady(view uint64) bool {
	correctAttemptCount := 0
	for _, vc := range p.viewChanges {
		if vc.AttemptedView == view {
			correctAttemptCount++
		}
	}
	return p.quorum(correctAttemptCount)
}

// updateViewChangeState records that the ViewChange message has been observered.
func (p *paxos) updateViewChangeState(vc *pb.ViewChange) {
	if _, ok := p.viewChanges[vc.NodeId]; !ok {
		p.viewChanges[vc.NodeId] = vc
	}
}

func (p *paxos) onViewChangeProof(vcp *pb.ViewChangeProof) {
	// Only process the ViewChangeProof if we're already in a LeaderElection.
	switch p.state {
	case StateLeaderElection:
		if vcp.NodeId == p.id {
			p.logger.Debugf("ignoring ViewChangeProof, sent by ourselves: %v", vcp)
			return
		}
		if vcp.InstalledView > p.lastInstalled {
			// If someone else has installed a larger view, we can jump directly to this
			// view without performing a leader election.
			p.lastAttempted = vcp.InstalledView
			p.progressTimer.reset()
			if p.amILeaderOfView(vcp.InstalledView) {
				// Since we are the leader of this larger view, all other nodes must be
				// waiting for us to send a Prepare message to complete the installation
				// of the view.
				p.shiftToPreparePhase()
			} else {
				p.shiftToRegNonLeader()
			}
		}
	case StateRegLeader, StateRegNonLeader:
		p.logger.Debugf("ignoring ViewChangeProof, not in leader election (%s): %v", p.state, vcp)
	default:
		panic("unreachable")
	}
}

// broadcastViewChangeProof broadcasts the current installed view number to all other
// nodes, informing them of the view we are in in case they have fallen behind.
func (p *paxos) broadcastViewChangeProof() {
	vcp := &pb.ViewChangeProof{
		NodeId:        p.id,
		InstalledView: p.lastInstalled,
	}
	p.broadcast(pb.WrapMessage(vcp))
}
