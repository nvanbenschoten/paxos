package paxos

import (
	"github.com/google/btree"
	pb "github.com/nvanbenschoten/paxos/paxos/paxospb"
)

func (p *paxos) shiftToPreparePhase() {
	p.lastInstalled = p.lastAttempted
	p.prepareOKs = make(map[uint64]*pb.PrepareOK)
	p.lastEnqueued = make(map[uint64]uint64)

	pr := &pb.Prepare{
		NodeId:   p.id,
		View:     p.lastInstalled,
		LocalAru: p.localAru,
	}
	p.prepare = pr
	p.broadcast(pb.WrapMessage(pr))

	datalist := p.constructDataList(p.localAru)
	prepOK := &pb.PrepareOK{
		NodeId:   p.id,
		View:     p.lastInstalled,
		DataList: datalist,
	}
	p.prepareOKs[p.id] = prepOK

	// TODO sync
}

func (p *paxos) onPrepare(prep *pb.Prepare) {
	if prep.NodeId == p.id {
		p.logger.Debugf("ignoring Prepare, sent by ourselves: %v", prep)
		return
	}
	if prep.View != p.lastAttempted {
		p.logger.Debugf("ignoring Prepare, wrong attempt: %v", prep)
		return
	}

	if p.state == StateLeaderElection {
		p.prepare = prep
		if !p.progressTimer.isSet() {
			p.progressTimer.reset()
		}

		datalist := p.constructDataList(prep.LocalAru)
		prepOK := &pb.PrepareOK{
			NodeId:   p.id,
			View:     prep.View,
			DataList: datalist,
		}
		p.prepareOKs[p.id] = prepOK
		p.sendTo(pb.WrapMessage(prepOK), p.leaderOfView(prepOK.View))

		p.shiftToRegNonLeader()
	} else {
		prepOK := p.prepareOKs[p.id]
		p.sendTo(pb.WrapMessage(prepOK), p.leaderOfView(prepOK.View))
	}
}

func (p *paxos) onPrepareOK(prepOK *pb.PrepareOK) {
	if p.state != StateLeaderElection {
		p.logger.Debugf("ignoring PrepareOK, wrong state: %v", p.state)
		return
	}
	if prepOK.View != p.lastAttempted {
		p.logger.Debugf("ignoring PrepareOK, wrong attempt: %v", prepOK)
		return
	}

	if _, ok := p.prepareOKs[prepOK.NodeId]; !ok {
		p.prepareOKs[prepOK.NodeId] = prepOK
		for _, m := range prepOK.DataList {
			switch t := m.Type.(type) {
			case *pb.Message_Proposal:
				p.onProposalInDatalist(t.Proposal)
			case *pb.Message_GloballyOrderedUpdate:
				p.onGloballyOrderedUpdateInDatalist(t.GloballyOrderedUpdate)
			default:
				p.logger.Panicf("unexpected Message type in datalist: %T", t)
			}
		}

		if p.viewPreparedReady(prepOK.View) {
			p.shiftToRegLeader()
		}
	}
}

func (p *paxos) constructDataList(aru uint64) []*pb.Message {
	var datalist []*pb.Message
	startKey := globalUpdateKey(aru + 1)
	p.globalHistory.AscendGreaterOrEqual(startKey, func(i btree.Item) bool {
		gu := i.(*globalUpdate)
		if gu.globallyOrderedUpdate != nil {
			m := pb.WrapMessage(gu.globallyOrderedUpdate)
			datalist = append(datalist, &m)
		} else if gu.proposal != nil {
			m := pb.WrapMessage(gu.proposal)
			datalist = append(datalist, &m)
		}
		return true
	})
	return datalist
}

func (p *paxos) onProposalInDatalist(prop *pb.Proposal) {
	p.updateProposalState(prop)
}

func (p *paxos) onGloballyOrderedUpdateInDatalist(gou *pb.GloballyOrderedUpdate) {
	guItem := p.globalHistory.Get(globalUpdateKey(gou.Seq))
	if guItem == nil {
		p.globalHistory.ReplaceOrInsert(&globalUpdate{
			seqNum:                gou.Seq,
			globallyOrderedUpdate: gou,
		})
		p.advanceARU()
	} else {
		gu := guItem.(*globalUpdate)
		if gu.globallyOrderedUpdate == nil {
			gu.globallyOrderedUpdate = gou
			p.advanceARU()
		}
	}
}

// viewPreparedReady checks whether the view is ready to be installed, based
// on the number of PrepareOK messages for that leader observered. A quorum of
// PrepareOK messages is required to install the view.
func (p *paxos) viewPreparedReady(view uint64) bool {
	correctPrepareCount := 0
	for _, prepOK := range p.prepareOKs {
		if prepOK.View == view {
			correctPrepareCount++
		}
	}
	return p.quorum(correctPrepareCount)
}
