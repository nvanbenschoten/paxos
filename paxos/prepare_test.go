package paxos

import (
	"reflect"
	"testing"

	pb "github.com/nvanbenschoten/paxos/paxos/paxospb"
)

// TestShiftToPreparePhase tests the state transition to the prepare phase. It
// verifies that the initial messages in the outbox of a Paxos instance after it
// shifts to the prepare phase, which should be attempting to install the new view,
// are correct. It also tests whether the leader configures its own data structures
// correctly during the state transition.
func TestShiftToPreparePhase(t *testing.T) {
	p := newPaxos(&Config{ID: 1, NodeCount: 3})

	// Simulate a completed leader election for view 1, then shift to the
	// prepare phase.
	p.lastAttempted = 1
	p.clearMsgs()
	p.shiftToPreparePhase()

	expPr := &pb.Prepare{
		NodeId:   1,
		View:     1,
		LocalAru: 0,
	}
	if pr := p.prepare; !reflect.DeepEqual(pr, expPr) {
		t.Errorf("expected the Prepare message %+v, found %+v", expPr, pr)
	}

	mpr := &pb.Message_Prepare{Prepare: expPr}
	exp := []pb.Message{
		{To: 0, Type: mpr},
		{To: 2, Type: mpr},
	}
	if msgs := p.msgs; !reflect.DeepEqual(msgs, exp) {
		t.Errorf("expected the outbound messages %+v, found %+v", exp, msgs)
	}

	if exp, a := 1, len(p.prepareOKs); a != exp {
		t.Errorf("expected %d prepareOK messages, found %d", exp, a)
	}
}

// TestPrepareReceived verifies that Paxos instances handle Prepare messages
// correctly, both when in the LeaderElection state, and when not.
func TestPrepareReceived(t *testing.T) {
	p := newPaxos(&Config{ID: 1, NodeCount: 3})
	p.lastAttempted = 2
	p.clearMsgs()
	assertState(t, p, StateLeaderElection)

	// Create a global history that includes messages above the Prepare
	// message's ARU.
	const prepareAru = 1
	h1 := globalUpdate{seqNum: prepareAru + 1, globallyOrderedUpdate: &pb.GloballyOrderedUpdate{
		NodeId: p.id,
		Seq:    prepareAru + 1,
		Update: &pb.ClientUpdate{
			ClientId:  12,
			NodeId:    p.id,
			Timestamp: 3,
			Update:    []byte("update"),
		},
	}}
	h2 := globalUpdate{seqNum: prepareAru + 2, proposal: &pb.Proposal{
		NodeId: p.id,
		View:   12,
		Seq:    prepareAru + 2,
		Update: &pb.ClientUpdate{
			ClientId:  12,
			NodeId:    p.id,
			Timestamp: 4,
			Update:    []byte("update2"),
		},
	}}
	p.globalHistory.ReplaceOrInsert(&h1)
	p.globalHistory.ReplaceOrInsert(&h2)
	assertGlobalHistorySize(t, p, 2)

	// Prepare message from self should be ignored.
	pr := &pb.Prepare{NodeId: 1, View: 2, LocalAru: prepareAru}
	p.onPrepare(pr)
	if p.prepare != nil {
		t.Fatalf("expected nil *pb.Prepare")
	}

	// Prepare message for different view should be ignored.
	pr = &pb.Prepare{NodeId: 0, View: 1, LocalAru: prepareAru}
	p.onPrepare(pr)
	if p.prepare != nil {
		t.Fatalf("expected nil *pb.Prepare")
	}

	// Prepare message should be successful, and prompt a PrepareOK response.
	pr = &pb.Prepare{NodeId: 0, View: 2, LocalAru: prepareAru}
	p.onPrepare(pr)
	if prA := p.prepare; !reflect.DeepEqual(prA, pr) {
		t.Errorf("expected the Prepare message %+v, found %+v", pr, prA)
	}
	assertState(t, p, StateRegNonLeader)

	// The Prepare message should have prompted a PrepareOK response, which
	// should be sent to the leader (2 % 3 = 2).
	h1m := pb.WrapMessage(h1.globallyOrderedUpdate)
	h2m := pb.WrapMessage(h2.proposal)
	mprOK := &pb.Message_PrepareOk{PrepareOk: &pb.PrepareOK{
		NodeId:   1,
		View:     2,
		DataList: []*pb.Message{&h1m, &h2m},
	}}
	exp := []pb.Message{{To: 2, Type: mprOK}}
	if msgs := p.msgs; !reflect.DeepEqual(msgs, exp) {
		t.Errorf("expected the outbound messages %+v, found %+v", exp, msgs)
	}

	// Other prepare messages received should be handled in an idempotent manned,
	// even when not in the leader election state.
	p.clearMsgs()
	p.onPrepare(pr)
	assertState(t, p, StateRegNonLeader)
	if msgs := p.msgs; !reflect.DeepEqual(msgs, exp) {
		t.Errorf("expected the outbound messages %+v, found %+v", exp, msgs)
	}
}

// TestPrepareOKReceived verifies that Paxos instances handle Prepare messages
// correctly, both when in the LeaderElection state, and when not.
func TestPrepareOKReceived(t *testing.T) {
	p := newPaxos(&Config{ID: 1, NodeCount: 5})
	p.lastAttempted = 1
	p.shiftToPreparePhase()
	p.clearMsgs()
	assertState(t, p, StateLeaderElection)

	// Create a global history that includes messages above the Prepare
	// message's ARU for the PrepareOK message to return.
	const prepareAru = 1
	p.localAru = prepareAru
	h1 := globalUpdate{seqNum: prepareAru + 1, globallyOrderedUpdate: &pb.GloballyOrderedUpdate{
		NodeId: p.id,
		Seq:    prepareAru + 1,
		Update: &pb.ClientUpdate{
			ClientId:  12,
			NodeId:    p.id,
			Timestamp: 3,
			Update:    []byte("update"),
		},
	}}
	h2 := globalUpdate{seqNum: prepareAru + 2, proposal: &pb.Proposal{
		NodeId: p.id,
		View:   12,
		Seq:    prepareAru + 2,
		Update: &pb.ClientUpdate{
			ClientId:  12,
			NodeId:    p.id,
			Timestamp: 4,
			Update:    []byte("update2"),
		},
	}}
	h1m := pb.WrapMessage(h1.globallyOrderedUpdate)
	h2m := pb.WrapMessage(h2.proposal)
	datalist := []*pb.Message{&h1m, &h2m}

	assertPrepareState := func(expGlobalHistorySize, expPrepareOks int) {
		assertGlobalHistorySize(t, p, expGlobalHistorySize)
		if exp, a := expPrepareOks, len(p.prepareOKs); a != exp {
			t.Fatalf("expected %d ViewChange message, found %d", exp, a)
		}
	}
	assertPrepareState(0, 1)

	// PrepareOK message from node 0 should be applied successfully.
	prOK := &pb.PrepareOK{NodeId: 0, View: 1, DataList: datalist}
	p.onPrepareOK(prOK)
	assertPrepareState(2, 2)

	// Replayed PrepareOK message should be ignored. Idempotent.
	p.onPrepareOK(prOK)
	assertPrepareState(2, 2)

	// PrepareOK message from self should be ignored.
	prOK = &pb.PrepareOK{NodeId: 1, View: 1, DataList: datalist}
	p.onPrepareOK(prOK)
	assertPrepareState(2, 2)

	// PrepareOK message for past view should be ignored.
	prOK = &pb.PrepareOK{NodeId: 1, View: 0, DataList: datalist}
	p.onPrepareOK(prOK)
	assertPrepareState(2, 2)

	// PrepareOK message from node 2 should be applied successfully.
	// Prepare phase should complete, because quorum of 3 nodes achieved.
	prOK = &pb.PrepareOK{NodeId: 2, View: 1, DataList: datalist}
	p.onPrepareOK(prOK)
	assertPrepareState(2, 3)
	assertState(t, p, StateRegLeader)

	// The rest of the PrepareOK messages should be ignored.
	prOK = &pb.PrepareOK{NodeId: 3, View: 1, DataList: datalist}
	p.onPrepareOK(prOK)
	prOK = &pb.PrepareOK{NodeId: 4, View: 1, DataList: datalist}
	p.onPrepareOK(prOK)
	assertPrepareState(2, 3)
	assertState(t, p, StateRegLeader)
}
