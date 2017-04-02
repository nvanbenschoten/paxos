package paxos

import (
	"reflect"
	"testing"

	pb "github.com/nvanbenschoten/paxos/paxos/paxospb"
)

// TestLeaderElectionInitialMessages tests the initial messages in the outbox
// of a Paxos instance, which should be attempting to perform the initial
// leader election.
func TestLeaderElectionInitialMessages(t *testing.T) {
	p := newPaxos(&Config{ID: 1, NodeCount: 3})

	vc := &pb.ViewChange{
		NodeId:        1,
		AttemptedView: 1,
	}
	mvc := &pb.Message_ViewChange{ViewChange: vc}
	exp := []pb.Message{
		{To: 0, Type: mvc},
		{To: 2, Type: mvc},
	}
	if msgs := p.msgs; !reflect.DeepEqual(msgs, exp) {
		t.Errorf("expected the outbound messages %+v, found %+v", exp, msgs)
	}
}

// TestShiftToLeaderElection verifies that the state transition to the leader
// election state sets all related data structures correctly.
func TestShiftToLeaderElection(t *testing.T) {
	p := newPaxos(&Config{ID: 1, NodeCount: 3})

	const newView = 7
	p.shiftToLeaderElection(newView)

	assertState(t, p, StateLeaderElection)
	if p.progressTimer.isSet() {
		t.Fatalf("expected unset progress timer")
	}
	if p.prepare != nil {
		t.Fatalf("expected nil *pb.Prepare")
	}
	if len(p.prepareOKs) > 0 {
		t.Fatalf("expected empty prepareOKs set")
	}
	if len(p.lastEnqueued) > 0 {
		t.Fatalf("expected empty lastEnqueued set")
	}
	if p.lastAttempted != newView {
		t.Fatalf("expected lastAttempted view %d, found %d", newView, p.lastAttempted)
	}

	expViewChanges := map[uint64]*pb.ViewChange{
		1: &pb.ViewChange{
			NodeId:        1,
			AttemptedView: 7,
		},
	}
	if !reflect.DeepEqual(p.viewChanges, expViewChanges) {
		t.Errorf("expected view changes %+v, found %+v", expViewChanges, p.viewChanges)
	}
}

// TestLeaderElectionReceiveMessages verifies that a Paxos instance successfully
// progresses through a leader election while receiving ViewChange messages.
func TestLeaderElectionReceiveMessages(t *testing.T) {
	p := newPaxos(&Config{ID: 1, NodeCount: 5})

	if p.progressTimer.isSet() {
		t.Fatalf("expected unset progress timer")
	}
	if p.prepare != nil {
		t.Fatalf("expected nil *pb.Prepare")
	}
	if exp, a := uint64(0), p.lastInstalled; a != exp {
		t.Fatalf("expected last installed view %d; found %d", exp, a)
	}
	assertLeaderElectionState := func(expLastAttempted, expViewChanges int) {
		assertState(t, p, StateLeaderElection)
		if exp, a := uint64(expLastAttempted), p.lastAttempted; a != exp {
			t.Fatalf("expected last attempted view %d; found %d", exp, a)
		}
		if exp, a := expViewChanges, len(p.viewChanges); a != exp {
			t.Fatalf("expected %d ViewChange message, found %d", exp, a)
		}
	}
	assertLeaderElectionState(1, 1)

	// ViewChange message from node 0 should be applied successfully.
	vc := &pb.ViewChange{NodeId: 0, AttemptedView: 1}
	p.onViewChange(vc)
	assertLeaderElectionState(1, 2)

	// Replayed ViewChange message should be ignored. Idempotent.
	p.onViewChange(vc)
	assertLeaderElectionState(1, 2)

	// ViewChange message from self should be ignored.
	vc = &pb.ViewChange{NodeId: 1, AttemptedView: 1}
	p.onViewChange(vc)
	assertLeaderElectionState(1, 2)

	// ViewChange message for past view should be ignored.
	vc = &pb.ViewChange{NodeId: 2, AttemptedView: 0}
	p.onViewChange(vc)
	assertLeaderElectionState(1, 2)

	// ViewChange message from node 2 should be applied successfully.
	// Leader election should complete, because quorum of 3 nodes achieved.
	vc = &pb.ViewChange{NodeId: 2, AttemptedView: 1}
	p.onViewChange(vc)
	assertLeaderElectionState(1, 3)
	if exp, a := uint64(1), p.lastInstalled; a != exp {
		t.Fatalf("expected last installed view %d after leader election; found %d", exp, a)
	}
	if !p.progressTimer.isSet() {
		t.Fatalf("expected progress timer to be set after completed leader election")
	}

	// The rest of the ViewChange messages should be ignored.
	vc = &pb.ViewChange{NodeId: 3, AttemptedView: 1}
	p.onViewChange(vc)
	vc = &pb.ViewChange{NodeId: 4, AttemptedView: 1}
	p.onViewChange(vc)
	assertLeaderElectionState(1, 3)
}

// TestLeaderElectionJumpToGreaterView verifies that a Paxos instance jumps to
// a larger view when it receives a ViewChange message for larger view during
// leadership election.
func TestLeaderElectionJumpToGreaterView(t *testing.T) {
	p := newPaxos(&Config{ID: 1, NodeCount: 5})

	if p.progressTimer.isSet() {
		t.Fatalf("expected unset progress timer")
	}
	if p.prepare != nil {
		t.Fatalf("expected nil *pb.Prepare")
	}
	if exp, a := uint64(0), p.lastInstalled; a != exp {
		t.Fatalf("expected last installed view %d; found %d", exp, a)
	}
	assertLeaderElectionState := func(expLastAttempted, expViewChanges int) {
		assertState(t, p, StateLeaderElection)
		if exp, a := uint64(expLastAttempted), p.lastAttempted; a != exp {
			t.Fatalf("expected last attempted view %d; found %d", exp, a)
		}
		if exp, a := expViewChanges, len(p.viewChanges); a != exp {
			t.Fatalf("expected %d ViewChange message, found %d", exp, a)
		}
	}
	assertLeaderElectionState(1, 1)

	// ViewChange message from node 0 should be applied successfully.
	vc := &pb.ViewChange{NodeId: 0, AttemptedView: 1}
	p.onViewChange(vc)
	assertLeaderElectionState(1, 2)

	// ViewChange message for larger view should replace current attempt.
	vc = &pb.ViewChange{NodeId: 2, AttemptedView: 6}
	p.onViewChange(vc)
	assertLeaderElectionState(6, 2)

	// ViewChange message from node 1 should be applied successfully.
	// Leader election should complete, because quorum of 3 nodes achieved.
	vc = &pb.ViewChange{NodeId: 3, AttemptedView: 6}
	p.onViewChange(vc)
	assertLeaderElectionState(6, 3)
	if exp, a := uint64(6), p.lastInstalled; a != exp {
		t.Fatalf("expected last installed view %d after leader election; found %d", exp, a)
	}
	if !p.progressTimer.isSet() {
		t.Fatalf("expected progress timer to be set after completed leader election")
	}
}

// TestViewChangeProofReceiveMessages verifies that if a Paxos instance receives a
// ViewChangeProof message for a later view, it jumps to it.
func TestViewChangeProofReceiveMessages(t *testing.T) {
	p := newPaxos(&Config{ID: 1, NodeCount: 5})

	if exp, a := uint64(1), p.lastAttempted; a != exp {
		t.Fatalf("expected last attempted view %d; found %d", exp, a)
	}
	if exp, a := uint64(0), p.lastInstalled; a != exp {
		t.Fatalf("expected last installed view %d; found %d", exp, a)
	}

	// View change proofs from current or previous views are discarded.
	vcp := &pb.ViewChangeProof{NodeId: 0, InstalledView: 0}
	p.onViewChangeProof(vcp)
	if exp, a := uint64(0), p.lastInstalled; a != exp {
		t.Fatalf("expected last installed view %d; found %d", exp, a)
	}

	// View change proofs from self are discarded.
	vcp = &pb.ViewChangeProof{NodeId: 1, InstalledView: 4}
	p.onViewChangeProof(vcp)
	if exp, a := uint64(0), p.lastInstalled; a != exp {
		t.Fatalf("expected last installed view %d; found %d", exp, a)
	}

	// View change proofs from node 0 with later view should be applied successfully.
	vcp = &pb.ViewChangeProof{NodeId: 0, InstalledView: 4}
	p.onViewChangeProof(vcp)
	if exp, a := uint64(4), p.lastInstalled; a != exp {
		t.Fatalf("expected last installed view %d after view change proof; found %d", exp, a)
	}
}
