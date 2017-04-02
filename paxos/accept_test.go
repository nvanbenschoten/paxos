package paxos

import "testing"

// TestShiftToRegNonLeader verifies that the state transition to the non leader
// state sets all related data structures correctly.
func TestShiftToRegNonLeader(t *testing.T) {
	p := newPaxos(&Config{ID: 1, NodeCount: 3})
	p.lastAttempted = 7
	p.shiftToRegNonLeader()

	assertState(t, p, StateRegNonLeader)
	if exp, a := p.lastAttempted, p.lastInstalled; a != exp {
		t.Fatalf("expected last attempted view %d to be installed, found %d", exp, a)
	}
	if p.updateQueue.Len() > 0 {
		t.Fatalf("expected empty updateQueue, found %v", p.updateQueue)
	}
}
