package paxos

import (
	"testing"
)

// TestShiftToRegLeader verifies that the state transition to the leader state
// sets all related data structures correctly.
func TestShiftToRegLeader(t *testing.T) {
	p := newPaxos(&Config{ID: 1, NodeCount: 3})
	p.shiftToRegLeader()

	assertState(t, p, StateRegLeader)

	// TODO set messages before and set ARU.
}
