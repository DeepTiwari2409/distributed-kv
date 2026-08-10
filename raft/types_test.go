package raft

import (
	"testing"
)

func TestNodeIDValidity(t *testing.T) {
	var id NodeID = 1
	if !id.Valid() {
		t.Fatal("expected valid NodeID")
	}
	if NoNode.Valid() {
		t.Fatal("expected NoNode to be invalid")
	}
}

func TestNodeIDComparison(t *testing.T) {
	ids := []NodeID{1, 2, 1}
	if ids[0] != ids[2] || ids[0] == ids[1] {
		t.Fatal("NodeID comparison failed")
	}
}

func TestNodeIDCanBeMapKey(t *testing.T) {
	m := map[NodeID]string{1: "one", 2: "two"}
	if m[1] != "one" || m[2] != "two" {
		t.Fatal("NodeID map key failed")
	}
}

func TestStateInspection(t *testing.T) {
	if Follower.String() != "Follower" {
		t.Fatal("unexpected state string")
	}
	if State(99).Valid() {
		t.Fatal("unexpected valid unknown state")
	}
}

func TestCommandTypes(t *testing.T) {
	cmd := NewPutCommand("k", []byte("v"))
	if cmd.Type != CommandPut {
		t.Fatal("expected put command")
	}
	if err := cmd.Validate(); err != nil {
		t.Fatal(err)
	}
	if NewDeleteCommand("k").Type != CommandDelete {
		t.Fatal("expected delete command")
	}
}

func TestInvalidCommandType(t *testing.T) {
	cmd := Command{Type: 99, Key: "k"}
	if err := cmd.Validate(); err == nil {
		t.Fatal("expected invalid command type")
	}
}
