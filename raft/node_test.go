package raft

import "testing"

func TestNewNodeStartsAsFollower(t *testing.T) {
	n, err := NewNode(1)
	if err != nil {
		t.Fatal(err)
	}
	if n.State() != Follower {
		t.Fatal("expected follower state")
	}
}

func TestNewNodeStartsAtTermZero(t *testing.T) {
	n, _ := NewNode(1)
	if n.CurrentTerm() != 0 {
		t.Fatal("expected term 0")
	}
}

func TestNewNodeHasNoVote(t *testing.T) {
	n, _ := NewNode(1)
	if n.VotedFor() != NoNode {
		t.Fatal("expected no vote")
	}
}

func TestNewNodeHasEmptyLog(t *testing.T) {
	n, _ := NewNode(1)
	if n.Log().LastIndex() != 0 {
		t.Fatal("expected empty log")
	}
}

func TestNewNodeHasZeroCommitIndex(t *testing.T) {
	n, _ := NewNode(1)
	if n.CommitIndex() != 0 {
		t.Fatal("expected commitIndex 0")
	}
}

func TestNewNodeHasZeroLastApplied(t *testing.T) {
	n, _ := NewNode(1)
	if n.LastApplied() != 0 {
		t.Fatal("expected lastApplied 0")
	}
}

func TestAdvanceTerm(t *testing.T) {
	n, _ := NewNode(1)
	if err := n.AdvanceTerm(1); err != nil {
		t.Fatal(err)
	}
	if n.CurrentTerm() != 1 {
		t.Fatal("expected term 1")
	}
}

func TestTermCannotDecrease(t *testing.T) {
	n, _ := NewNode(1)
	n.AdvanceTerm(2)
	if err := n.AdvanceTerm(1); err == nil {
		t.Fatal("expected term decrease rejection")
	}
}

func TestTermMonotonicity(t *testing.T) {
	n, _ := NewNode(1)
	n.AdvanceTerm(1)
	n.AdvanceTerm(2)
	if n.CurrentTerm() != 2 {
		t.Fatal("expected term 2")
	}
}

func TestVoteStateCanBeRepresentedPerTerm(t *testing.T) {
	n, _ := NewNode(1)
	n.AdvanceTerm(1)
	if err := n.SetVotedFor(2); err != nil {
		t.Fatal(err)
	}
	if n.VotedFor() != 2 {
		t.Fatal("expected votedFor 2")
	}
}

func TestCommitIndexStartsAtZero(t *testing.T) {
	n, _ := NewNode(1)
	if n.CommitIndex() != 0 {
		t.Fatal("expected commitIndex 0")
	}
}

func TestAdvanceCommitIndex(t *testing.T) {
	n, _ := NewNode(1)
	if err := n.AdvanceCommitIndex(0); err != nil {
		t.Fatal(err)
	}
}

func TestCommitIndexCannotDecrease(t *testing.T) {
	n, _ := NewNode(1)
	if err := n.AppendEntry(LogEntry{Index: 1, Term: 1, Command: NewPutCommand("k", []byte("v"))}); err != nil {
		t.Fatal(err)
	}
	if err := n.AdvanceCommitIndex(1); err != nil {
		t.Fatal(err)
	}
	if err := n.AdvanceCommitIndex(0); err == nil {
		t.Fatal("expected commit index decrease rejection")
	}
}

func TestLastAppliedStartsAtZero(t *testing.T) {
	n, _ := NewNode(1)
	if n.LastApplied() != 0 {
		t.Fatal("expected lastApplied 0")
	}
}

func TestAdvanceLastApplied(t *testing.T) {
	n, _ := NewNode(1)
	if err := n.AppendEntry(LogEntry{Index: 1, Term: 1, Command: NewPutCommand("k", []byte("v"))}); err != nil {
		t.Fatal(err)
	}
	if err := n.AdvanceCommitIndex(1); err != nil {
		t.Fatal(err)
	}
	if err := n.AdvanceLastApplied(1); err != nil {
		t.Fatal(err)
	}
}

func TestLastAppliedCannotExceedCommitIndex(t *testing.T) {
	n, _ := NewNode(1)
	if err := n.AdvanceLastApplied(1); err == nil {
		t.Fatal("expected lastApplied exceed commitIndex rejection")
	}
}

func TestLastAppliedCannotDecrease(t *testing.T) {
	n, _ := NewNode(1)
	if err := n.AppendEntry(LogEntry{Index: 1, Term: 1, Command: NewPutCommand("k", []byte("v"))}); err != nil {
		t.Fatal(err)
	}
	if err := n.AppendEntry(LogEntry{Index: 2, Term: 1, Command: NewPutCommand("k2", []byte("v2"))}); err != nil {
		t.Fatal(err)
	}
	if err := n.AdvanceCommitIndex(2); err != nil {
		t.Fatal(err)
	}
	if err := n.AdvanceLastApplied(2); err != nil {
		t.Fatal(err)
	}
	if err := n.AdvanceLastApplied(1); err == nil {
		t.Fatal("expected lastApplied decrease rejection")
	}
}

func TestLogIndexInvariant(t *testing.T) {
	log := NewRaftLog()
	if err := log.Append(LogEntry{Index: 1, Term: 1, Command: NewPutCommand("k", []byte("v"))}); err != nil {
		t.Fatal(err)
	}
	if err := log.Append(LogEntry{Index: 1, Term: 1, Command: NewPutCommand("k2", []byte("v2"))}); err == nil {
		t.Fatal("expected duplicate index rejection")
	}
}

func TestCommitApplyInvariant(t *testing.T) {
	n, _ := NewNode(1)
	n.AdvanceCommitIndex(1)
	if err := n.AdvanceLastApplied(2); err == nil {
		t.Fatal("expected lastApplied exceed commitIndex rejection")
	}
}
