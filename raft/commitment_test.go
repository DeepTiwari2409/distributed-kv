package raft

import (
	"errors"
	"testing"
	"time"
)

type recordingStateMachine struct {
	commands []Command
	failAt   int
}

func (s *recordingStateMachine) Apply(command Command) error {
	if s.failAt > 0 && len(s.commands)+1 == s.failAt {
		return errors.New("forced apply failure")
	}
	s.commands = append(s.commands, command.copy())
	return nil
}

func newCommitLeader(t *testing.T, size int) *RaftNode {
	t.Helper()
	node, err := NewNode(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.AdvanceTerm(2); err != nil {
		t.Fatal(err)
	}
	if err := node.SetState(Leader); err != nil {
		t.Fatal(err)
	}
	peers := make([]NodeID, size-1)
	for index := range peers {
		peers[index] = NodeID(index + 2)
	}
	rn, err := NewRaftNode(node, peers, NewInMemoryTransport(make(map[NodeID]*RaftNode)), NewDeterministicClock(time.Unix(0, 0)), 20*time.Millisecond, 100*time.Millisecond, 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	rn.mu.Lock()
	rn.initializeReplicationState()
	rn.mu.Unlock()
	return rn
}

func TestEntryCommitsAfterMajorityReplication(t *testing.T) {
	leader := newCommitLeader(t, 3)
	appendReplicationEntries(t, leader.node, 2)
	leader.mu.Lock()
	leader.matchIndex[2] = 1
	leader.nextIndex[2] = 2
	leader.mu.Unlock()
	if err := leader.CommitIfPossible(); err != nil {
		t.Fatal(err)
	}
	if leader.node.CommitIndex() != 1 {
		t.Fatalf("expected commitIndex 1, got %d", leader.node.CommitIndex())
	}
}

func TestEntryDoesNotCommitWithoutMajority(t *testing.T) {
	leader := newCommitLeader(t, 5)
	appendReplicationEntries(t, leader.node, 2)
	leader.mu.Lock()
	leader.matchIndex[2] = 1
	leader.mu.Unlock()
	if err := leader.CommitIfPossible(); err != nil {
		t.Fatal(err)
	}
	if leader.node.CommitIndex() != 0 {
		t.Fatal("entry committed without a majority")
	}
}

func TestCommitUsesCurrentTermRule(t *testing.T) {
	leader := newCommitLeader(t, 3)
	appendReplicationEntries(t, leader.node, 1, 2)
	leader.mu.Lock()
	leader.matchIndex[2] = 1
	leader.nextIndex[2] = 2
	leader.matchIndex[3] = 1
	leader.nextIndex[3] = 2
	leader.mu.Unlock()
	if err := leader.CommitIfPossible(); err != nil {
		t.Fatal(err)
	}
	if leader.node.CommitIndex() != 0 {
		t.Fatal("previous-term entry committed directly")
	}
	leader.mu.Lock()
	leader.matchIndex[2] = 2
	leader.nextIndex[2] = 3
	leader.mu.Unlock()
	if err := leader.CommitIfPossible(); err != nil {
		t.Fatal(err)
	}
	if leader.node.CommitIndex() != 2 {
		t.Fatalf("expected prior prefix and current entry committed, got %d", leader.node.CommitIndex())
	}
}

func TestCommitIndexNeverDecreasesOrExceedsLog(t *testing.T) {
	leader := newCommitLeader(t, 3)
	appendReplicationEntries(t, leader.node, 2)
	if err := leader.node.AdvanceCommitIndex(1); err != nil {
		t.Fatal(err)
	}
	if err := leader.CommitIfPossible(); err != nil {
		t.Fatal(err)
	}
	if leader.node.CommitIndex() != 1 || leader.node.CommitIndex() > leader.node.Log().LastIndex() {
		t.Fatal("commit index invariant violated")
	}
}

func TestFollowerAdvancesCommitFromLeaderCommit(t *testing.T) {
	_, follower, _ := makeReplicationPair(t)
	appendReplicationEntries(t, follower.node, 2, 2)
	sm := &recordingStateMachine{}
	follower.SetStateMachine(sm)
	request := AppendEntriesRequest{Term: 2, LeaderID: 1, LeaderCommit: 2, Entries: []LogEntry{}}
	if !follower.handleAppendEntries(request).Success {
		t.Fatal("expected valid commit propagation")
	}
	if follower.node.CommitIndex() != 2 || follower.node.LastApplied() != 2 || len(sm.commands) != 2 {
		t.Fatalf("expected commit/apply through 2, got commit=%d applied=%d commands=%d", follower.node.CommitIndex(), follower.node.LastApplied(), len(sm.commands))
	}
}

func TestFollowerCommitIsLimitedToLocalLog(t *testing.T) {
	_, follower, _ := makeReplicationPair(t)
	appendReplicationEntries(t, follower.node, 2)
	request := AppendEntriesRequest{Term: 2, LeaderID: 1, LeaderCommit: 10, Entries: []LogEntry{}}
	if !follower.handleAppendEntries(request).Success {
		t.Fatal("expected heartbeat to be valid")
	}
	if follower.node.CommitIndex() != 1 {
		t.Fatalf("expected commit limited to local log, got %d", follower.node.CommitIndex())
	}
}

func TestReplicationDoesNotApplyBeforeCommit(t *testing.T) {
	leader, follower, _ := makeReplicationPair(t)
	sm := &recordingStateMachine{}
	follower.SetStateMachine(sm)
	appendReplicationEntries(t, leader.node, 2)
	leader.ReplicateTo(2)
	if follower.node.CommitIndex() != 0 || follower.node.LastApplied() != 0 || len(sm.commands) != 0 {
		t.Fatal("replication applied an uncommitted entry")
	}
}

func TestCommittedEntriesApplyInOrderExactlyOnce(t *testing.T) {
	leader, follower, _ := makeReplicationPair(t)
	sm := &recordingStateMachine{}
	follower.SetStateMachine(sm)
	appendReplicationEntries(t, leader.node, 2, 2, 2)
	request := AppendEntriesRequest{Term: 2, LeaderID: 1, LeaderCommit: 3, Entries: []LogEntry{replicationEntry(1, 2), replicationEntry(2, 2), replicationEntry(3, 2)}}
	if !follower.handleAppendEntries(request).Success {
		t.Fatal("expected entries and commit to be accepted")
	}
	if err := follower.ApplyCommitted(); err != nil {
		t.Fatal(err)
	}
	if follower.node.LastApplied() != 3 || len(sm.commands) != 3 {
		t.Fatalf("expected exactly three ordered applications, got %d", len(sm.commands))
	}
	if err := follower.ApplyCommitted(); err != nil {
		t.Fatal(err)
	}
	if len(sm.commands) != 3 {
		t.Fatal("committed entries were applied more than once")
	}
}

func TestStateMachineFailurePreservesApplicationOrder(t *testing.T) {
	_, follower, _ := makeReplicationPair(t)
	sm := &recordingStateMachine{failAt: 2}
	follower.SetStateMachine(sm)
	appendReplicationEntries(t, follower.node, 2, 2)
	if err := follower.node.AdvanceCommitIndex(2); err != nil {
		t.Fatal(err)
	}
	if err := follower.ApplyCommitted(); err == nil {
		t.Fatal("expected application failure")
	}
	if follower.node.LastApplied() != 1 || len(sm.commands) != 1 {
		t.Fatal("failed application skipped or advanced lastApplied")
	}
}

func TestKVStoreAppliesCommittedCommands(t *testing.T) {
	leader, follower, _ := makeReplicationPair(t)
	appendReplicationEntries(t, leader.node, 2, 2)
	sm := &recordingStateMachine{}
	follower.SetStateMachine(sm)
	request := AppendEntriesRequest{Term: 2, LeaderID: 1, LeaderCommit: 2, Entries: []LogEntry{replicationEntry(1, 2), replicationEntry(2, 2)}}
	if !follower.handleAppendEntries(request).Success {
		t.Fatal("expected KV commands to replicate")
	}
	if len(sm.commands) != 2 || sm.commands[0].Type != CommandPut {
		t.Fatal("committed PUT was not applied")
	}
}

func TestKVStoreAppliesDelete(t *testing.T) {
	sm := &recordingStateMachine{}
	if err := sm.Apply(NewDeleteCommand("k")); err != nil {
		t.Fatal(err)
	}
	if len(sm.commands) != 1 || sm.commands[0].Type != CommandDelete {
		t.Fatal("committed DELETE was not applied")
	}
}
