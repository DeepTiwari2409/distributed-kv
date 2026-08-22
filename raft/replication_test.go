package raft

import (
	"testing"
	"time"
)

func replicationEntry(index, term uint64) LogEntry {
	return LogEntry{Index: index, Term: term, Command: NewPutCommand("k", []byte("v"))}
}

func appendReplicationEntries(t *testing.T, node *Node, terms ...uint64) {
	t.Helper()
	for index, term := range terms {
		if err := node.log.Append(replicationEntry(uint64(index+1), term)); err != nil {
			t.Fatal(err)
		}
	}
}

func makeReplicationPair(t *testing.T) (*RaftNode, *RaftNode, *InMemoryTransport) {
	t.Helper()
	peers := make(map[NodeID]*RaftNode)
	transport := NewInMemoryTransport(peers)
	leaderNode, _ := NewNode(1)
	leaderNode.AdvanceTerm(2)
	leaderNode.SetState(Leader)
	leaderClock := NewDeterministicClock(time.Unix(0, 0))
	leader, err := NewRaftNode(leaderNode, []NodeID{2}, transport, leaderClock, 20*time.Millisecond, 100*time.Millisecond, 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	followerNode, _ := NewNode(2)
	followerClock := NewDeterministicClock(time.Unix(0, 0))
	follower, err := NewRaftNode(followerNode, []NodeID{1}, transport, followerClock, 20*time.Millisecond, 100*time.Millisecond, 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	peers[1], peers[2] = leader, follower
	leader.mu.Lock()
	leader.startHeartbeats()
	leader.mu.Unlock()
	return leader, follower, transport
}

func TestLeaderInitializesReplicationIndexes(t *testing.T) {
	leader, _, _ := makeReplicationPair(t)
	if leader.NextIndex(2) != 1 || leader.MatchIndex(2) != 0 {
		t.Fatalf("expected nextIndex=1 matchIndex=0, got %d/%d", leader.NextIndex(2), leader.MatchIndex(2))
	}
}

func TestLeaderReplicatesSingleAndMultipleEntries(t *testing.T) {
	leader, follower, _ := makeReplicationPair(t)
	appendReplicationEntries(t, leader.node, 1, 1, 2)
	leader.ReplicateTo(2)
	if follower.node.Log().Len() != 3 {
		t.Fatalf("expected 3 follower entries, got %d", follower.node.Log().Len())
	}
	if leader.MatchIndex(2) != 3 || leader.NextIndex(2) != 4 {
		t.Fatalf("expected replication indexes 3/4, got %d/%d", leader.MatchIndex(2), leader.NextIndex(2))
	}
}

func TestFollowerRejectsInvalidPrefixWithoutMutation(t *testing.T) {
	_, follower, _ := makeReplicationPair(t)
	appendReplicationEntries(t, follower.node, 1, 2)
	before := follower.node.Log()
	request := AppendEntriesRequest{Term: 2, LeaderID: 1, PrevLogIndex: 3, PrevLogTerm: 2, Entries: []LogEntry{replicationEntry(4, 2)}}
	if follower.handleAppendEntries(request).Success {
		t.Fatal("expected missing prefix to be rejected")
	}
	if follower.node.Log().Len() != before.Len() {
		t.Fatal("rejected request modified follower log")
	}
}

func TestFollowerRejectsPrevTermMismatch(t *testing.T) {
	_, follower, _ := makeReplicationPair(t)
	appendReplicationEntries(t, follower.node, 1, 2)
	request := AppendEntriesRequest{Term: 2, LeaderID: 1, PrevLogIndex: 2, PrevLogTerm: 9, Entries: []LogEntry{replicationEntry(3, 2)}}
	if follower.handleAppendEntries(request).Success {
		t.Fatal("expected prev term mismatch to be rejected")
	}
	if follower.node.Log().Len() != 2 {
		t.Fatal("rejected request modified follower log")
	}
}

func TestFollowerAppendsMissingEntries(t *testing.T) {
	_, follower, _ := makeReplicationPair(t)
	appendReplicationEntries(t, follower.node, 1)
	request := AppendEntriesRequest{Term: 2, LeaderID: 1, PrevLogIndex: 1, PrevLogTerm: 1, Entries: []LogEntry{replicationEntry(2, 1), replicationEntry(3, 2)}}
	if !follower.handleAppendEntries(request).Success {
		t.Fatal("expected missing suffix to be accepted")
	}
	if follower.node.Log().Len() != 3 || follower.node.Log().LastTerm() != 2 {
		t.Fatal("follower did not append missing suffix")
	}
}

func TestFollowerRepairsConflictingSuffix(t *testing.T) {
	_, follower, _ := makeReplicationPair(t)
	appendReplicationEntries(t, follower.node, 1, 1, 3, 3)
	request := AppendEntriesRequest{Term: 2, LeaderID: 1, PrevLogIndex: 2, PrevLogTerm: 1, Entries: []LogEntry{replicationEntry(3, 2), replicationEntry(4, 2)}}
	if !follower.handleAppendEntries(request).Success {
		t.Fatal("expected conflicting suffix to be repaired")
	}
	log := follower.node.Log()
	if log.Len() != 4 {
		t.Fatalf("expected four entries, got %d", log.Len())
	}
	for index, term := range []uint64{1, 1, 2, 2} {
		entry, _ := log.Entry(uint64(index + 1))
		if entry.Term != term {
			t.Fatalf("entry %d has term %d, want %d", index+1, entry.Term, term)
		}
	}
}

func TestCommittedConflictIsRejected(t *testing.T) {
	_, follower, _ := makeReplicationPair(t)
	appendReplicationEntries(t, follower.node, 1, 1, 3)
	if err := follower.node.AdvanceCommitIndex(3); err != nil {
		t.Fatal(err)
	}
	request := AppendEntriesRequest{Term: 2, LeaderID: 1, PrevLogIndex: 2, PrevLogTerm: 1, Entries: []LogEntry{replicationEntry(3, 2)}}
	if follower.handleAppendEntries(request).Success {
		t.Fatal("expected committed conflict to be rejected")
	}
	entry, _ := follower.node.Log().Entry(3)
	if entry.Term != 3 {
		t.Fatal("committed entry was changed")
	}
}

func TestLeaderBacktracksAndRetriesRejectedAppendEntries(t *testing.T) {
	leader, follower, transport := makeReplicationPair(t)
	appendReplicationEntries(t, leader.node, 1, 1, 2)
	appendReplicationEntries(t, follower.node, 1)
	leader.mu.Lock()
	leader.nextIndex[2] = 4
	leader.mu.Unlock()
	leader.ReplicateTo(2)
	if follower.node.Log().Len() != 3 {
		t.Fatalf("expected retry to catch up follower, got %d entries", follower.node.Log().Len())
	}
	if leader.NextIndex(2) != 4 {
		t.Fatalf("expected retry to advance nextIndex to 4, got %d", leader.NextIndex(2))
	}
	_ = transport
}

func TestStaleResponsesDoNotRegressReplicationState(t *testing.T) {
	leader, _, _ := makeReplicationPair(t)
	appendReplicationEntries(t, leader.node, 1, 1, 2)
	leader.mu.Lock()
	leader.matchIndex[2] = 3
	leader.nextIndex[2] = 4
	leader.handleAppendEntriesResponse(AppendEntriesResponse{Term: 2, Success: false}, 2)
	leader.handleAppendEntriesResponse(AppendEntriesResponse{Term: 2, Success: true}, 2)
	leader.mu.Unlock()
	if leader.MatchIndex(2) != 3 || leader.NextIndex(2) != 4 {
		t.Fatalf("stale responses regressed state to %d/%d", leader.MatchIndex(2), leader.NextIndex(2))
	}
}

func TestReplicationDoesNotAdvanceCommitOrApply(t *testing.T) {
	leader, follower, _ := makeReplicationPair(t)
	appendReplicationEntries(t, leader.node, 1)
	leader.ReplicateTo(2)
	if leader.node.CommitIndex() != 0 || follower.node.CommitIndex() != 0 || follower.node.LastApplied() != 0 {
		t.Fatal("replication advanced commit or applied state")
	}
}

func TestCaughtUpFollowerReceivesEmptyAppendEntries(t *testing.T) {
	leader, follower, _ := makeReplicationPair(t)
	appendReplicationEntries(t, leader.node, 1)
	leader.ReplicateTo(2)
	leader.mu.Lock()
	request, ok := leader.buildAppendEntries(2)
	leader.mu.Unlock()
	if !ok || len(request.Entries) != 0 || request.PrevLogIndex != follower.node.Log().LastIndex() {
		t.Fatal("caught-up follower did not receive an empty heartbeat-shaped request")
	}
}
