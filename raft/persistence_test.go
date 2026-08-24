package raft

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DeepTiwari2409/distributed-kv/storage"
)

type testRaftStorage struct {
	state storage.RaftState
	fail  bool
}

func (s *testRaftStorage) LoadRaftState() (storage.RaftState, error) {
	if s.fail {
		return storage.RaftState{}, errors.New("read failure")
	}
	return s.state, nil
}

func (s *testRaftStorage) SaveRaftState(state storage.RaftState) error {
	if s.fail {
		return errors.New("write failure")
	}
	s.state = state
	return nil
}

func (s *testRaftStorage) PersistCurrentTerm(term uint64) error {
	s.state.CurrentTerm = term
	return nil
}

func (s *testRaftStorage) PersistVotedFor(votedFor uint64) error {
	s.state.VotedFor = votedFor
	return nil
}

func (s *testRaftStorage) AppendRaftLog(record storage.RaftLogRecord) error {
	s.state.Log = append(s.state.Log, record)
	return nil
}

func (s *testRaftStorage) TruncateRaftLog(index uint64) error {
	if index == 0 || index > uint64(len(s.state.Log))+1 {
		return errors.New("invalid log index")
	}
	s.state.Log = s.state.Log[:index-1]
	return nil
}

func (s *testRaftStorage) Sync() error { return nil }

func persistentNode(t *testing.T, stateStore storage.RaftStateStore) (*RaftNode, *Node) {
	t.Helper()
	node, err := NewNode(1)
	if err != nil {
		t.Fatal(err)
	}
	transport := NewInMemoryTransport(make(map[NodeID]*RaftNode))
	clock := NewDeterministicClock(time.Unix(0, 0))
	rn, err := NewRaftNodeWithStorage(node, nil, transport, clock, stateStore, 20*time.Millisecond, 100*time.Millisecond, 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	return rn, node
}

func TestPersistentTermVoteAndLogRecovery(t *testing.T) {
	stateStore := &testRaftStorage{}
	rn, node := persistentNode(t, stateStore)
	if err := node.AdvanceTerm(4); err != nil {
		t.Fatal(err)
	}
	if err := rn.persistState(); err != nil {
		t.Fatal(err)
	}
	if err := node.SetVotedFor(2); err != nil {
		t.Fatal(err)
	}
	if err := rn.persistState(); err != nil {
		t.Fatal(err)
	}
	if err := rn.AppendEntry(replicationEntry(1, 4)); err != nil {
		t.Fatal(err)
	}

	recovered, recoveredNode := persistentNode(t, stateStore)
	if recoveredNode.CurrentTerm() != 4 || recoveredNode.VotedFor() != 2 || recoveredNode.Log().Len() != 1 {
		t.Fatalf("recovery lost state: term=%d vote=%d log=%d", recoveredNode.CurrentTerm(), recoveredNode.VotedFor(), recoveredNode.Log().Len())
	}
	if recoveredNode.State() != Follower || recovered == nil {
		t.Fatal("recovered node must start as follower")
	}
}

func TestPersistentVoteFailureDoesNotGrantVote(t *testing.T) {
	stateStore := &testRaftStorage{}
	rn, node := persistentNode(t, stateStore)
	stateStore.fail = true
	response := rn.handleRequestVote(RequestVoteRequest{Term: 1, CandidateID: 2})
	if response.VoteGranted || node.VotedFor() != NoNode {
		t.Fatal("failed vote persistence produced a granted or retained vote")
	}
}

func TestHigherTermRequestVotePersistsBeforeResponse(t *testing.T) {
	stateStore := &testRaftStorage{}
	rn, _ := persistentNode(t, stateStore)
	response := rn.handleRequestVote(RequestVoteRequest{Term: 6, CandidateID: 2})
	if !response.VoteGranted || stateStore.state.CurrentTerm != 6 || stateStore.state.VotedFor != 2 {
		t.Fatal("request vote state was not persisted before success")
	}
	recovered, node := persistentNode(t, stateStore)
	if recovered == nil || node.CurrentTerm() != 6 || node.VotedFor() != 2 {
		t.Fatal("persisted request vote state was not recoverable")
	}
	conflict := recovered.handleRequestVote(RequestVoteRequest{Term: 6, CandidateID: 3})
	if conflict.VoteGranted {
		t.Fatal("recovered vote allowed a conflicting candidate")
	}
}

func TestPersistentLogFailureDoesNotAppend(t *testing.T) {
	stateStore := &testRaftStorage{}
	rn, node := persistentNode(t, stateStore)
	stateStore.fail = true
	if err := rn.AppendEntry(replicationEntry(1, 1)); err == nil {
		t.Fatal("expected log persistence failure")
	}
	if node.Log().Len() != 0 {
		t.Fatal("failed log persistence retained an in-memory entry")
	}
}

func TestUncommittedRecoveredLogIsNotApplied(t *testing.T) {
	stateStore := &testRaftStorage{state: storage.RaftState{CurrentTerm: 2, Log: []storage.RaftLogRecord{{Index: 1, Term: 2, Type: uint8(CommandPut), Key: "k", Value: []byte("v")}}}}
	rn, node := persistentNode(t, stateStore)
	applied := &recordingStateMachine{}
	rn.SetStateMachine(applied)
	if err := rn.ApplyCommitted(); err != nil {
		t.Fatal(err)
	}
	if node.CommitIndex() != 0 || node.LastApplied() != 0 || len(applied.commands) != 0 {
		t.Fatal("recovered uncommitted entry was applied")
	}
	if err := rn.RecoverCommitted(1); err != nil {
		t.Fatal(err)
	}
	if len(applied.commands) != 1 || node.LastApplied() != 1 {
		t.Fatal("explicitly recovered committed entry was not applied")
	}
}

func TestFileRaftStateStoreRoundTripAndCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft", "state.json")
	stateStore, err := storage.OpenRaftStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	state := storage.RaftState{CurrentTerm: 3, VotedFor: 2, Log: []storage.RaftLogRecord{{Index: 1, Term: 3, Type: uint8(CommandDelete), Key: "k"}}}
	if err := stateStore.SaveRaftState(state); err != nil {
		t.Fatal(err)
	}
	loaded, err := stateStore.LoadRaftState()
	if err != nil {
		t.Fatal(err)
	}
	if loaded.CurrentTerm != 3 || loaded.VotedFor != 2 || len(loaded.Log) != 1 {
		t.Fatal("file state did not round-trip")
	}
	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := stateStore.LoadRaftState(); err == nil {
		t.Fatal("corrupt state was accepted")
	}
}
