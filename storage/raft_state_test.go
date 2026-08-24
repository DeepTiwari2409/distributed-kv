package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRaftStateStoreOperations(t *testing.T) {
	store, err := OpenRaftStateStore(filepath.Join(t.TempDir(), "state", "raft.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PersistCurrentTerm(3); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistVotedFor(2); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendRaftLog(RaftLogRecord{Index: 1, Term: 3, Type: 1, Key: "k", Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	state, err := store.LoadRaftState()
	if err != nil || state.CurrentTerm != 3 || state.VotedFor != 2 || len(state.Log) != 1 {
		t.Fatal("state operations did not persist correctly")
	}
	if err := store.TruncateRaftLog(1); err != nil {
		t.Fatal(err)
	}
	state, err = store.LoadRaftState()
	if err != nil || len(state.Log) != 0 {
		t.Fatal("log truncation did not persist")
	}
}

func TestRaftStateStoreRejectsInvalidLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.json")
	if err := os.WriteFile(path, []byte(`{"CurrentTerm":1,"Log":[{"Index":2,"Term":1,"Type":1}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := OpenRaftStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadRaftState(); err == nil {
		t.Fatal("invalid log index was accepted")
	}
}
