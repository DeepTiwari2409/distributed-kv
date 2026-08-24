package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

var ErrInvalidRaftState = errors.New("invalid raft state")

type RaftLogRecord struct {
	Index uint64
	Term  uint64
	Type  uint8
	Key   string
	Value []byte
}

type RaftState struct {
	CurrentTerm uint64
	VotedFor    uint64
	Log         []RaftLogRecord
}

type RaftStateStore interface {
	LoadRaftState() (RaftState, error)
	SaveRaftState(RaftState) error
	PersistCurrentTerm(uint64) error
	PersistVotedFor(uint64) error
	AppendRaftLog(RaftLogRecord) error
	TruncateRaftLog(uint64) error
	Sync() error
}

type FileRaftStateStore struct {
	mu   sync.Mutex
	path string
}

func OpenRaftStateStore(path string) (*FileRaftStateStore, error) {
	if path == "" {
		return nil, fmt.Errorf("raft state path is empty")
	}
	return &FileRaftStateStore{path: path}, nil
}

func (s *FileRaftStateStore) LoadRaftState() (RaftState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return RaftState{}, nil
	}
	if err != nil {
		return RaftState{}, fmt.Errorf("read raft state: %w", err)
	}
	var state RaftState
	if err := json.Unmarshal(data, &state); err != nil {
		return RaftState{}, fmt.Errorf("decode raft state: %w", err)
	}
	if err := validateRaftState(state); err != nil {
		return RaftState{}, err
	}
	return cloneRaftState(state), nil
}

func (s *FileRaftStateStore) SaveRaftState(state RaftState) error {
	if err := validateRaftState(state); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode raft state: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create raft state directory: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".raft-state-*")
	if err != nil {
		return fmt.Errorf("create raft state temporary file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write raft state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync raft state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close raft state: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("replace raft state: %w", err)
	}
	return nil
}

func (s *FileRaftStateStore) PersistCurrentTerm(term uint64) error {
	state, err := s.LoadRaftState()
	if err != nil {
		return err
	}
	state.CurrentTerm = term
	if state.VotedFor != 0 && term == 0 {
		state.VotedFor = 0
	}
	return s.SaveRaftState(state)
}

func (s *FileRaftStateStore) PersistVotedFor(votedFor uint64) error {
	state, err := s.LoadRaftState()
	if err != nil {
		return err
	}
	state.VotedFor = votedFor
	return s.SaveRaftState(state)
}

func (s *FileRaftStateStore) AppendRaftLog(record RaftLogRecord) error {
	state, err := s.LoadRaftState()
	if err != nil {
		return err
	}
	state.Log = append(state.Log, record)
	return s.SaveRaftState(state)
}

func (s *FileRaftStateStore) TruncateRaftLog(index uint64) error {
	state, err := s.LoadRaftState()
	if err != nil {
		return err
	}
	if index == 0 || index > uint64(len(state.Log))+1 {
		return fmt.Errorf("%w: log index %d", ErrInvalidRaftState, index)
	}
	state.Log = state.Log[:index-1]
	return s.SaveRaftState(state)
}

func (s *FileRaftStateStore) Sync() error {
	state, err := s.LoadRaftState()
	if err != nil {
		return err
	}
	return s.SaveRaftState(state)
}

func validateRaftState(state RaftState) error {
	if state.CurrentTerm == 0 && state.VotedFor != 0 {
		return fmt.Errorf("%w: vote without a term", ErrInvalidRaftState)
	}
	for index, entry := range state.Log {
		if entry.Index != uint64(index+1) || entry.Term == 0 || entry.Type == 0 {
			return fmt.Errorf("%w: log entry %d", ErrInvalidRaftState, index+1)
		}
	}
	return nil
}

func cloneRaftState(state RaftState) RaftState {
	clone := state
	clone.Log = make([]RaftLogRecord, len(state.Log))
	for index, entry := range state.Log {
		clone.Log[index] = entry
		clone.Log[index].Value = append([]byte(nil), entry.Value...)
	}
	return clone
}
