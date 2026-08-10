package raft

import (
	"fmt"
)

// RaftLog is a one-based append-only log of Raft entries.
//
// Internally it uses a slice, but external consumers see one-based indexes.
// This design preserves Raft terminology while hiding internal offsets.
type RaftLog struct {
	entries []LogEntry
}

// NewRaftLog creates an empty Raft log.
func NewRaftLog() *RaftLog {
	return &RaftLog{entries: make([]LogEntry, 0)}
}

// Clone returns a deep copy of the Raft log.
func (l *RaftLog) Clone() *RaftLog {
	clone := NewRaftLog()
	clone.entries = make([]LogEntry, len(l.entries))
	for i, entry := range l.entries {
		clone.entries[i] = entry.Clone()
	}
	return clone
}

// LastIndex returns the last logical index in the log.
//
// An empty log returns 0.
func (l *RaftLog) LastIndex() uint64 {
	return uint64(len(l.entries))
}

// LastTerm returns the term of the last entry, or 0 when the log is empty.
func (l *RaftLog) LastTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// Len returns the number of entries in the log.
func (l *RaftLog) Len() int {
	return len(l.entries)
}

// Append adds a new entry to the end of the log.
//
// The entry's Index must be LastIndex()+1 and Term must be positive.
func (l *RaftLog) Append(entry LogEntry) error {
	if entry.Index != l.LastIndex()+1 {
		return fmt.Errorf("%w: expected index %d, got %d", ErrInvalidLogEntry, l.LastIndex()+1, entry.Index)
	}
	if entry.Term == 0 {
		return fmt.Errorf("%w: term must be positive", ErrInvalidTerm)
	}
	if err := entry.Command.Validate(); err != nil {
		return err
	}
	l.entries = append(l.entries, entry.Clone())
	return nil
}

// HasIndex reports whether the log contains the given one-based index.
func (l *RaftLog) HasIndex(index uint64) bool {
	return index > 0 && index <= l.LastIndex()
}

// Entry returns the entry at the one-based index.
func (l *RaftLog) Entry(index uint64) (LogEntry, error) {
	if !l.HasIndex(index) {
		return LogEntry{}, fmt.Errorf("%w: %d", ErrIndexOutOfRange, index)
	}
	return l.entries[index-1].Clone(), nil
}

// Entries returns a defensive copy of entries in [from, to] inclusive.
func (l *RaftLog) Entries(from, to uint64) ([]LogEntry, error) {
	if from == 0 || to == 0 || from > to {
		return nil, fmt.Errorf("%w: from=%d to=%d", ErrInvalidRange, from, to)
	}
	if to > l.LastIndex() {
		return nil, fmt.Errorf("%w: to=%d last=%d", ErrIndexOutOfRange, to, l.LastIndex())
	}
	start := from - 1
	end := to
	result := make([]LogEntry, end-start)
	for i := start; i < end; i++ {
		result[i-start] = l.entries[i].Clone()
	}
	return result, nil
}
