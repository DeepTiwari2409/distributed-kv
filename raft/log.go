package raft

import (
	"fmt"
)

type RaftLog struct {
	entries []LogEntry
}

func NewRaftLog() *RaftLog {
	return &RaftLog{entries: make([]LogEntry, 0)}
}
func (l *RaftLog) Clone() *RaftLog {
	clone := NewRaftLog()
	clone.entries = make([]LogEntry, len(l.entries))
	for i, entry := range l.entries {
		clone.entries[i] = entry.Clone()
	}
	return clone
}
func (l *RaftLog) LastIndex() uint64 {
	return uint64(len(l.entries))
}
func (l *RaftLog) LastTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}
func (l *RaftLog) Len() int {
	return len(l.entries)
}
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
func (l *RaftLog) HasIndex(index uint64) bool {
	return index > 0 && index <= l.LastIndex()
}
func (l *RaftLog) Entry(index uint64) (LogEntry, error) {
	if !l.HasIndex(index) {
		return LogEntry{}, fmt.Errorf("%w: %d", ErrIndexOutOfRange, index)
	}
	return l.entries[index-1].Clone(), nil
}
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
func (l *RaftLog) TruncateFrom(index uint64) error {
	if index == 0 || index > l.LastIndex()+1 {
		return fmt.Errorf("%w: %d", ErrIndexOutOfRange, index)
	}
	l.entries = l.entries[:index-1]
	return nil
}
func (l *RaftLog) AppendEntries(entries []LogEntry) error {
	for _, entry := range entries {
		if err := l.Append(entry); err != nil {
			return err
		}
	}
	return nil
}
