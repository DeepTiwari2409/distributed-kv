package raft

import (
	"errors"
	"fmt"
)

type NodeID uint64

const NoNode NodeID = 0

func (id NodeID) Valid() bool {
	return id != NoNode
}

func (id NodeID) String() string {
	if id == NoNode {
		return "NoNode"
	}
	return fmt.Sprintf("%d", uint64(id))
}

type State uint8

const (
	Follower State = iota + 1
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

func (s State) Valid() bool {
	switch s {
	case Follower, Candidate, Leader:
		return true
	default:
		return false
	}
}

type CommandType uint8

const (
	CommandPut CommandType = iota + 1
	CommandDelete
)

func (ct CommandType) String() string {
	switch ct {
	case CommandPut:
		return "Put"
	case CommandDelete:
		return "Delete"
	default:
		return "Unknown"
	}
}

type Command struct {
	Type  CommandType
	Key   string
	Value []byte
}

func NewPutCommand(key string, value []byte) Command {
	return Command{Type: CommandPut, Key: key, Value: cloneBytes(value)}
}
func NewDeleteCommand(key string) Command {
	return Command{Type: CommandDelete, Key: key}
}

func (c Command) Validate() error {
	switch c.Type {
	case CommandPut, CommandDelete:
	default:
		return fmt.Errorf("%w: %d", ErrInvalidCommandType, c.Type)
	}
	return nil
}

func (c Command) copy() Command {
	return Command{Type: c.Type, Key: c.Key, Value: cloneBytes(c.Value)}
}

type LogEntry struct {
	Index   uint64
	Term    uint64
	Command Command
}

func (e LogEntry) Clone() LogEntry {
	entry := e
	entry.Command = e.Command.copy()
	return entry
}

var (
	ErrInvalidNodeID            = errors.New("invalid node id")
	ErrInvalidState             = errors.New("invalid state")
	ErrInvalidCommandType       = errors.New("invalid command type")
	ErrInvalidLogEntry          = errors.New("invalid log entry")
	ErrInvalidTerm              = errors.New("invalid term")
	ErrIndexOutOfRange          = errors.New("index out of range")
	ErrInvalidRange             = errors.New("invalid range")
	ErrTermDecreased            = errors.New("term cannot decrease")
	ErrCommitIndexDecreased     = errors.New("commit index cannot decrease")
	ErrLastAppliedDecreased     = errors.New("lastApplied cannot decrease")
	ErrLastAppliedExceedsCommit = errors.New("lastApplied cannot exceed commitIndex")
	ErrVoteAlreadyCast          = errors.New("vote already cast for a different candidate in current term")
)

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	copyValue := make([]byte, len(value))
	copy(copyValue, value)
	return copyValue
}
