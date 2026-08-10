package raft

import (
	"fmt"
	"sync"
)

// Node holds the core Raft persistent and volatile state.
//
// This model is intentionally limited to state only and does not implement
// election or replication logic.
type Node struct {
	mu sync.RWMutex

	id          NodeID
	state       State
	currentTerm uint64
	votedFor    NodeID
	log         *RaftLog

	commitIndex uint64
	lastApplied uint64
}

// NewNode constructs a Raft node with the given identity.
func NewNode(id NodeID) (*Node, error) {
	if !id.Valid() {
		return nil, ErrInvalidNodeID
	}
	return &Node{
		id:          id,
		state:       Follower,
		currentTerm: 0,
		votedFor:    NoNode,
		log:         NewRaftLog(),
		commitIndex: 0,
		lastApplied: 0,
	}, nil
}

func (n *Node) ID() NodeID {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.id
}

func (n *Node) State() State {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

func (n *Node) CurrentTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentTerm
}

func (n *Node) VotedFor() NodeID {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.votedFor
}

func (n *Node) CommitIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.commitIndex
}

func (n *Node) LastApplied() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.lastApplied
}

func (n *Node) Log() *RaftLog {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.log.Clone()
}

// AdvanceTerm updates the current term. Term may only move forward.
func (n *Node) AdvanceTerm(term uint64) error {
	if term == 0 {
		return ErrInvalidTerm
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if term < n.currentTerm {
		return fmt.Errorf("%w: current=%d new=%d", ErrTermDecreased, n.currentTerm, term)
	}
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = NoNode
	}
	return nil
}

// SetVotedFor records the candidate this node voted for in the current term.
func (n *Node) SetVotedFor(candidate NodeID) error {
	if candidate != NoNode && !candidate.Valid() {
		return ErrInvalidNodeID
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.votedFor != NoNode && candidate != n.votedFor {
		return fmt.Errorf("%w: existing=%s candidate=%s", ErrVoteAlreadyCast, n.votedFor, candidate)
	}
	n.votedFor = candidate
	return nil
}

// SetState transitions the node state without protocol logic.
func (n *Node) SetState(state State) error {
	if !state.Valid() {
		return ErrInvalidState
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state = state
	return nil
}

// AdvanceCommitIndex moves commitIndex forward only.
func (n *Node) AdvanceCommitIndex(index uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if index < n.commitIndex {
		return fmt.Errorf("%w: current=%d new=%d", ErrCommitIndexDecreased, n.commitIndex, index)
	}
	if index > n.log.LastIndex() {
		return fmt.Errorf("%w: commit index %d exceeds last log index %d", ErrIndexOutOfRange, index, n.log.LastIndex())
	}
	n.commitIndex = index
	if n.lastApplied > n.commitIndex {
		return fmt.Errorf("%w: lastApplied=%d commitIndex=%d", ErrLastAppliedExceedsCommit, n.lastApplied, n.commitIndex)
	}
	return nil
}

// AdvanceLastApplied moves lastApplied up to commitIndex only.
func (n *Node) AdvanceLastApplied(index uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if index < n.lastApplied {
		return fmt.Errorf("%w: current=%d new=%d", ErrLastAppliedDecreased, n.lastApplied, index)
	}
	if index > n.commitIndex {
		return fmt.Errorf("%w: %d > %d", ErrLastAppliedExceedsCommit, index, n.commitIndex)
	}
	n.lastApplied = index
	return nil
}

// AppendEntry adds a new log entry to the node's log.
func (n *Node) AppendEntry(entry LogEntry) error {
	if err := entry.Command.Validate(); err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.log.Append(entry)
}
