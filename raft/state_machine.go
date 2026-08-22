package raft

import "fmt"

type StateMachine interface {
	Apply(command Command) error
}

func (rn *RaftNode) SetStateMachine(stateMachine StateMachine) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.stateMachine = stateMachine
}

func (rn *RaftNode) ApplicationError() error {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.applicationError
}

func (rn *RaftNode) ApplyCommitted() error {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.applyCommittedEntries()
}

func (rn *RaftNode) applyCommittedEntries() error {
	if rn.stateMachine == nil {
		return nil
	}
	for rn.node.LastApplied() < rn.node.CommitIndex() {
		index := rn.node.LastApplied() + 1
		entry, err := rn.node.Log().Entry(index)
		if err != nil {
			rn.applicationError = fmt.Errorf("load committed entry %d: %w", index, err)
			return rn.applicationError
		}
		if err := rn.stateMachine.Apply(entry.Command); err != nil {
			rn.applicationError = fmt.Errorf("apply committed entry %d: %w", index, err)
			return rn.applicationError
		}
		if err := rn.node.AdvanceLastApplied(index); err != nil {
			rn.applicationError = err
			return err
		}
	}
	rn.applicationError = nil
	return nil
}
