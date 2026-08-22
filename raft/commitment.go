package raft

func (rn *RaftNode) CommitIfPossible() error {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.advanceCommitIndex()
}

func (rn *RaftNode) advanceCommitIndex() error {
	if rn.node.State() != Leader {
		return nil
	}
	last := rn.node.Log().LastIndex()
	for index := last; index > rn.node.CommitIndex(); index-- {
		entry, err := rn.node.Log().Entry(index)
		if err != nil || entry.Term != rn.node.CurrentTerm() {
			continue
		}
		votes := 1
		for _, peer := range rn.peers {
			if peer != rn.node.ID() && rn.matchIndex[peer] >= index {
				votes++
			}
		}
		if votes*2 <= len(rn.peers)+1 {
			continue
		}
		if err := rn.node.AdvanceCommitIndex(index); err != nil {
			return err
		}
		return rn.applyCommittedEntries()
	}
	return nil
}

func (rn *RaftNode) advanceFollowerCommit(leaderCommit uint64) error {
	last := rn.node.Log().LastIndex()
	if leaderCommit > last {
		leaderCommit = last
	}
	if leaderCommit > rn.node.CommitIndex() {
		if err := rn.node.AdvanceCommitIndex(leaderCommit); err != nil {
			rn.applicationError = err
			return err
		}
	}
	return rn.applyCommittedEntries()
}
