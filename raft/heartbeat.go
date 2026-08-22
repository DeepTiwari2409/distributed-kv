package raft

func (rn *RaftNode) newHeartbeatTimer() Timer {
	return rn.clock.NewTimer(rn.heartbeatInterval, func() {
		rn.sendHeartbeat()
	})
}
func (rn *RaftNode) startHeartbeats() {
	if rn.node.State() == Leader {
		rn.initializeReplicationStateIfNeeded()
	}
	if rn.heartbeatTimer != nil {
		rn.heartbeatTimer.Stop()
	}
	rn.heartbeatTimer = rn.newHeartbeatTimer()
}

func (rn *RaftNode) initializeReplicationStateIfNeeded() {
	if len(rn.nextIndex) == len(rn.peers) {
		return
	}
	rn.initializeReplicationState()
}
func (rn *RaftNode) stopHeartbeats() {
	if rn.heartbeatTimer != nil {
		rn.heartbeatTimer.Stop()
		rn.heartbeatTimer = nil
	}
}
func (rn *RaftNode) sendHeartbeat() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.stopped || rn.node.State() != Leader {
		return
	}

	rn.sendHeartbeatUnlocked()
}
func (rn *RaftNode) sendHeartbeatUnlocked() {
	if rn.node.State() != Leader {
		return
	}
	peers := append([]NodeID(nil), rn.peers...)
	rn.mu.Unlock()
	for _, peerID := range peers {
		rn.ReplicateTo(peerID)
	}
	rn.mu.Lock()
	if rn.heartbeatTimer != nil {
		rn.heartbeatTimer.Reset(rn.heartbeatInterval)
	}
}
func (rn *RaftNode) buildAppendEntries(peer NodeID) (AppendEntriesRequest, bool) {
	if rn.node.State() != Leader {
		return AppendEntriesRequest{}, false
	}
	next, ok := rn.nextIndex[peer]
	if !ok {
		next = rn.node.Log().LastIndex() + 1
		rn.nextIndex[peer] = next
	}
	if next < 1 {
		next = 1
		rn.nextIndex[peer] = next
	}
	last := rn.node.Log().LastIndex()
	if next > last+1 {
		next = last + 1
		rn.nextIndex[peer] = next
	}
	prevIndex := next - 1
	prevTerm := uint64(0)
	if prevIndex > 0 {
		entry, err := rn.node.Log().Entry(prevIndex)
		if err != nil {
			return AppendEntriesRequest{}, false
		}
		prevTerm = entry.Term
	}
	entries := []LogEntry{}
	if next <= last {
		var err error
		entries, err = rn.node.Log().Entries(next, last)
		if err != nil {
			return AppendEntriesRequest{}, false
		}
	}
	rn.sentLastIndex[peer] = prevIndex + uint64(len(entries))
	return AppendEntriesRequest{
		Term:         rn.node.CurrentTerm(),
		LeaderID:     rn.node.ID(),
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rn.node.CommitIndex(),
	}, true
}
func (rn *RaftNode) ReplicateTo(peer NodeID) {
	rn.mu.Lock()
	if rn.stopped || rn.node.State() != Leader {
		rn.mu.Unlock()
		return
	}
	request, ok := rn.buildAppendEntries(peer)
	transport := rn.transport
	rn.mu.Unlock()
	if ok {
		transport.SendAppendEntries(request, peer)
	}
}
func (rn *RaftNode) Replicate() {
	rn.mu.Lock()
	if rn.stopped || rn.node.State() != Leader {
		rn.mu.Unlock()
		return
	}
	peers := append([]NodeID(nil), rn.peers...)
	rn.mu.Unlock()
	for _, peer := range peers {
		rn.ReplicateTo(peer)
	}
}
func (rn *RaftNode) ReceiveAppendEntries(request AppendEntriesRequest) {
	rn.mu.Lock()
	if rn.stopped {
		rn.mu.Unlock()
		return
	}
	response := rn.handleAppendEntries(request)
	rn.mu.Unlock()
	rn.transport.SendAppendEntriesResponse(response, request.LeaderID, rn.node.ID())
}
func (rn *RaftNode) ReceiveAppendEntriesResponse(response AppendEntriesResponse, from NodeID) {
	rn.mu.Lock()
	if rn.stopped {
		rn.mu.Unlock()
		return
	}
	previousNext := rn.nextIndex[from]
	rn.handleAppendEntriesResponse(response, from)
	retry := response.Term == rn.node.CurrentTerm() && !response.Success && rn.node.State() == Leader && rn.nextIndex[from] < previousNext
	rn.mu.Unlock()
	if retry {
		rn.ReplicateTo(from)
	}
}
func (rn *RaftNode) handleAppendEntries(request AppendEntriesRequest) AppendEntriesResponse {
	if request.Term < rn.node.CurrentTerm() {
		return AppendEntriesResponse{Term: rn.node.CurrentTerm(), Success: false}
	}
	if request.Term > rn.node.CurrentTerm() {
		rn.node.AdvanceTerm(request.Term)
		rn.node.SetState(Follower)
		rn.stopHeartbeats()
		rn.resetElectionTimer()
	}
	if rn.node.State() == Candidate {
		rn.node.SetState(Follower)
		rn.votesReceived = make(map[NodeID]bool)
		rn.electionTerm = 0
	}
	if rn.node.State() != Follower {
		rn.node.SetState(Follower)
	}
	if request.PrevLogIndex > rn.node.Log().LastIndex() {
		return AppendEntriesResponse{Term: rn.node.CurrentTerm(), Success: false}
	}
	if request.PrevLogIndex > 0 {
		entry, err := rn.node.Log().Entry(request.PrevLogIndex)
		if err != nil || entry.Term != request.PrevLogTerm {
			return AppendEntriesResponse{Term: rn.node.CurrentTerm(), Success: false}
		}
	}
	for offset, entry := range request.Entries {
		expected := request.PrevLogIndex + uint64(offset) + 1
		if entry.Index != expected || entry.Term == 0 || entry.Command.Validate() != nil {
			return AppendEntriesResponse{Term: rn.node.CurrentTerm(), Success: false}
		}
	}
	for offset, incoming := range request.Entries {
		index := request.PrevLogIndex + uint64(offset) + 1
		if index <= rn.node.Log().LastIndex() {
			existing, _ := rn.node.Log().Entry(index)
			if existing.Term == incoming.Term {
				continue
			}
			if index <= rn.node.CommitIndex() {
				return AppendEntriesResponse{Term: rn.node.CurrentTerm(), Success: false}
			}
			if err := rn.node.log.TruncateFrom(index); err != nil {
				return AppendEntriesResponse{Term: rn.node.CurrentTerm(), Success: false}
			}
			if err := rn.node.log.AppendEntries(request.Entries[offset:]); err != nil {
				return AppendEntriesResponse{Term: rn.node.CurrentTerm(), Success: false}
			}
			break
		}
		if err := rn.node.log.AppendEntries(request.Entries[offset:]); err != nil {
			return AppendEntriesResponse{Term: rn.node.CurrentTerm(), Success: false}
		}
		break
	}
	rn.resetElectionTimer()
	return AppendEntriesResponse{Term: rn.node.CurrentTerm(), Success: true}
}
func (rn *RaftNode) handleAppendEntriesResponse(response AppendEntriesResponse, from NodeID) {
	if response.Term > rn.node.CurrentTerm() {
		rn.node.AdvanceTerm(response.Term)
		rn.node.SetState(Follower)
		rn.stopHeartbeats()
		rn.resetElectionTimer()
		return
	}
	if response.Term < rn.node.CurrentTerm() {
		return
	}
	if rn.node.State() != Leader {
		return
	}
	if _, ok := rn.nextIndex[from]; !ok {
		return
	}
	if response.Success {
		last := rn.sentLastIndex[from]
		if last > rn.node.Log().LastIndex() {
			last = rn.node.Log().LastIndex()
		}
		if last > rn.matchIndex[from] {
			rn.matchIndex[from] = last
		}
		if rn.nextIndex[from] < rn.matchIndex[from]+1 {
			rn.nextIndex[from] = rn.matchIndex[from] + 1
		}
		return
	}
	if rn.nextIndex[from] > rn.matchIndex[from]+1 {
		rn.nextIndex[from]--
	}
}
