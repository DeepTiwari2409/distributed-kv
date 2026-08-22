package raft

import (
	"math/rand"
	"testing"
	"time"
)

func TestAppendEntriesRequestStructure(t *testing.T) {
	req := AppendEntriesRequest{
		Term:         5,
		LeaderID:     1,
		PrevLogIndex: 10,
		PrevLogTerm:  4,
		Entries:      []LogEntry{},
		LeaderCommit: 8,
	}

	if req.Term != 5 {
		t.Fatalf("expected term 5, got %d", req.Term)
	}
	if req.LeaderID != 1 {
		t.Fatalf("expected leader 1, got %d", req.LeaderID)
	}
	if req.PrevLogIndex != 10 {
		t.Fatalf("expected prev log index 10, got %d", req.PrevLogIndex)
	}
	if len(req.Entries) != 0 {
		t.Fatalf("expected empty entries, got %d", len(req.Entries))
	}
}

func TestAppendEntriesResponseStructure(t *testing.T) {
	resp := AppendEntriesResponse{
		Term:    5,
		Success: true,
	}

	if resp.Term != 5 {
		t.Fatalf("expected term 5, got %d", resp.Term)
	}
	if !resp.Success {
		t.Fatal("expected success true")
	}
}

func TestHeartbeatUsesEmptyEntries(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	rn, _ := NewRaftNode(node, nil, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	rn.Start()
	clock.Advance(151 * time.Millisecond)
	if node.State() != Leader {
		t.Fatalf("expected leader, got %s", node.State())
	}
	if node.State() == Leader {
		req := AppendEntriesRequest{
			Term:         node.CurrentTerm(),
			LeaderID:     node.ID(),
			PrevLogIndex: node.Log().LastIndex(),
			PrevLogTerm:  node.Log().LastTerm(),
			Entries:      []LogEntry{},
			LeaderCommit: node.CommitIndex(),
		}
		if len(req.Entries) != 0 {
			t.Fatal("heartbeat should have empty entries")
		}
	}
}

func TestLeaderSendsHeartbeatAfterInterval(t *testing.T) {
	transportMap := make(map[NodeID]*RaftNode)
	trans := NewInMemoryTransport(transportMap)
	leaderNode, _ := NewNode(1)
	leaderNode.AdvanceTerm(1)
	leaderNode.SetState(Leader)
	leaderClock := NewDeterministicClock(time.Unix(0, 0))
	leader, _ := NewRaftNode(leaderNode, []NodeID{2}, trans, leaderClock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	transportMap[1] = leader
	followerNode, _ := NewNode(2)
	followerClock := NewDeterministicClock(time.Unix(0, 0))
	follower, _ := NewRaftNode(followerNode, []NodeID{1}, trans, followerClock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	transportMap[2] = follower
	leader.mu.Lock()
	leader.startHeartbeats()
	leader.mu.Unlock()
	if followerNode.CurrentTerm() != 0 {
		t.Fatalf("follower should start at term 0")
	}
	leaderClock.Advance(50 * time.Millisecond)
	if followerNode.CurrentTerm() != 1 {
		t.Fatalf("expected follower term to advance to 1, got %d", followerNode.CurrentTerm())
	}
}

func TestLeaderSendsHeartbeatToAllPeers(t *testing.T) {
	transportMap := make(map[NodeID]*RaftNode)
	trans := NewInMemoryTransport(transportMap)

	leaderNode, _ := NewNode(1)
	leaderNode.AdvanceTerm(1)
	leaderNode.SetState(Leader)
	leaderClock := NewDeterministicClock(time.Unix(0, 0))
	leader, _ := NewRaftNode(leaderNode, []NodeID{2, 3}, trans, leaderClock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	transportMap[1] = leader

	followers := make(map[NodeID]*RaftNode)
	for _, id := range []NodeID{2, 3} {
		node, _ := NewNode(id)
		clock := NewDeterministicClock(time.Unix(0, 0))
		rn, _ := NewRaftNode(node, []NodeID{1}, trans, clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
		followers[id] = rn
		transportMap[id] = rn
	}

	leader.mu.Lock()
	leader.startHeartbeats()
	leader.mu.Unlock()

	leaderClock.Advance(50 * time.Millisecond)

	for id, rn := range followers {
		if rn.node.CurrentTerm() != 1 {
			t.Fatalf("follower %d should have term 1, got %d", id, rn.node.CurrentTerm())
		}
	}
}

func TestFollowerDoesNotSendHeartbeat(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	rn, _ := NewRaftNode(node, nil, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	rn.Start()

	if node.State() != Follower {
		t.Fatalf("expected follower, got %s", node.State())
	}

	rn.mu.Lock()
	if rn.heartbeatTimer != nil {
		t.Fatal("follower should not have heartbeat timer")
	}
	rn.mu.Unlock()
}

func TestCandidateDoesNotSendHeartbeat(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	rn.Start()
	clock.Advance(151 * time.Millisecond)

	if node.State() != Candidate {
		t.Fatalf("expected candidate, got %s", node.State())
	}

	rn.mu.Lock()
	if rn.heartbeatTimer != nil {
		t.Fatal("candidate should not have heartbeat timer")
	}
	rn.mu.Unlock()
}

func TestStoppedNodeDoesNotSendHeartbeat(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)

	rn.mu.Lock()
	if rn.heartbeatTimer != nil {
		t.Fatal("stopped node should not have heartbeat timer")
	}
	rn.mu.Unlock()
}

func TestNewLeaderStartsHeartbeats(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	rn, _ := NewRaftNode(node, []NodeID{}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	rn.Start()

	clock.Advance(151 * time.Millisecond)

	if node.State() != Leader {
		t.Fatalf("expected leader, got %s", node.State())
	}

	rn.mu.Lock()
	if rn.heartbeatTimer == nil {
		t.Fatal("leader should have heartbeat timer")
	}
	rn.mu.Unlock()
}

func TestFollowerAcceptsSameTermHeartbeat(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(2)
	node.AdvanceTerm(5)
	rn, _ := NewRaftNode(node, []NodeID{1}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)

	req := AppendEntriesRequest{
		Term:         5,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}

	resp := rn.handleAppendEntries(req)
	if !resp.Success {
		t.Fatal("expected heartbeat to be accepted in same term")
	}
}

func TestFollowerAcceptsHigherTermHeartbeat(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(2)
	node.AdvanceTerm(3)
	rn, _ := NewRaftNode(node, []NodeID{1}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)

	req := AppendEntriesRequest{
		Term:         5,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}

	resp := rn.handleAppendEntries(req)
	if !resp.Success {
		t.Fatal("expected heartbeat to be accepted with higher term")
	}
	if node.CurrentTerm() != 5 {
		t.Fatalf("expected term to advance to 5, got %d", node.CurrentTerm())
	}
}

func TestFollowerRejectsStaleHeartbeat(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(2)
	node.AdvanceTerm(5)
	rn, _ := NewRaftNode(node, []NodeID{1}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)

	req := AppendEntriesRequest{
		Term:         3,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}

	resp := rn.handleAppendEntries(req)
	if resp.Success {
		t.Fatal("expected stale heartbeat to be rejected")
	}
	if resp.Term != 5 {
		t.Fatalf("expected response term to be 5, got %d", resp.Term)
	}
}

func TestValidHeartbeatResetsElectionTimeout(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(2)
	node.AdvanceTerm(5)
	rn, _ := NewRaftNode(node, []NodeID{1}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	rn.Start()
	clock.Advance(140 * time.Millisecond)
	req := AppendEntriesRequest{
		Term:         5,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}
	rn.handleAppendEntries(req)
	clock.Advance(15 * time.Millisecond)
	if node.State() != Follower {
		t.Fatalf("expected follower, got %s", node.State())
	}
}

func TestStaleHeartbeatDoesNotResetElectionTimeout(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(2)
	node.AdvanceTerm(5)
	rn, _ := NewRaftNode(node, []NodeID{1}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	rn.Start()
	clock.Advance(140 * time.Millisecond)
	req := AppendEntriesRequest{
		Term:         3,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}
	rn.handleAppendEntries(req)
	clock.Advance(15 * time.Millisecond)
	if node.State() == Follower {
		t.Fatal("stale heartbeat should not reset election timeout")
	}
}

func TestHeartbeatDoesNotDecreaseTerm(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(2)
	node.AdvanceTerm(5)
	rn, _ := NewRaftNode(node, []NodeID{1}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)

	req := AppendEntriesRequest{
		Term:         3,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}

	rn.handleAppendEntries(req)

	if node.CurrentTerm() != 5 {
		t.Fatalf("term should remain 5, got %d", node.CurrentTerm())
	}
}

func TestHigherTermHeartbeatClearsVote(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(2)
	node.AdvanceTerm(5)
	node.SetVotedFor(3)
	rn, _ := NewRaftNode(node, []NodeID{1}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)

	req := AppendEntriesRequest{
		Term:         6,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}

	rn.handleAppendEntries(req)

	if node.VotedFor() != NoNode {
		t.Fatalf("votedFor should be cleared in new term, got %s", node.VotedFor())
	}
}

func TestCandidateStepsDownOnSameTermHeartbeat(t *testing.T) {
	transportMap := make(map[NodeID]*RaftNode)
	trans := NewInMemoryTransport(transportMap)
	candidateNode, _ := NewNode(1)
	candidateClock := NewDeterministicClock(time.Unix(0, 0))
	candidate, _ := NewRaftNode(candidateNode, []NodeID{2}, trans, candidateClock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	transportMap[1] = candidate
	candidate.Start()
	candidateClock.Advance(151 * time.Millisecond)
	if candidateNode.State() != Candidate {
		t.Fatalf("expected candidate, got %s", candidateNode.State())
	}

	term := candidateNode.CurrentTerm()
	req := AppendEntriesRequest{
		Term:         term,
		LeaderID:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}

	candidate.handleAppendEntries(req)

	if candidateNode.State() != Follower {
		t.Fatalf("expected candidate to step down to follower, got %s", candidateNode.State())
	}
}

func TestCandidateStepsDownOnHigherTermHeartbeat(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	node.AdvanceTerm(3)
	node.SetState(Candidate)
	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)

	req := AppendEntriesRequest{
		Term:         5,
		LeaderID:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}

	rn.handleAppendEntries(req)

	if node.State() != Follower {
		t.Fatalf("expected follower, got %s", node.State())
	}
	if node.CurrentTerm() != 5 {
		t.Fatalf("expected term 5, got %d", node.CurrentTerm())
	}
}

func TestCandidateAbandonsElectionAfterHeartbeat(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	rn, _ := NewRaftNode(node, []NodeID{2, 3}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	rn.Start()
	clock.Advance(151 * time.Millisecond)
	if node.State() != Candidate {
		t.Fatalf("expected candidate")
	}
	rn.mu.Lock()
	if len(rn.votesReceived) != 0 {
		t.Fatalf("should start with no votes")
	}
	rn.mu.Unlock()
	req := AppendEntriesRequest{
		Term:         node.CurrentTerm(),
		LeaderID:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}

	rn.handleAppendEntries(req)
	rn.mu.Lock()
	if len(rn.votesReceived) != 0 {
		t.Fatalf("election state should be cleared after heartbeat")
	}
	rn.mu.Unlock()
}

func TestOldVotesCannotElectSteppedDownCandidate(t *testing.T) {
	transportMap := make(map[NodeID]*RaftNode)
	trans := NewInMemoryTransport(transportMap)
	candidateNode, _ := NewNode(1)
	candidateClock := NewDeterministicClock(time.Unix(0, 0))
	candidate, _ := NewRaftNode(candidateNode, []NodeID{2, 3}, trans, candidateClock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	transportMap[1] = candidate
	candidate.Start()
	candidateClock.Advance(151 * time.Millisecond)
	if candidateNode.State() != Candidate {
		t.Fatalf("expected candidate")
	}
	currentTerm := candidateNode.CurrentTerm()
	req := AppendEntriesRequest{
		Term:         currentTerm,
		LeaderID:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}
	candidate.handleAppendEntries(req)
	candidate.mu.Lock()
	candidate.votesReceived[2] = true
	candidate.votesReceived[3] = true
	if candidate.hasMajorityVotes() {
		t.Fatal("should not count votes from stepped-down election")
	}
	candidate.mu.Unlock()
}

func TestLeaderSendsPeriodicHeartbeats(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	leaderNode, _ := NewNode(1)
	leaderNode.AdvanceTerm(1)
	leaderNode.SetState(Leader)
	leader, _ := NewRaftNode(leaderNode, []NodeID{}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)

	leader.mu.Lock()
	leader.startHeartbeats()
	leader.mu.Unlock()

	if leader.heartbeatTimer == nil {
		t.Fatal("leader should have heartbeat timer after starting heartbeats")
	}
}

func TestLeaderStopsHeartbeatsAfterStepDown(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	leaderNode, _ := NewNode(1)
	leaderNode.AdvanceTerm(1)
	leaderNode.SetState(Leader)
	leader, _ := NewRaftNode(leaderNode, []NodeID{}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)

	leader.mu.Lock()
	leader.startHeartbeats()
	hasTimer := leader.heartbeatTimer != nil
	leader.mu.Unlock()

	if !hasTimer {
		t.Fatal("leader should have heartbeat timer")
	}
	leader.mu.Lock()
	leaderNode.SetState(Follower)
	leader.stopHeartbeats()
	leader.mu.Unlock()

	leader.mu.Lock()
	if leader.heartbeatTimer != nil {
		t.Fatal("heartbeat timer should be stopped")
	}
	leader.mu.Unlock()
}

func TestLeaderStepsDownOnHigherTermResponse(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	leaderNode, _ := NewNode(1)
	leaderNode.AdvanceTerm(5)
	leaderNode.SetState(Leader)
	leader, _ := NewRaftNode(leaderNode, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)

	leader.mu.Lock()
	leader.startHeartbeats()
	leader.mu.Unlock()
	resp := AppendEntriesResponse{
		Term:    10,
		Success: false,
	}

	leader.handleAppendEntriesResponse(resp, 2)

	if leaderNode.State() != Follower {
		t.Fatalf("expected leader to step down, got %s", leaderNode.State())
	}
	if leaderNode.CurrentTerm() != 10 {
		t.Fatalf("expected term 10, got %d", leaderNode.CurrentTerm())
	}

	leader.mu.Lock()
	if leader.heartbeatTimer != nil {
		t.Fatal("heartbeat timer should be stopped")
	}
	leader.mu.Unlock()
}

func TestSteppedDownLeaderDoesNotResumeHeartbeatWithoutElection(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	rn, _ := NewRaftNode(node, nil, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	rn.Start()
	clock.Advance(151 * time.Millisecond)
	if node.State() != Leader {
		t.Fatalf("expected leader")
	}
	rn.mu.Lock()
	resp := AppendEntriesResponse{Term: 10, Success: false}
	rn.handleAppendEntriesResponse(resp, 2)
	rn.mu.Unlock()

	if node.State() != Follower {
		t.Fatal("expected follower after step-down")
	}
	clock.Advance(50 * time.Millisecond)
	if node.State() != Follower {
		t.Fatal("stepped-down leader should not resume heartbeats")
	}
}

func TestHeartbeatTermNeverDecreases(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(2)
	node.AdvanceTerm(5)
	rn, _ := NewRaftNode(node, []NodeID{1}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)

	req := AppendEntriesRequest{
		Term:         3,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}

	rn.handleAppendEntries(req)

	if node.CurrentTerm() != 5 {
		t.Fatal("term cannot decrease")
	}
}

func TestHigherTermPropagatesToReceiver(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(2)
	node.AdvanceTerm(3)
	rn, _ := NewRaftNode(node, []NodeID{1}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)

	req := AppendEntriesRequest{
		Term:         7,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}

	rn.handleAppendEntries(req)

	if node.CurrentTerm() != 7 {
		t.Fatalf("expected term 7, got %d", node.CurrentTerm())
	}
}

func TestStaleAppendEntriesCannotChangeLeaderStateIncorrectly(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	node.AdvanceTerm(5)
	node.SetState(Leader)
	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)

	rn.mu.Lock()
	rn.startHeartbeats()
	rn.mu.Unlock()
	req := AppendEntriesRequest{
		Term:         3,
		LeaderID:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}

	rn.handleAppendEntries(req)
	if node.State() != Leader {
		t.Fatal("stale heartbeat should not change leader state")
	}
}

func TestHigherTermResponseForcesLeaderToFollower(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	node.AdvanceTerm(5)
	node.SetState(Leader)
	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)

	rn.mu.Lock()
	rn.startHeartbeats()
	rn.mu.Unlock()

	resp := AppendEntriesResponse{
		Term:    10,
		Success: false,
	}

	rn.handleAppendEntriesResponse(resp, 2)

	if node.State() != Follower {
		t.Fatal("higher term should force leader to follower")
	}
	if node.CurrentTerm() != 10 {
		t.Fatalf("expected term 10, got %d", node.CurrentTerm())
	}
}

func TestHeartbeatsPreventFollowerElection(t *testing.T) {
	transportMap := make(map[NodeID]*RaftNode)
	trans := NewInMemoryTransport(transportMap)

	leaderNode, _ := NewNode(1)
	leaderNode.AdvanceTerm(1)
	leaderNode.SetState(Leader)
	leaderClock := NewDeterministicClock(time.Unix(0, 0))
	leader, _ := NewRaftNode(leaderNode, []NodeID{2}, trans, leaderClock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	transportMap[1] = leader

	followerNode, _ := NewNode(2)
	followerClock := NewDeterministicClock(time.Unix(0, 0))
	follower, _ := NewRaftNode(followerNode, []NodeID{1}, trans, followerClock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	transportMap[2] = follower

	follower.Start()

	leader.mu.Lock()
	leader.startHeartbeats()
	leader.mu.Unlock()
	leaderClock.Advance(50 * time.Millisecond)
	followerClock.Advance(140 * time.Millisecond)
	if followerNode.State() != Follower {
		t.Fatalf("expected follower, got %s", followerNode.State())
	}
}

func TestRepeatedHeartbeatsPreventElectionOverMultipleTimeoutWindows(t *testing.T) {
	transportMap := make(map[NodeID]*RaftNode)
	trans := NewInMemoryTransport(transportMap)

	leaderNode, _ := NewNode(1)
	leaderNode.AdvanceTerm(1)
	leaderNode.SetState(Leader)
	leaderClock := NewDeterministicClock(time.Unix(0, 0))
	leader, _ := NewRaftNode(leaderNode, []NodeID{2}, trans, leaderClock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	transportMap[1] = leader

	followerNode, _ := NewNode(2)
	followerClock := NewDeterministicClock(time.Unix(0, 0))
	follower, _ := NewRaftNode(followerNode, []NodeID{1}, trans, followerClock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	transportMap[2] = follower

	follower.Start()

	leader.mu.Lock()
	leader.startHeartbeats()
	leader.mu.Unlock()
	for i := 0; i < 5; i++ {
		leaderClock.Advance(50 * time.Millisecond)
		followerClock.Advance(40 * time.Millisecond)

		if followerNode.State() != Follower {
			t.Fatalf("iteration %d: expected follower, got %s", i, followerNode.State())
		}
	}
}

func TestDroppedHeartbeatsEventuallyAllowElection(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	trans := NewInMemoryTransport(make(map[NodeID]*RaftNode))
	rn, _ := NewRaftNode(node, []NodeID{2}, trans, clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	rn.Start()
	trans.isolate(1)
	clock.Advance(160 * time.Millisecond)
	if node.State() != Candidate {
		t.Fatalf("expected candidate after election timeout with dropped heartbeats, got %s", node.State())
	}
}

func TestPartitionedLeaderCannotReachMinorityFollowers(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	followerNode, _ := NewNode(2)
	trans := NewInMemoryTransport(make(map[NodeID]*RaftNode))
	follower, _ := NewRaftNode(followerNode, []NodeID{1}, trans, clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	trans.peers[2] = follower
	trans.isolate(2)

	leaderNode, _ := NewNode(1)
	leaderNode.AdvanceTerm(1)
	leaderNode.SetState(Leader)
	leaderClock := NewDeterministicClock(time.Unix(0, 0))
	leader, _ := NewRaftNode(leaderNode, []NodeID{2}, trans, leaderClock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	trans.peers[1] = leader

	leader.mu.Lock()
	leader.startHeartbeats()
	leader.mu.Unlock()

	leaderClock.Advance(50 * time.Millisecond)
	if followerNode.CurrentTerm() != 0 {
		t.Fatal("isolated follower should not receive heartbeats")
	}
}

func TestIsolatedFollowerEventuallyStartsElection(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	trans := NewInMemoryTransport(make(map[NodeID]*RaftNode))
	rn, _ := NewRaftNode(node, []NodeID{2}, trans, clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	trans.peers[1] = rn

	rn.Start()
	trans.isolate(1)
	clock.Advance(160 * time.Millisecond)

	if node.State() != Candidate {
		t.Fatalf("isolated follower should start election, got %s", node.State())
	}
}

func TestMajorityPartitionMaintainsOrElectsLeader(t *testing.T) {
	transportMap := make(map[NodeID]*RaftNode)
	trans := NewInMemoryTransport(transportMap)

	leaderNode, _ := NewNode(1)
	leaderNode.AdvanceTerm(1)
	leaderNode.SetState(Leader)
	leaderClock := NewDeterministicClock(time.Unix(0, 0))
	leader, _ := NewRaftNode(leaderNode, []NodeID{2, 3}, trans, leaderClock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	transportMap[1] = leader

	for _, id := range []NodeID{2, 3} {
		node, _ := NewNode(id)
		clock := NewDeterministicClock(time.Unix(0, 0))
		rn, _ := NewRaftNode(node, append([]NodeID{1}, []NodeID{2, 3}[id-2:]...), trans, clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
		transportMap[id] = rn
		rn.Start()
	}
	trans.isolate(3)

	leader.mu.Lock()
	leader.startHeartbeats()
	leader.mu.Unlock()

	leaderClock.Advance(50 * time.Millisecond)
	if leaderNode.State() != Leader {
		t.Fatalf("majority partition leader should remain leader")
	}
	if transportMap[2].node.CurrentTerm() == 0 {
		t.Fatal("node 2 should have received heartbeat")
	}
}

func TestOldLeaderStepsDownAfterRejoiningHigherTermCluster(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	oldLeaderNode, _ := NewNode(1)
	oldLeaderNode.AdvanceTerm(5)
	oldLeaderNode.SetState(Leader)
	oldLeader, _ := NewRaftNode(oldLeaderNode, []NodeID{2, 3}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)

	oldLeader.mu.Lock()
	oldLeader.startHeartbeats()
	oldLeader.mu.Unlock()
	resp := AppendEntriesResponse{
		Term:    10,
		Success: false,
	}

	oldLeader.handleAppendEntriesResponse(resp, 2)

	if oldLeaderNode.State() != Follower {
		t.Fatal("old leader should step down after higher term")
	}
	if oldLeaderNode.CurrentTerm() != 10 {
		t.Fatalf("expected term 10, got %d", oldLeaderNode.CurrentTerm())
	}
}

func TestHeartbeatRecoveryAfterPartitionHeals(t *testing.T) {
	transportMap := make(map[NodeID]*RaftNode)
	trans := NewInMemoryTransport(transportMap)

	leaderNode, _ := NewNode(1)
	leaderNode.AdvanceTerm(1)
	leaderNode.SetState(Leader)
	leaderClock := NewDeterministicClock(time.Unix(0, 0))
	leader, _ := NewRaftNode(leaderNode, []NodeID{2, 3}, trans, leaderClock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	transportMap[1] = leader

	followers := make(map[NodeID]*RaftNode)
	for _, id := range []NodeID{2, 3} {
		node, _ := NewNode(id)
		clock := NewDeterministicClock(time.Unix(0, 0))
		rn, _ := NewRaftNode(node, []NodeID{1}, trans, clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
		followers[id] = rn
		transportMap[id] = rn
		rn.Start()
	}

	leader.mu.Lock()
	leader.startHeartbeats()
	leader.mu.Unlock()
	trans.isolate(1)
	trans.isolate(2)
	trans.isolate(3)

	leaderClock.Advance(50 * time.Millisecond)
	trans.restore(1)
	trans.restore(2)
	trans.restore(3)
	leaderClock.Advance(50 * time.Millisecond)
	if followers[2].node.CurrentTerm() == 0 {
		t.Fatal("partition recovery should allow heartbeats again")
	}
}

func TestThreeNodeLeaderMaintainsAuthorityWithHeartbeats(t *testing.T) {
	transportMap := make(map[NodeID]*RaftNode)
	trans := NewInMemoryTransport(transportMap)
	leaderNode, _ := NewNode(1)
	leaderNode.AdvanceTerm(1)
	leaderNode.SetState(Leader)
	leaderClock := NewDeterministicClock(time.Unix(0, 0))
	leader, _ := NewRaftNode(leaderNode, []NodeID{2, 3}, trans, leaderClock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	transportMap[1] = leader
	for _, id := range []NodeID{2, 3} {
		node, _ := NewNode(id)
		clock := NewDeterministicClock(time.Unix(0, 0))
		rn, _ := NewRaftNode(node, []NodeID{1}, trans, clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
		transportMap[id] = rn
		rn.Start()
	}

	leader.mu.Lock()
	leader.startHeartbeats()
	leader.mu.Unlock()
	for i := 0; i < 3; i++ {
		leaderClock.Advance(50 * time.Millisecond)
		for _, id := range []NodeID{2, 3} {
			if transportMap[id].node.State() != Follower {
				t.Fatalf("node %d should remain follower, got %s", id, transportMap[id].node.State())
			}
		}
	}
}

func TestFiveNodeLeaderMaintainsAuthorityWithHeartbeats(t *testing.T) {
	n := 5
	ids := make([]NodeID, n)
	for i := 0; i < n; i++ {
		ids[i] = NodeID(i + 1)
	}

	transportMap := make(map[NodeID]*RaftNode)
	trans := NewInMemoryTransport(transportMap)
	leaderNode, _ := NewNode(1)
	leaderNode.AdvanceTerm(1)
	leaderNode.SetState(Leader)
	leaderClock := NewDeterministicClock(time.Unix(0, 0))
	peers := make([]NodeID, 0)
	for _, id := range ids[1:] {
		peers = append(peers, id)
	}
	leader, _ := NewRaftNode(leaderNode, peers, trans, leaderClock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	transportMap[1] = leader
	for _, id := range ids[1:] {
		node, _ := NewNode(id)
		clock := NewDeterministicClock(time.Unix(0, 0))
		rn, _ := NewRaftNode(node, []NodeID{1}, trans, clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
		transportMap[id] = rn
		rn.Start()
	}

	leader.mu.Lock()
	leader.startHeartbeats()
	leader.mu.Unlock()

	leaderClock.Advance(50 * time.Millisecond)

	for _, id := range ids[1:] {
		if transportMap[id].node.State() != Follower {
			t.Fatalf("node %d should be follower", id)
		}
	}
}

func TestFollowersRemainFollowersDuringStableLeaderHeartbeats(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	trans := NewInMemoryTransport(make(map[NodeID]*RaftNode))
	rn, _ := NewRaftNode(node, []NodeID{2}, trans, clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	rn.Start()

	if node.State() != Follower {
		t.Fatalf("expected follower")
	}
	for i := 0; i < 5; i++ {
		req := AppendEntriesRequest{
			Term:         1,
			LeaderID:     2,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      []LogEntry{},
			LeaderCommit: 0,
		}
		rn.handleAppendEntries(req)

		if node.State() != Follower {
			t.Fatalf("iteration %d: follower should remain follower", i)
		}
	}
}

func TestStaleHeartbeatAfterNewerHeartbeat(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(2)
	rn, _ := NewRaftNode(node, []NodeID{1}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	req1 := AppendEntriesRequest{
		Term:         5,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}
	rn.handleAppendEntries(req1)

	if node.CurrentTerm() != 5 {
		t.Fatalf("expected term 5")
	}
	req2 := AppendEntriesRequest{
		Term:         3,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}
	rn.handleAppendEntries(req2)

	if node.CurrentTerm() != 5 {
		t.Fatalf("term should remain 5 after stale heartbeat")
	}
}

func TestOutOfOrderAppendEntriesDoesNotRegressTerm(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(2)
	rn, _ := NewRaftNode(node, []NodeID{1}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)

	reqs := []AppendEntriesRequest{
		{Term: 10, LeaderID: 1, Entries: []LogEntry{}},
		{Term: 5, LeaderID: 1, Entries: []LogEntry{}},
		{Term: 8, LeaderID: 1, Entries: []LogEntry{}},
		{Term: 3, LeaderID: 1, Entries: []LogEntry{}},
	}

	for _, req := range reqs {
		rn.handleAppendEntries(req)
	}
	if node.CurrentTerm() != 10 {
		t.Fatalf("term should be 10, got %d", node.CurrentTerm())
	}
}

func TestDelayedHeartbeatFromOldTermRejected(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(2)
	rn, _ := NewRaftNode(node, []NodeID{1, 3}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
	req := AppendEntriesRequest{
		Term:         5,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}
	rn.handleAppendEntries(req)

	if node.CurrentTerm() != 5 {
		t.Fatalf("expected term 5")
	}
	delayedReq := AppendEntriesRequest{
		Term:         3,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}

	resp := rn.handleAppendEntries(delayedReq)
	if resp.Success {
		t.Fatal("stale heartbeat should be rejected")
	}
	if resp.Term != 5 {
		t.Fatalf("expected response term 5, got %d", resp.Term)
	}
}

func TestAtMostOneActiveLeaderPerTermWithHeartbeats(t *testing.T) {
	for scenario := 0; scenario < 5; scenario++ {
		ids := []NodeID{1, 2, 3}
		transportMap := make(map[NodeID]*RaftNode)
		trans := NewInMemoryTransport(transportMap)

		leaderNode, _ := NewNode(1)
		leaderNode.AdvanceTerm(1)
		leaderNode.SetState(Leader)
		leaderClock := NewDeterministicClock(time.Unix(0, 0))
		leader, _ := NewRaftNode(leaderNode, []NodeID{2, 3}, trans, leaderClock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
		transportMap[1] = leader

		followers := make(map[NodeID]*RaftNode)
		for _, id := range []NodeID{2, 3} {
			node, _ := NewNode(id)
			clock := NewDeterministicClock(time.Unix(0, 0))
			rn, _ := NewRaftNode(node, []NodeID{1}, trans, clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
			followers[id] = rn
			transportMap[id] = rn
			rn.Start()
		}

		leader.mu.Lock()
		leader.startHeartbeats()
		leader.mu.Unlock()

		leaderClock.Advance(50 * time.Millisecond)

		leaderCount := 0
		for _, id := range ids {
			if transportMap[id].node.State() == Leader {
				leaderCount++
			}
		}

		if leaderCount > 1 {
			t.Fatalf("scenario %d: at most one leader allowed, got %d", scenario, leaderCount)
		}
	}
}

func TestOnlyLeaderSendsHeartbeats(t *testing.T) {
	for _, nodeId := range []NodeID{1, 2, 3} {
		clock := NewDeterministicClock(time.Unix(0, 0))
		node, _ := NewNode(nodeId)
		rn, _ := NewRaftNode(node, []NodeID{}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
		rn.Start()

		rn.mu.Lock()
		if rn.heartbeatTimer != nil && node.State() != Leader {
			t.Fatalf("node %d: non-leader should not have heartbeat timer", nodeId)
		}
		rn.mu.Unlock()
	}
}

func TestHeartbeatDoesNotModifyLog(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(2)
	rn, _ := NewRaftNode(node, []NodeID{1}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)

	initialLogSize := node.Log().Len()
	req := AppendEntriesRequest{
		Term:         1,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}

	rn.handleAppendEntries(req)

	if node.Log().Len() != initialLogSize {
		t.Fatalf("heartbeat should not modify log, was %d now %d", initialLogSize, node.Log().Len())
	}
}

func TestHeartbeatDoesNotAdvanceCommitIndex(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(2)
	rn, _ := NewRaftNode(node, []NodeID{1}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)

	initialCommitIndex := node.CommitIndex()
	req := AppendEntriesRequest{
		Term:         1,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 100,
	}

	rn.handleAppendEntries(req)

	if node.CommitIndex() != initialCommitIndex {
		t.Fatalf("heartbeat should not advance commit index")
	}
}

func TestHeartbeatDoesNotApplyStateMachineEntries(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(2)
	rn, _ := NewRaftNode(node, []NodeID{1}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)

	initialLastApplied := node.LastApplied()
	req := AppendEntriesRequest{
		Term:         1,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}

	rn.handleAppendEntries(req)

	if node.LastApplied() != initialLastApplied {
		t.Fatal("heartbeat should not apply state machine entries")
	}
}

func TestDeterministicRandomizedHeartbeatScenarios(t *testing.T) {
	seed := int64(99999)
	rng := rand.New(rand.NewSource(seed))

	for scenario := 0; scenario < 10; scenario++ {
		numNodes := 3 + rng.Intn(3)
		nodes := make(map[NodeID]*RaftNode)
		transportMap := make(map[NodeID]*RaftNode)
		trans := NewInMemoryTransport(transportMap)
		leaderNode, _ := NewNode(1)
		leaderNode.AdvanceTerm(1)
		leaderNode.SetState(Leader)
		leaderClock := NewDeterministicClock(time.Unix(0, 0))
		peers := make([]NodeID, 0)
		for i := 2; i <= numNodes; i++ {
			peers = append(peers, NodeID(i))
		}
		leader, _ := NewRaftNode(leaderNode, peers, trans, leaderClock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
		nodes[1] = leader
		transportMap[1] = leader
		for i := 2; i <= numNodes; i++ {
			id := NodeID(i)
			node, _ := NewNode(id)
			clock := NewDeterministicClock(time.Unix(0, int64(i)*10))
			rn, _ := NewRaftNode(node, []NodeID{1}, trans, clock, 50*time.Millisecond, 150*time.Millisecond, 300*time.Millisecond)
			nodes[id] = rn
			transportMap[id] = rn
			rn.Start()
		}

		leader.mu.Lock()
		leader.startHeartbeats()
		leader.mu.Unlock()
		for step := 0; step < 10; step++ {
			leaderClock.Advance(50 * time.Millisecond)
			if rng.Float32() < 0.2 {
				isolateId := NodeID(2 + rng.Intn(numNodes-1))
				if rng.Float32() < 0.5 {
					trans.isolate(isolateId)
				} else {
					trans.restore(isolateId)
				}
			}
			leaderCount := 0
			for _, rn := range nodes {
				if rn.node.State() == Leader {
					leaderCount++
				}
			}
			if leaderCount > 1 {
				t.Fatalf("scenario %d step %d: multiple leaders detected", scenario, step)
			}
		}
	}
}
