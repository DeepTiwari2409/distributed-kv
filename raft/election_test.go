package raft

import (
	"math/rand"
	"testing"
	"time"
)

// ============================================================================
// Test Helpers and Cluster Utilities
// ============================================================================

// testCluster simulates a deterministic Raft cluster for testing.
type testCluster struct {
	nodes  map[NodeID]*RaftNode
	clocks map[NodeID]*DeterministicClock
	trans  *InMemoryTransport
	ids    []NodeID
}

// newTestCluster creates a cluster with n nodes, each with deterministic time.
func newTestCluster(n int) *testCluster {
	transportMap := make(map[NodeID]*RaftNode)
	trans := NewInMemoryTransport(transportMap)
	cluster := &testCluster{
		nodes:  make(map[NodeID]*RaftNode),
		clocks: make(map[NodeID]*DeterministicClock),
		trans:  trans,
		ids:    make([]NodeID, n),
	}

	for i := 0; i < n; i++ {
		id := NodeID(i + 1)
		cluster.ids[i] = id

		node, err := NewNode(id)
		if err != nil {
			panic(err)
		}

		// Each node gets a unique start time to avoid split votes
		startTime := time.Unix(0, int64(i))
		clock := NewDeterministicClock(startTime)

		// Build peer list (all nodes except self)
		peers := make([]NodeID, 0, n-1)
		for j := 0; j < n; j++ {
			if i != j {
				peers = append(peers, NodeID(j+1))
			}
		}

		rn, err := NewRaftNode(node, peers, trans, clock, 150*time.Millisecond, 300*time.Millisecond)
		if err != nil {
			panic(err)
		}

		cluster.nodes[id] = rn
		cluster.clocks[id] = clock
		transportMap[id] = rn
	}

	return cluster
}

// start initializes all nodes as followers.
func (tc *testCluster) start() {
	for _, rn := range tc.nodes {
		rn.Start()
	}
}

// advance advances the clock for a specific node.
func (tc *testCluster) advance(id NodeID, d time.Duration) {
	if clock, ok := tc.clocks[id]; ok {
		clock.Advance(d)
	}
}

// advanceAll advances all clocks by the same amount.
func (tc *testCluster) advanceAll(d time.Duration) {
	for _, clock := range tc.clocks {
		clock.Advance(d)
	}
}

// getLeader returns the leader node ID, or 0 if no leader or multiple leaders.
func (tc *testCluster) getLeader() NodeID {
	var leader NodeID
	for id, rn := range tc.nodes {
		if rn.node.State() == Leader {
			if leader != 0 {
				// Multiple leaders detected
				return 0
			}
			leader = id
		}
	}
	return leader
}

// getState returns the current state of a node.
func (tc *testCluster) getState(id NodeID) State {
	if rn, ok := tc.nodes[id]; ok {
		return rn.node.State()
	}
	return Follower
}

// getTerm returns the current term of a node.
func (tc *testCluster) getTerm(id NodeID) uint64 {
	if rn, ok := tc.nodes[id]; ok {
		return rn.node.CurrentTerm()
	}
	return 0
}

// getVotedFor returns who the node voted for in current term.
func (tc *testCluster) getVotedFor(id NodeID) NodeID {
	if rn, ok := tc.nodes[id]; ok {
		return rn.node.VotedFor()
	}
	return NoNode
}

// isolate blocks all communication to/from a node.
func (tc *testCluster) isolate(id NodeID) {
	tc.trans.isolate(id)
}

// restore re-enables communication to/from a node.
func (tc *testCluster) restore(id NodeID) {
	tc.trans.restore(id)
}

// ============================================================================
// Timer and Clock Tests
// ============================================================================

func TestDeterministicClockAdvancesAndTriggersTimer(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	fired := false
	clock.NewTimer(10*time.Millisecond, func() {
		fired = true
	})

	clock.Advance(10 * time.Millisecond)
	if !fired {
		t.Fatal("expected timer callback to fire")
	}
}

func TestDeterministicClockMultipleTimers(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	count := 0

	clock.NewTimer(5*time.Millisecond, func() {
		count++
	})
	clock.NewTimer(10*time.Millisecond, func() {
		count++
	})
	clock.NewTimer(15*time.Millisecond, func() {
		count++
	})

	clock.Advance(10 * time.Millisecond)
	if count != 2 {
		t.Fatalf("expected 2 timers fired, got %d", count)
	}

	clock.Advance(5 * time.Millisecond)
	if count != 3 {
		t.Fatalf("expected 3 timers fired total, got %d", count)
	}
}

// ============================================================================
// RequestVote Rule Tests (RULE 1-5)
// ============================================================================

// RULE 1: Stale term - reject if candidate term < receiver current term
func TestRequestVoteRejectedForStaleTerm(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	node.AdvanceTerm(5)
	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)

	req := RequestVoteRequest{
		Term:         3,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	response := rn.handleRequestVote(req)
	if response.VoteGranted {
		t.Fatal("expected vote to be rejected for stale term")
	}
	if response.Term != 5 {
		t.Fatalf("expected response term to be 5, got %d", response.Term)
	}
}

// RULE 2: Higher term - update term and become follower
func TestRequestVoteUpdatesTermForHigherTerm(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	node.AdvanceTerm(3)
	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)

	req := RequestVoteRequest{
		Term:         5,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	response := rn.handleRequestVote(req)
	if node.CurrentTerm() != 5 {
		t.Fatalf("expected term to advance to 5, got %d", node.CurrentTerm())
	}
	if response.Term != 5 {
		t.Fatalf("expected response term to be 5, got %d", response.Term)
	}
}

// RULE 2: Higher term causes follower state transition
func TestRequestVoteUpdatesFollowerStateForHigherTerm(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	node.AdvanceTerm(3)
	node.SetState(Candidate)
	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)

	req := RequestVoteRequest{
		Term:         5,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	rn.handleRequestVote(req)
	if node.State() != Follower {
		t.Fatalf("expected state to be Follower, got %s", node.State())
	}
}

// RULE 3: One vote per term - reject if already voted for different candidate
func TestRequestVoteRejectsAlreadyVotedForOtherCandidate(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	node.AdvanceTerm(5)
	node.SetVotedFor(2)
	rn, _ := NewRaftNode(node, []NodeID{2, 3}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)

	req := RequestVoteRequest{
		Term:         5,
		CandidateID:  3,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	response := rn.handleRequestVote(req)
	if response.VoteGranted {
		t.Fatal("expected vote to be rejected, already voted for different candidate")
	}
}

// RULE 3: Allow repeat vote for same candidate (idempotency)
func TestRequestVoteAllowsRepeatVoteForSameCandidate(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	node.AdvanceTerm(5)
	node.SetVotedFor(2)
	rn, _ := NewRaftNode(node, []NodeID{2, 3}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)

	req := RequestVoteRequest{
		Term:         5,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	response := rn.handleRequestVote(req)
	if !response.VoteGranted {
		t.Fatal("expected vote to be granted for same candidate (idempotent)")
	}
}

// RULE 4 & 5: Grant vote if up-to-date
func TestRequestVoteGrantedForUpToDateCandidate(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	node.AdvanceTerm(5)
	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)

	req := RequestVoteRequest{
		Term:         5,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	response := rn.handleRequestVote(req)
	if !response.VoteGranted {
		t.Fatal("expected vote to be granted")
	}
	if node.VotedFor() != 2 {
		t.Fatalf("expected votedFor to be 2, got %s", node.VotedFor())
	}
}

// ============================================================================
// Log Comparison Tests (Rule 4)
// ============================================================================

func TestHigherLastLogTermWins(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	node.AdvanceTerm(5)
	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)

	req := RequestVoteRequest{
		Term:         5,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  10, // Candidate has higher term log
	}

	response := rn.handleRequestVote(req)
	if !response.VoteGranted {
		t.Fatal("expected vote to be granted for higher last log term")
	}
}

func TestLowerLastLogTermRejected(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	node.AdvanceTerm(5)

	// Add an entry with term 10
	entry := LogEntry{
		Index:   1,
		Term:    10,
		Command: NewPutCommand("key", []byte("value")),
	}
	node.log.Append(entry)

	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)

	req := RequestVoteRequest{
		Term:         5,
		CandidateID:  2,
		LastLogIndex: 100, // Higher index but lower term
		LastLogTerm:  5,
	}

	response := rn.handleRequestVote(req)
	if response.VoteGranted {
		t.Fatal("expected vote to be rejected for lower last log term")
	}
}

func TestSameTermHigherLastLogIndexWins(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	node.AdvanceTerm(5)

	// Add two entries with same term
	for i := 1; i <= 3; i++ {
		entry := LogEntry{
			Index:   uint64(i),
			Term:    5,
			Command: NewPutCommand("key", []byte("value")),
		}
		node.log.Append(entry)
	}

	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)

	req := RequestVoteRequest{
		Term:         5,
		CandidateID:  2,
		LastLogIndex: 5, // Higher index, same term
		LastLogTerm:  5,
	}

	response := rn.handleRequestVote(req)
	if !response.VoteGranted {
		t.Fatal("expected vote to be granted for higher last log index with same term")
	}
}

func TestSameTermLowerLastLogIndexRejected(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	node.AdvanceTerm(5)

	// Add entries
	for i := 1; i <= 5; i++ {
		entry := LogEntry{
			Index:   uint64(i),
			Term:    5,
			Command: NewPutCommand("key", []byte("value")),
		}
		node.log.Append(entry)
	}

	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)

	req := RequestVoteRequest{
		Term:         5,
		CandidateID:  2,
		LastLogIndex: 2, // Lower index, same term
		LastLogTerm:  5,
	}

	response := rn.handleRequestVote(req)
	if response.VoteGranted {
		t.Fatal("expected vote to be rejected for lower last log index with same term")
	}
}

// ============================================================================
// Election Behavior Tests
// ============================================================================

func TestFollowerElectionTimeoutStartsElection(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)
	rn.Start()

	if node.State() != Follower {
		t.Fatalf("expected initial state to be Follower, got %s", node.State())
	}

	clock.Advance(150 * time.Millisecond)

	if node.State() != Candidate {
		t.Fatalf("expected state to be Candidate after election timeout, got %s", node.State())
	}
}

func TestElectionIncrementsTerm(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)
	rn.Start()

	initialTerm := node.CurrentTerm()
	clock.Advance(150 * time.Millisecond)

	if node.CurrentTerm() != initialTerm+1 {
		t.Fatalf("expected term to increment from %d to %d, got %d", initialTerm, initialTerm+1, node.CurrentTerm())
	}
}

func TestCandidateVotesForSelf(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)
	rn.Start()

	clock.Advance(150 * time.Millisecond)

	if node.State() != Candidate {
		t.Fatalf("expected state to be Candidate, got %s", node.State())
	}
	if node.VotedFor() != 1 {
		t.Fatalf("expected to vote for self (1), got %s", node.VotedFor())
	}
}

func TestCandidateSendsRequestVote(t *testing.T) {
	ids := []NodeID{1, 2, 3}
	transportMap := make(map[NodeID]*RaftNode)
	trans := NewInMemoryTransport(transportMap)

	nodes := make(map[NodeID]*RaftNode)
	for _, id := range ids {
		node, _ := NewNode(id)
		peers := make([]NodeID, 0)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		// Use same start time for simplicity
		clock := NewDeterministicClock(time.Unix(0, 0))
		rn, _ := NewRaftNode(node, peers, trans, clock, 150*time.Millisecond, 300*time.Millisecond)
		nodes[id] = rn
		transportMap[id] = rn
	}

	for _, rn := range nodes {
		rn.Start()
	}

	// Trigger election on node 1 - advance enough to exceed timeout
	nodes[1].clock.(*DeterministicClock).Advance(151 * time.Millisecond)

	// Node 1 should become candidate initially, then receive votes and become leader
	if nodes[1].node.State() != Leader {
		t.Fatalf("expected node 1 to be Leader after receiving votes, got %s", nodes[1].node.State())
	}

	// Nodes 2 and 3 should have updated their term and voted
	if nodes[2].node.CurrentTerm() < 1 {
		t.Fatalf("expected node 2 term to advance after receiving RequestVote")
	}
	if nodes[3].node.CurrentTerm() < 1 {
		t.Fatalf("expected node 3 term to advance after receiving RequestVote")
	}
}

func TestCandidateDoesNotBecomeLeaderWithoutMajority(t *testing.T) {
	tc := newTestCluster(5)
	tc.start()

	// Isolate the candidate's votes
	tc.isolate(1)

	// Advance node 1's clock to timeout
	tc.advance(1, 150*time.Millisecond)

	// Candidate shouldn't become leader without other votes
	if tc.getState(1) == Leader {
		t.Fatal("expected candidate not to become leader without majority")
	}
}

func TestSingleNodeElection(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	rn, _ := NewRaftNode(node, nil, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)
	rn.Start()

	clock.Advance(150 * time.Millisecond)

	if node.State() != Leader {
		t.Fatalf("expected single node to become leader immediately, got %s", node.State())
	}
}

func TestThreeNodeElectionSucceedsWithMajority(t *testing.T) {
	tc := newTestCluster(3)
	tc.start()

	tc.advance(1, 150*time.Millisecond)
	tc.advanceAll(0)

	leaderCount := 0
	if tc.getLeader() != 0 {
		leaderCount = 1
	}

	if leaderCount != 1 {
		t.Fatalf("expected exactly one leader in 3-node cluster")
	}
}

func TestFiveNodeElection(t *testing.T) {
	tc := newTestCluster(5)
	tc.start()

	tc.advance(1, 150*time.Millisecond)
	tc.advanceAll(0)

	if tc.getLeader() != 1 {
		t.Fatalf("expected node 1 to be elected leader in 5-node cluster")
	}
}

// ============================================================================
// Term Handling Tests
// ============================================================================

func TestHigherTermResponseCausesCandidateToStepDown(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)
	rn.Start()

	clock.Advance(150 * time.Millisecond)
	if node.State() != Candidate {
		t.Fatalf("expected to be Candidate after timeout")
	}

	response := RequestVoteResponse{
		Term:        10,
		VoteGranted: false,
	}
	rn.handleRequestVoteResponse(response, 2)

	if node.State() != Follower {
		t.Fatalf("expected Candidate to step down to Follower on higher term response, got %s", node.State())
	}
	if node.CurrentTerm() != 10 {
		t.Fatalf("expected term to advance to 10, got %d", node.CurrentTerm())
	}
}

func TestStaleResponseIgnored(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)
	rn.Start()

	clock.Advance(150 * time.Millisecond)
	initialTerm := node.CurrentTerm()
	initialState := node.State()

	// Send response from older term
	response := RequestVoteResponse{
		Term:        initialTerm - 1,
		VoteGranted: true,
	}
	rn.handleRequestVoteResponse(response, 2)

	if node.CurrentTerm() != initialTerm {
		t.Fatalf("expected term to remain %d, got %d", initialTerm, node.CurrentTerm())
	}
	if node.State() != initialState {
		t.Fatalf("expected state to remain %s, got %s", initialState, node.State())
	}
}

func TestHigherTermRequestVoteCausesFollowerTransition(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)
	rn.Start()

	clock.Advance(150 * time.Millisecond)
	if node.State() != Candidate {
		t.Fatalf("expected Candidate, got %s", node.State())
	}

	req := RequestVoteRequest{
		Term:         node.CurrentTerm() + 5,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	rn.handleRequestVote(req)

	if node.State() != Follower {
		t.Fatalf("expected Candidate to transition to Follower on higher term RequestVote, got %s", node.State())
	}
}

func TestStaleTermCannotChangeState(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	node.AdvanceTerm(10)
	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)

	req := RequestVoteRequest{
		Term:         5,
		CandidateID:  2,
		LastLogIndex: 100,
		LastLogTerm:  100,
	}

	rn.handleRequestVote(req)

	if node.State() != Follower {
		t.Fatalf("expected state to remain unchanged for stale term")
	}
	if node.CurrentTerm() != 10 {
		t.Fatalf("expected term to remain 10, got %d", node.CurrentTerm())
	}
}

// ============================================================================
// Voting Safety Tests
// ============================================================================

func TestOneVotePerTerm(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	node.AdvanceTerm(5)
	rn, _ := NewRaftNode(node, []NodeID{2, 3}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)

	// Vote for node 2
	req1 := RequestVoteRequest{
		Term:         5,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	rn.handleRequestVote(req1)

	if node.VotedFor() != 2 {
		t.Fatalf("expected to vote for node 2")
	}

	// Try to vote for node 3 in same term
	req2 := RequestVoteRequest{
		Term:         5,
		CandidateID:  3,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	resp := rn.handleRequestVote(req2)

	if resp.VoteGranted {
		t.Fatal("expected vote to be rejected for different candidate in same term")
	}
	if node.VotedFor() != 2 {
		t.Fatalf("expected votedFor to remain 2")
	}
}

func TestDifferentCandidatesCannotBothReceiveSameVote(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	node.AdvanceTerm(5)
	rn, _ := NewRaftNode(node, []NodeID{2, 3}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)

	req1 := RequestVoteRequest{
		Term:         5,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	resp1 := rn.handleRequestVote(req1)

	req2 := RequestVoteRequest{
		Term:         5,
		CandidateID:  3,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	resp2 := rn.handleRequestVote(req2)

	if resp1.VoteGranted && resp2.VoteGranted {
		t.Fatal("cannot grant votes to two different candidates in same term")
	}
}

func TestVoteResetsElectionTimer(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)
	rn.Start()

	// Don't let the timeout trigger yet
	clock.Advance(140 * time.Millisecond)

	// Receive a vote request - should reset timer
	req := RequestVoteRequest{
		Term:         1,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	rn.handleRequestVote(req)

	// Advance just enough to expire old timeout, but not the reset one
	clock.Advance(15 * time.Millisecond)

	// Should still be follower (election timer was reset)
	if node.State() != Follower {
		t.Fatalf("expected to still be Follower, got %s", node.State())
	}
}

func TestVoteStateScopedToTerm(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	node.AdvanceTerm(5)
	rn, _ := NewRaftNode(node, []NodeID{2, 3}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)

	// Vote for node 2 in term 5
	req1 := RequestVoteRequest{
		Term:         5,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	rn.handleRequestVote(req1)

	if node.VotedFor() != 2 {
		t.Fatalf("expected to vote for node 2 in term 5")
	}

	// Receive higher term - should clear vote
	req2 := RequestVoteRequest{
		Term:         6,
		CandidateID:  3,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	rn.handleRequestVote(req2)

	if node.VotedFor() != 3 {
		t.Fatalf("expected to vote for node 3 in term 6, got %s", node.VotedFor())
	}
}

// ============================================================================
// Split Vote Test
// ============================================================================

func TestSplitVoteDoesNotCreateLeaderWithoutMajority(t *testing.T) {
	// In a 5-node cluster, majority is 3 votes
	// This test verifies that a split vote scenario doesn't create multiple leaders
	tc := newTestCluster(5)
	tc.start()

	// Advance all nodes to trigger elections
	tc.advanceAll(151 * time.Millisecond)

	// Count leaders - should be exactly 1
	leaderCount := 0
	for _, id := range tc.ids {
		if tc.getState(id) == Leader {
			leaderCount++
		}
	}

	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader after election, got %d", leaderCount)
	}
}

// ============================================================================
// Partition Tests
// ============================================================================

func TestMinorityPartitionCannotElectLeader(t *testing.T) {
	tc := newTestCluster(5)
	tc.start()

	// Partition: node 1 isolated
	tc.isolate(1)

	// Trigger election on node 1
	tc.advance(1, 150*time.Millisecond)

	// Node 1 should remain candidate (needs 3 votes out of 5)
	if tc.getState(1) == Leader {
		t.Fatal("expected isolated node to remain candidate, not become leader")
	}
}

func TestMajorityPartitionCanElectLeader(t *testing.T) {
	tc := newTestCluster(5)
	tc.start()

	// Partition: nodes 4,5 isolated
	tc.isolate(4)
	tc.isolate(5)

	// Trigger election on node 1
	tc.advance(1, 150*time.Millisecond)
	tc.advanceAll(0)

	// Majority partition should elect a leader
	leader := tc.getLeader()
	if leader == 0 {
		t.Fatal("expected majority partition to elect a leader")
	}
	if leader == 4 || leader == 5 {
		t.Fatalf("leader shouldn't be from isolated partition")
	}
}

func TestPartitionRecoveryAllowsFutureElection(t *testing.T) {
	tc := newTestCluster(3)
	tc.start()

	// Trigger initial election
	tc.advance(1, 150*time.Millisecond)
	tc.advanceAll(0)

	leader := tc.getLeader()

	// Isolate the leader
	tc.isolate(leader)

	// Remaining nodes should eventually elect new leader
	tc.advanceAll(200 * time.Millisecond)

	newLeader := tc.getLeader()
	if newLeader == leader {
		t.Fatalf("expected new leader after isolating old one")
	}
}

// ============================================================================
// Leader Safety Tests
// ============================================================================

func TestAtMostOneLeaderPerTerm(t *testing.T) {
	// Run multiple scenarios to verify at most one leader per term
	for scenario := 0; scenario < 5; scenario++ {
		tc := newTestCluster(3)
		tc.start()

		// Let election proceed
		tc.advance(1, 150*time.Millisecond)
		tc.advanceAll(0)

		leaderCount := 0
		for _, id := range tc.ids {
			if tc.getState(id) == Leader {
				leaderCount++
			}
		}

		if leaderCount > 1 {
			t.Fatalf("scenario %d: expected at most one leader, got %d", scenario, leaderCount)
		}
	}
}

// ============================================================================
// Invariant Tests
// ============================================================================

func TestCurrentTermNeverDecreases(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)
	rn.Start()

	clock.Advance(150 * time.Millisecond)
	term1 := node.CurrentTerm()

	// Stale response shouldn't decrease term
	resp := RequestVoteResponse{
		Term:        term1 - 1,
		VoteGranted: false,
	}
	rn.handleRequestVoteResponse(resp, 2)

	if node.CurrentTerm() < term1 {
		t.Fatal("term decreased, violating invariant")
	}
}

func TestVoteStateNotViolated(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	rn, _ := NewRaftNode(node, []NodeID{2, 3}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)

	node.AdvanceTerm(5)

	// Vote for candidate 2
	req1 := RequestVoteRequest{
		Term:         5,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	rn.handleRequestVote(req1)

	// Try to vote for candidate 3
	req2 := RequestVoteRequest{
		Term:         5,
		CandidateID:  3,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	rn.handleRequestVote(req2)

	votedFor := node.VotedFor()
	if votedFor != 2 {
		t.Fatalf("voted for multiple candidates in same term")
	}
}

// ============================================================================
// Deterministic Randomized Tests
// ============================================================================

func TestDeterministicRandomizedElection(t *testing.T) {
	seed := int64(12345)
	rng := rand.New(rand.NewSource(seed))

	for scenario := 0; scenario < 10; scenario++ {
		numNodes := 3 + rng.Intn(3) // 3-5 nodes
		tc := newTestCluster(numNodes)
		tc.start()

		// Random delays
		for _, id := range tc.ids {
			delay := time.Duration(rng.Intn(200)+100) * time.Millisecond
			tc.advance(id, delay)
		}
		tc.advanceAll(0)

		leaderCount := 0
		for _, id := range tc.ids {
			if tc.getState(id) == Leader {
				leaderCount++
			}
		}

		if leaderCount != 1 {
			t.Fatalf("scenario %d (seed %d): expected 1 leader, got %d", scenario, seed, leaderCount)
		}
	}
}

func TestDeterministicRandomizedPartitions(t *testing.T) {
	seed := int64(54321)
	rng := rand.New(rand.NewSource(seed))

	for scenario := 0; scenario < 10; scenario++ {
		tc := newTestCluster(5)
		tc.start()

		// Random partition of minority
		isolateCount := rng.Intn(2) + 1 // 1-2 nodes
		for i := 0; i < isolateCount; i++ {
			idx := rng.Intn(len(tc.ids))
			tc.isolate(tc.ids[idx])
		}

		// Let election proceed
		tc.advanceAll(200 * time.Millisecond)

		leaderCount := 0
		for _, id := range tc.ids {
			if tc.getState(id) == Leader {
				leaderCount++
			}
		}

		if leaderCount > 1 {
			t.Fatalf("scenario %d (seed %d): expected at most 1 leader, got %d", scenario, seed, leaderCount)
		}
	}
}
