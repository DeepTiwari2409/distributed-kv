package raft

import (
	"math/rand"
	"testing"
	"time"
)

type testCluster struct {
	nodes  map[NodeID]*RaftNode
	clocks map[NodeID]*DeterministicClock
	trans  *InMemoryTransport
	ids    []NodeID
}

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
		startTime := time.Unix(0, int64(i))
		clock := NewDeterministicClock(startTime)
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
func (tc *testCluster) start() {
	for _, rn := range tc.nodes {
		rn.Start()
	}
}
func (tc *testCluster) advance(id NodeID, d time.Duration) {
	if clock, ok := tc.clocks[id]; ok {
		clock.Advance(d)
	}
}
func (tc *testCluster) advanceAll(d time.Duration) {
	for _, clock := range tc.clocks {
		clock.Advance(d)
	}
}
func (tc *testCluster) getLeader() NodeID {
	var leader NodeID
	for id, rn := range tc.nodes {
		if rn.node.State() == Leader {
			if leader != 0 {
				return 0
			}
			leader = id
		}
	}
	return leader
}
func (tc *testCluster) getState(id NodeID) State {
	if rn, ok := tc.nodes[id]; ok {
		return rn.node.State()
	}
	return Follower
}
func (tc *testCluster) getTerm(id NodeID) uint64 {
	if rn, ok := tc.nodes[id]; ok {
		return rn.node.CurrentTerm()
	}
	return 0
}
func (tc *testCluster) getVotedFor(id NodeID) NodeID {
	if rn, ok := tc.nodes[id]; ok {
		return rn.node.VotedFor()
	}
	return NoNode
}
func (tc *testCluster) isolate(id NodeID) {
	tc.trans.isolate(id)
}
func (tc *testCluster) restore(id NodeID) {
	tc.trans.restore(id)
}

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

func TestHigherLastLogTermWins(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	node.AdvanceTerm(5)
	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)

	req := RequestVoteRequest{
		Term:         5,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  10,
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
		LastLogIndex: 100,
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
		LastLogIndex: 5,
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
		LastLogIndex: 2,
		LastLogTerm:  5,
	}

	response := rn.handleRequestVote(req)
	if response.VoteGranted {
		t.Fatal("expected vote to be rejected for lower last log index with same term")
	}
}

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
		clock := NewDeterministicClock(time.Unix(0, 0))
		rn, _ := NewRaftNode(node, peers, trans, clock, 150*time.Millisecond, 300*time.Millisecond)
		nodes[id] = rn
		transportMap[id] = rn
	}

	for _, rn := range nodes {
		rn.Start()
	}
	nodes[1].clock.(*DeterministicClock).Advance(151 * time.Millisecond)
	if nodes[1].node.State() != Leader {
		t.Fatalf("expected node 1 to be Leader after receiving votes, got %s", nodes[1].node.State())
	}
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
	tc.isolate(1)
	tc.advance(1, 150*time.Millisecond)
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

func TestOneVotePerTerm(t *testing.T) {
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
	rn.handleRequestVote(req1)

	if node.VotedFor() != 2 {
		t.Fatalf("expected to vote for node 2")
	}
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
	clock.Advance(140 * time.Millisecond)
	req := RequestVoteRequest{
		Term:         1,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	rn.handleRequestVote(req)
	clock.Advance(15 * time.Millisecond)
	if node.State() != Follower {
		t.Fatalf("expected to still be Follower, got %s", node.State())
	}
}

func TestVoteStateScopedToTerm(t *testing.T) {
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
	rn.handleRequestVote(req1)

	if node.VotedFor() != 2 {
		t.Fatalf("expected to vote for node 2 in term 5")
	}
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

func TestSplitVoteDoesNotCreateLeaderWithoutMajority(t *testing.T) {
	tc := newTestCluster(5)
	tc.start()
	tc.advanceAll(151 * time.Millisecond)
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

func TestMinorityPartitionCannotElectLeader(t *testing.T) {
	tc := newTestCluster(5)
	tc.start()
	tc.isolate(1)
	tc.advance(1, 150*time.Millisecond)
	if tc.getState(1) == Leader {
		t.Fatal("expected isolated node to remain candidate, not become leader")
	}
}

func TestMajorityPartitionCanElectLeader(t *testing.T) {
	tc := newTestCluster(5)
	tc.start()
	tc.isolate(4)
	tc.isolate(5)
	tc.advance(1, 150*time.Millisecond)
	tc.advanceAll(0)
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
	tc.advance(1, 150*time.Millisecond)
	tc.advanceAll(0)

	leader := tc.getLeader()
	tc.isolate(leader)
	tc.advanceAll(200 * time.Millisecond)

	newLeader := tc.getLeader()
	if newLeader == leader {
		t.Fatalf("expected new leader after isolating old one")
	}
}

func TestAtMostOneLeaderPerTerm(t *testing.T) {
	for scenario := 0; scenario < 5; scenario++ {
		tc := newTestCluster(3)
		tc.start()
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

func TestCurrentTermNeverDecreases(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, _ := NewNode(1)
	rn, _ := NewRaftNode(node, []NodeID{2}, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)
	rn.Start()

	clock.Advance(150 * time.Millisecond)
	term1 := node.CurrentTerm()
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
	req1 := RequestVoteRequest{
		Term:         5,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	rn.handleRequestVote(req1)
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

func TestDeterministicRandomizedElection(t *testing.T) {
	seed := int64(12345)
	rng := rand.New(rand.NewSource(seed))

	for scenario := 0; scenario < 10; scenario++ {
		numNodes := 3 + rng.Intn(3)
		tc := newTestCluster(numNodes)
		tc.start()
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
		isolateCount := rng.Intn(2) + 1
		for i := 0; i < isolateCount; i++ {
			idx := rng.Intn(len(tc.ids))
			tc.isolate(tc.ids[idx])
		}
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
