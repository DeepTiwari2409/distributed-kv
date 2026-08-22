package raft

import (
	"fmt"
	"sync"
	"time"
)

type RequestVoteRequest struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}
type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}
type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

type RaftNode struct {
	mu                 sync.Mutex
	node               *Node
	peers              []NodeID
	transport          Transport
	clock              Clock
	heartbeatInterval  time.Duration
	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	electionTimer      Timer
	heartbeatTimer     Timer
	votesReceived      map[NodeID]bool
	nextIndex          map[NodeID]uint64
	matchIndex         map[NodeID]uint64
	sentLastIndex      map[NodeID]uint64
	electionTerm       uint64
	stopped            bool
	stateMachine       StateMachine
	applicationError   error
	started            bool
}

func NewRaftNode(node *Node, peers []NodeID, transport Transport, clock Clock, timeouts ...time.Duration) (*RaftNode, error) {
	if node == nil {
		return nil, fmt.Errorf("node cannot be nil")
	}
	var heartbeatInterval, electionTimeoutMin, electionTimeoutMax time.Duration
	switch len(timeouts) {
	case 2:
		electionTimeoutMin, electionTimeoutMax = timeouts[0], timeouts[1]
		heartbeatInterval = electionTimeoutMin / 3
	case 3:
		heartbeatInterval, electionTimeoutMin, electionTimeoutMax = timeouts[0], timeouts[1], timeouts[2]
	default:
		return nil, fmt.Errorf("expected two or three timeout durations")
	}
	if heartbeatInterval <= 0 {
		return nil, fmt.Errorf("heartbeat interval must be positive")
	}
	if electionTimeoutMin <= 0 || electionTimeoutMax < electionTimeoutMin {
		return nil, fmt.Errorf("invalid election timeout range")
	}
	rn := &RaftNode{
		node:               node,
		peers:              peers,
		transport:          transport,
		clock:              clock,
		heartbeatInterval:  heartbeatInterval,
		electionTimeoutMin: electionTimeoutMin,
		electionTimeoutMax: electionTimeoutMax,
		votesReceived:      make(map[NodeID]bool),
		nextIndex:          make(map[NodeID]uint64),
		matchIndex:         make(map[NodeID]uint64),
		sentLastIndex:      make(map[NodeID]uint64),
	}
	ran := rn.newElectionTimer()
	rn.electionTimer = ran
	rn.resetElectionTimer()
	return rn, nil
}

func (rn *RaftNode) newElectionTimer() Timer {
	return rn.clock.NewTimer(rn.randomElectionTimeout(), func() {
		rn.startElection()
	})
}

func (rn *RaftNode) randomElectionTimeout() time.Duration {
	now := rn.clock.Now()
	delta := rn.electionTimeoutMax - rn.electionTimeoutMin
	if delta <= 0 {
		return rn.electionTimeoutMin
	}
	return rn.electionTimeoutMin + time.Duration(now.UnixNano()%int64(delta))
}

func (rn *RaftNode) resetElectionTimer() {
	if rn.electionTimer == nil {
		rn.electionTimer = rn.newElectionTimer()
		return
	}
	rn.electionTimer.Reset(rn.randomElectionTimeout())
}

func (rn *RaftNode) Start() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.node.SetState(Follower)
	rn.stopped = false
	rn.resetElectionTimer()
}
func (rn *RaftNode) Stop() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.stopped = true
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}
	rn.stopHeartbeats()
}

func (rn *RaftNode) initializeReplicationState() {
	rn.nextIndex = make(map[NodeID]uint64, len(rn.peers))
	rn.matchIndex = make(map[NodeID]uint64, len(rn.peers))
	next := rn.node.Log().LastIndex() + 1
	for _, peer := range rn.peers {
		if peer != rn.node.ID() {
			rn.nextIndex[peer] = next
			rn.matchIndex[peer] = 0
		}
	}
}

func (rn *RaftNode) NextIndex(peer NodeID) uint64 {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.nextIndex[peer]
}

func (rn *RaftNode) MatchIndex(peer NodeID) uint64 {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.matchIndex[peer]
}

func (rn *RaftNode) ReceiveRequestVote(request RequestVoteRequest) {
	rn.mu.Lock()
	if rn.stopped {
		rn.mu.Unlock()
		return
	}
	response := rn.handleRequestVote(request)
	rn.mu.Unlock()
	rn.transport.SendRequestVoteResponse(response, request.CandidateID, rn.node.ID())
}

func (rn *RaftNode) ReceiveRequestVoteResponse(response RequestVoteResponse, from NodeID) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.stopped {
		return
	}
	rn.handleRequestVoteResponse(response, from)
}

func (rn *RaftNode) handleRequestVote(request RequestVoteRequest) RequestVoteResponse {
	if request.Term < rn.node.CurrentTerm() {
		return RequestVoteResponse{Term: rn.node.CurrentTerm(), VoteGranted: false}
	}
	if request.Term > rn.node.CurrentTerm() {
		rn.node.AdvanceTerm(request.Term)
		rn.node.SetState(Follower)
		rn.stopHeartbeats()
		rn.resetElectionTimer()
	}
	if rn.node.VotedFor() != NoNode && rn.node.VotedFor() != request.CandidateID {
		return RequestVoteResponse{Term: rn.node.CurrentTerm(), VoteGranted: false}
	}
	if !rn.isCandidateUpToDate(request.LastLogIndex, request.LastLogTerm) {
		return RequestVoteResponse{Term: rn.node.CurrentTerm(), VoteGranted: false}
	}
	rn.node.SetVotedFor(request.CandidateID)
	rn.resetElectionTimer()
	return RequestVoteResponse{Term: rn.node.CurrentTerm(), VoteGranted: true}
}

func (rn *RaftNode) handleRequestVoteResponse(response RequestVoteResponse, from NodeID) {
	if response.Term > rn.node.CurrentTerm() {
		rn.node.AdvanceTerm(response.Term)
		rn.node.SetState(Follower)
		rn.stopHeartbeats()
		rn.resetElectionTimer()
		return
	}
	if rn.node.State() != Candidate || response.Term < rn.node.CurrentTerm() {
		return
	}
	if response.VoteGranted {
		rn.votesReceived[from] = true
	}
	if rn.hasMajorityVotes() {
		rn.node.SetState(Leader)
		rn.initializeReplicationState()
		rn.startHeartbeats()
	}
}

func (rn *RaftNode) isCandidateUpToDate(lastLogIndex, lastLogTerm uint64) bool {
	if lastLogTerm != rn.node.Log().LastTerm() {
		return lastLogTerm > rn.node.Log().LastTerm()
	}
	return lastLogIndex >= rn.node.Log().LastIndex()
}

func (rn *RaftNode) hasMajorityVotes() bool {
	if rn.node.State() != Candidate || rn.electionTerm != rn.node.CurrentTerm() {
		return false
	}
	votes := 1
	for _, granted := range rn.votesReceived {
		if granted {
			votes++
		}
	}
	return votes*2 > len(rn.peers)+1
}

func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.stopped {
		return
	}

	rn.node.AdvanceTerm(rn.node.CurrentTerm() + 1)
	rn.node.SetState(Candidate)
	rn.electionTerm = rn.node.CurrentTerm()
	rn.node.SetVotedFor(rn.node.ID())
	rn.votesReceived = map[NodeID]bool{}
	rn.resetElectionTimer()

	if rn.hasMajorityVotes() {
		rn.node.SetState(Leader)
		rn.initializeReplicationState()
		rn.startHeartbeats()
		return
	}

	request := RequestVoteRequest{
		Term:         rn.node.CurrentTerm(),
		CandidateID:  rn.node.ID(),
		LastLogIndex: rn.node.Log().LastIndex(),
		LastLogTerm:  rn.node.Log().LastTerm(),
	}
	peers := append([]NodeID(nil), rn.peers...)
	transport := rn.transport

	rn.mu.Unlock()
	for _, peerID := range peers {
		transport.SendRequestVote(request, peerID)
	}
	rn.mu.Lock()
}
