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

type RaftNode struct {
	mu                 sync.Mutex
	node               *Node
	peers              []NodeID
	transport          Transport
	clock              Clock
	heartbeat          time.Duration
	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	electionTimer      Timer
	votesReceived      map[NodeID]bool
}

func NewRaftNode(node *Node, peers []NodeID, transport Transport, clock Clock, electionTimeoutMin, electionTimeoutMax time.Duration) (*RaftNode, error) {
	if node == nil {
		return nil, fmt.Errorf("node cannot be nil")
	}
	if electionTimeoutMin <= 0 || electionTimeoutMax < electionTimeoutMin {
		return nil, fmt.Errorf("invalid election timeout range")
	}
	rn := &RaftNode{
		node:               node,
		peers:              peers,
		transport:          transport,
		clock:              clock,
		electionTimeoutMin: electionTimeoutMin,
		electionTimeoutMax: electionTimeoutMax,
		votesReceived:      make(map[NodeID]bool),
	}
	ran := rn.newElectionTimer()
	rn.electionTimer = ran
	rn.resetElectionTimer()
	return rn, nil
}

func (rn *RaftNode) newElectionTimer() Timer {
	return rn.clock.NewTimer(rn.randomElectionTimeout(), func() {
		rn.mu.Lock()
		rn.startElection()
		rn.mu.Unlock()
	})
}

func (rn *RaftNode) randomElectionTimeout() time.Duration {
	// Deterministic using the current time nanos as a simple source.
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
	rn.resetElectionTimer()
}

func (rn *RaftNode) ReceiveRequestVote(request RequestVoteRequest) {
	rn.mu.Lock()
	response := rn.handleRequestVote(request)
	rn.mu.Unlock()
	rn.transport.SendRequestVoteResponse(response, request.CandidateID, rn.node.ID())
}

func (rn *RaftNode) ReceiveRequestVoteResponse(response RequestVoteResponse, from NodeID) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.handleRequestVoteResponse(response, from)
}

func (rn *RaftNode) handleRequestVote(request RequestVoteRequest) RequestVoteResponse {
	if request.Term < rn.node.CurrentTerm() {
		return RequestVoteResponse{Term: rn.node.CurrentTerm(), VoteGranted: false}
	}
	if request.Term > rn.node.CurrentTerm() {
		rn.node.AdvanceTerm(request.Term)
		rn.node.SetState(Follower)
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
	}
}

func (rn *RaftNode) isCandidateUpToDate(lastLogIndex, lastLogTerm uint64) bool {
	if lastLogTerm != rn.node.Log().LastTerm() {
		return lastLogTerm > rn.node.Log().LastTerm()
	}
	return lastLogIndex >= rn.node.Log().LastIndex()
}

func (rn *RaftNode) hasMajorityVotes() bool {
	votes := 1
	for _, granted := range rn.votesReceived {
		if granted {
			votes++
		}
	}
	return votes*2 > len(rn.peers)+1
}

func (rn *RaftNode) startElection() {
	rn.node.AdvanceTerm(rn.node.CurrentTerm() + 1)
	rn.node.SetState(Candidate)
	rn.node.SetVotedFor(rn.node.ID())
	rn.votesReceived = map[NodeID]bool{}
	rn.resetElectionTimer()

	if rn.hasMajorityVotes() {
		rn.node.SetState(Leader)
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
