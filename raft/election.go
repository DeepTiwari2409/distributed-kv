package raft

import (
	"fmt"
	"sync"
	"time"

	"github.com/DeepTiwari2409/distributed-kv/storage"
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
	storage            storage.RaftStateStore
	storageError       error
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

func NewRaftNodeWithStorage(node *Node, peers []NodeID, transport Transport, clock Clock, stateStore storage.RaftStateStore, timeouts ...time.Duration) (*RaftNode, error) {
	if node == nil {
		return nil, fmt.Errorf("node cannot be nil")
	}
	if stateStore == nil {
		return nil, fmt.Errorf("raft state store cannot be nil")
	}
	state, err := stateStore.LoadRaftState()
	if err != nil {
		return nil, fmt.Errorf("load raft state: %w", err)
	}
	if state.CurrentTerm > 0 {
		node.currentTerm = state.CurrentTerm
	}
	node.votedFor = NodeID(state.VotedFor)
	node.log = NewRaftLog()
	for _, record := range state.Log {
		if err := node.log.Append(LogEntry{Index: record.Index, Term: record.Term, Command: Command{Type: CommandType(record.Type), Key: record.Key, Value: append([]byte(nil), record.Value...)}}); err != nil {
			return nil, fmt.Errorf("recover raft log: %w", err)
		}
	}
	rn, err := NewRaftNode(node, peers, transport, clock, timeouts...)
	if err != nil {
		return nil, err
	}
	rn.storage = stateStore
	return rn, nil
}

func (rn *RaftNode) RecoverCommitted(commitIndex uint64) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if err := rn.node.AdvanceCommitIndex(commitIndex); err != nil {
		return err
	}
	return rn.applyCommittedEntries()
}

func (rn *RaftNode) persistState() error {
	if rn.storage == nil {
		return nil
	}
	state := storage.RaftState{CurrentTerm: rn.node.CurrentTerm(), VotedFor: uint64(rn.node.VotedFor())}
	for _, entry := range rn.node.Log().EntriesOrEmpty() {
		state.Log = append(state.Log, storage.RaftLogRecord{Index: entry.Index, Term: entry.Term, Type: uint8(entry.Command.Type), Key: entry.Command.Key, Value: append([]byte(nil), entry.Command.Value...)})
	}
	if err := rn.storage.SaveRaftState(state); err != nil {
		rn.storageError = err
		return err
	}
	rn.storageError = nil
	return nil
}

func (rn *RaftNode) restoreDurableState(term uint64, votedFor NodeID, log *RaftLog) {
	rn.node.mu.Lock()
	rn.node.currentTerm = term
	rn.node.votedFor = votedFor
	rn.node.log = log.Clone()
	rn.node.mu.Unlock()
}

func (rn *RaftNode) StorageError() error {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.storageError
}

func (rn *RaftNode) AppendEntry(entry LogEntry) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	previous := rn.node.Log()
	if err := rn.node.AppendEntry(entry); err != nil {
		return err
	}
	if err := rn.persistState(); err != nil {
		rn.restoreDurableState(rn.node.CurrentTerm(), rn.node.VotedFor(), previous)
		return err
	}
	return nil
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
		oldTerm, oldVote := rn.node.CurrentTerm(), rn.node.VotedFor()
		rn.node.AdvanceTerm(request.Term)
		rn.node.SetState(Follower)
		rn.stopHeartbeats()
		if err := rn.persistState(); err != nil {
			rn.restoreDurableState(oldTerm, oldVote, rn.node.Log())
			return RequestVoteResponse{Term: oldTerm, VoteGranted: false}
		}
		rn.resetElectionTimer()
	}
	if rn.node.VotedFor() != NoNode && rn.node.VotedFor() != request.CandidateID {
		return RequestVoteResponse{Term: rn.node.CurrentTerm(), VoteGranted: false}
	}
	if !rn.isCandidateUpToDate(request.LastLogIndex, request.LastLogTerm) {
		return RequestVoteResponse{Term: rn.node.CurrentTerm(), VoteGranted: false}
	}
	oldVote := rn.node.VotedFor()
	if err := rn.node.SetVotedFor(request.CandidateID); err != nil {
		return RequestVoteResponse{Term: rn.node.CurrentTerm(), VoteGranted: false}
	}
	if err := rn.persistState(); err != nil {
		rn.node.mu.Lock()
		rn.node.votedFor = oldVote
		rn.node.mu.Unlock()
		return RequestVoteResponse{Term: rn.node.CurrentTerm(), VoteGranted: false}
	}
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

	oldTerm, oldVote, oldState := rn.node.CurrentTerm(), rn.node.VotedFor(), rn.node.State()
	rn.node.AdvanceTerm(rn.node.CurrentTerm() + 1)
	rn.node.SetState(Candidate)
	rn.electionTerm = rn.node.CurrentTerm()
	if err := rn.node.SetVotedFor(rn.node.ID()); err != nil {
		return
	}
	if err := rn.persistState(); err != nil {
		rn.restoreDurableState(oldTerm, oldVote, rn.node.Log())
		rn.node.SetState(oldState)
		return
	}
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
