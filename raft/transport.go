package raft

type Transport interface {
	SendRequestVote(request RequestVoteRequest, to NodeID)
	SendRequestVoteResponse(response RequestVoteResponse, to NodeID, from NodeID)
	SendAppendEntries(request AppendEntriesRequest, to NodeID)
	SendAppendEntriesResponse(response AppendEntriesResponse, to NodeID, from NodeID)
}

type InMemoryTransport struct {
	peers                       map[NodeID]*RaftNode
	isolated                    map[NodeID]bool
	blocked                     map[[2]NodeID]bool
	dropAppendEntries           bool
	dropAppendEntriesResponses  bool
	delayAppendEntries          bool
	delayAppendEntriesResponses bool
	pending                     []func()
}

func NewInMemoryTransport(peers map[NodeID]*RaftNode) *InMemoryTransport {
	return &InMemoryTransport{
		peers:    peers,
		isolated: make(map[NodeID]bool),
		blocked:  make(map[[2]NodeID]bool),
	}
}

func (t *InMemoryTransport) SendRequestVote(request RequestVoteRequest, to NodeID) {
	if t.isolated[to] {
		return
	}
	if peer, ok := t.peers[to]; ok {
		peer.ReceiveRequestVote(request)
	}
}

func (t *InMemoryTransport) SendRequestVoteResponse(response RequestVoteResponse, to NodeID, from NodeID) {
	if t.isolated[to] || t.isolated[from] {
		return
	}
	if peer, ok := t.peers[to]; ok {
		peer.ReceiveRequestVoteResponse(response, from)
	}
}

func (t *InMemoryTransport) SendAppendEntries(request AppendEntriesRequest, to NodeID) {
	if t.isolated[to] || t.dropAppendEntries || t.blocked[[2]NodeID{request.LeaderID, to}] {
		return
	}
	deliver := func() {
		if peer, ok := t.peers[to]; ok {
			peer.ReceiveAppendEntries(request)
		}
	}
	if t.delayAppendEntries {
		t.pending = append(t.pending, deliver)
	} else {
		deliver()
	}
}

func (t *InMemoryTransport) SendAppendEntriesResponse(response AppendEntriesResponse, to NodeID, from NodeID) {
	if t.isolated[to] || t.isolated[from] || t.dropAppendEntriesResponses || t.blocked[[2]NodeID{from, to}] {
		return
	}
	deliver := func() {
		if peer, ok := t.peers[to]; ok {
			peer.ReceiveAppendEntriesResponse(response, from)
		}
	}
	if t.delayAppendEntriesResponses {
		t.pending = append(t.pending, deliver)
	} else {
		deliver()
	}
}

func (t *InMemoryTransport) Block(from, to NodeID)          { t.blocked[[2]NodeID{from, to}] = true }
func (t *InMemoryTransport) Unblock(from, to NodeID)        { delete(t.blocked, [2]NodeID{from, to}) }
func (t *InMemoryTransport) SetDropAppendEntries(drop bool) { t.dropAppendEntries = drop }
func (t *InMemoryTransport) SetDropAppendEntriesResponses(drop bool) {
	t.dropAppendEntriesResponses = drop
}
func (t *InMemoryTransport) SetDelayAppendEntries(delay bool) { t.delayAppendEntries = delay }
func (t *InMemoryTransport) SetDelayAppendEntriesResponses(delay bool) {
	t.delayAppendEntriesResponses = delay
}
func (t *InMemoryTransport) DeliverPending() {
	for len(t.pending) > 0 {
		message := t.pending[0]
		t.pending = t.pending[1:]
		message()
	}
}
func (t *InMemoryTransport) isolate(id NodeID) {
	t.isolated[id] = true
}
func (t *InMemoryTransport) restore(id NodeID) {
	t.isolated[id] = false
}
