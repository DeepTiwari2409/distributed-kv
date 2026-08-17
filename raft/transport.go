package raft

type Transport interface {
	SendRequestVote(request RequestVoteRequest, to NodeID)
	SendRequestVoteResponse(response RequestVoteResponse, to NodeID, from NodeID)
}

type InMemoryTransport struct {
	peers    map[NodeID]*RaftNode
	isolated map[NodeID]bool
}

func NewInMemoryTransport(peers map[NodeID]*RaftNode) *InMemoryTransport {
	return &InMemoryTransport{
		peers:    peers,
		isolated: make(map[NodeID]bool),
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

// isolate blocks all communication to and from a node.
func (t *InMemoryTransport) isolate(id NodeID) {
	t.isolated[id] = true
}

// restore re-enables communication to and from a node.
func (t *InMemoryTransport) restore(id NodeID) {
	t.isolated[id] = false
}
