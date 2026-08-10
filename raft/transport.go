package raft

type Transport interface {
	SendRequestVote(request RequestVoteRequest, to NodeID)
	SendRequestVoteResponse(response RequestVoteResponse, to NodeID, from NodeID)
}

type InMemoryTransport struct {
	peers map[NodeID]*RaftNode
}

func NewInMemoryTransport(peers map[NodeID]*RaftNode) *InMemoryTransport {
	return &InMemoryTransport{peers: peers}
}

func (t *InMemoryTransport) SendRequestVote(request RequestVoteRequest, to NodeID) {
	if peer, ok := t.peers[to]; ok {
		peer.ReceiveRequestVote(request)
	}
}

func (t *InMemoryTransport) SendRequestVoteResponse(response RequestVoteResponse, to NodeID, from NodeID) {
	if peer, ok := t.peers[to]; ok {
		peer.ReceiveRequestVoteResponse(response, from)
	}
}
