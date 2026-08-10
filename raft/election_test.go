package raft

import (
	"testing"
	"time"
)

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

func TestSingleNodeBecomesLeaderAfterElectionTimeout(t *testing.T) {
	clock := NewDeterministicClock(time.Unix(0, 0))
	node, err := NewNode(1)
	if err != nil {
		t.Fatal(err)
	}
	rn, err := NewRaftNode(node, nil, NewInMemoryTransport(make(map[NodeID]*RaftNode)), clock, 150*time.Millisecond, 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	rn.Start()

	clock.Advance(150 * time.Millisecond)

	if node.State() != Leader {
		t.Fatalf("expected node to become leader, got %s", node.State())
	}
}

func TestThreeNodeElectionSucceedsWithMajority(t *testing.T) {
	ids := []NodeID{1, 2, 3}
	startTimes := []time.Time{
		time.Unix(0, 0),
		time.Unix(0, 0).Add(1 * time.Nanosecond),
		time.Unix(0, 0).Add(2 * time.Nanosecond),
	}
	transportMap := make(map[NodeID]*RaftNode)
	transport := NewInMemoryTransport(transportMap)
	cluster := make(map[NodeID]*RaftNode)

	for i, id := range ids {
		node, err := NewNode(id)
		if err != nil {
			t.Fatal(err)
		}
		peers := make([]NodeID, 0, len(ids)-1)
		for _, peerID := range ids {
			if peerID != id {
				peers = append(peers, peerID)
			}
		}
		clock := NewDeterministicClock(startTimes[i])
		rn, err := NewRaftNode(node, peers, transport, clock, 150*time.Millisecond, 300*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		cluster[id] = rn
		transportMap[id] = rn
	}

	for _, rn := range cluster {
		rn.Start()
	}

	cluster[1].clock.(*DeterministicClock).Advance(150 * time.Millisecond)

	leaderCount := 0
	for _, rn := range cluster {
		if rn.node.State() == Leader {
			leaderCount++
		}
	}

	if leaderCount != 1 {
		t.Fatalf("expected exactly one leader, got %d", leaderCount)
	}
}
