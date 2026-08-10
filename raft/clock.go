package raft

import (
	"sync"
	"time"
)

type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration, callback func()) Timer
}

type Timer interface {
	Reset(time.Duration)
	Stop()
}

type DeterministicClock struct {
	mu     sync.Mutex
	now    time.Time
	timers map[int]*deterministicTimer
	nextID int
}

type deterministicTimer struct {
	id       int
	clock    *DeterministicClock
	deadline time.Time
	callback func()
	active   bool
}

func NewDeterministicClock(start time.Time) *DeterministicClock {
	return &DeterministicClock{now: start, timers: make(map[int]*deterministicTimer)}
}

func (c *DeterministicClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *DeterministicClock) NewTimer(d time.Duration, callback func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.nextID
	c.nextID++
	if d < 0 {
		d = 0
	}
	deadline := c.now.Add(d)
	t := &deterministicTimer{id: id, clock: c, deadline: deadline, callback: callback, active: true}
	c.timers[id] = t
	return t
}

func (c *DeterministicClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	for {
		due := make([]*deterministicTimer, 0)
		for _, t := range c.timers {
			if t.active && !t.deadline.After(c.now) {
				due = append(due, t)
			}
		}
		if len(due) == 0 {
			c.mu.Unlock()
			return
		}
		for _, t := range due {
			t.active = false
		}
		c.mu.Unlock()
		for _, t := range due {
			t.callback()
		}
		c.mu.Lock()
	}
}

func (t *deterministicTimer) Reset(d time.Duration) {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if d < 0 {
		d = 0
	}
	t.deadline = t.clock.now.Add(d)
	t.active = true
}

func (t *deterministicTimer) Stop() {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	t.active = false
}
