package kv

import "sync"

// Store is a thread-safe in-memory key-value store.
//
// It is designed to serve as the deterministic state machine that a Raft
// consensus layer will eventually apply. This implementation does not provide
// persistence, networking, or Raft.
type Store struct {
	mu sync.RWMutex
	m  map[string][]byte
}

// NewStore creates and returns an initialized Store.
func NewStore() *Store {
	return &Store{m: make(map[string][]byte)}
}

// Put stores a copy of value under key.
//
// If value is nil, it is treated as a valid empty value. Get will return
// nil, true for a key with a nil value. The caller may modify the input slice
// after Put without affecting stored state.
func (s *Store) Put(key string, value []byte) error {
	if s == nil {
		return nil
	}
	copyValue := cloneBytes(value)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = copyValue
	return nil
}

// Get returns a copy of the value stored under key and true if key exists.
//
// If the key does not exist, Get returns nil, false. The returned slice is a
// copy and callers may safely modify it without affecting the store.
func (s *Store) Get(key string) ([]byte, bool) {
	if s == nil {
		return nil, false
	}

	s.mu.RLock()
	value, ok := s.m[key]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return cloneBytes(value), true
}

// Delete removes key from the store.
//
// It returns true if the key was present and deleted. Deleting a missing key
// returns false and does not modify the store.
func (s *Store) Delete(key string) bool {
	if s == nil {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.m[key]; !ok {
		return false
	}
	delete(s.m, key)
	return true
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	copyValue := make([]byte, len(value))
	copy(copyValue, value)
	return copyValue
}
