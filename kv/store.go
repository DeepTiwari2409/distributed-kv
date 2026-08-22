package kv

import "sync"

type Store struct {
	mu sync.RWMutex
	m  map[string][]byte
}

func NewStore() *Store {
	return &Store{m: make(map[string][]byte)}
}
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
