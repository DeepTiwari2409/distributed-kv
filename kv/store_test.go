package kv

import (
	"bytes"
	"sync"
	"testing"

	"github.com/DeepTiwari2409/distributed-kv/raft"
)

func TestNewStore(t *testing.T) {
	s := NewStore()
	if s == nil {
		t.Fatal("expected non-nil store")
	}
	if _, ok := s.Get("missing"); ok {
		t.Fatal("expected empty store")
	}
}

func TestApplyRaftCommands(t *testing.T) {
	s := NewStore()
	if err := s.Apply(raft.NewPutCommand("key", []byte("value"))); err != nil {
		t.Fatal(err)
	}
	if value, ok := s.Get("key"); !ok || !bytes.Equal(value, []byte("value")) {
		t.Fatal("PUT command was not applied")
	}
	if err := s.Apply(raft.NewDeleteCommand("key")); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get("key"); ok {
		t.Fatal("DELETE command was not applied")
	}
}

func TestPutAndGet(t *testing.T) {
	s := NewStore()
	value := []byte("value")
	if err := s.Put("key", value); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Get("key")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("expected %v, got %v", value, got)
	}
}

func TestPutOverwrite(t *testing.T) {
	s := NewStore()
	if err := s.Put("key", []byte("A")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("key", []byte("B")); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Get("key")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if !bytes.Equal(got, []byte("B")) {
		t.Fatalf("expected B, got %v", got)
	}
}

func TestGetMissingKey(t *testing.T) {
	s := NewStore()
	got, ok := s.Get("missing")
	if ok {
		t.Fatal("expected missing key")
	}
	if got != nil {
		t.Fatalf("expected nil value for missing key, got %v", got)
	}
}

func TestDeleteExistingKey(t *testing.T) {
	s := NewStore()
	if err := s.Put("key", []byte("value")); err != nil {
		t.Fatal(err)
	}
	if !s.Delete("key") {
		t.Fatal("expected delete to return true")
	}
	if _, ok := s.Get("key"); ok {
		t.Fatal("expected key to be missing after delete")
	}
}

func TestDeleteMissingKey(t *testing.T) {
	s := NewStore()
	if s.Delete("missing") {
		t.Fatal("expected delete on missing key to return false")
	}
}

func TestEmptyKey(t *testing.T) {
	s := NewStore()
	if err := s.Put("", []byte("value")); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Get("")
	if !ok {
		t.Fatal("expected empty key to exist")
	}
	if !bytes.Equal(got, []byte("value")) {
		t.Fatalf("expected value, got %v", got)
	}
}

func TestEmptyValue(t *testing.T) {
	s := NewStore()
	if err := s.Put("key", []byte{}); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Get("key")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if got == nil {
		t.Fatal("expected empty slice, got nil")
	}
	if len(got) != 0 {
		t.Fatalf("expected empty slice, got length %d", len(got))
	}
}

func TestBinaryValue(t *testing.T) {
	s := NewStore()
	value := []byte{0, 255, 1, 2, 3, 128}
	if err := s.Put("key", value); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Get("key")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("expected %v, got %v", value, got)
	}
}

func TestPutInputIsCopied(t *testing.T) {
	s := NewStore()
	value := []byte("mutable")
	if err := s.Put("key", value); err != nil {
		t.Fatal(err)
	}
	value[0] = 'M'
	got, ok := s.Get("key")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if bytes.Equal(got, value) {
		t.Fatalf("expected stored value to remain unchanged, got %v", got)
	}
}

func TestGetOutputIsCopied(t *testing.T) {
	s := NewStore()
	if err := s.Put("key", []byte("value")); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Get("key")
	if !ok {
		t.Fatal("expected key to exist")
	}
	got[0] = 'V'
	got2, ok := s.Get("key")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if bytes.Equal(got2, got) {
		t.Fatalf("expected store copy to remain unchanged, got %v", got2)
	}
}

func TestDeleteAndReinsert(t *testing.T) {
	s := NewStore()
	if err := s.Put("key", []byte("first")); err != nil {
		t.Fatal(err)
	}
	if !s.Delete("key") {
		t.Fatal("expected delete to succeed")
	}
	if err := s.Put("key", []byte("second")); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Get("key")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if !bytes.Equal(got, []byte("second")) {
		t.Fatalf("expected second, got %v", got)
	}
}

func TestMultipleKeys(t *testing.T) {
	s := NewStore()
	entries := map[string][]byte{
		"a": []byte("1"),
		"b": []byte("2"),
		"c": []byte("3"),
	}
	for k, v := range entries {
		if err := s.Put(k, v); err != nil {
			t.Fatal(err)
		}
	}
	for k, want := range entries {
		got, ok := s.Get(k)
		if !ok {
			t.Fatalf("expected key %q", k)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("key %q expected %v, got %v", k, want, got)
		}
	}
}

func TestConcurrentReads(t *testing.T) {
	s := NewStore()
	const count = 100
	for i := 0; i < count; i++ {
		s.Put(string(rune('a'+i)), []byte{byte(i)})
	}
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		key := string(rune('a' + i))
		want := []byte{byte(i)}
		go func() {
			defer wg.Done()
			got, ok := s.Get(key)
			if !ok || !bytes.Equal(got, want) {
				t.Errorf("key %q expected %v, got %v", key, want, got)
			}
		}()
	}
	wg.Wait()
}

func TestConcurrentWrites(t *testing.T) {
	s := NewStore()
	const count = 100
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		key := string(rune('A' + i))
		value := []byte{byte(i)}
		go func() {
			defer wg.Done()
			if err := s.Put(key, value); err != nil {
				t.Errorf("put error: %v", err)
			}
		}()
	}
	wg.Wait()
	for i := 0; i < count; i++ {
		key := string(rune('A' + i))
		want := []byte{byte(i)}
		got, ok := s.Get(key)
		if !ok || !bytes.Equal(got, want) {
			t.Fatalf("key %q expected %v, got %v", key, want, got)
		}
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	s := NewStore()
	const writers = 20
	const readers = 20
	const iterations = 100
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := string(rune('a' + (writer % 26)))
				s.Put(key, []byte{byte(writer), byte(j)})
				if j%3 == 0 {
					s.Delete(key)
				}
			}
		}(i)
	}
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(reader int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := string(rune('a' + (j % 26)))
				_, _ = s.Get(key)
			}
		}(i)
	}
	wg.Wait()
}

func TestConcurrentOverwrite(t *testing.T) {
	s := NewStore()
	const goroutines = 50
	const iterations = 1000
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = s.Put("shared", []byte{byte(id), byte(j)})
			}
		}(i)
	}
	wg.Wait()
	got, ok := s.Get("shared")
	if !ok {
		t.Fatal("expected shared key to exist")
	}
	if len(got) != 2 {
		t.Fatalf("expected stored value length 2, got %d", len(got))
	}
}

func TestNilValueIsStored(t *testing.T) {
	s := NewStore()
	if err := s.Put("key", nil); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Get("key")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if got != nil {
		t.Fatalf("expected nil value, got %v", got)
	}
}
