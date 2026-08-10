package raft

import (
	"bytes"
	"testing"
)

func TestEmptyLog(t *testing.T) {
	log := NewRaftLog()
	if log.LastIndex() != 0 {
		t.Fatal("expected empty log last index 0")
	}
	if log.LastTerm() != 0 {
		t.Fatal("expected empty log last term 0")
	}
}

func TestAppendEntry(t *testing.T) {
	log := NewRaftLog()
	entry := LogEntry{Index: 1, Term: 1, Command: NewPutCommand("k", []byte("v"))}
	if err := log.Append(entry); err != nil {
		t.Fatal(err)
	}
	if log.LastIndex() != 1 {
		t.Fatal("expected last index 1")
	}
}

func TestAppendMultipleEntries(t *testing.T) {
	log := NewRaftLog()
	for i := 1; i <= 3; i++ {
		entry := LogEntry{Index: uint64(i), Term: 1, Command: NewPutCommand(string(rune('a'+i-1)), []byte{byte(i)})}
		if err := log.Append(entry); err != nil {
			t.Fatal(err)
		}
	}
	if log.LastIndex() != 3 {
		t.Fatal("expected 3 entries")
	}
}

func TestLastTerm(t *testing.T) {
	log := NewRaftLog()
	log.Append(LogEntry{Index: 1, Term: 1, Command: NewPutCommand("k", []byte("v"))})
	log.Append(LogEntry{Index: 2, Term: 2, Command: NewPutCommand("k2", []byte("v2"))})
	if log.LastTerm() != 2 {
		t.Fatal("expected last term 2")
	}
}

func TestGetEntry(t *testing.T) {
	log := NewRaftLog()
	entry := LogEntry{Index: 1, Term: 1, Command: NewPutCommand("k", []byte("v"))}
	log.Append(entry)
	got, err := log.Entry(1)
	if err != nil {
		t.Fatal(err)
	}
	if got.Index != 1 || got.Term != 1 || got.Command.Key != "k" {
		t.Fatal("unexpected entry")
	}
}

func TestGetMissingEntry(t *testing.T) {
	log := NewRaftLog()
	if _, err := log.Entry(1); err == nil {
		t.Fatal("expected error for missing entry")
	}
}

func TestGetRange(t *testing.T) {
	log := NewRaftLog()
	for i := 1; i <= 3; i++ {
		log.Append(LogEntry{Index: uint64(i), Term: 1, Command: NewPutCommand(string(rune('a'+i-1)), []byte{byte(i)})})
	}
	entries, err := log.Entries(1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatal("expected 3 entries")
	}
}

func TestGetInvalidRange(t *testing.T) {
	log := NewRaftLog()
	if _, err := log.Entries(2, 1); err == nil {
		t.Fatal("expected error for invalid range")
	}
}

func TestLogPreservesOrder(t *testing.T) {
	log := NewRaftLog()
	for i := 1; i <= 3; i++ {
		log.Append(LogEntry{Index: uint64(i), Term: 1, Command: NewPutCommand(string(rune('a'+i-1)), []byte{byte(i)})})
	}
	entries, _ := log.Entries(1, 3)
	for i, entry := range entries {
		if entry.Index != uint64(i+1) {
			t.Fatalf("expected index %d, got %d", i+1, entry.Index)
		}
	}
}

func TestLogIndexesIncrease(t *testing.T) {
	log := NewRaftLog()
	log.Append(LogEntry{Index: 1, Term: 1, Command: NewPutCommand("k", []byte("v"))})
	if err := log.Append(LogEntry{Index: 1, Term: 1, Command: NewPutCommand("k2", []byte("v2"))}); err == nil {
		t.Fatal("expected duplicate index rejection")
	}
}

func TestInvalidTermRejected(t *testing.T) {
	log := NewRaftLog()
	if err := log.Append(LogEntry{Index: 1, Term: 0, Command: NewPutCommand("k", []byte("v"))}); err == nil {
		t.Fatal("expected invalid term rejection")
	}
}

func TestReturnedEntryDoesNotExposeMutableState(t *testing.T) {
	log := NewRaftLog()
	log.Append(LogEntry{Index: 1, Term: 1, Command: NewPutCommand("k", []byte("v"))})
	entry, _ := log.Entry(1)
	entry.Command.Value[0] = 'x'
	entry2, _ := log.Entry(1)
	if bytes.Equal(entry2.Command.Value, []byte("x")) {
		t.Fatal("expected defensive copy")
	}
}

func TestReturnedRangeDoesNotExposeInternalSlice(t *testing.T) {
	log := NewRaftLog()
	log.Append(LogEntry{Index: 1, Term: 1, Command: NewPutCommand("k", []byte("v"))})
	entries, _ := log.Entries(1, 1)
	entries[0].Command.Value[0] = 'x'
	entry2, _ := log.Entry(1)
	if bytes.Equal(entry2.Command.Value, []byte("x")) {
		t.Fatal("expected defensive range copy")
	}
}
