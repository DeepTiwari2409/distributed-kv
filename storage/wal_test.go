package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
)

func TestOpenCreatesWAL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Append(Record{Type: RecordPut, Key: "a", Value: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Type: RecordDelete, Key: "a"}); err != nil {
		t.Fatal(err)
	}
	records, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if records[0].Type != RecordPut || records[0].Key != "a" || !bytes.Equal(records[0].Value, []byte("1")) {
		t.Fatalf("unexpected first record: %v", records[0])
	}
	if records[1].Type != RecordDelete || records[1].Key != "a" {
		t.Fatalf("unexpected second record: %v", records[1])
	}
}

func TestPutRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Append(Record{Type: RecordPut, Key: "key", Value: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	records, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].Type != RecordPut || records[0].Key != "key" || !bytes.Equal(records[0].Value, []byte("value")) {
		t.Fatalf("unexpected record: %v", records[0])
	}
}

func TestDeleteRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Append(Record{Type: RecordDelete, Key: "key"}); err != nil {
		t.Fatal(err)
	}
	records, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].Type != RecordDelete || records[0].Key != "key" {
		t.Fatalf("unexpected record: %v", records[0])
	}
}

func TestBinaryValue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	payload := []byte{0, 255, 1, 2, 3, 128, 200}
	if err := w.Append(Record{Type: RecordPut, Key: "binary", Value: payload}); err != nil {
		t.Fatal(err)
	}
	records, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(records[0].Value, payload) {
		t.Fatalf("expected %v, got %v", payload, records[0].Value)
	}
}

func TestEmptyKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Append(Record{Type: RecordPut, Key: "", Value: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	records, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if records[0].Key != "" {
		t.Fatalf("expected empty key, got %q", records[0].Key)
	}
}

func TestEmptyValue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Append(Record{Type: RecordPut, Key: "key", Value: []byte{}}); err != nil {
		t.Fatal(err)
	}
	records, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if records[0].Value == nil {
		t.Fatal("expected empty slice, got nil")
	}
	if len(records[0].Value) != 0 {
		t.Fatalf("expected empty slice, got length %d", len(records[0].Value))
	}
}

func TestMultipleAppendsPreserveOrder(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for i := 0; i < 10; i++ {
		if err := w.Append(Record{Type: RecordPut, Key: string('a' + rune(i)), Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	records, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 10 {
		t.Fatalf("expected 10 records, got %d", len(records))
	}
	for i, rec := range records {
		if rec.Key != string(rune('a'+i)) {
			t.Fatalf("record %d expected key %q, got %q", i, string(rune('a'+i)), rec.Key)
		}
	}
}

func TestReopenPreservesRecords(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	{
		w, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Append(Record{Type: RecordPut, Key: "key", Value: []byte("value")}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	records, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Key != "key" {
		t.Fatalf("unexpected records after reopen: %v", records)
	}
}

func TestAppendAfterReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Type: RecordPut, Key: "a", Value: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Append(Record{Type: RecordPut, Key: "b", Value: []byte("2")}); err != nil {
		t.Fatal(err)
	}
	records, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].Key != "a" || records[1].Key != "b" {
		t.Fatalf("unexpected records: %v", records)
	}
}

func TestSync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Append(Record{Type: RecordPut, Key: "key", Value: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
}

func TestChecksumCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Type: RecordPut, Key: "key", Value: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Seek(walHeaderSize+1, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xff}); err != nil {
		t.Fatal(err)
	}
	w, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	_, err = w.Replay()
	if err == nil || !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected checksum mismatch, got %v", err)
	}
}

func TestTruncatedHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	f.Close()
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	_, err = w.Replay()
	if err == nil || !errors.Is(err, ErrIncompleteTail) {
		t.Fatalf("expected incomplete tail, got %v", err)
	}
}

func TestTruncatedPayload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte{byte(RecordPut), 0, 0, 0, 3, 'k', 'e', 'y'}
	header := make([]byte, walHeaderSize)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(payload)+4))
	binary.BigEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))
	if _, err := f.Write(header); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(payload); err != nil {
		t.Fatal(err)
	}
	f.Close()
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	_, err = w.Replay()
	if err == nil || !errors.Is(err, ErrIncompleteTail) {
		t.Fatalf("expected incomplete tail, got %v", err)
	}
}

func TestCorruptedMiddleRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Type: RecordPut, Key: "a", Value: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Type: RecordPut, Key: "b", Value: []byte("2")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Type: RecordPut, Key: "c", Value: []byte("3")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	header := make([]byte, walHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		t.Fatal(err)
	}
	payloadLen := binary.BigEndian.Uint32(header[0:4])
	if _, err := f.Seek(int64(walHeaderSize+payloadLen+1), io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xff}); err != nil {
		t.Fatal(err)
	}
	w, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	_, err = w.Replay()
	if err == nil {
		t.Fatalf("expected corrupt middle record error, got nil")
	}
}

func TestInvalidRecordType(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte{0xff, 0, 0, 0, 0}
	header := make([]byte, walHeaderSize)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))
	if _, err := f.Write(header); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(payload); err != nil {
		t.Fatal(err)
	}
	f.Close()
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	_, err = w.Replay()
	if err == nil || !errors.Is(err, ErrInvalidRecordType) {
		t.Fatalf("expected invalid record type, got %v", err)
	}
}

func TestUnreasonableRecordLength(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	header := make([]byte, walHeaderSize)
	binary.BigEndian.PutUint32(header[0:4], maxRecordPayload+1)
	binary.BigEndian.PutUint32(header[4:8], 0)
	if _, err := f.Write(header); err != nil {
		t.Fatal(err)
	}
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	_, err = w.Replay()
	if err == nil || !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("expected record too large, got %v", err)
	}
}

func TestNoDataLossAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	const count = 100
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		if err := w.Append(Record{Type: RecordPut, Key: string('a' + rune(i)), Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	records, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != count {
		t.Fatalf("expected %d records, got %d", count, len(records))
	}
	for i, rec := range records {
		if rec.Key != string(rune('a'+i)) || len(rec.Value) != 1 || rec.Value[0] != byte(i) {
			t.Fatalf("record %d mismatch: %v", i, rec)
		}
	}
}

func TestConcurrentAppend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	const goroutines = 20
	const recordsPer = 50
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < recordsPer; j++ {
				if err := w.Append(Record{Type: RecordPut, Key: strconv.Itoa(id), Value: []byte{byte(j)}}); err != nil {
					t.Errorf("append error: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()
	records, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != goroutines*recordsPer {
		t.Fatalf("expected %d records, got %d", goroutines*recordsPer, len(records))
	}
}

func TestClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOperationsAfterClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Type: RecordPut, Key: "key", Value: []byte("value")}); err == nil || !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if err := w.Sync(); err == nil || !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if _, err := w.Replay(); err == nil || !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestRepeatedOpenClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	for i := 0; i < 5; i++ {
		w, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Append(Record{Type: RecordPut, Key: string('a' + rune(i)), Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	records, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 5 {
		t.Fatalf("expected 5 records, got %d", len(records))
	}
}

func TestRecoveryWithTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Type: RecordPut, Key: "a", Value: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Type: RecordPut, Key: "b", Value: []byte("2")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(-2, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(fi.Size() - 2); err != nil {
		t.Fatal(err)
	}
	f.Close()
	w, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	records, err := w.Replay()
	if err == nil || !errors.Is(err, ErrIncompleteTail) {
		t.Fatalf("expected incomplete tail, got %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 complete record, got %d", len(records))
	}
}

func TestFuzzDecodeReplay(t *testing.T) {
	data := []byte("not a wal")
	records, err := readRecords(bytes.NewReader(data))
	if err == nil {
		t.Fatalf("expected error, got records %v", records)
	}
}

func TestRandomizedReplayProperty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	const count = 50
	for i := 0; i < count; i++ {
		if err := w.Append(Record{Type: RecordPut, Key: string(rune('a' + (i % 26))), Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	records, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != count {
		t.Fatalf("expected %d records, got %d", count, len(records))
	}
}
