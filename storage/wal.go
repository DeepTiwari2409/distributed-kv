package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

const (
	walHeaderSize    = 8
	maxRecordPayload = 64 << 20 // 64 MiB
)

var (
	ErrClosed            = errors.New("wal is closed")
	ErrInvalidRecordType = errors.New("invalid record type")
	ErrChecksumMismatch  = errors.New("checksum mismatch")
	ErrIncompleteTail    = errors.New("incomplete tail")
	ErrRecordTooLarge    = errors.New("record too large")
)

// RecordType describes the kind of WAL operation.
type RecordType uint8

const (
	RecordPut    RecordType = 1
	RecordDelete RecordType = 2
)

// Record is a logical WAL entry for a mutation.
//
// The WAL stores operations in append order. PUT records contain both a key
// and value. DELETE records contain only a key.
type Record struct {
	Type  RecordType
	Key   string
	Value []byte
}

// WAL provides a durable append-only write-ahead log.
//
// Append is safe for concurrent use. Replay is serialized with append and close
// to ensure it never observes a partially written record.
type WAL struct {
	mu     sync.Mutex
	file   *os.File
	path   string
	closed bool
}

// Open opens an existing WAL file or creates a new one when the file does not exist.
// The WAL is opened for append and replay without truncating existing data.
func Open(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		file.Close()
		return nil, fmt.Errorf("seek wal: %w", err)
	}
	return &WAL{file: file, path: path}, nil
}

// Append writes a record to the WAL.
//
// A successful Append means the record bytes have been written to the WAL file.
// Durability to physical media is provided by Sync.
func (w *WAL) Append(record Record) error {
	if err := validateRecord(record); err != nil {
		return err
	}
	encoded, err := encodeRecord(record)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	n, err := w.file.Write(encoded)
	if err != nil {
		return fmt.Errorf("write wal: %w", err)
	}
	if n != len(encoded) {
		return io.ErrShortWrite
	}
	return nil
}

// Sync flushes WAL contents to durable storage.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync wal: %w", err)
	}
	return nil
}

// Replay returns all complete WAL records in append order.
//
// If the WAL contains an incomplete final record, Replay returns all complete
// records before the tail and ErrIncompleteTail.
func (w *WAL) Replay() ([]Record, error) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil, ErrClosed
	}
	path := w.path
	w.mu.Unlock()

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open wal for replay: %w", err)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat wal: %w", err)
	}
	limit := io.LimitReader(file, fi.Size())
	records, err := readRecords(limit)
	if err != nil {
		return records, err
	}
	return records, nil
}

// Close closes the WAL file. Subsequent operations return ErrClosed.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	w.closed = true
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close wal: %w", err)
	}
	return nil
}

func validateRecord(record Record) error {
	switch record.Type {
	case RecordPut, RecordDelete:
	default:
		return fmt.Errorf("validate record: %w: %d", ErrInvalidRecordType, record.Type)
	}
	if len(record.Key) > maxRecordPayload {
		return fmt.Errorf("validate record: %w: key length %d", ErrRecordTooLarge, len(record.Key))
	}
	payloadLen := 1 + 4 + len(record.Key)
	if record.Type == RecordPut {
		payloadLen += 1
		if record.Value != nil {
			payloadLen += 4 + len(record.Value)
		}
	}
	if payloadLen > maxRecordPayload {
		return fmt.Errorf("validate record: %w: payload length %d", ErrRecordTooLarge, payloadLen)
	}
	return nil
}

func encodeRecord(record Record) ([]byte, error) {
	keyBytes := []byte(record.Key)
	payloadLen := 1 + 4 + len(keyBytes)
	if record.Type == RecordPut {
		payloadLen += 1
		if record.Value != nil {
			payloadLen += 4 + len(record.Value)
		}
	}
	if payloadLen > maxRecordPayload {
		return nil, fmt.Errorf("encode record: %w: payload length %d", ErrRecordTooLarge, payloadLen)
	}

	buf := make([]byte, walHeaderSize+payloadLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(payloadLen))
	// checksum is written after payload is filled.
	off := walHeaderSize
	buf[off] = byte(record.Type)
	off++
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(keyBytes)))
	off += 4
	copy(buf[off:off+len(keyBytes)], keyBytes)
	off += len(keyBytes)
	if record.Type == RecordPut {
		if record.Value == nil {
			buf[off] = 0
			off++
		} else {
			buf[off] = 1
			off++
			binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(record.Value)))
			off += 4
			copy(buf[off:off+len(record.Value)], record.Value)
			off += len(record.Value)
		}
	}
	checksum := crc32.ChecksumIEEE(buf[walHeaderSize : walHeaderSize+payloadLen])
	binary.BigEndian.PutUint32(buf[4:8], checksum)
	return buf, nil
}

func readRecords(reader io.Reader) ([]Record, error) {
	var records []Record
	header := make([]byte, walHeaderSize)
	for {
		n, err := io.ReadFull(reader, header)
		if err != nil {
			if errors.Is(err, io.EOF) && n == 0 {
				return records, nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return records, fmt.Errorf("read header: %w", ErrIncompleteTail)
			}
			return records, fmt.Errorf("read header: %w", err)
		}

		payloadLen := binary.BigEndian.Uint32(header[0:4])
		checksum := binary.BigEndian.Uint32(header[4:8])
		if payloadLen == 0 || payloadLen > maxRecordPayload {
			return records, fmt.Errorf("read record: %w: payload length %d", ErrRecordTooLarge, payloadLen)
		}
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(reader, payload); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return records, fmt.Errorf("read payload: %w", ErrIncompleteTail)
			}
			return records, fmt.Errorf("read payload: %w", err)
		}
		if got := crc32.ChecksumIEEE(payload); got != checksum {
			return records, fmt.Errorf("read record: %w", ErrChecksumMismatch)
		}
		record, err := decodePayload(payload)
		if err != nil {
			return records, err
		}
		records = append(records, record)
	}
}

func decodePayload(payload []byte) (Record, error) {
	if len(payload) < 5 {
		return Record{}, fmt.Errorf("decode payload: %w: payload too short", ErrIncompleteTail)
	}
	recordType := RecordType(payload[0])
	keyLen := binary.BigEndian.Uint32(payload[1:5])
	if keyLen > uint32(len(payload)-5) {
		return Record{}, fmt.Errorf("decode payload: %w: key length %d", ErrIncompleteTail, keyLen)
	}
	off := 5
	key := string(payload[off : off+int(keyLen)])
	off += int(keyLen)
	switch recordType {
	case RecordPut:
		if len(payload[off:]) < 1 {
			return Record{}, fmt.Errorf("decode payload: %w: missing nil flag", ErrIncompleteTail)
		}
		nilFlag := payload[off]
		off++
		if nilFlag > 1 {
			return Record{}, fmt.Errorf("decode payload: %w: invalid nil flag %d", ErrInvalidRecordType, nilFlag)
		}
		if nilFlag == 0 {
			if off != len(payload) {
				return Record{}, fmt.Errorf("decode payload: %w: unexpected extra bytes", ErrIncompleteTail)
			}
			return Record{Type: recordType, Key: key, Value: nil}, nil
		}
		if len(payload[off:]) < 4 {
			return Record{}, fmt.Errorf("decode payload: %w: missing value length", ErrIncompleteTail)
		}
		valueLen := binary.BigEndian.Uint32(payload[off : off+4])
		off += 4
		if valueLen > uint32(len(payload)-off) {
			return Record{}, fmt.Errorf("decode payload: %w: value length %d", ErrIncompleteTail, valueLen)
		}
		if off+int(valueLen) != len(payload) {
			return Record{}, fmt.Errorf("decode payload: %w: unexpected extra bytes", ErrIncompleteTail)
		}
		value := make([]byte, valueLen)
		if valueLen > 0 {
			copy(value, payload[off:off+int(valueLen)])
		}
		return Record{Type: recordType, Key: key, Value: value}, nil
	case RecordDelete:
		if off != len(payload) {
			return Record{}, fmt.Errorf("decode payload: %w: unexpected extra bytes", ErrIncompleteTail)
		}
		return Record{Type: recordType, Key: key}, nil
	default:
		return Record{}, fmt.Errorf("decode payload: %w: %d", ErrInvalidRecordType, recordType)
	}
}
