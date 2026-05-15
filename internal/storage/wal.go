package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

type WAL struct {
	mu         sync.Mutex
	file       *os.File
	path       string
	size       int64
	dirtyCount int
	syncEvery  int // fsync batch interval (0 = every write)
}

// OpenWAL opens or creates a WAL file.
func OpenWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return &WAL{
		file:      file,
		path:      path,
		size:      stat.Size(),
		syncEvery: 100,
	}, nil
}

// Append writes a payload to the WAL and fsyncs it.
func (w *WAL) Append(payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// [length (4)] [crc32 (4)] [payload (N)]
	length := uint32(len(payload))
	crc := crc32.ChecksumIEEE(payload)

	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], length)
	binary.BigEndian.PutUint32(buf[4:8], crc)

	if _, err := w.file.Write(buf); err != nil {
		return err
	}
	if _, err := w.file.Write(payload); err != nil {
		return err
	}

	w.size += int64(8 + len(payload))
	w.dirtyCount++
	if w.syncEvery <= 0 || w.dirtyCount >= w.syncEvery {
		w.dirtyCount = 0
		return w.file.Sync()
	}
	return nil
}

// Replay reads all valid entries from the WAL.
func (w *WAL) Replay() ([][]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, err
	}

	var entries [][]byte
	reader := bufio.NewReader(w.file)
	
	for {
		header := make([]byte, 8)
		if _, err := io.ReadFull(reader, header); err != nil {
			if err == io.EOF {
				break
			}
			return entries, err // unexpected EOF or other error
		}

		length := binary.BigEndian.Uint32(header[0:4])
		crc := binary.BigEndian.Uint32(header[4:8])

		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return entries, err
		}

		if crc32.ChecksumIEEE(payload) != crc {
			return entries, fmt.Errorf("WAL crc mismatch")
		}
		
		entries = append(entries, payload)
	}

	if _, err := w.file.Seek(0, 2); err != nil {
		return entries, err
	}

	return entries, nil
}

// Truncate clears the WAL after it has been safely written to a segment.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}
	w.size = 0
	return w.file.Sync()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
