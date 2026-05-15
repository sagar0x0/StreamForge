package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

type Segment struct {
	mu         sync.RWMutex
	file       *os.File
	path       string
	BaseOffset int64
	size       int64
	maxSize    int64
}

// NewSegment opens a new or existing segment.
func NewSegment(path string, baseOffset int64, maxSize int64) (*Segment, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return &Segment{
		file:       file,
		path:       path,
		BaseOffset: baseOffset,
		size:       stat.Size(),
		maxSize:    maxSize,
	}, nil
}

// Append writes data to segment and returns its byte position relative to start.
func (s *Segment) Append(offset int64, data []byte) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.size >= s.maxSize {
		return 0, fmt.Errorf("segment full")
	}

	pos := s.size

	// [length (4)] [data (N)]
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(len(data)))

	if _, err := s.file.Write(buf); err != nil {
		return 0, err
	}
	if _, err := s.file.Write(data); err != nil {
		return 0, err
	}

	s.size += int64(4 + len(data))
	return pos, nil
}

// Read reads a record at the specified byte position.
func (s *Segment) Read(pos int64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	lenBuf := make([]byte, 4)
	if _, err := s.file.ReadAt(lenBuf, pos); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(lenBuf)
	data := make([]byte, length)
	
	if _, err := s.file.ReadAt(data, pos+4); err != nil {
		return nil, err
	}

	return data, nil
}

func (s *Segment) IsFull() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size >= s.maxSize
}

func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}

func (s *Segment) Remove() error {
	s.Close()
	return os.Remove(s.path)
}
