package storage

import (
	"encoding/binary"
	"os"
	"sync"
)

type IndexEntry struct {
	RelativeOffset uint32
	Position       uint32
}

type Index struct {
	mu      sync.RWMutex
	file    *os.File
	path    string
	entries []IndexEntry
}

func NewIndex(path string) (*Index, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	
	idx := &Index{
		file: file,
		path: path,
	}

	stat, err := file.Stat()
	if err == nil && stat.Size() > 0 {
		data := make([]byte, stat.Size())
		if _, err := file.ReadAt(data, 0); err == nil {
			for i := 0; i < len(data); i += 8 {
				if i+8 <= len(data) {
					idx.entries = append(idx.entries, IndexEntry{
						RelativeOffset: binary.BigEndian.Uint32(data[i : i+4]),
						Position:       binary.BigEndian.Uint32(data[i+4 : i+8]),
					})
				}
			}
		}
	}
	return idx, nil
}

func (idx *Index) Append(relOffset uint32, pos uint32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], relOffset)
	binary.BigEndian.PutUint32(buf[4:8], pos)

	if _, err := idx.file.Write(buf); err != nil {
		return err
	}

	idx.entries = append(idx.entries, IndexEntry{
		RelativeOffset: relOffset,
		Position:       pos,
	})
	return nil
}

func (idx *Index) Lookup(relOffset uint32) (IndexEntry, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.entries) == 0 {
		return IndexEntry{}, false
	}

	low := 0
	high := len(idx.entries) - 1
	var best IndexEntry
	found := false

	for low <= high {
		mid := low + (high-low)/2
		if idx.entries[mid].RelativeOffset == relOffset {
			return idx.entries[mid], true
		} else if idx.entries[mid].RelativeOffset < relOffset {
			best = idx.entries[mid]
			found = true
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return best, found
}

func (idx *Index) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.file.Close()
}

func (idx *Index) Remove() error {
	idx.Close()
	return os.Remove(idx.path)
}
