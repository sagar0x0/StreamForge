package raft

import (
	"sync"
)

type LogEntry struct {
	Term    int64
	Index   int64
	Command []byte
}

type Log struct {
	mu      sync.RWMutex
	entries []LogEntry
}

func NewLog() *Log {
	return &Log{
		// start with a dummy entry at index 0 to simplify indexing
		entries: []LogEntry{{Term: 0, Index: 0}},
	}
}

func (l *Log) LastIndex() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.entries[len(l.entries)-1].Index
}

func (l *Log) LastTerm() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.entries[len(l.entries)-1].Term
}

func (l *Log) Term(index int64) int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index >= int64(len(l.entries)) || index < 0 {
		return 0
	}
	return l.entries[index].Term
}

func (l *Log) Append(entries []LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entries...)
}

func (l *Log) Truncate(index int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index <= int64(len(l.entries)) && index >= 0 {
		l.entries = l.entries[:index]
	}
}

func (l *Log) EntriesFrom(index int64) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index >= int64(len(l.entries)) {
		return nil
	}
	res := make([]LogEntry, len(l.entries)-int(index))
	copy(res, l.entries[index:])
	return res
}
