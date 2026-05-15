package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sagar/streamforge/pkg/types"
)

type Partition struct {
	mu           sync.RWMutex
	dir          string
	wal          *WAL
	segments     []*Segment
	indices      []*Index
	activeSeg    *Segment
	activeIndex  *Index
	nextOffset   int64
	maxSegSize   int64
	indexCounter int
}

func OpenPartition(dir string, maxSegSize int64) (*Partition, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	walFile := filepath.Join(dir, "wal.log")
	wal, err := OpenWAL(walFile)
	if err != nil {
		return nil, err
	}

	p := &Partition{
		dir:        dir,
		wal:        wal,
		maxSegSize: maxSegSize,
	}

	if err := p.loadSegments(); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *Partition) loadSegments() error {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return err
	}

	var bases []int64
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".log") && entry.Name() != "wal.log" {
			baseStr := strings.TrimSuffix(entry.Name(), ".log")
			base, err := strconv.ParseInt(baseStr, 10, 64)
			if err == nil {
				bases = append(bases, base)
			}
		}
	}

	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })

	for _, base := range bases {
		segPath := filepath.Join(p.dir, fmt.Sprintf("%020d.log", base))
		idxPath := filepath.Join(p.dir, fmt.Sprintf("%020d.index", base))

		seg, err := NewSegment(segPath, base, p.maxSegSize)
		if err != nil {
			return err
		}
		p.segments = append(p.segments, seg)

		idx, err := NewIndex(idxPath)
		if err != nil {
			return err
		}
		p.indices = append(p.indices, idx)
	}

	if len(p.segments) == 0 {
		return p.rotate()
	}

	p.activeSeg = p.segments[len(p.segments)-1]
	p.activeIndex = p.indices[len(p.indices)-1]

	p.nextOffset = p.activeSeg.BaseOffset
	
	entriesReplay, err := p.wal.Replay()
	if err != nil {
		return err
	}
	
	for _, payload := range entriesReplay {
		_, _ = p.activeSeg.Append(p.nextOffset, payload)
		p.nextOffset++
	}
	_ = p.wal.Truncate()

	// Update nextOffset roughly from active segment size (simplification without scanning)
	// In a real system, we'd recover the last exact offset.
	p.nextOffset = p.activeSeg.BaseOffset + int64(len(entriesReplay))

	return nil
}

func (p *Partition) rotate() error {
	base := p.nextOffset
	segPath := filepath.Join(p.dir, fmt.Sprintf("%020d.log", base))
	idxPath := filepath.Join(p.dir, fmt.Sprintf("%020d.index", base))

	seg, err := NewSegment(segPath, base, p.maxSegSize)
	if err != nil {
		return err
	}
	idx, err := NewIndex(idxPath)
	if err != nil {
		seg.Close()
		return err
	}

	p.segments = append(p.segments, seg)
	p.indices = append(p.indices, idx)
	p.activeSeg = seg
	p.activeIndex = idx
	p.indexCounter = 0

	return nil
}

func (p *Partition) encode(m types.Message) []byte {
	// [ts(8)] [key_len(4)] [key] [val_len(4)] [val]
	kLen := len(m.Key)
	vLen := len(m.Value)
	buf := make([]byte, 8+4+kLen+4+vLen)
	
	binary.BigEndian.PutUint64(buf[0:8], uint64(m.Timestamp.UnixNano()))
	binary.BigEndian.PutUint32(buf[8:12], uint32(kLen))
	copy(buf[12:12+kLen], m.Key)
	binary.BigEndian.PutUint32(buf[12+kLen:16+kLen], uint32(vLen))
	copy(buf[16+kLen:], m.Value)
	
	return buf
}

func (p *Partition) decode(offset int64, data []byte) types.Message {
	ts := binary.BigEndian.Uint64(data[0:8])
	kLen := binary.BigEndian.Uint32(data[8:12])
	key := make([]byte, kLen)
	copy(key, data[12:12+kLen])
	
	vLen := binary.BigEndian.Uint32(data[12+kLen:16+kLen])
	val := make([]byte, vLen)
	copy(val, data[16+kLen:])
	
	return types.Message{
		Offset:    types.Offset(offset),
		Key:       key,
		Value:     val,
		Timestamp: time.Unix(0, int64(ts)),
	}
}

func (p *Partition) Append(key, value []byte, timestamp time.Time) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	msg := types.Message{Key: key, Value: value, Timestamp: timestamp}
	encoded := p.encode(msg)

	if err := p.wal.Append(encoded); err != nil {
		return 0, err
	}

	if p.activeSeg.IsFull() {
		if err := p.wal.Truncate(); err != nil {
			return 0, err
		}
		if err := p.rotate(); err != nil {
			return 0, err
		}
	}

	offset := p.nextOffset
	pos, err := p.activeSeg.Append(offset, encoded)
	if err != nil {
		return 0, err
	}

	if p.indexCounter%10 == 0 {
		relOffset := uint32(offset - p.activeSeg.BaseOffset)
		if err := p.activeIndex.Append(relOffset, uint32(pos)); err != nil {
			return 0, err
		}
	}
	p.indexCounter++
	p.nextOffset++

	return offset, nil
}

func (p *Partition) Read(offset int64) (types.Message, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if offset >= p.nextOffset {
		return types.Message{}, fmt.Errorf("offset out of bounds")
	}

	var targetSeg *Segment
	var targetIdx *Index
	
	for i := len(p.segments) - 1; i >= 0; i-- {
		if p.segments[i].BaseOffset <= offset {
			targetSeg = p.segments[i]
			targetIdx = p.indices[i]
			break
		}
	}

	if targetSeg == nil {
		return types.Message{}, fmt.Errorf("offset not found")
	}

	relOffset := uint32(offset - targetSeg.BaseOffset)
	entry, found := targetIdx.Lookup(relOffset)
	
	pos := int64(0)
	if found {
		pos = int64(entry.Position)
	}

	currOff := targetSeg.BaseOffset + int64(entry.RelativeOffset)
	if !found {
		currOff = targetSeg.BaseOffset
	}

	for currOff <= offset {
		data, err := targetSeg.Read(pos)
		if err != nil {
			return types.Message{}, err
		}
		if currOff == offset {
			return p.decode(offset, data), nil
		}
		pos += int64(4 + len(data))
		currOff++
	}

	return types.Message{}, fmt.Errorf("not found")
}

func (p *Partition) TruncateBefore(offset int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var activeIdx int
	for i, seg := range p.segments {
		if seg.BaseOffset > offset { 
			activeIdx = i
			break
		} else if i == len(p.segments)-1 { 
			activeIdx = i
			break
		} else if p.segments[i+1].BaseOffset > offset { 
			activeIdx = i
			break
		}
	}

	for i := 0; i < activeIdx; i++ {
		p.segments[i].Remove()
		p.indices[i].Remove()
	}

	p.segments = p.segments[activeIdx:]
	p.indices = p.indices[activeIdx:]
	return nil
}

func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	p.wal.Close()
	for _, s := range p.segments {
		s.Close()
	}
	for _, idx := range p.indices {
		idx.Close()
	}
	return nil
}
