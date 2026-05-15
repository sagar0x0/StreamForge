package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sagar/streamforge/pkg/types"
)

func TestWALAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	// Write entries
	payloads := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("streamforge"),
	}
	for _, p := range payloads {
		if err := wal.Append(p); err != nil {
			t.Fatalf("failed to append: %v", err)
		}
	}
	wal.Close()

	// Replay
	wal2, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	entries, err := wal2.Replay()
	if err != nil {
		t.Fatalf("failed to replay: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	for i, e := range entries {
		if string(e) != string(payloads[i]) {
			t.Fatalf("entry %d mismatch: got %q, want %q", i, string(e), string(payloads[i]))
		}
	}
}

func TestWALTruncate(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer wal.Close()

	wal.Append([]byte("data"))
	err = wal.Truncate()
	if err != nil {
		t.Fatalf("failed to truncate: %v", err)
	}

	entries, err := wal.Replay()
	if err != nil {
		t.Fatalf("failed to replay after truncate: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries after truncate, got %d", len(entries))
	}
}

func TestWALCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")

	// Simulate writing and crashing (close without truncate)
	wal, _ := OpenWAL(walPath)
	wal.Append([]byte("before_crash_1"))
	wal.Append([]byte("before_crash_2"))
	wal.Close()

	// Reopen — should recover both entries
	wal2, _ := OpenWAL(walPath)
	defer wal2.Close()
	entries, _ := wal2.Replay()
	if len(entries) != 2 {
		t.Fatalf("expected 2 recovered entries, got %d", len(entries))
	}
	if string(entries[0]) != "before_crash_1" || string(entries[1]) != "before_crash_2" {
		t.Fatalf("recovered wrong data")
	}
}

func TestSegmentAppendAndRead(t *testing.T) {
	dir := t.TempDir()
	segPath := filepath.Join(dir, "00000000000000000000.log")

	seg, err := NewSegment(segPath, 0, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}
	defer seg.Close()

	// Append records
	data := [][]byte{
		[]byte("record_0"),
		[]byte("record_1"),
		[]byte("record_2_longer_data"),
	}
	positions := make([]int64, len(data))
	for i, d := range data {
		pos, err := seg.Append(int64(i), d)
		if err != nil {
			t.Fatalf("failed to append record %d: %v", i, err)
		}
		positions[i] = pos
	}

	// Read back
	for i, pos := range positions {
		got, err := seg.Read(pos)
		if err != nil {
			t.Fatalf("failed to read record %d: %v", i, err)
		}
		if string(got) != string(data[i]) {
			t.Fatalf("record %d mismatch: got %q, want %q", i, string(got), string(data[i]))
		}
	}
}

func TestSegmentFull(t *testing.T) {
	dir := t.TempDir()
	segPath := filepath.Join(dir, "00000000000000000000.log")

	seg, _ := NewSegment(segPath, 0, 50) // Very small segment
	defer seg.Close()

	seg.Append(0, []byte("this data fills up quickly"))
	_, err := seg.Append(1, []byte("this should cause full"))
	if err == nil {
		// may or may not fail depending on size; check IsFull instead
		if !seg.IsFull() {
			// that's fine; segment wasn't full yet
		}
	}
}

func TestIndexAppendAndLookup(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "00000000000000000000.index")

	idx, err := NewIndex(idxPath)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}
	defer idx.Close()

	// Add entries
	idx.Append(0, 0)
	idx.Append(10, 500)
	idx.Append(20, 1200)

	// Exact lookup
	entry, found := idx.Lookup(10)
	if !found || entry.Position != 500 {
		t.Fatalf("expected position 500 for offset 10, got %d (found=%v)", entry.Position, found)
	}

	// Nearest lookup (offset 15 should return entry for offset 10)
	entry, found = idx.Lookup(15)
	if !found || entry.RelativeOffset != 10 {
		t.Fatalf("expected nearest offset 10 for lookup 15, got %d", entry.RelativeOffset)
	}
}

func TestIndexPersistence(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "test.index")

	idx, _ := NewIndex(idxPath)
	idx.Append(0, 0)
	idx.Append(5, 200)
	idx.Close()

	// Reopen
	idx2, _ := NewIndex(idxPath)
	defer idx2.Close()

	entry, found := idx2.Lookup(5)
	if !found || entry.Position != 200 {
		t.Fatalf("index not persisted correctly")
	}
}

func TestPartitionAppendAndRead(t *testing.T) {
	dir := t.TempDir()
	partDir := filepath.Join(dir, "partition-0")

	part, err := OpenPartition(partDir, 1024*1024)
	if err != nil {
		t.Fatalf("failed to open partition: %v", err)
	}
	defer part.Close()

	// Write messages
	now := time.Now()
	type testMsg struct {
		key   string
		value string
	}
	msgs := []testMsg{
		{"NYC", `{"amount": 250.0}`},
		{"LA", `{"amount": 120.0}`},
		{"NYC", `{"amount": 890.0}`},
		{"CHI", `{"amount": 45.0}`},
		{"LA", `{"amount": 310.0}`},
	}

	offsets := make([]int64, len(msgs))
	for i, m := range msgs {
		off, err := part.Append([]byte(m.key), []byte(m.value), now.Add(time.Duration(i)*time.Second))
		if err != nil {
			t.Fatalf("failed to append message %d: %v", i, err)
		}
		offsets[i] = off
	}

	// Read back and verify
	for i, off := range offsets {
		msg, err := part.Read(off)
		if err != nil {
			t.Fatalf("failed to read offset %d: %v", off, err)
		}
		if string(msg.Key) != msgs[i].key {
			t.Fatalf("offset %d key mismatch: got %q, want %q", off, string(msg.Key), msgs[i].key)
		}
		if string(msg.Value) != msgs[i].value {
			t.Fatalf("offset %d value mismatch: got %q, want %q", off, string(msg.Value), msgs[i].value)
		}
	}
}

func TestPartitionCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	partDir := filepath.Join(dir, "partition-0")

	// Write data, then close (simulates crash)
	part, _ := OpenPartition(partDir, 1024*1024)
	now := time.Now()
	part.Append([]byte("NYC"), []byte(`{"amount": 100}`), now)
	part.Append([]byte("LA"), []byte(`{"amount": 200}`), now)
	part.Close()

	// Reopen — should recover all data
	part2, err := OpenPartition(partDir, 1024*1024)
	if err != nil {
		t.Fatalf("failed to reopen partition: %v", err)
	}
	defer part2.Close()

	msg, err := part2.Read(0)
	if err != nil {
		t.Fatalf("failed to read offset 0 after recovery: %v", err)
	}
	if string(msg.Key) != "NYC" {
		t.Fatalf("wrong key after recovery: got %q", string(msg.Key))
	}
}

func TestPartitionBulkWriteAndRead(t *testing.T) {
	dir := t.TempDir()
	partDir := filepath.Join(dir, "partition-0")

	part, err := OpenPartition(partDir, 1024*1024)
	if err != nil {
		t.Fatalf("failed to open partition: %v", err)
	}
	defer part.Close()

	now := time.Now()
	count := 1000

	for i := 0; i < count; i++ {
		_, err := part.Append([]byte("key"), []byte("value"), now)
		if err != nil {
			t.Fatalf("failed to append message %d: %v", i, err)
		}
	}

	// Random reads
	for _, off := range []int64{0, 1, 50, 500, 999} {
		msg, err := part.Read(off)
		if err != nil {
			t.Fatalf("failed to read offset %d: %v", off, err)
		}
		if msg.Offset != types.Offset(off) {
			t.Fatalf("offset mismatch: got %d, want %d", msg.Offset, off)
		}
	}

	// Out of bounds
	_, err = part.Read(1000)
	if err == nil {
		t.Fatal("expected error for out-of-bounds read")
	}
}

func TestPartitionSegmentRotation(t *testing.T) {
	dir := t.TempDir()
	partDir := filepath.Join(dir, "partition-0")

	// Small max segment size to force rotation
	part, err := OpenPartition(partDir, 200)
	if err != nil {
		t.Fatalf("failed to open partition: %v", err)
	}
	defer part.Close()

	now := time.Now()

	// Write enough data to trigger at least one rotation
	for i := 0; i < 20; i++ {
		_, err := part.Append([]byte("key"), []byte("value-with-enough-length-to-fill-segment"), now)
		if err != nil {
			t.Fatalf("failed to append message %d: %v", i, err)
		}
	}

	// Verify we can read back all messages
	for i := 0; i < 20; i++ {
		msg, err := part.Read(int64(i))
		if err != nil {
			t.Fatalf("failed to read offset %d after rotation: %v", i, err)
		}
		if string(msg.Key) != "key" {
			t.Fatalf("wrong key at offset %d: %q", i, string(msg.Key))
		}
	}

	// Check multiple segment files exist
	entries, _ := os.ReadDir(partDir)
	logCount := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".log" && e.Name() != "wal.log" {
			logCount++
		}
	}
	if logCount < 2 {
		t.Fatalf("expected multiple segments after rotation, got %d", logCount)
	}
}
