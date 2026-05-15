package storage

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// ═══════════════════════════════════════════════════════════════════
// WAL BENCHMARKS — measures raw durable write throughput
// ═══════════════════════════════════════════════════════════════════

func BenchmarkWALAppend_128B(b *testing.B)  { benchmarkWALAppend(b, 128) }
func BenchmarkWALAppend_512B(b *testing.B)  { benchmarkWALAppend(b, 512) }
func BenchmarkWALAppend_1KB(b *testing.B)   { benchmarkWALAppend(b, 1024) }
func BenchmarkWALAppend_4KB(b *testing.B)   { benchmarkWALAppend(b, 4096) }

func benchmarkWALAppend(b *testing.B, payloadSize int) {
	dir := b.TempDir()
	wal, err := OpenWAL(filepath.Join(dir, "bench.wal"))
	if err != nil {
		b.Fatal(err)
	}
	defer wal.Close()

	payload := make([]byte, payloadSize)
	rand.Read(payload)

	b.SetBytes(int64(payloadSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := wal.Append(payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWALReplay(b *testing.B) {
	dir := b.TempDir()
	walPath := filepath.Join(dir, "bench.wal")

	// Pre-populate WAL with 10K entries
	wal, _ := OpenWAL(walPath)
	payload := make([]byte, 256)
	rand.Read(payload)
	for i := 0; i < 10000; i++ {
		wal.Append(payload)
	}
	wal.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal2, _ := OpenWAL(walPath)
		entries, err := wal2.Replay()
		if err != nil {
			b.Fatal(err)
		}
		if len(entries) != 10000 {
			b.Fatalf("expected 10000 entries, got %d", len(entries))
		}
		wal2.Close()
	}
}

// ═══════════════════════════════════════════════════════════════════
// SEGMENT BENCHMARKS — measures raw log I/O
// ═══════════════════════════════════════════════════════════════════

func BenchmarkSegmentAppend(b *testing.B) {
	dir := b.TempDir()
	seg, _ := NewSegment(filepath.Join(dir, "bench.log"), 0, 1<<30)
	defer seg.Close()

	data := make([]byte, 256)
	rand.Read(data)

	b.SetBytes(256)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seg.Append(int64(i), data)
	}
}

func BenchmarkSegmentRead(b *testing.B) {
	dir := b.TempDir()
	seg, _ := NewSegment(filepath.Join(dir, "bench.log"), 0, 1<<30)

	data := make([]byte, 256)
	rand.Read(data)

	// Pre-populate
	positions := make([]int64, 10000)
	for i := 0; i < 10000; i++ {
		pos, _ := seg.Append(int64(i), data)
		positions[i] = pos
	}

	b.SetBytes(256)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := seg.Read(positions[i%10000])
		if err != nil {
			b.Fatal(err)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════
// INDEX BENCHMARKS — measures offset lookup performance
// ═══════════════════════════════════════════════════════════════════

func BenchmarkIndexLookup(b *testing.B) {
	dir := b.TempDir()
	idx, _ := NewIndex(filepath.Join(dir, "bench.index"))
	defer idx.Close()

	// Build sparse index: every 10th offset, 10K entries
	for i := uint32(0); i < 10000; i++ {
		idx.Append(i*10, i*260) // simulates sparse indexing
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := uint32(rand.Intn(100000))
		idx.Lookup(offset)
	}
}

// ═══════════════════════════════════════════════════════════════════
// PARTITION BENCHMARKS — full stack: WAL → segment → index
// ═══════════════════════════════════════════════════════════════════

var cities = []string{"NYC", "LA", "CHI", "SF", "BOS", "ATL", "SEA", "DEN"}

func makeEvent() ([]byte, []byte) {
	city := cities[rand.Intn(len(cities))]
	amount := 10.0 + rand.Float64()*990.0
	val, _ := json.Marshal(map[string]float64{"amount": amount})
	return []byte(city), val
}

func BenchmarkPartitionAppend_SingleWriter(b *testing.B) {
	dir := b.TempDir()
	part, err := OpenPartition(filepath.Join(dir, "p0"), 64<<20) // 64MB segments
	if err != nil {
		b.Fatal(err)
	}
	defer part.Close()

	now := time.Now()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key, val := makeEvent()
		_, err := part.Append(key, val, now)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPartitionAppend_8Writers(b *testing.B) {
	benchmarkPartitionParallel(b, 8)
}

func BenchmarkPartitionAppend_16Writers(b *testing.B) {
	benchmarkPartitionParallel(b, 16)
}

func benchmarkPartitionParallel(b *testing.B, writers int) {
	dir := b.TempDir()

	// Use 4 partitions to spread lock contention
	parts := make([]*Partition, 4)
	for i := 0; i < 4; i++ {
		p, err := OpenPartition(filepath.Join(dir, fmt.Sprintf("p%d", i)), 64<<20)
		if err != nil {
			b.Fatal(err)
		}
		defer p.Close()
		parts[i] = p
	}

	now := time.Now()
	b.SetParallelism(writers)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			city := cities[r.Intn(len(cities))]
			amount := 10.0 + r.Float64()*990.0
			val, _ := json.Marshal(map[string]float64{"amount": amount})
			pID := r.Intn(4)
			parts[pID].Append([]byte(city), val, now)
		}
	})
}

func BenchmarkPartitionRead_Sequential(b *testing.B) {
	dir := b.TempDir()
	part, _ := OpenPartition(filepath.Join(dir, "p0"), 64<<20)
	defer part.Close()

	now := time.Now()
	n := 50000
	for i := 0; i < n; i++ {
		key, val := makeEvent()
		part.Append(key, val, now)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		part.Read(int64(i % n))
	}
}

func BenchmarkPartitionRead_Random(b *testing.B) {
	dir := b.TempDir()
	part, _ := OpenPartition(filepath.Join(dir, "p0"), 64<<20)
	defer part.Close()

	now := time.Now()
	n := 50000
	for i := 0; i < n; i++ {
		key, val := makeEvent()
		part.Append(key, val, now)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		part.Read(int64(rand.Intn(n)))
	}
}

// ═══════════════════════════════════════════════════════════════════
// THROUGHPUT BENCHMARK — sustained msg/sec measurement
// ═══════════════════════════════════════════════════════════════════

func BenchmarkPartitionThroughput_4P_8W(b *testing.B) {
	benchmarkThroughput(b, 4, 8)
}

func benchmarkThroughput(b *testing.B, numPartitions, numWorkers int) {
	dir := b.TempDir()

	parts := make([]*Partition, numPartitions)
	for i := 0; i < numPartitions; i++ {
		p, _ := OpenPartition(filepath.Join(dir, fmt.Sprintf("p%d", i)), 64<<20)
		defer p.Close()
		parts[i] = p
	}

	now := time.Now()

	b.ResetTimer()

	var wg sync.WaitGroup
	perWorker := b.N / numWorkers
	if perWorker < 1 {
		perWorker = 1
	}

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(rand.Int63()))
			for i := 0; i < perWorker; i++ {
				city := cities[r.Intn(len(cities))]
				val, _ := json.Marshal(map[string]float64{"amount": r.Float64() * 1000})
				parts[r.Intn(numPartitions)].Append([]byte(city), val, now)
			}
		}()
	}
	wg.Wait()
}
