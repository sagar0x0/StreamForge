#!/usr/bin/env bash
set -euo pipefail

# ═══════════════════════════════════════════════════════════════════
# StreamForge — Production Benchmark Runner
#
# Usage:
#   ./scripts/bench.sh              # Full suite (5 runs + benchstat)
#   ./scripts/bench.sh --quick      # Single run, no statistics
#   ./scripts/bench.sh --profile    # Full suite + CPU/mem profiles
# ═══════════════════════════════════════════════════════════════════

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="$PROJECT_ROOT/benchmarks"
PROFILE_DIR="$RESULTS_DIR/profiles"
RUNS=5
BENCHTIME="3s"
QUICK=false
PROFILE=false

for arg in "$@"; do
  case $arg in
    --quick)   QUICK=true; RUNS=1; BENCHTIME="2s" ;;
    --profile) PROFILE=true ;;
  esac
done

# ── Environment ──────────────────────────────────────────────────

# Try to find go binary
if command -v go &>/dev/null; then
  GO=go
elif [ -x "$PROJECT_ROOT/.local/go/bin/go" ]; then
  GO="$PROJECT_ROOT/.local/go/bin/go"
  export PATH="$PROJECT_ROOT/.local/go/bin:$PATH"
else
  echo "ERROR: Go not found. Install Go or set PATH." >&2
  exit 1
fi

mkdir -p "$RESULTS_DIR" "$PROFILE_DIR"

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║      StreamForge — Benchmark Suite                         ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "  Go:          $($GO version | awk '{print $3}')"
echo "  OS/Arch:     $(uname -s)/$(uname -m)"
echo "  CPU:         $(sysctl -n machdep.cpu.brand_string 2>/dev/null || lscpu 2>/dev/null | grep 'Model name' | sed 's/.*: *//')"
echo "  Cores:       $(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null)"
echo "  RAM:         $(( $(sysctl -n hw.memsize 2>/dev/null || grep MemTotal /proc/meminfo 2>/dev/null | awk '{print $2*1024}') / 1073741824 )) GB"
echo "  Runs:        $RUNS"
echo "  Benchtime:   $BENCHTIME"
echo "  Date:        $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo ""

# ── Pre-flight ───────────────────────────────────────────────────

echo "▶ Compiling tests..."
$GO test -run='^$' -bench='^$' ./internal/storage/ ./internal/processor/ ./internal/speculative/ > /dev/null 2>&1
echo "  ✓ Compilation clean"
echo ""

# ── Run benchmarks ───────────────────────────────────────────────

run_bench() {
  local pkg=$1
  local name=$2
  local run=$3
  local outfile="$RESULTS_DIR/${name}_run${run}.txt"

  $GO test -bench=. -benchmem -benchtime="$BENCHTIME" -count=1 \
    -timeout=300s -run='^$' \
    "$pkg" > "$outfile" 2>&1

  echo "  ✓ $name run $run"
}

for run in $(seq 1 $RUNS); do
  echo "── Run $run / $RUNS ──"
  run_bench ./internal/storage/     storage     $run
  run_bench ./internal/processor/   processor   $run
  run_bench ./internal/speculative/ speculative $run
  echo ""
done

# ── Merge results ────────────────────────────────────────────────

echo "▶ Merging results..."
for name in storage processor speculative; do
  cat "$RESULTS_DIR/${name}_run"*.txt > "$RESULTS_DIR/${name}_all.txt"
done
echo "  ✓ Merged"

# ── Statistical analysis (benchstat) ─────────────────────────────

if command -v benchstat &>/dev/null; then
  echo ""
  echo "▶ Statistical analysis (benchstat)..."
  echo ""
  for name in storage processor speculative; do
    echo "═══════════════════════════════════════════════════════════"
    echo "  $name"
    echo "═══════════════════════════════════════════════════════════"
    benchstat "$RESULTS_DIR/${name}_all.txt"
    echo ""
  done
elif $QUICK; then
  echo ""
  echo "▶ Results (single run)..."
  echo ""
  for name in storage processor speculative; do
    echo "═══════════════════════════════════════════════════════════"
    echo "  $name"
    echo "═══════════════════════════════════════════════════════════"
    grep "^Benchmark" "$RESULTS_DIR/${name}_run1.txt" || true
    echo ""
  done
else
  echo ""
  echo "  ⚠ benchstat not installed. Install for statistical analysis:"
  echo "    go install golang.org/x/perf/cmd/benchstat@latest"
  echo ""
  echo "▶ Raw results (last run)..."
  echo ""
  for name in storage processor speculative; do
    echo "═══════════════════════════════════════════════════════════"
    echo "  $name"
    echo "═══════════════════════════════════════════════════════════"
    grep "^Benchmark" "$RESULTS_DIR/${name}_run${RUNS}.txt" || true
    echo ""
  done
fi

# ── CPU/Memory Profiling ─────────────────────────────────────────

if $PROFILE; then
  echo "▶ Generating CPU + Memory profiles..."
  echo ""

  # Storage (the hot path)
  $GO test -bench='BenchmarkPartitionAppend_SingleWriter' \
    -benchtime=5s -cpuprofile="$PROFILE_DIR/storage_cpu.prof" \
    -memprofile="$PROFILE_DIR/storage_mem.prof" \
    -run='^$' ./internal/storage/ > /dev/null 2>&1
  echo "  ✓ Storage CPU profile: $PROFILE_DIR/storage_cpu.prof"
  echo "  ✓ Storage Mem profile: $PROFILE_DIR/storage_mem.prof"

  # Processor
  $GO test -bench='BenchmarkEngineProcessEvent' \
    -benchtime=5s -cpuprofile="$PROFILE_DIR/processor_cpu.prof" \
    -memprofile="$PROFILE_DIR/processor_mem.prof" \
    -run='^$' ./internal/processor/ > /dev/null 2>&1
  echo "  ✓ Processor CPU profile: $PROFILE_DIR/processor_cpu.prof"
  echo "  ✓ Processor Mem profile: $PROFILE_DIR/processor_mem.prof"

  echo ""
  echo "  View profiles with:"
  echo "    go tool pprof -http=:8080 $PROFILE_DIR/storage_cpu.prof"
  echo "    go tool pprof -http=:8081 $PROFILE_DIR/processor_cpu.prof"
fi

# ── Summary ──────────────────────────────────────────────────────

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Results saved to: $RESULTS_DIR/"
if $PROFILE; then
  echo "  Profiles saved to: $PROFILE_DIR/"
fi
echo "═══════════════════════════════════════════════════════════════"
