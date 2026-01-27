#!/bin/bash

# UpdateOrInsert Benchmark Runner with Flamegraph Generation
# This script runs the updateOrInsert benchmarks and generates flamegraphs

set -e

BENCHMARK_TIME="${BENCHMARK_TIME:-10s}"
OUTPUT_DIR="./benchmark_results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo "=================================="
echo "UpdateOrInsert Benchmark Suite"
echo "=================================="
echo "Timestamp: $TIMESTAMP"
echo "Benchmark Time: $BENCHMARK_TIME"
echo "Usage: BENCHMARK_TIME=<duration> $0"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Function to run benchmark with profiling
run_benchmark() {
    local bench_name=$1
    local description=$2
    
    echo "Running: $bench_name"
    echo "Description: $description"
    echo ""
    
    # Run benchmark with CPU, memory, and mutex profiling
    # -run='^$' skips all regular tests and runs only benchmarks
    if go test -run='^$' -bench="^${bench_name}$" \
        -race \
        -benchmem \
        -benchtime="$BENCHMARK_TIME" \
        -cpuprofile="${OUTPUT_DIR}/${bench_name}_${TIMESTAMP}_cpu.prof" \
        -memprofile="${OUTPUT_DIR}/${bench_name}_${TIMESTAMP}_mem.prof" \
        -mutexprofile="${OUTPUT_DIR}/${bench_name}_${TIMESTAMP}_mutex.prof" \
        2>&1 | tee "${OUTPUT_DIR}/${bench_name}_${TIMESTAMP}_results.txt"; then
        echo ""
        echo "✓ Benchmark complete. Profiles saved to $OUTPUT_DIR"
    else
        echo ""
        echo "✗ Benchmark failed or no matching benchmarks found"
    fi
    echo ""
}

# Run all benchmarks
echo "1. High Concurrency Benchmark"
echo "------------------------------"
run_benchmark "BenchmarkUpdateOrInsertHighConcurrency" \
    "Tests with 100 goroutines across 200 namespaces"

echo ""
echo "2. High Contention Benchmark"
echo "----------------------------"
run_benchmark "BenchmarkUpdateOrInsertHighConcurrencyContended" \
    "Tests maximum contention with competing goroutines"

echo ""
echo "3. Scalability Benchmark"
echo "------------------------"
run_benchmark "BenchmarkUpdateOrInsertScalability" \
    "Tests scalability with varying goroutine counts"

echo ""
echo "=================================="
echo "Benchmark Complete!"
echo "=================================="
echo ""
echo "Results saved to: $OUTPUT_DIR"
echo ""

# Check if pprof is available and offer to start web UI
if command -v go &> /dev/null; then
    echo "Flamegraph Generation Options:"
    echo ""
    echo "1. View CPU profile flamegraph (High Concurrency):"
    echo "   go tool pprof -http=:8080 ${OUTPUT_DIR}/BenchmarkUpdateOrInsertHighConcurrency_${TIMESTAMP}_cpu.prof"
    echo ""
    echo "2. View Memory profile (High Concurrency):"
    echo "   go tool pprof -http=:8080 ${OUTPUT_DIR}/BenchmarkUpdateOrInsertHighConcurrency_${TIMESTAMP}_mem.prof"
    echo ""
    echo "3. View Mutex contention (High Concurrency):"
    echo "   go tool pprof -http=:8080 ${OUTPUT_DIR}/BenchmarkUpdateOrInsertHighConcurrency_${TIMESTAMP}_mutex.prof"
    echo ""
    
    # Generate text report only if profile file exists
    CPU_PROFILE="${OUTPUT_DIR}/BenchmarkUpdateOrInsertHighConcurrency_${TIMESTAMP}_cpu.prof"
    if [[ -f "$CPU_PROFILE" ]]; then
        echo "Generating CPU profile text report..."
        go tool pprof -text "$CPU_PROFILE" \
            > "${OUTPUT_DIR}/BenchmarkUpdateOrInsertHighConcurrency_${TIMESTAMP}_cpu_report.txt"

        echo "Top CPU consumers:"
        head -n 20 "${OUTPUT_DIR}/BenchmarkUpdateOrInsertHighConcurrency_${TIMESTAMP}_cpu_report.txt"
        echo ""

        read -p "Open CPU flamegraph in browser? (y/n) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo "Opening flamegraph at http://localhost:8080 ..."
            echo "Press Ctrl+C to stop the server"
            go tool pprof -http=:8080 "$CPU_PROFILE"
        fi
    else
        echo "No CPU profile found. Benchmark may have failed to connect to Aerospike at $HOST:$PORT"
        echo "Check the results files in $OUTPUT_DIR for error details."
    fi
fi

echo ""
echo "All profiles and results are available in: $OUTPUT_DIR"
echo ""
echo "To view flamegraphs later, use:"
echo "  go tool pprof -http=:8080 ${OUTPUT_DIR}/<profile_file>.prof"
echo ""
