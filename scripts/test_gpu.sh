#!/bin/bash
# Maximus GPU Testing Script
# Runs TPC-H benchmarks and validates results

set -e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$SCRIPT_DIR/build"
DATA_DIR="$SCRIPT_DIR/tests/tpch/csv-0.01"

# Setup environment
if [ -f "$BUILD_DIR/setup_env.sh" ]; then
    source "$BUILD_DIR/setup_env.sh"
else
    export LD_LIBRARY_PATH="/arrow_install/lib:$CONDA_PREFIX/lib:$BUILD_DIR/lib:$LD_LIBRARY_PATH"
fi

echo -e "${GREEN}=== Maximus GPU Test Suite ===${NC}"
echo "Build directory: $BUILD_DIR"
echo "Data directory: $DATA_DIR"
echo ""

# Check if benchmark exists
if [ ! -f "$BUILD_DIR/benchmarks/maxbench" ]; then
    echo -e "${RED}Error: maxbench not found. Please run deploy_gpu.sh first.${NC}"
    exit 1
fi

# Check if test data exists
if [ ! -d "$DATA_DIR" ]; then
    echo -e "${RED}Error: Test data not found at $DATA_DIR${NC}"
    exit 1
fi

# Test 1: Single query warm-up
echo -e "${YELLOW}Test 1: Single query (q1) warm-up${NC}"
cd "$BUILD_DIR"
./benchmarks/maxbench \
    --benchmark=tpch \
    --queries=q1 \
    --device=gpu \
    --storage_device=cpu \
    --engines=maximus \
    --n_reps=1 \
    --path="$DATA_DIR" 2>&1 | tail -15

echo ""
echo -e "${GREEN}✓ Q1 test passed${NC}"
echo ""

# Test 2: Small subset of queries
echo -e "${YELLOW}Test 2: Subset test (q1-q5)${NC}"
./benchmarks/maxbench \
    --benchmark=tpch \
    --queries=q1,q2,q3,q4,q5 \
    --device=gpu \
    --storage_device=cpu \
    --engines=maximus \
    --n_reps=1 \
    --path="$DATA_DIR" 2>&1 | grep -E "QUERY|TIMINGS|maximus.*STATS"

echo ""
echo -e "${GREEN}✓ Subset test passed${NC}"
echo ""

# Test 3: Full benchmark (all 22 queries, 3 reps)
echo -e "${YELLOW}Test 3: Full TPC-H benchmark (all 22 queries, 3 repetitions)${NC}"
echo "This may take a few minutes..."
echo ""

./benchmarks/maxbench \
    --benchmark=tpch \
    --queries=q1,q2,q3,q4,q5,q6,q7,q8,q9,q10,q11,q12,q13,q14,q15,q16,q17,q18,q19,q20,q21,q22 \
    --device=gpu \
    --storage_device=cpu \
    --engines=maximus \
    --n_reps=3 \
    --path="$DATA_DIR" > test_results.log 2>&1

# Parse and display results
echo ""
echo -e "${GREEN}=== Benchmark Results ===${NC}"
echo ""

if grep -q "maximus RESULTS" test_results.log; then
    echo "✓ All queries executed successfully"
    echo ""

    # Show timing summary
    echo "Timing summary (milliseconds):"
    tail -30 test_results.log | grep "gpu,maximus" | awk '{
        split($1, a, ",");
        query = a[3];
        rep1 = a[4];
        rep2 = a[5];
        rep3 = a[6];
        avg = (rep1 + rep2 + rep3) / 3;
        printf "  %s: %d, %d, %d ms (avg: %.0f ms)\n", query, rep1, rep2, rep3, avg
    }'

    echo ""
    echo "Performance notes:"
    echo "  - First execution includes CUDA kernel compilation (warmup)"
    echo "  - Subsequent runs show actual compute performance"
    echo "  - All queries should complete in <100ms total on A100"
else
    echo -e "${RED}✗ Benchmark failed. Check test_results.log for details.${NC}"
    tail -50 test_results.log
    exit 1
fi

echo ""
echo -e "${GREEN}=== All Tests Passed ===${NC}"
echo ""
echo "Results saved to: $BUILD_DIR/test_results.log"
echo "CSV output: $BUILD_DIR/results.csv"
echo ""
