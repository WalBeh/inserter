#!/bin/bash

# CrateDB Stress Test Script
# This script runs progressively intensive stress tests to evaluate CrateDB performance

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Function to print colored output
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_test() {
    echo -e "${PURPLE}[TEST]${NC} $1"
}

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if CrateDB connection is available
check_connection() {
    log_info "Checking CrateDB connection..."

    # Try to connect with a simple test using run.sh which loads .env properly
    if ./run.sh --table-name connection_test --duration 1 --batch-size 1 --threads 1 > /dev/null 2>&1; then
        log_success "CrateDB connection verified"
        return 0
    else
        log_error "Cannot connect to CrateDB"
        echo "Please ensure CrateDB is running and accessible"
        echo "Check your .env file for correct connection string"
        echo "Current .env CRATE_CONNECTION_STRING:"
        grep "^CRATE_CONNECTION_STRING" .env 2>/dev/null || echo "  Not found in .env"
        return 1
    fi
}

# Run a single stress test
run_stress_test() {
    local test_name="$1"
    local table_name="$2"
    local duration="$3"
    local threads="$4"
    local batch_size="$5"
    local batch_interval="$6"

    log_test "Running: $test_name"
    echo -e "${CYAN}Configuration:${NC}"
    echo "  Table: $table_name"
    echo "  Duration: $duration minutes"
    echo "  Threads: $threads"
    echo "  Batch Size: $batch_size"
    echo "  Batch Interval: $batch_interval"
    echo ""

    ./run.sh \
        --table-name "$table_name" \
        --duration "$duration" \
        --threads "$threads" \
        --batch-size "$batch_size" \
        --batch-interval "$batch_interval"

    echo ""
    log_success "Completed: $test_name"
    echo ""
}

# Main stress test suite
main() {
    echo -e "${PURPLE}🔥 CrateDB Stress Test Suite${NC}"
    echo -e "${PURPLE}============================${NC}"
    echo ""

    # Check connection first
    if ! check_connection; then
        exit 1
    fi

    echo ""
    log_info "Starting progressive stress tests..."
    log_warning "Each test will run for the specified duration"
    log_warning "Press Ctrl+C to stop any test early"
    echo ""

    # Test 1: Baseline (single thread)
    run_stress_test \
        "Baseline Single Thread" \
        "stress_baseline" \
        1 \
        1 \
        100 \
        0.1

    # Test 2: Multi-thread moderate
    run_stress_test \
        "Multi-Thread Moderate" \
        "stress_moderate" \
        1 \
        4 \
        100 \
        0.1

    # Test 3: High throughput
    run_stress_test \
        "High Throughput" \
        "stress_high" \
        1 \
        4 \
        500 \
        0.05

    # Test 4: Maximum pressure
    run_stress_test \
        "Maximum Pressure" \
        "stress_max" \
        1 \
        8 \
        1000 \
        0

    # Test 5: Sustained load
    run_stress_test \
        "Sustained Load Test" \
        "stress_sustained" \
        3 \
        6 \
        200 \
        0.1

    # Summary
    echo ""
    echo -e "${GREEN}🎉 Stress Test Suite Completed!${NC}"
    echo -e "${GREEN}================================${NC}"
    echo ""
    echo -e "${CYAN}Generated Tables:${NC}"
    echo "  • stress_baseline  - Single thread baseline"
    echo "  • stress_moderate  - Multi-thread moderate load"
    echo "  • stress_high      - High throughput test"
    echo "  • stress_max       - Maximum pressure test"
    echo "  • stress_sustained - Sustained load test"
    echo ""
    echo -e "${BLUE}To analyze results in CrateDB:${NC}"
    echo ""
    echo "-- Record counts per test"
    echo "SELECT 'stress_baseline' as test, COUNT(*) as records FROM stress_baseline"
    echo "UNION ALL SELECT 'stress_moderate', COUNT(*) FROM stress_moderate"
    echo "UNION ALL SELECT 'stress_high', COUNT(*) FROM stress_high"
    echo "UNION ALL SELECT 'stress_max', COUNT(*) FROM stress_max"
    echo "UNION ALL SELECT 'stress_sustained', COUNT(*) FROM stress_sustained"
    echo "ORDER BY records DESC;"
    echo ""
    echo "-- Performance comparison"
    echo "SELECT"
    echo "  'stress_baseline' as test,"
    echo "  COUNT(*) as total_records,"
    echo "  COUNT(DISTINCT DATE_TRUNC('second', timestamp)) as active_seconds,"
    echo "  COUNT(*) / COUNT(DISTINCT DATE_TRUNC('second', timestamp)) as avg_records_per_sec"
    echo "FROM stress_baseline"
    echo "UNION ALL"
    echo "SELECT 'stress_max', COUNT(*), COUNT(DISTINCT DATE_TRUNC('second', timestamp)),"
    echo "       COUNT(*) / COUNT(DISTINCT DATE_TRUNC('second', timestamp))"
    echo "FROM stress_max"
    echo "ORDER BY avg_records_per_sec DESC;"
    echo ""
    echo -e "${YELLOW}💡 Tip: Run individual tests with custom parameters:${NC}"
    echo "   ./run.sh --table-name my_test --duration 2 --threads 16 --batch-size 2000"
    echo ""
}

# Handle script arguments
if [ "$1" = "--quick" ]; then
    echo -e "${YELLOW}🚀 Quick Stress Test Mode${NC}"
    echo ""

    if ! check_connection; then
        exit 1
    fi

    run_stress_test \
        "Quick Stress Test" \
        "quick_stress" \
        1 \
        4 \
        200 \
        0.05

elif [ "$1" = "--extreme" ]; then
    echo -e "${RED}💥 EXTREME Stress Test Mode${NC}"
    echo -e "${RED}WARNING: This will put maximum load on your database!${NC}"
    echo ""

    read -p "Are you sure you want to continue? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Cancelled."
        exit 0
    fi

    if ! check_connection; then
        exit 1
    fi

    run_stress_test \
        "EXTREME Load Test" \
        "extreme_stress" \
        2 \
        16 \
        8000 \
        0

elif [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "CrateDB Stress Test Script"
    echo ""
    echo "Usage:"
    echo "  $0              Run full stress test suite"
    echo "  $0 --quick     Run quick stress test (1 minute)"
    echo "  $0 --extreme   Run extreme stress test (WARNING: High load!)"
    echo "  $0 --help      Show this help"
    echo ""
    echo "The script will progressively test your CrateDB instance with:"
    echo "• Single thread baseline"
    echo "• Multi-threaded moderate load"
    echo "• High throughput testing"
    echo "• Maximum pressure testing"
    echo "• Sustained load testing"
    echo ""
    echo "Make sure CrateDB is running before starting the tests."

else
    main
fi
