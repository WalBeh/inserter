#!/bin/bash

# CrateDB Record Generator - Demo Script
# This script demonstrates the crate-write tool with a quick 1-minute test

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}🚀 CrateDB Record Generator Demo${NC}"
echo -e "${BLUE}=================================${NC}"
echo ""

# Check if CrateDB is running (optional)
echo -e "${YELLOW}Note: Make sure CrateDB is running before proceeding${NC}"
echo -e "${YELLOW}Default connection: http://localhost:4200${NC}"
echo ""

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Run a quick 1-minute demo
echo -e "${GREEN}Running 1-minute demo with 'demo_events' table...${NC}"
echo ""

./run.sh \
    --table-name demo_events \
    --duration 1 \
    --batch-size 50 \
    --batch-interval 0.1

echo ""
echo -e "${GREEN}✅ Demo completed!${NC}"
echo ""
echo -e "${BLUE}To run your own test:${NC}"
echo "  ./run.sh --table-name my_table --duration 5"
echo ""
echo -e "${BLUE}To see detailed usage:${NC}"
echo "  ./run.sh --help"
echo ""
echo -e "${BLUE}To check the data in CrateDB:${NC}"
echo "  SELECT COUNT(*) FROM demo_events;"
echo "  SELECT * FROM demo_events LIMIT 10;"
