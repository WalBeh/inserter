#!/bin/bash

# CrateDB Record Generator - Run Script
# This script sets up the environment and runs the crate-write tool using uv

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    log_error "uv is not installed. Please install it first:"
    echo "curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

log_info "Setting up CrateDB Record Generator..."

# Check if .env file exists
if [ ! -f ".env" ]; then
    log_warning ".env file not found. Creating from template..."
    cat > .env << EOF
# CrateDB connection string
# Format: http://username:password@host:port
# Example: http://admin:password@localhost:4200
CRATE_CONNECTION_STRING=http://localhost:4200

# Optional: Set log level
LOG_LEVEL=INFO
EOF
    log_warning "Please edit .env file with your CrateDB connection details"
fi

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    log_info "Creating virtual environment..."
    uv venv
    log_success "Virtual environment created"
else
    log_info "Virtual environment already exists"
fi

# Activate virtual environment
log_info "Activating virtual environment..."
source .venv/bin/activate

# Install dependencies
log_info "Installing dependencies..."
uv pip install -e .
log_success "Dependencies installed"

# Check if any arguments were provided
if [ $# -eq 0 ]; then
    log_info "No arguments provided. Showing help..."
    echo ""
    python -m crate_write.main --help
    echo ""
    log_info "Example usage:"
    echo "  $0 --table-name test_events --duration 5"
    echo "  $0 --table-name my_table --duration 10 --batch-size 200"
    echo ""
    log_info "You can also run directly after sourcing the virtual environment:"
    echo "  source .venv/bin/activate"
    echo "  crate-write --table-name test_events --duration 5"
    exit 0
fi

# Run the application with provided arguments
log_info "Starting crate-write with arguments: $*"
echo ""

python -m crate_write.main "$@"

# Check exit code
if [ $? -eq 0 ]; then
    log_success "crate-write completed successfully!"
else
    log_error "crate-write failed with exit code $?"
    exit 1
fi
