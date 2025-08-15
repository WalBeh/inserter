# CrateDB Record Generator

A Python script for generating and inserting random records into CrateDB with performance monitoring and reporting.

## Features

- **Random Data Generation**: Creates realistic test data with controlled cardinality
- **Bulk Insertions**: Efficient batch insertions to maximize throughput
- **Performance Monitoring**: Real-time reporting every 10 seconds
- **Configurable Duration**: Run for a specified number of minutes
- **CLI Interface**: Easy-to-use command-line interface with Click
- **Environment Configuration**: Connection strings via `.env` file
- **Structured Logging**: Beautiful logs with Loguru
- **Load Balancer Testing**: Built-in 5-tuple load balancer distribution analysis
- **Error Handling**: Robust error handling with retry logic

## Installation

This project uses [uv](https://astral.sh/uv) for dependency management. Make sure you have uv installed:

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Clone and set up the project:

```bash
git clone <repository-url>
cd crate-write

# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -e .
```

## Configuration

Copy the example `.env` file and configure your CrateDB connection:

```bash
cp .env.example .env
```

Edit `.env` to set your CrateDB connection string:

```env
# CrateDB connection string
CRATE_CONNECTION_STRING=http://admin:password@localhost:4200

# Optional: Set log level
LOG_LEVEL=INFO
```

## Table Schema

The script creates a table with the following schema:

```sql
CREATE TABLE IF NOT EXISTS your_table_name (
    id TEXT PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE,
    region TEXT,
    product_category TEXT,
    event_type TEXT,
    user_id INTEGER,
    user_segment TEXT,
    amount DOUBLE PRECISION,
    quantity INTEGER,
    metadata OBJECT(DYNAMIC)
    -- Additional obj_0, obj_1, ... obj_N columns when using --objects flag
) WITH (
    number_of_replicas = 0,
    "refresh_interval" = 1000
);
```

### Data Characteristics

The generated data includes:

**Base Fields (10 columns):**
- **Regions**: 4 options (us-east, us-west, eu-central, ap-southeast)
- **Product Categories**: 5 options (electronics, books, clothing, home, sports)
- **Event Types**: 5 options (view, click, purchase, cart_add, cart_remove)
- **User Segments**: 4 options (premium, standard, basic, trial)
- **User IDs**: Random integers 1-10,000
- **Amounts**: Random decimals 1.0-1000.0
- **Quantities**: Random integers 1-100
- **Metadata**: JSON with browser, OS, and session information

**Object Fields (when using --objects):**
- **obj_0, obj_1, ... obj_N**: Low cardinality TEXT fields
- **Values**: Each object has 3-8 possible values (e.g., "val_0", "val_1", "val_2")
- **Use Cases**: Wide table testing, column performance analysis, realistic schemas

## Usage

### Basic Usage

```bash
# Activate virtual environment
source .venv/bin/activate

# Run for 5 minutes inserting into 'test_events' table
crate-write --table-name test_events --duration 5
```

### Advanced Usage

```bash
# Custom batch size and interval
crate-write \
    --table-name my_table \
    --duration 10 \
    --batch-size 200 \
    --batch-interval 0.05

# High-pressure testing with multiple threads
crate-write \
    --table-name stress_test \
    --duration 5 \
    --batch-size 100 \
    --threads 8

# Wide table testing with many columns
crate-write \
    --table-name wide_table \
    --duration 3 \
    --objects 100

# Override connection string
crate-write \
    --table-name my_table \
    --duration 5 \
    --connection-string "http://admin:mypass@crate.example.com:4200"
```

### Command Line Options

- `--table-name`: **Required**. Name of the CrateDB table to create/insert into
- `--duration`: **Required**. Duration to run in minutes
- `--connection-string`: CrateDB connection string (overrides .env)
- `--batch-size`: Records per batch (default: 100)
- `--batch-interval`: Seconds between batches (default: 0.1)
- `--threads`: Number of parallel worker threads (default: 1)
- `--objects`: Number of additional low-cardinality object columns (default: 0)

## Performance Monitoring

The script provides real-time performance monitoring:

### During Execution
- Reports every 10 seconds with current and average insertion rates
- Shows total records inserted and error count
- Displays batch statistics

### Final Summary
After completion, you'll see a comprehensive performance report:

```
============================================================
FINAL PERFORMANCE SUMMARY
============================================================
Worker threads: 8
Total records inserted: 1,234,567
Total batches: 12,346
Total runtime: 300.5 seconds
Average insertion rate: 4,109.2 records/second
Records per thread: 154,320 avg
Total errors: 0
============================================================
```

## Example Output

### Basic Usage
```
2024-01-15 10:30:00 | INFO     | Starting CrateDB record generator
2024-01-15 10:30:00 | INFO     | Table: test_events
2024-01-15 10:30:00 | INFO     | Duration: 5 minutes
2024-01-15 10:30:00 | INFO     | Batch size: 100
2024-01-15 10:30:00 | INFO     | Batch interval: 0.1s
2024-01-15 10:30:00 | SUCCESS  | Table 'test_events' created successfully
2024-01-15 10:30:00 | INFO     | Starting record generation and insertion...
2024-01-15 10:30:10 | INFO     | Performance: 985.2 records/sec (current), 987.1 records/sec (avg), Total: 9,871 records, Batches: 98, Threads: 1, Errors: 0
```

### Wide Table Usage (--objects 50)
```
2024-01-15 10:30:00 | INFO     | Starting CrateDB record generator
2024-01-15 10:30:00 | INFO     | Table: wide_test
2024-01-15 10:30:00 | INFO     | Object columns: 50
2024-01-15 10:30:00 | SUCCESS  | Table 'wide_test' created successfully (60 total columns)
2024-01-15 10:30:10 | INFO     | Performance: 750.3 records/sec (current), 750.3 records/sec (avg), Total: 7,503 records, Batches: 75, Threads: 1, Errors: 0
...
```

## Load Balancer Testing

The script automatically performs a 5-tuple load balancer test at startup to verify proper distribution across CrateDB cluster nodes.

### 5-Tuple Test Details

Before starting the main workload, the script:

1. **Creates 30 fresh TCP connections** to the CrateDB cluster
2. **Uses different source ports** for each connection (5-tuple hashing)
3. **Measures distribution** across available nodes
4. **Displays visual summary** of load balancer behavior

### Example Output

```
🔍 5-TUPLE LOAD BALANCER TEST
============================================================
Target: your-cluster.cratedb.net:4200 (HTTPS)
Requests: 30 (each with fresh TCP connection)

📈 NODE DISTRIBUTION:
   data-hot-0      |  10 hits |  33.3% | ████████████████
   data-hot-1      |   7 hits |  23.3% | ███████████
   data-hot-2      |  13 hits |  43.3% | █████████████████████

✅ Load balancer IS distributing across nodes
Evidence: 30 source ports hit 3 different nodes
```

### What This Means

- **✅ Optimal Distribution**: Each worker thread will connect to a different node at startup
- **🔄 Persistent Connections**: After initial distribution, each thread reuses its connection efficiently
- **📊 Performance Confidence**: You can trust that multi-threaded tests will utilize all cluster nodes

### Interpreting Results

- **Multiple nodes hit**: Load balancer is working correctly
- **Single node hit**: May indicate load balancer misconfiguration or single healthy node
- **Uneven distribution**: Normal due to hash distribution; becomes more even with more connections

## Development

### Project Structure

```
crate-write/
├── crate_write/
│   ├── __init__.py
│   └── main.py          # Main application logic
├── pyproject.toml       # Project configuration and dependencies
├── .env                 # Environment configuration
└── README.md           # This file
```

### Running in Development

```bash
# Activate virtual environment
source .venv/bin/activate

# Install in development mode
uv pip install -e .

# Run directly with python
python -m crate_write.main --table-name test --duration 1
```

### Dependencies

- **click**: Command-line interface
- **loguru**: Structured logging
- **python-dotenv**: Environment variable management
- **crate[sqlalchemy]**: CrateDB Python client
- **requests**: HTTP client for CrateDB REST API
- **faker**: Realistic fake data generation

## Troubleshooting

### Connection Issues

If you encounter connection errors:

1. Verify CrateDB is running and accessible
2. Check the connection string format: `http://[username:password@]host:port`
3. Ensure firewall allows connections to CrateDB port (default: 4200)

### Performance Issues

To optimize performance:

1. Increase `--batch-size` for higher throughput
2. Decrease `--batch-interval` for faster insertion
3. Increase `--threads` for parallel load (start with CPU cores)
4. Ensure CrateDB has sufficient resources
5. Consider adjusting the table's `refresh_interval`

### Memory Issues

For long-running sessions:

1. Monitor memory usage of both the script and CrateDB
2. Consider reducing batch size if memory usage is high
3. Ensure adequate swap space is available

## License

This project is licensed under the MIT License.