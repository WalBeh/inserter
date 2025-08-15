#!/usr/bin/env python3
"""
CrateDB Record Generator and Inserter

A script that generates random records and inserts them into CrateDB with
performance monitoring and reporting.
"""

import os
import sys
import time
import threading
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
import random
import json

import click
from loguru import logger
from dotenv import load_dotenv
from faker import Faker
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urlparse


class CrateDBClient:
    """Simple HTTP client for CrateDB."""

    def __init__(self, connection_string: str):
        """Initialize the CrateDB client."""
        # Clean up malformed URLs (e.g., https:/// -> https://)
        cleaned_url = connection_string.replace("://:", "://").replace(":///", "://")

        try:
            parsed = urlparse(cleaned_url)

            # Validate required components
            if not parsed.scheme:
                raise ValueError(f"Missing scheme in connection string: {connection_string}")
            if not parsed.hostname:
                raise ValueError(f"Missing hostname in connection string: {connection_string}")

            self.base_url = f"{parsed.scheme}://{parsed.hostname}:{parsed.port or 4200}"
            self.auth = None

            if parsed.username and parsed.password:
                self.auth = HTTPBasicAuth(parsed.username, parsed.password)

            logger.info(f"Connecting to CrateDB at: {parsed.scheme}://{parsed.hostname}:{parsed.port or 4200}")

        except Exception as e:
            logger.error(f"Failed to parse connection string '{connection_string}': {e}")
            raise ValueError(f"Invalid connection string format: {e}")

        self.session = requests.Session()
        if self.auth:
            self.session.auth = self.auth

    def execute(self, sql: str, args: Optional[List] = None) -> Dict[str, Any]:
        """Execute a SQL statement."""
        payload = {"stmt": sql}
        if args:
            payload["args"] = args

        try:
            response = self.session.post(
                f"{self.base_url}/_sql",
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error executing SQL: {e}")
            logger.error(f"SQL statement: {sql}")
            if args:
                logger.error(f"SQL args: {args}")
            raise

    def execute_bulk(self, sql: str, bulk_args: List[List]) -> Dict[str, Any]:
        """Execute a SQL statement with bulk parameters."""
        payload = {
            "stmt": sql,
            "bulk_args": bulk_args
        }

        try:
            response = self.session.post(
                f"{self.base_url}/_sql",
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error executing bulk SQL: {e}")
            logger.error(f"SQL statement: {sql}")
            logger.error(f"Bulk args count: {len(bulk_args) if bulk_args else 0}")
            if bulk_args and len(bulk_args) > 0:
                logger.error(f"First record sample: {bulk_args[0]}")
            raise


class PerformanceMonitor:
    """Monitor and report performance metrics."""

    def __init__(self):
        self.start_time = time.time()
        self.total_records = 0
        self.total_batches = 0
        self.last_report_time = time.time()
        self.last_report_records = 0
        self.errors = 0
        self.lock = threading.Lock()

    def add_records(self, count: int):
        """Add records to the counter."""
        with self.lock:
            self.total_records += count
            self.total_batches += 1

    def add_error(self):
        """Add an error to the counter."""
        with self.lock:
            self.errors += 1

    def get_current_rate(self) -> float:
        """Get the current insertion rate (records/second)."""
        with self.lock:
            current_time = time.time()
            time_diff = current_time - self.last_report_time

            if time_diff < 1.0:  # Avoid division by very small numbers
                return 0.0

            records_diff = self.total_records - self.last_report_records
            rate = records_diff / time_diff

            self.last_report_time = current_time
            self.last_report_records = self.total_records

            return rate

    def get_overall_stats(self) -> Dict[str, Any]:
        """Get overall performance statistics."""
        with self.lock:
            elapsed_time = time.time() - self.start_time
            overall_rate = self.total_records / elapsed_time if elapsed_time > 0 else 0

            return {
                "total_records": self.total_records,
                "total_batches": self.total_batches,
                "elapsed_time": elapsed_time,
                "overall_rate": overall_rate,
                "errors": self.errors
            }


class RecordGenerator:
    """Generate random records for testing."""

    def __init__(self, num_objects: int = 0):
        self.fake = Faker()
        self.num_objects = num_objects
        # Keep cardinality reasonable by limiting choices
        self.regions = ["us-east", "us-west", "eu-central", "ap-southeast"]
        self.product_categories = ["electronics", "books", "clothing", "home", "sports"]
        self.event_types = ["view", "click", "purchase", "cart_add", "cart_remove"]
        self.user_segments = ["premium", "standard", "basic", "trial"]
        self.base_time = datetime.now(timezone.utc)

        # Generate object field data with low cardinality
        self.object_data = {}
        for i in range(num_objects):
            cardinality = random.randint(3, 8)  # 3-8 possible values per object
            self.object_data[f"obj_{i}"] = [f"val_{j}" for j in range(cardinality)]

    def generate_record(self) -> List[Any]:
        """Generate a single random record."""
        # Add slight randomization to timestamp (within last 60 seconds)
        timestamp_offset = timedelta(seconds=random.randint(-60, 0))
        timestamp = (self.base_time + timestamp_offset).isoformat()

        # Base record fields
        record = [
            self.fake.uuid4(),  # id
            timestamp,  # timestamp
            random.choice(self.regions),  # region
            random.choice(self.product_categories),  # product_category
            random.choice(self.event_types),  # event_type
            random.randint(1, 10000),  # user_id
            random.choice(self.user_segments),  # user_segment
            round(random.uniform(1.0, 1000.0), 2),  # amount
            random.randint(1, 100),  # quantity
            json.dumps({
                "browser": random.choice(["chrome", "firefox", "safari", "edge"]),
                "os": random.choice(["windows", "macos", "linux", "ios", "android"]),
                "session_id": self.fake.uuid4()
            })  # metadata
        ]

        # Add object fields with low cardinality
        for i in range(self.num_objects):
            obj_key = f"obj_{i}"
            record.append(random.choice(self.object_data[obj_key]))

        return record

    def generate_batch(self, batch_size: int) -> List[List[Any]]:
        """Generate a batch of records."""
        return [self.generate_record() for _ in range(batch_size)]


def sample_load_balancer_5tuple(connection_string: str, samples: int = None) -> Dict[str, int]:
    """Sample load balancer distribution using fresh TCP connections (5-tuple test)."""
    import socket
    import ssl
    import json
    import base64
    
    logger.info("Starting 5-tuple load balancer analysis...")

    parsed = urlparse(connection_string)
    if not parsed.hostname:
        raise ValueError("Invalid connection string - missing hostname")
        
    host = parsed.hostname
    port = parsed.port or 4200
    use_ssl = parsed.scheme == 'https'
    
    # Prepare authentication header if needed
    auth_header = None
    if parsed.username and parsed.password:
        credentials = f"{parsed.username}:{parsed.password}"
        encoded = base64.b64encode(credentials.encode()).decode()
        auth_header = f"Basic {encoded}"

    # Calculate samples
    if samples is None:
        samples = 30
    
    logger.info(f"Testing load balancer with {samples} fresh TCP connections...")

    node_counts = {}
    successful_samples = 0
    source_ports = []

    for i in range(samples):
        sock = None
        try:
            # Create a fresh TCP socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            
            # Connect to the server
            sock.connect((host, port))
            
            # Get source port for 5-tuple analysis
            source_ip, source_port = sock.getsockname()
            source_ports.append(source_port)
            
            # Wrap with SSL if needed
            if use_ssl:
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                sock = context.wrap_socket(sock, server_hostname=host)
            
            # Prepare HTTP request
            http_request = f"GET / HTTP/1.1\r\n"
            http_request += f"Host: {host}:{port}\r\n"
            http_request += "Connection: close\r\n"
            http_request += "User-Agent: CrateDB-5Tuple-Tester/1.0\r\n"
            
            if auth_header:
                http_request += f"Authorization: {auth_header}\r\n"
            
            http_request += "\r\n"
            
            # Send request and read response
            sock.sendall(http_request.encode())
            
            response_data = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                response_data += chunk
            
            # Parse HTTP response
            response_text = response_data.decode('utf-8', errors='ignore')
            headers, body = response_text.split('\r\n\r\n', 1)
            
            # Extract JSON from body
            try:
                json_data = json.loads(body)
                node_name = json_data.get('name', 'unknown')
                
                # Simple node name shortening
                import re
                match = re.search(r'([a-z]+).*?(\d+)', node_name, re.IGNORECASE)
                if match:
                    short_name = f"{match.group(1)}-{match.group(2)}"
                else:
                    short_name = node_name[:10]
                
                node_counts[short_name] = node_counts.get(short_name, 0) + 1
                successful_samples += 1
                
            except Exception:
                pass  # Ignore JSON parsing errors
                
        except Exception:
            pass  # Ignore failed connections
        finally:
            if sock:
                try:
                    sock.close()
                except:
                    pass
        
        # Small delay to ensure different source ports
        time.sleep(0.05)

    logger.info(f"5-tuple test complete: {successful_samples}/{samples} successful")
    
    if node_counts:
        unique_ports = len(set(source_ports))
        unique_nodes = len(node_counts)
        
        logger.info(f"Unique source ports: {unique_ports}, Unique nodes hit: {unique_nodes}")
        
        # Display visual distribution
        print("\n📈 NODE DISTRIBUTION:")
        for node_name, count in sorted(node_counts.items()):
            percentage = (count / successful_samples) * 100
            bar_length = int(percentage / 2)  # Scale bar to reasonable length
            bar = "█" * bar_length
            print(f"   {node_name:15} | {count:3d} hits | {percentage:5.1f}% | {bar}")
        
        # Analysis
        if unique_nodes == 1 and unique_ports > 5:
            logger.warning("⚠️  Load balancer may NOT be using 5-tuple distribution")
            logger.info(f"Evidence: {unique_ports} different source ports, but all hit same node")
        elif unique_nodes > 1:
            logger.info("✅ Load balancer IS distributing across nodes")
            logger.info(f"Evidence: {unique_ports} source ports hit {unique_nodes} different nodes")
        else:
            logger.info("❓ Inconclusive results - need more data")
    
    return node_counts


def sample_load_balancer(connection_string: str, samples: int = None) -> Dict[str, int]:
    """Sample load balancer distribution with multiple requests."""
    logger.info("Starting load balancer analysis...")

    # Create temporary session for sampling
    parsed = urlparse(connection_string)
    base_url = f"{parsed.scheme}://{parsed.hostname}:{parsed.port or 4200}"

    session = requests.Session()
    if parsed.username and parsed.password:
        session.auth = HTTPBasicAuth(parsed.username, parsed.password)

    # First, query sys.nodes to see how many nodes are in the cluster
    expected_nodes = 1  # Default fallback
    try:
        logger.info("Querying sys.nodes to determine cluster size...")
        payload = {"stmt": "SELECT count(*) as node_count FROM sys.nodes"}
        response = session.post(
            f"{base_url}/_sql",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        if response.status_code == 200:
            data = response.json()
            if data.get('rows') and len(data['rows']) > 0:
                expected_nodes = data['rows'][0][0]
                logger.info(f"Cluster has {expected_nodes} node(s)")
        else:
            logger.warning(f"Failed to query sys.nodes: HTTP {response.status_code}")
    except Exception as e:
        logger.warning(f"Could not determine cluster size: {e}")
        logger.info("Using default assumption of 1 node")

    # Calculate samples: 10 requests per node, minimum 30
    if samples is None:
        samples = max(30, expected_nodes * 10)

    logger.info(f"Sampling load balancer with {samples} requests ({samples//expected_nodes} per expected node)...")

    node_counts = {}
    successful_samples = 0

    for i in range(samples):
        try:
            response = session.get(base_url, timeout=3)
            if response.status_code == 200:
                data = response.json()
                node_name = data.get('name')
                if node_name:
                    # Simple node name shortening
                    import re
                    # Extract meaningful part + number
                    match = re.search(r'([a-z]+).*?(\d+)', node_name, re.IGNORECASE)
                    if match:
                        short_name = f"{match.group(1)}-{match.group(2)}"
                    else:
                        short_name = node_name[:10]  # Fallback truncation

                    node_counts[short_name] = node_counts.get(short_name, 0) + 1
                    successful_samples += 1
        except Exception:
            pass  # Ignore failed samples

    logger.info(f"Load balancer sampling complete: {successful_samples}/{samples} successful")

    if node_counts:
        actual_nodes = len(node_counts)
        summary = ', '.join([f"{node}={count}" for node, count in sorted(node_counts.items())])
        logger.info(f"Expected nodes: {expected_nodes}, Actual nodes seen: {actual_nodes}")
        logger.info(f"Load balancer distribution: {summary}")
    
        if actual_nodes != expected_nodes:
            logger.warning(f"Node count mismatch! Expected {expected_nodes} but saw {actual_nodes} nodes")
    
    return node_counts


def create_table(client: CrateDBClient, table_name: str, num_objects: int = 0) -> None:
    """Create the target table in CrateDB."""

    # Base table definition
    base_columns = """
        id TEXT PRIMARY KEY,
        timestamp TIMESTAMP WITH TIME ZONE,
        region TEXT,
        product_category TEXT,
        event_type TEXT,
        user_id INTEGER,
        user_segment TEXT,
        amount DOUBLE PRECISION,
        quantity INTEGER,
        metadata OBJECT(DYNAMIC)"""

    # Add object columns
    object_columns = ""
    if num_objects > 0:
        object_cols = [f"        obj_{i} TEXT" for i in range(num_objects)]
        object_columns = ",\n" + ",\n".join(object_cols)

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} ({base_columns}{object_columns}
    ) WITH (
        number_of_replicas = 1
        -- "refresh_interval" = 1000
    )
    """

    logger.info(f"Creating table: {table_name}")
    logger.info(f"SQL: {create_sql}")

    try:
        result = client.execute(create_sql)
        logger.success(f"Table '{table_name}' created successfully")
        logger.debug(f"Create table result: {result}")
    except Exception as e:
        logger.error(f"Failed to create table '{table_name}': {e}")
        logger.error(f"SQL: {create_sql}")
        raise


def worker_thread(worker_id: int, connection_string: str, table_name: str,
                 insert_sql: str, batch_size: int, batch_interval: float,
                 monitor: PerformanceMonitor, stop_event: threading.Event, num_objects: int = 0,
                 lb_distribution: Dict[str, int] = None):
    """Worker thread that generates and inserts records."""
    thread_logger = logger.bind(worker=worker_id)
    thread_logger.info(f"Worker {worker_id} starting...")

    try:
        # Each worker gets its own client and generator
        client = CrateDBClient(connection_string)
        generator = RecordGenerator(num_objects)

        thread_logger.info(f"Worker {worker_id} connected - load balancer distribution determined at startup")

        while not stop_event.is_set():
            try:
                # Generate batch of records
                batch = generator.generate_batch(batch_size)

                # Insert batch
                result = client.execute_bulk(insert_sql, batch)
                monitor.add_records(batch_size)

                # Log successful batch (debug level)
                thread_logger.debug(f"Worker {worker_id} inserted batch of {batch_size} records")

                # Wait before next batch
                if batch_interval > 0:
                    time.sleep(batch_interval)

            except Exception as e:
                thread_logger.error(f"Worker {worker_id} error inserting batch: {e}")
                monitor.add_error()

                # Exponential backoff on errors
                error_delay = min(5.0, 1.0 * (monitor.errors + 1))
                thread_logger.warning(f"Worker {worker_id} waiting {error_delay:.1f}s before retry...")
                time.sleep(error_delay)

    except Exception as e:
        thread_logger.error(f"Worker {worker_id} fatal error: {e}")
        monitor.add_error()

    thread_logger.info(f"Worker {worker_id} finished")


def reporter_thread(monitor: PerformanceMonitor, stop_event: threading.Event, num_threads: int):
    """Background thread to report performance every 10 seconds."""
    while not stop_event.wait(10):
        rate = monitor.get_current_rate()
        stats = monitor.get_overall_stats()

        logger.info(
            f"Performance: {rate:.1f} records/sec (current), "
            f"{stats['overall_rate']:.1f} records/sec (avg), "
            f"Total: {stats['total_records']:,} records, "
            f"Batches: {stats['total_batches']:,}, "
            f"Threads: {num_threads}, "
            f"Errors: {stats['errors']}"
        )


@click.command()
@click.option(
    "--table-name",
    required=True,
    help="Name of the CrateDB table to insert records into"
)
@click.option(
    "--connection-string",
    help="CrateDB connection string (can be read from .env file)"
)
@click.option(
    "--duration",
    type=int,
    required=True,
    help="Duration to run the generator (in minutes)"
)
@click.option(
    "--batch-size",
    type=int,
    default=100,
    help="Number of records to insert in each batch (default: 100)"
)
@click.option(
    "--batch-interval",
    type=float,
    default=0.1,
    help="Interval between batches in seconds (default: 0.1)"
)
@click.option(
    "--threads",
    type=int,
    default=1,
    help="Number of parallel worker threads (default: 1)"
)
@click.option(
    "--objects",
    type=int,
    default=0,
    help="Number of additional low-cardinality object columns to create (default: 0)"
)
def cli(table_name: str, connection_string: Optional[str], duration: int,
        batch_size: int, batch_interval: float, threads: int, objects: int):
    """
    Generate and insert random records into CrateDB for testing purposes.

    This script generates realistic test data and inserts it into a CrateDB table
    with performance monitoring and reporting.
    """
    # Load environment variables
    load_dotenv()

    # Configure logging
    log_level = os.getenv("LOG_LEVEL", "INFO")
    logger.remove()
    logger.add(
        sys.stderr,
        level=log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>"
    )

    # Get connection string
    if not connection_string:
        connection_string = os.getenv("CRATE_CONNECTION_STRING")
        if not connection_string:
            logger.error("Connection string not provided via --connection-string or CRATE_CONNECTION_STRING env var")
            sys.exit(1)

    logger.info(f"Starting CrateDB record generator")
    logger.info(f"Table: {table_name}")
    logger.info(f"Duration: {duration} minutes")
    logger.info(f"Batch size: {batch_size}")
    logger.info(f"Batch interval: {batch_interval}s")
    logger.info(f"Worker threads: {threads}")
    if objects > 0:
        logger.info(f"Object columns: {objects}")

    try:
        # Initialize components
        client = CrateDBClient(connection_string)
        monitor = PerformanceMonitor()

        # Sample load balancer distribution first (using 5-tuple test)
        lb_distribution = sample_load_balancer_5tuple(connection_string)

        # Create table
        create_table(client, table_name, objects)

        # Prepare insert statement
        base_fields = "id, timestamp, region, product_category, event_type, user_id, user_segment, amount, quantity, metadata"
        base_placeholders = "?, ?, ?, ?, ?, ?, ?, ?, ?, ?"

        if objects > 0:
            object_fields = ", " + ", ".join([f"obj_{i}" for i in range(objects)])
            object_placeholders = ", " + ", ".join(["?" for _ in range(objects)])
        else:
            object_fields = ""
            object_placeholders = ""

        insert_sql = f"""
        INSERT INTO {table_name}
        ({base_fields}{object_fields})
        VALUES ({base_placeholders}{object_placeholders})
        """

        # Start performance reporting thread
        stop_event = threading.Event()
        reporter = threading.Thread(
            target=reporter_thread,
            args=(monitor, stop_event, threads),
            daemon=True
        )
        reporter.start()

        # Start worker threads
        workers = []
        for i in range(threads):
            worker = threading.Thread(
                target=worker_thread,
                args=(i, connection_string, table_name, insert_sql,
                      batch_size, batch_interval, monitor, stop_event, objects, lb_distribution),
                daemon=True
            )
            workers.append(worker)
            worker.start()

        logger.info(f"Started {threads} worker threads...")

        # Wait for duration
        try:
            time.sleep(duration * 60)
            logger.info("Duration completed, stopping workers...")
        except KeyboardInterrupt:
            logger.warning("Received interrupt signal, stopping workers...")

        # Signal all threads to stop
        stop_event.set()

        # Wait for all workers to finish (with timeout)
        logger.info("Waiting for workers to finish...")
        for i, worker in enumerate(workers):
            worker.join(timeout=5.0)
            if worker.is_alive():
                logger.warning(f"Worker {i} did not finish within timeout")

        # Stop reporting thread
        stop_event.set()

        # Final performance summary
        final_stats = monitor.get_overall_stats()

        logger.info("=" * 60)
        logger.info("FINAL PERFORMANCE SUMMARY")
        logger.info("=" * 60)
        logger.success(f"Worker threads: {threads}")
        logger.success(f"Total records inserted: {final_stats['total_records']:,}")
        logger.success(f"Total batches: {final_stats['total_batches']:,}")
        logger.success(f"Total runtime: {final_stats['elapsed_time']:.1f} seconds")
        logger.success(f"Average insertion rate: {final_stats['overall_rate']:.1f} records/second")
        logger.success(f"Records per thread: {final_stats['total_records'] // threads:,} avg")
        logger.success(f"Total errors: {final_stats['errors']}")

        if final_stats['errors'] > 0:
            error_rate = (final_stats['errors'] / final_stats['total_batches']) * 100
            logger.warning(f"Error rate: {error_rate:.2f}%")

        # Load balancer distribution summary
        if lb_distribution:
            logger.info("Load Balancer Distribution:")
            node_summary = []
            total_samples = sum(lb_distribution.values())
            for node, count in sorted(lb_distribution.items()):
                percentage = (count / total_samples) * 100 if total_samples > 0 else 0
                node_summary.append(f"{node}={count} ({percentage:.1f}%)")
            logger.success(f"Distribution: {', '.join(node_summary)}")
            logger.info("(Based on 5-tuple HTTP connection test - actual SQL connections may behave differently)")

        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli()
