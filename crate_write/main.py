#!/usr/bin/env python3
"""
CrateDB Record Generator and Inserter

A script that generates random records and inserts them into CrateDB with
performance monitoring and reporting.
"""

import os
import sys
import time
import asyncio
import threading
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Tuple
import random
import json
import ssl

import click
from loguru import logger
from dotenv import load_dotenv
from faker import Faker
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urlparse
import aiohttp
import orjson


def sanitize_connection_string(connection_string: str) -> str:
    """Remove credentials from connection string for safe logging."""
    try:
        parsed = urlparse(connection_string)
        # Reconstruct URL without credentials
        sanitized = f"{parsed.scheme}://{parsed.hostname}:{parsed.port or 4200}"
        return sanitized
    except Exception:
        return "invalid-connection-string"


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
                raise ValueError(f"Missing scheme in connection string: {sanitize_connection_string(connection_string)}")
            if not parsed.hostname:
                raise ValueError(f"Missing hostname in connection string: {sanitize_connection_string(connection_string)}")

            self.base_url = f"{parsed.scheme}://{parsed.hostname}:{parsed.port or 4200}"
            self.auth = None

            if parsed.username and parsed.password:
                self.auth = HTTPBasicAuth(parsed.username, parsed.password)

            logger.info(f"Connecting to CrateDB at: {sanitize_connection_string(connection_string)}")

        except Exception as e:
            logger.error(f"Failed to parse connection string '{sanitize_connection_string(connection_string)}': {e}")
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


class AsyncCrateClient:
    """Async HTTP client for CrateDB using aiohttp for maximum throughput."""

    def __init__(self, session: aiohttp.ClientSession, base_url: str, compress: bool = True):
        self.session = session
        self.base_url = base_url
        self.sql_url = f"{base_url}/_sql"
        self.compress = compress

    def _compress(self, data: bytes) -> bytes:
        """Gzip compress payload for smaller network transfer."""
        import gzip
        return gzip.compress(data, compresslevel=1)  # level 1 = fastest, still ~8x ratio on JSON

    async def execute_bulk(self, sql: str, bulk_args: List[List], monitor: "AsyncPerformanceMonitor" = None) -> dict:
        """Execute bulk insert with async HTTP, optionally gzip-compressed."""
        payload = orjson.dumps({"stmt": sql, "bulk_args": bulk_args})

        if self.compress:
            payload = self._compress(payload)
            headers = {"Content-Type": "application/json", "Content-Encoding": "gzip"}
        else:
            headers = {"Content-Type": "application/json"}

        payload_size = len(payload)
        start = time.monotonic()

        async with self.session.post(
            self.sql_url,
            data=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=60),
        ) as response:
            response.raise_for_status()
            result = await response.json()

        latency_ms = (time.monotonic() - start) * 1000.0
        if monitor:
            monitor.add_request_stats(payload_size, latency_ms)

        return result

    async def execute(self, sql: str) -> dict:
        """Execute a SQL statement."""
        payload = {"stmt": sql}
        async with self.session.post(
            self.sql_url,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as response:
            response.raise_for_status()
            return await response.json()


class AsyncPerformanceMonitor:
    """Lock-free performance monitor for single-threaded async event loop."""

    def __init__(self):
        self.start_time = time.monotonic()
        self.total_records = 0
        self.total_batches = 0
        self.errors = 0
        self.last_report_time = time.monotonic()
        self.last_report_records = 0
        self.rate_samples: List[float] = []
        self.total_bytes_sent = 0
        self.latency_samples: List[float] = []

    def add_records(self, count: int):
        self.total_records += count
        self.total_batches += 1

    def add_request_stats(self, bytes_sent: int, latency_ms: float):
        self.total_bytes_sent += bytes_sent
        self.latency_samples.append(latency_ms)

    def add_error(self):
        self.errors += 1

    def get_current_rate(self) -> float:
        now = time.monotonic()
        time_diff = now - self.last_report_time
        if time_diff < 1.0:
            return 0.0
        records_diff = self.total_records - self.last_report_records
        rate = records_diff / time_diff
        self.last_report_time = now
        self.last_report_records = self.total_records
        self.rate_samples.append(rate)
        return rate

    def get_overall_stats(self) -> Dict[str, Any]:
        elapsed = time.monotonic() - self.start_time
        return {
            "total_records": self.total_records,
            "total_batches": self.total_batches,
            "elapsed_time": elapsed,
            "overall_rate": self.total_records / elapsed if elapsed > 0 else 0,
            "errors": self.errors,
        }

    def get_percentile_stats(self) -> Dict[str, float]:
        """Compute min/max/avg/p90/p95 from collected rate samples."""
        if not self.rate_samples:
            return {"avg": 0, "min": 0, "max": 0, "p90": 0, "p95": 0}
        s = sorted(self.rate_samples)
        n = len(s)
        return {
            "avg": round(sum(s) / n, 1),
            "min": round(s[0], 1),
            "max": round(s[-1], 1),
            "p90": round(s[int(n * 0.9)] if n > 1 else s[0], 1),
            "p95": round(s[int(n * 0.95)] if n > 1 else s[0], 1),
        }

    def _percentiles(self, samples: List[float]) -> Dict[str, float]:
        if not samples:
            return {"avg": 0, "min": 0, "max": 0, "p90": 0, "p95": 0}
        s = sorted(samples)
        n = len(s)
        return {
            "avg": round(sum(s) / n, 1),
            "min": round(s[0], 1),
            "max": round(s[-1], 1),
            "p90": round(s[int(n * 0.9)] if n > 1 else s[0], 1),
            "p95": round(s[int(n * 0.95)] if n > 1 else s[0], 1),
        }

    def get_latency_stats(self) -> Dict[str, float]:
        return self._percentiles(self.latency_samples)

    def get_network_stats(self) -> Dict[str, Any]:
        elapsed = time.monotonic() - self.start_time
        mbps = (self.total_bytes_sent * 8 / 1_000_000) / elapsed if elapsed > 0 else 0
        return {
            "bytes_sent": self.total_bytes_sent,
            "bandwidth_mbps": round(mbps, 2),
        }


async def async_worker(
    worker_id: int,
    client: AsyncCrateClient,
    insert_sql: str,
    batch_size: int,
    batch_interval: float,
    monitor: AsyncPerformanceMonitor,
    stop_event: asyncio.Event,
    num_objects: int,
):
    """Async worker task: generate batch → POST → repeat. No GIL contention."""
    generator = RecordGenerator(num_objects)

    while not stop_event.is_set():
        try:
            batch = generator.generate_batch(batch_size)
            await client.execute_bulk(insert_sql, batch, monitor=monitor)
            monitor.add_records(batch_size)

            if batch_interval > 0:
                await asyncio.sleep(batch_interval)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Worker {worker_id} error: {e}")
            monitor.add_error()
            await asyncio.sleep(min(5.0, 1.0))


async def async_reporter(monitor: AsyncPerformanceMonitor, stop_event: asyncio.Event, num_tasks: int, quiet: bool = False):
    """Report performance every 10 seconds. In quiet mode, collect samples silently."""
    while not stop_event.is_set():
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            break
        rate = monitor.get_current_rate()
        if not quiet:
            stats = monitor.get_overall_stats()
            logger.info(
                f"Performance: {rate:.1f} records/sec (current), "
                f"{stats['overall_rate']:.1f} records/sec (avg), "
                f"Total: {stats['total_records']:,} records, "
                f"Batches: {stats['total_batches']:,}, "
                f"Tasks: {num_tasks}, "
                f"Errors: {stats['errors']}"
            )


async def query_cluster_info(client: AsyncCrateClient) -> Dict[str, Any]:
    """Query CrateDB sys tables for cluster hardware/version info."""
    info: Dict[str, Any] = {}
    try:
        r = await client.execute("SELECT os_info['available_processors'] FROM sys.nodes")
        cpus = [row[0] for row in r.get("rows", [])]
        info["cpus_per_node"] = cpus
        info["total_cpus"] = sum(cpus)
        info["nodes"] = len(cpus)

        r = await client.execute("SELECT mem['used'] FROM sys.nodes")
        info["memory_used_bytes"] = [row[0] for row in r.get("rows", [])]

        r = await client.execute("SELECT fs['total']['size'] FROM sys.nodes")
        info["disk_total_bytes"] = [row[0] for row in r.get("rows", [])]

        r = await client.execute("SELECT heap['max'], version['number'] FROM sys.nodes LIMIT 1")
        if r.get("rows"):
            info["heap_max_bytes"] = r["rows"][0][0]
            info["version"] = r["rows"][0][1]
    except Exception as e:
        logger.warning(f"Failed to query cluster info: {e}")
    return info


async def run_async_engine(
    connection_string: str,
    table_name: str,
    duration: int,
    batch_size: int,
    batch_interval: float,
    num_tasks: int,
    objects: int,
    benchmark: bool = False,
    shards: int = 4,
    replicas: int = 1,
    compress: bool = True,
):
    """Main async engine: creates aiohttp session, spawns workers, runs for duration."""

    # Skip LB test in benchmark mode
    if not benchmark:
        logger.info("Running load balancer distribution test...")
        lb_distribution = sample_load_balancer_5tuple(connection_string)
        if lb_distribution:
            node_summary = []
            total_samples = sum(lb_distribution.values())
            for node, count in sorted(lb_distribution.items()):
                pct = (count / total_samples) * 100 if total_samples > 0 else 0
                node_summary.append(f"{node}={count} ({pct:.1f}%)")
            logger.info(f"Load balancer distribution: {', '.join(node_summary)}")

    parsed = urlparse(connection_string)
    base_url = f"{parsed.scheme}://{parsed.hostname}:{parsed.port or 4200}"

    # Build auth header
    auth = None
    if parsed.username and parsed.password:
        auth = aiohttp.BasicAuth(parsed.username, parsed.password)

    # TLS: skip verification for self-signed certs (like the sync client)
    ssl_ctx = None
    if parsed.scheme == "https":
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

    # Connection pool: unlimited connections, persistent keep-alive
    connector = aiohttp.TCPConnector(
        limit=0,  # no limit on concurrent connections
        ttl_dns_cache=300,
        keepalive_timeout=60,
        ssl=ssl_ctx,
    )

    async with aiohttp.ClientSession(
        connector=connector,
        auth=auth,
        headers={"Content-Type": "application/json"},
    ) as session:
        client = AsyncCrateClient(session, base_url, compress=compress)

        # Create table (reuse sync client for setup)
        setup_client = CrateDBClient(connection_string)
        create_table(setup_client, table_name, objects, shards=shards, replicas=replicas)

        # Prepare insert SQL
        base_fields = "id, timestamp, region, product_category, event_type, user_id, user_segment, amount, quantity, metadata"
        base_placeholders = "?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
        if objects > 0:
            object_fields = ", " + ", ".join([f"obj_{i}" for i in range(objects)])
            object_placeholders = ", " + ", ".join(["?" for _ in range(objects)])
        else:
            object_fields = ""
            object_placeholders = ""

        insert_sql = f"INSERT INTO {table_name} ({base_fields}{object_fields}) VALUES ({base_placeholders}{object_placeholders})"

        monitor = AsyncPerformanceMonitor()
        stop_event = asyncio.Event()

        # Query cluster info (needed for benchmark JSON)
        cluster_info = await query_cluster_info(client)

        # Get pre-existing record count and rejected writes baseline
        pre_count = 0
        pre_rejected = 0
        try:
            r = await client.execute(f"REFRESH TABLE {table_name}")
            r = await client.execute(f"SELECT COUNT(*) FROM {table_name}")
            pre_count = r.get("rows", [[0]])[0][0]
        except Exception:
            pass
        try:
            r = await client.execute(
                "SELECT SUM(pool['rejected']) FROM (SELECT UNNEST(thread_pools) AS pool FROM sys.nodes) x WHERE pool['name'] = 'write'"
            )
            pre_rejected = r.get("rows", [[0]])[0][0] or 0
        except Exception:
            pass

        # Spawn reporter
        reporter_task = asyncio.create_task(
            async_reporter(monitor, stop_event, num_tasks, quiet=benchmark)
        )

        # Spawn workers
        worker_tasks = []
        for i in range(num_tasks):
            task = asyncio.create_task(
                async_worker(i, client, insert_sql, batch_size, batch_interval, monitor, stop_event, objects)
            )
            worker_tasks.append(task)

        logger.info(f"Started {num_tasks} async worker tasks...")

        # Run for duration or until Ctrl+C
        try:
            await asyncio.sleep(duration * 60)
            logger.info("Duration completed, stopping workers...")
        except asyncio.CancelledError:
            logger.warning("Received interrupt, stopping workers...")

        # Signal stop and wait for workers
        stop_event.set()
        for task in worker_tasks:
            task.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        reporter_task.cancel()
        try:
            await reporter_task
        except asyncio.CancelledError:
            pass

        # Collect one final rate sample
        monitor.get_current_rate()

        # Final stats
        final_stats = monitor.get_overall_stats()

        # Verify records in CrateDB
        verified_count = 0
        try:
            await client.execute(f"REFRESH TABLE {table_name}")
            result = await client.execute(f"SELECT COUNT(*) FROM {table_name}")
            db_count = result.get("rows", [[0]])[0][0]
            verified_count = db_count - pre_count
        except Exception as e:
            logger.warning(f"Failed to verify record count: {e}")

        # Check for rejected writes (delta from pre-run baseline)
        rejected_writes = 0
        try:
            result = await client.execute(
                "SELECT SUM(pool['rejected']) FROM (SELECT UNNEST(thread_pools) AS pool FROM sys.nodes) x WHERE pool['name'] = 'write'"
            )
            post_rejected = result.get("rows", [[0]])[0][0] or 0
            rejected_writes = max(0, post_rejected - pre_rejected)
        except Exception as e:
            logger.warning(f"Failed to query rejected writes: {e}")

        if benchmark:
            # Benchmark mode: output single-line JSON to stdout
            rate_stats = monitor.get_percentile_stats()
            total_cpus = cluster_info.get("total_cpus", 1)
            per_cpu = {k: round(v / total_cpus, 1) for k, v in rate_stats.items()}

            benchmark_result = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "client": "python-http",
                "cluster": cluster_info,
                "config": {
                    "threads": num_tasks,
                    "batch_size": batch_size,
                    "batch_interval": batch_interval,
                    "duration_minutes": duration,
                    "table_name": table_name,
                    "shards": shards,
                    "replicas": replicas,
                },
                "results": {
                    "total_records": final_stats["total_records"],
                    "total_batches": final_stats["total_batches"],
                    "runtime_seconds": round(final_stats["elapsed_time"], 1),
                    "errors": final_stats["errors"],
                    "records_per_second": rate_stats,
                    "records_per_cpu_second": per_cpu,
                    "request_latency_ms": monitor.get_latency_stats(),
                    **monitor.get_network_stats(),
                    "verified_count": verified_count,
                    "rejected_writes": rejected_writes,
                    "rejected_pct": round((rejected_writes / max(final_stats["total_records"], 1)) * 100, 2),
                },
            }
            # JSONL to stdout (one line, appendable)
            sys.stdout.buffer.write(orjson.dumps(benchmark_result))
            sys.stdout.buffer.write(b"\n")
            sys.stdout.buffer.flush()
            # Summary to stderr for quick reading
            version = cluster_info.get("version", "?")
            p90_total = rate_stats.get('p90', 0)
            rej_pct = (rejected_writes / max(final_stats["total_records"], 1)) * 100
            rej = f" | REJECTED: {rejected_writes} ({rej_pct:.1f}%)" if rejected_writes > 0 else ""
            print(f"CrateDB {version} | {total_cpus} CPUs | p90={p90_total:.0f} rec/s | per CPU: avg={per_cpu['avg']:.0f} p95={per_cpu['p95']:.0f} max={per_cpu['max']:.0f}{rej}", file=sys.stderr)
        else:
            # Normal mode: human-readable output
            sent = final_stats["total_records"]
            logger.info("=" * 60)
            logger.info("FINAL PERFORMANCE SUMMARY")
            logger.info("=" * 60)
            logger.success(f"Async worker tasks: {num_tasks}")
            logger.success(f"Total records sent: {sent:,}")
            logger.success(f"Total batches: {final_stats['total_batches']:,}")
            logger.success(f"Total runtime: {final_stats['elapsed_time']:.1f} seconds")
            logger.success(f"Average insertion rate: {final_stats['overall_rate']:.1f} records/second")
            logger.success(f"Records per task: {sent // max(num_tasks, 1):,} avg")
            logger.success(f"Total errors: {final_stats['errors']}")
            logger.info("=" * 60)

            logger.info("RECORD VERIFICATION")
            logger.info(f"Records sent: {sent:,}  |  Verified in CrateDB: {verified_count:,}")
            if verified_count == sent:
                logger.success("MATCH")
            elif verified_count > sent:
                logger.info(f"CrateDB has {verified_count - sent:,} extra (pre-existing data)")
            else:
                missing = sent - verified_count
                logger.warning(f"MISMATCH - {missing:,} missing ({(missing/sent)*100:.2f}% loss)")
            logger.info("=" * 60)

            if final_stats["errors"] > 0:
                error_rate = (final_stats["errors"] / max(final_stats["total_batches"], 1)) * 100
                logger.warning(f"Error rate: {error_rate:.2f}%")

        # Clean up: drop the benchmark table
        try:
            await client.execute(f"DROP TABLE IF EXISTS {table_name}")
            if not benchmark:
                logger.info(f"Cleaned up table '{table_name}'")
        except Exception:
            pass


def make_fresh_request(connection_string: str) -> Tuple[Dict, str, int, Dict]:
    """
    Make a single HTTP request with a fresh TCP connection.
    Returns: (response_data, node_name, source_port, connection_info)
    """
    import socket
    import ssl
    import json
    import base64

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

    sock = None
    try:
        # Create a fresh TCP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10.0)

        # Connect to the server
        start_connect = time.time()
        sock.connect((host, port))
        connect_time = time.time() - start_connect

        # Get local address (source IP and port)
        source_ip, source_port = sock.getsockname()
        dest_ip, dest_port = sock.getpeername()

        # Wrap with SSL if needed
        if use_ssl:
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            sock = context.wrap_socket(sock, server_hostname=host)

        # Prepare HTTP request
        http_request = f"GET / HTTP/1.1\r\n"
        http_request += f"Host: {host}:{port}\r\n"
        http_request += "Connection: close\r\n"  # Force connection close
        http_request += "User-Agent: CrateDB-5Tuple-Tester/1.0\r\n"

        if auth_header:
            http_request += f"Authorization: {auth_header}\r\n"

        http_request += "\r\n"

        # Send request
        start_request = time.time()
        sock.sendall(http_request.encode())

        # Read response
        response_data = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            response_data += chunk

        request_time = time.time() - start_request

        # Parse HTTP response
        response_text = response_data.decode('utf-8', errors='ignore')
        headers, body = response_text.split('\r\n\r\n', 1)

        # Extract JSON from body
        try:
            json_data = json.loads(body)
            node_name = json_data.get('name', 'unknown')
        except:
            node_name = 'parse_error'

        connection_info = {
            'source_ip': source_ip,
            'source_port': source_port,
            'dest_ip': dest_ip,
            'dest_port': dest_port,
            'connect_time_ms': connect_time * 1000,
            'request_time_ms': request_time * 1000,
            'total_time_ms': (connect_time + request_time) * 1000
        }

        return json_data, node_name, source_port, connection_info

    except Exception as e:
        return {}, f"error: {e}", 0, {}
    finally:
        if sock:
            try:
                sock.close()
            except:
                pass


def test_5tuple_distribution_comprehensive(connection_string: str, num_requests: int = None) -> Dict:
    """
    Test load balancer distribution using fresh connections with comprehensive analysis.
    Each request will have a different source port, allowing proper 5-tuple load balancing testing.
    """
    parsed = urlparse(connection_string)
    host = parsed.hostname
    port = parsed.port or 4200
    use_ssl = parsed.scheme == 'https'
    base_url = f"{parsed.scheme}://{host}:{port}"

    print(f"🔍 5-TUPLE LOAD BALANCER TEST")
    print("=" * 60)
    print(f"Target: {host}:{port} ({'HTTPS' if use_ssl else 'HTTP'})")
    
    # Query sys.nodes to determine cluster size
    expected_nodes = 1  # Default fallback
    try:
        print("📋 Querying sys.nodes to determine cluster size...")
        
        # Create session for sys.nodes query
        session = requests.Session()
        if parsed.username and parsed.password:
            session.auth = HTTPBasicAuth(parsed.username, parsed.password)
        
        payload = {"stmt": "SELECT count(*) as node_count FROM sys.nodes"}
        response = session.post(
            f"{base_url}/_sql",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('rows') and len(data['rows']) > 0:
                expected_nodes = data['rows'][0][0]
                print(f"✅ Cluster has {expected_nodes} node(s)")
        else:
            print(f"⚠️  Failed to query sys.nodes: HTTP {response.status_code}")
            print(f"   Using default assumption of 1 node")
    except Exception as e:
        print(f"⚠️  Could not determine cluster size: {e}")
        print(f"   Using default assumption of 1 node")
    
    # Calculate test requests: 30 per node, minimum 30
    if num_requests is None:
        num_requests = max(30, expected_nodes * 30)
    
    print(f"📊 Test plan: {num_requests} requests ({num_requests//expected_nodes} per expected node)")
    print(f"Requests: {num_requests} (each with fresh TCP connection)")
    print()

    results = []
    node_counts = {}
    source_ports = []
    failed_requests = 0

    print("📊 Request Details:")
    print("Req# |    Node    | SrcPort | ConnTime | ReqTime | TotalTime")
    print("-" * 65)

    for i in range(num_requests):
        try:
            json_data, node_name, source_port, conn_info = make_fresh_request(connection_string)

            if node_name.startswith('error:'):
                failed_requests += 1
                print(f"{i+1:4d} | {'ERROR':10} | {'N/A':7} | {'N/A':8} | {'N/A':7} | {node_name}")
                continue

            # Track results
            results.append({
                'request_num': i + 1,
                'node_name': node_name,
                'source_port': source_port,
                'connection_info': conn_info
            })

            node_counts[node_name] = node_counts.get(node_name, 0) + 1
            source_ports.append(source_port)

            # Print request details
            conn_time = conn_info.get('connect_time_ms', 0)
            req_time = conn_info.get('request_time_ms', 0)
            total_time = conn_info.get('total_time_ms', 0)

            print(f"{i+1:4d} | {node_name:10} | {source_port:7d} | {conn_time:6.1f}ms | {req_time:5.1f}ms | {total_time:7.1f}ms")

            # Small delay to ensure different source ports
            time.sleep(0.1)

        except KeyboardInterrupt:
            print(f"\n⚠️  Test interrupted by user")
            break
        except Exception as e:
            failed_requests += 1
            print(f"{i+1:4d} | {'ERROR':10} | {'N/A':7} | {'N/A':8} | {'N/A':7} | {e}")

    print("-" * 65)

    # Analyze results
    successful_requests = len(results)
    unique_ports = len(set(source_ports)) if source_ports else 0
    unique_nodes = len(node_counts)

    print(f"\n📊 SUMMARY:")
    print(f"   Total requests: {num_requests}")
    print(f"   Successful: {successful_requests}")
    print(f"   Failed: {failed_requests}")
    print(f"   Unique source ports: {unique_ports}")
    print(f"   Unique nodes hit: {unique_nodes}")

    if successful_requests > 0:
        avg_times = {
            'connect': sum(r['connection_info'].get('connect_time_ms', 0) for r in results) / successful_requests,
            'request': sum(r['connection_info'].get('request_time_ms', 0) for r in results) / successful_requests,
            'total': sum(r['connection_info'].get('total_time_ms', 0) for r in results) / successful_requests
        }
        print(f"   Avg connect time: {avg_times['connect']:.1f}ms")
        print(f"   Avg request time: {avg_times['request']:.1f}ms")
        print(f"   Avg total time: {avg_times['total']:.1f}ms")

    # Distribution analysis
    print(f"\n📈 NODE DISTRIBUTION:")
    if node_counts:
        for node_name, count in sorted(node_counts.items()):
            percentage = (count / successful_requests) * 100
            bar = "█" * int(percentage / 2)
            print(f"   {node_name:15} | {count:3d} hits | {percentage:5.1f}% | {bar}")

# 5-tuple analysis
    print(f"\n🔍 5-TUPLE LOAD BALANCING ANALYSIS:")

    if unique_ports < 2:
        print("   ❌ INCONCLUSIVE: Need more unique source ports to test")
        print("   💡 Try increasing request count or reducing delay")
    else:
        print(f"   ✅ Good test conditions: {unique_ports} different source ports")

        if unique_nodes == 1:
            print("   🚨 VERDICT: Load balancer NOT using 5-tuple distribution")
            print(f"   📝 Evidence: {unique_ports} different source ports, but all hit same node")
            print("   🔧 All requests had different 5-tuples but same destination")
            print("   💭 Possible causes:")
            print("      - Load balancer using different algorithm (round-robin, least-conn)")
            print("      - Sticky sessions based on client IP")
            print("      - Only one healthy backend node")
            print("      - Load balancer misconfiguration")

        elif unique_nodes > 1:
            print("   ✅ VERDICT: Load balancer IS distributing across nodes")
            print(f"   📝 Evidence: {unique_ports} source ports hit {unique_nodes} different nodes")

            # Check if distribution correlates with source port
            port_to_node = {}
            for result in results:
                port = result['source_port']
                node = result['node_name']
                port_to_node[port] = node

            # Simple test: check if similar ports tend to hit same nodes
            sorted_ports = sorted(port_to_node.items())
            print("   🔍 Port-to-Node mapping (first 10):")
            for port, node in sorted_ports[:10]:
                print(f"      Port {port} → {node}")

            # Check for patterns
            if len(set(port_to_node.values())) == len(port_to_node):
                print("   📊 Pattern: Each port hits a different node (possible 5-tuple)")
            else:
                print("   📊 Pattern: Some ports hit same nodes (possible hash collision)")

    # Port range analysis
    if source_ports:
        port_range = max(source_ports) - min(source_ports)
        print(f"\n📡 SOURCE PORT ANALYSIS:")
        print(f"   Port range: {min(source_ports)} - {max(source_ports)} (span: {port_range})")
        print(f"   Port utilization: {unique_ports}/{port_range+1} ports used")

        if port_range > 100:
            print("   ✅ Good port diversity for 5-tuple testing")
        else:
            print("   ⚠️  Limited port range - may affect 5-tuple hash distribution")

    print("\n" + "=" * 60)
    print("🎯 FINAL VERDICT")
    print("=" * 60)
    
    print(f"📊 CLUSTER ANALYSIS:")
    print(f"   Expected nodes: {expected_nodes}")
    print(f"   Nodes hit during test: {unique_nodes}")
    
    if unique_nodes == expected_nodes:
        print(f"   ✅ Perfect distribution - hit all {expected_nodes} nodes")
    elif unique_nodes < expected_nodes:
        print(f"   ⚠️  Partial distribution - hit {unique_nodes}/{expected_nodes} nodes")
        print(f"   💭 Possible causes: hash distribution, unhealthy nodes, or more requests needed")
    else:
        print(f"   🤔 Unexpected - hit more nodes ({unique_nodes}) than expected ({expected_nodes})")

    if unique_nodes == 1 and unique_ports > 5:
        print("\n🚨 CONFIRMED: Load balancer NOT using 5-tuple distribution")
        print("📋 Evidence:")
        print(f"   • {unique_ports} different source ports")
        print(f"   • All requests hit the same node")
        print(f"   • Each request had unique 5-tuple values")
        print("\n💡 This explains why simple tests might show single-node routing!")
        print("   Even with connection pooling disabled, traffic goes to one node.")

    elif unique_nodes > 1:
        print("\n✅ Load balancer IS distributing traffic across nodes")
        print("📋 Evidence:")
        print(f"   • {unique_ports} different source ports")
        print(f"   • Traffic distributed across {unique_nodes} nodes")

    else:
        print("\n❓ Inconclusive results - need more data")

    print(f"\n🔧 Recommendation for your load testing:")
    if unique_nodes == 1:
        print("   • Contact CrateDB Cloud support about load balancer config")
        print("   • Performance tests will only stress one node")
        print("   • Consider using multiple client IPs if possible")
    elif unique_nodes == expected_nodes:
        print("   • Load balancer is working optimally")
        print("   • Performance tests will distribute perfectly across all nodes")
        print("   • Multiple worker threads will utilize different nodes")
    else:
        print("   • Load balancer appears to be working correctly")
        print("   • Performance tests should distribute across nodes")
        print("   • Multiple worker threads will utilize different nodes")
        if unique_nodes < expected_nodes:
            print(f"   • Consider running more requests to hit all {expected_nodes} nodes")

    return {
        'total_requests': num_requests,
        'successful_requests': successful_requests,
        'failed_requests': failed_requests,
        'unique_ports': unique_ports,
        'unique_nodes': unique_nodes,
        'expected_nodes': expected_nodes,
        'node_distribution': node_counts,
        'results': results
    }


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
        json_data, node_name, source_port, conn_info = make_fresh_request(connection_string)

        if not node_name.startswith('error:'):
            # Simple node name shortening
            import re
            match = re.search(r'([a-z]+).*?(\d+)', node_name, re.IGNORECASE)
            if match:
                short_name = f"{match.group(1)}-{match.group(2)}"
            else:
                short_name = node_name[:10]

            node_counts[short_name] = node_counts.get(short_name, 0) + 1
            successful_samples += 1
            source_ports.append(source_port)

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


def create_table(client: CrateDBClient, table_name: str, num_objects: int = 0, shards: int = 4, replicas: int = 1) -> None:
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
    ) CLUSTERED INTO {shards} SHARDS
    WITH (
        number_of_replicas = {replicas}
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
    required=False,
    help="Name of the CrateDB table to insert records into (required unless --test-loadbalancer)"
)
@click.option(
    "--connection-string",
    help="CrateDB connection string (can be read from .env file)"
)
@click.option(
    "--duration",
    type=int,
    required=False,
    help="Duration to run the generator (in minutes) (required unless --test-loadbalancer)"
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
@click.option(
    "--test-loadbalancer",
    is_flag=True,
    help="Run only the 5-tuple load balancer test (no table creation or data insertion)"
)
@click.option(
    "--benchmark",
    is_flag=True,
    help="Benchmark mode: minimal output during run, JSON result to stdout"
)
@click.option(
    "--shards",
    type=int,
    default=4,
    help="Number of shards for table creation (default: 4)"
)
@click.option(
    "--replicas",
    type=int,
    default=1,
    help="Number of replicas for table creation (default: 1)"
)
@click.option(
    "--no-compression",
    is_flag=True,
    help="Disable gzip compression (faster on localhost/low-latency)"
)
def cli(table_name: Optional[str], connection_string: Optional[str], duration: Optional[int],
        batch_size: int, batch_interval: float, threads: int, objects: int, test_loadbalancer: bool,
        benchmark: bool, shards: int, replicas: int, no_compression: bool):
    """
    Generate and insert random records into CrateDB for testing purposes.

    This script generates realistic test data and inserts it into a CrateDB table
    with performance monitoring and reporting.

    Use --test-loadbalancer to run only the load balancer distribution test.
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

    # Handle load balancer test mode
    if test_loadbalancer:
        logger.info("🚀 CrateDB 5-Tuple Load Balancer Test")
        logger.info("=" * 60)
        logger.info("This test creates fresh TCP connections to properly test")
        logger.info("whether load balancers use 5-tuple hashing for distribution.")
        logger.info("")
        logger.info(f"🔗 Connection: {sanitize_connection_string(connection_string)}")
        logger.info("")

        try:
            results = test_5tuple_distribution_comprehensive(connection_string)
            logger.info("✅ Load balancer test completed successfully")
            sys.exit(0)
        except Exception as e:
            logger.error(f"❌ Load balancer test failed: {e}")
            sys.exit(1)

    # Validate required arguments for data generation mode
    if not table_name:
        logger.error("--table-name is required when not using --test-loadbalancer")
        sys.exit(1)
    if duration is None:
        logger.error("--duration is required when not using --test-loadbalancer")
        sys.exit(1)

    logger.info(f"Starting CrateDB record generator (async engine)")
    logger.info(f"🔗 Connection: {sanitize_connection_string(connection_string)}")
    logger.info(f"Table: {table_name}")
    logger.info(f"Duration: {duration} minutes")
    logger.info(f"Batch size: {batch_size}")
    logger.info(f"Batch interval: {batch_interval}s")
    logger.info(f"Async worker tasks: {threads}")
    if objects > 0:
        logger.info(f"Object columns: {objects}")

    if benchmark:
        # In benchmark mode, suppress loguru output (logs go to stderr anyway)
        logger.remove()
        logger.add(sys.stderr, level="WARNING",
                   format="<level>{level: <8}</level> | <cyan>{message}</cyan>")

    try:
        asyncio.run(run_async_engine(
            connection_string=connection_string,
            table_name=table_name,
            duration=duration,
            batch_size=batch_size,
            batch_interval=batch_interval,
            num_tasks=threads,
            objects=objects,
            benchmark=benchmark,
            shards=shards,
            replicas=replicas,
            compress=not no_compression,
        ))
    except KeyboardInterrupt:
        logger.warning("Interrupted")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli()
