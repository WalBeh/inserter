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
from typing import Dict, List, Any, Optional, Tuple
import random
import json

import click
from loguru import logger
from dotenv import load_dotenv
from faker import Faker
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urlparse


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
def cli(table_name: Optional[str], connection_string: Optional[str], duration: Optional[int],
        batch_size: int, batch_interval: float, threads: int, objects: int, test_loadbalancer: bool):
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

    logger.info(f"Starting CrateDB record generator")
    logger.info(f"🔗 Connection: {sanitize_connection_string(connection_string)}")
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
            logger.info("(Based on 5-tuple HTTP connection test - SQL worker threads distribute similarly at startup)")

        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli()
