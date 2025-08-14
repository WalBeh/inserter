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
    
    def __init__(self):
        self.fake = Faker()
        # Keep cardinality reasonable by limiting choices
        self.regions = ["us-east", "us-west", "eu-central", "ap-southeast"]
        self.product_categories = ["electronics", "books", "clothing", "home", "sports"]
        self.event_types = ["view", "click", "purchase", "cart_add", "cart_remove"]
        self.user_segments = ["premium", "standard", "basic", "trial"]
        self.base_time = datetime.now(timezone.utc)
        
    def generate_record(self) -> List[Any]:
        """Generate a single random record."""
        # Add slight randomization to timestamp (within last 60 seconds)
        timestamp_offset = timedelta(seconds=random.randint(-60, 0))
        timestamp = (self.base_time + timestamp_offset).isoformat()
        
        return [
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
    
    def generate_batch(self, batch_size: int) -> List[List[Any]]:
        """Generate a batch of records."""
        return [self.generate_record() for _ in range(batch_size)]


def create_table(client: CrateDBClient, table_name: str) -> None:
    """Create the target table in CrateDB."""
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
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
    ) WITH (
        number_of_replicas = 0,
        "refresh_interval" = 1000
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


def reporter_thread(monitor: PerformanceMonitor, stop_event: threading.Event):
    """Background thread to report performance every 10 seconds."""
    while not stop_event.wait(10):
        rate = monitor.get_current_rate()
        stats = monitor.get_overall_stats()
        
        logger.info(
            f"Performance: {rate:.1f} records/sec (current), "
            f"{stats['overall_rate']:.1f} records/sec (avg), "
            f"Total: {stats['total_records']:,} records, "
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
def cli(table_name: str, connection_string: Optional[str], duration: int, 
        batch_size: int, batch_interval: float):
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
    
    try:
        # Initialize components
        client = CrateDBClient(connection_string)
        generator = RecordGenerator()
        monitor = PerformanceMonitor()
        
        # Create table
        create_table(client, table_name)
        
        # Prepare insert statement
        insert_sql = f"""
        INSERT INTO {table_name} 
        (id, timestamp, region, product_category, event_type, user_id, user_segment, amount, quantity, metadata)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Start performance reporting thread
        stop_event = threading.Event()
        reporter = threading.Thread(
            target=reporter_thread,
            args=(monitor, stop_event),
            daemon=True
        )
        reporter.start()
        
        # Calculate end time
        end_time = time.time() + (duration * 60)
        
        logger.info("Starting record generation and insertion...")
        
        # Main generation loop
        while time.time() < end_time:
            try:
                # Generate batch of records
                batch = generator.generate_batch(batch_size)
                
                # Insert batch
                result = client.execute_bulk(insert_sql, batch)
                monitor.add_records(batch_size)
                
                # Log successful batch (debug level)
                logger.debug(f"Successfully inserted batch of {batch_size} records")
                
                # Wait before next batch
                time.sleep(batch_interval)
                
            except KeyboardInterrupt:
                logger.warning("Received interrupt signal, stopping...")
                break
            except Exception as e:
                logger.error(f"Error inserting batch: {e}")
                logger.error(f"Batch size: {batch_size}")
                logger.error(f"Insert SQL: {insert_sql}")
                if 'batch' in locals() and batch:
                    logger.error(f"Sample record: {batch[0]}")
                monitor.add_error()
                
                # Exponential backoff on errors
                error_delay = min(5.0, 1.0 * (monitor.errors + 1))
                logger.warning(f"Waiting {error_delay:.1f}s before retry...")
                time.sleep(error_delay)
        
        # Stop reporting thread
        stop_event.set()
        
        # Final performance summary
        final_stats = monitor.get_overall_stats()
        
        logger.info("=" * 60)
        logger.info("FINAL PERFORMANCE SUMMARY")
        logger.info("=" * 60)
        logger.success(f"Total records inserted: {final_stats['total_records']:,}")
        logger.success(f"Total batches: {final_stats['total_batches']:,}")
        logger.success(f"Total runtime: {final_stats['elapsed_time']:.1f} seconds")
        logger.success(f"Average insertion rate: {final_stats['overall_rate']:.1f} records/second")
        logger.success(f"Total errors: {final_stats['errors']}")
        
        if final_stats['errors'] > 0:
            error_rate = (final_stats['errors'] / final_stats['total_batches']) * 100
            logger.warning(f"Error rate: {error_rate:.2f}%")
        
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli()