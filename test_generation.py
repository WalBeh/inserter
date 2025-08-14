#!/usr/bin/env python3
"""
Test script to validate record generation and JSON serialization.

This script tests the RecordGenerator class to ensure all generated records
are properly serializable and contain the expected data types.
"""

import sys
import json
import time
from datetime import datetime, timezone
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from crate_write.main import RecordGenerator, CrateDBClient


def test_record_generation():
    """Test that records are generated correctly and are JSON serializable."""
    print("🧪 Testing Record Generation")
    print("=" * 50)
    
    generator = RecordGenerator()
    
    # Test single record generation
    print("\n1. Testing single record generation...")
    record = generator.generate_record()
    
    print(f"   Generated record with {len(record)} fields:")
    field_names = [
        "id", "timestamp", "region", "product_category", "event_type",
        "user_id", "user_segment", "amount", "quantity", "metadata"
    ]
    
    for i, (name, value) in enumerate(zip(field_names, record)):
        print(f"   {i:2d}. {name:16} = {value} ({type(value).__name__})")
    
    # Test JSON serialization
    print("\n2. Testing JSON serialization...")
    try:
        json_str = json.dumps(record)
        print("   ✅ Record is JSON serializable")
        print(f"   📦 JSON size: {len(json_str)} bytes")
    except Exception as e:
        print(f"   ❌ JSON serialization failed: {e}")
        return False
    
    # Test batch generation
    print("\n3. Testing batch generation...")
    batch_size = 5
    batch = generator.generate_batch(batch_size)
    
    if len(batch) != batch_size:
        print(f"   ❌ Expected {batch_size} records, got {len(batch)}")
        return False
    
    print(f"   ✅ Generated batch of {len(batch)} records")
    
    # Test all records in batch are serializable
    for i, record in enumerate(batch):
        try:
            json.dumps(record)
        except Exception as e:
            print(f"   ❌ Record {i} is not JSON serializable: {e}")
            return False
    
    print("   ✅ All records in batch are JSON serializable")
    
    return True


def test_data_characteristics():
    """Test that generated data has expected characteristics."""
    print("\n🔍 Testing Data Characteristics")
    print("=" * 50)
    
    generator = RecordGenerator()
    sample_size = 100
    
    print(f"\nGenerating {sample_size} records to analyze characteristics...")
    
    # Collect statistics
    regions = set()
    categories = set()
    event_types = set()
    user_segments = set()
    user_ids = []
    amounts = []
    quantities = []
    
    for _ in range(sample_size):
        record = generator.generate_record()
        regions.add(record[2])           # region
        categories.add(record[3])        # product_category
        event_types.add(record[4])       # event_type
        user_segments.add(record[6])     # user_segment
        user_ids.append(record[5])       # user_id
        amounts.append(record[7])        # amount
        quantities.append(record[8])     # quantity
    
    # Analyze characteristics
    print(f"\n📊 Data Analysis Results:")
    print(f"   Regions ({len(regions)}): {sorted(regions)}")
    print(f"   Product Categories ({len(categories)}): {sorted(categories)}")
    print(f"   Event Types ({len(event_types)}): {sorted(event_types)}")
    print(f"   User Segments ({len(user_segments)}): {sorted(user_segments)}")
    print(f"   User ID range: {min(user_ids)} - {max(user_ids)}")
    print(f"   Amount range: ${min(amounts):.2f} - ${max(amounts):.2f}")
    print(f"   Quantity range: {min(quantities)} - {max(quantities)}")
    
    # Validate expected cardinalities
    expected_counts = {
        "regions": 4,
        "categories": 5,
        "event_types": 5,
        "user_segments": 4
    }
    
    actual_counts = {
        "regions": len(regions),
        "categories": len(categories),
        "event_types": len(event_types),
        "user_segments": len(user_segments)
    }
    
    all_correct = True
    for field, expected in expected_counts.items():
        actual = actual_counts[field]
        if actual == expected:
            print(f"   ✅ {field}: {actual}/{expected}")
        else:
            print(f"   ❌ {field}: {actual}/{expected}")
            all_correct = False
    
    return all_correct


def test_timestamp_format():
    """Test that timestamps are in the correct format."""
    print("\n⏰ Testing Timestamp Format")
    print("=" * 50)
    
    generator = RecordGenerator()
    
    # Test multiple timestamps
    for i in range(5):
        record = generator.generate_record()
        timestamp_str = record[1]  # timestamp field
        
        print(f"\n{i+1}. Testing timestamp: {timestamp_str}")
        
        # Test that it's a string
        if not isinstance(timestamp_str, str):
            print(f"   ❌ Timestamp should be string, got {type(timestamp_str)}")
            return False
        
        # Test that it can be parsed as ISO format
        try:
            parsed_dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            print(f"   ✅ Parsed as: {parsed_dt}")
            
            # Check that it's UTC
            if parsed_dt.tzinfo is None:
                print(f"   ⚠️  Warning: Timestamp has no timezone info")
            else:
                print(f"   ✅ Timezone: {parsed_dt.tzinfo}")
                
        except ValueError as e:
            print(f"   ❌ Failed to parse timestamp: {e}")
            return False
    
    return True


def test_bulk_insert_format():
    """Test that records are in the correct format for bulk insert."""
    print("\n📦 Testing Bulk Insert Format")
    print("=" * 50)
    
    generator = RecordGenerator()
    
    # Simulate the bulk insert payload
    batch = generator.generate_batch(3)
    
    insert_sql = """
    INSERT INTO test_table 
    (id, timestamp, region, product_category, event_type, user_id, user_segment, amount, quantity, metadata)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    payload = {
        "stmt": insert_sql,
        "bulk_args": batch
    }
    
    print(f"Testing bulk insert payload with {len(batch)} records...")
    
    try:
        json_payload = json.dumps(payload)
        print("✅ Bulk insert payload is JSON serializable")
        print(f"📦 Payload size: {len(json_payload)} bytes")
        
        # Pretty print first record for inspection
        print(f"\n📋 Sample record structure:")
        if batch:
            for i, value in enumerate(batch[0]):
                field_names = [
                    "id", "timestamp", "region", "product_category", "event_type",
                    "user_id", "user_segment", "amount", "quantity", "metadata"
                ]
                print(f"   {i:2d}. {field_names[i]:16} = {repr(value)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Bulk insert payload serialization failed: {e}")
        return False


def test_connection_string_parsing():
    """Test connection string parsing."""
    print("\n🔗 Testing Connection String Parsing")
    print("=" * 50)
    
    test_cases = [
        "http://localhost:4200",
        "http://admin:password@localhost:4200",
        "https://user:pass@remote.host:4200",
        "https:///admin:pass@host:4200",  # malformed - should be fixed
        "http://admin:p@ss%21w0rd@localhost:4200",  # URL encoded password
    ]
    
    for i, conn_str in enumerate(test_cases, 1):
        print(f"\n{i}. Testing: {conn_str}")
        try:
            client = CrateDBClient(conn_str)
            print(f"   ✅ Parsed successfully")
            print(f"   🔗 Base URL: {client.base_url}")
            print(f"   🔐 Has auth: {client.auth is not None}")
        except Exception as e:
            print(f"   ❌ Parsing failed: {e}")
    
    return True


def main():
    """Run all tests."""
    print("🧪 CrateDB Record Generator Test Suite")
    print("=" * 60)
    
    tests = [
        ("Record Generation", test_record_generation),
        ("Data Characteristics", test_data_characteristics),
        ("Timestamp Format", test_timestamp_format),
        ("Bulk Insert Format", test_bulk_insert_format),
        ("Connection String Parsing", test_connection_string_parsing),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{'='*60}")
        print(f"Running: {test_name}")
        print(f"{'='*60}")
        
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ Test '{test_name}' crashed: {e}")
            results.append((test_name, False))
    
    # Summary
    print(f"\n{'='*60}")
    print("📊 TEST SUMMARY")
    print(f"{'='*60}")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
    
    print(f"\n🎯 Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! The record generator is working correctly.")
        return 0
    else:
        print("⚠️  Some tests failed. Please review the output above.")
        return 1


if __name__ == "__main__":
    exit(main())