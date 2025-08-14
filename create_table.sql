-- CrateDB Table Creation Script
-- This file contains the SQL statement to create the test table for record generation

-- Create the main events table (base version with 10 columns)
CREATE TABLE IF NOT EXISTS events (
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
);

-- Example with --objects 100 flag (110 total columns)
-- This creates a wide table with many low-cardinality columns for testing
CREATE TABLE IF NOT EXISTS events_with_objects (
    id TEXT PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE,
    region TEXT,
    product_category TEXT,
    event_type TEXT,
    user_id INTEGER,
    user_segment TEXT,
    amount DOUBLE PRECISION,
    quantity INTEGER,
    metadata OBJECT(DYNAMIC),
    obj_0 TEXT,
    obj_1 TEXT,
    obj_2 TEXT,
    -- ... obj_3 through obj_97 would be here
    obj_98 TEXT,
    obj_99 TEXT
) WITH (
    number_of_replicas = 0,
    "refresh_interval" = 1000
);

-- Example usage:
-- Usage Examples:
--
-- Basic table (10 columns):
-- ./run.sh --table-name events --duration 5
--
-- Wide table with 100 object columns (110 total):
-- ./run.sh --table-name events_wide --duration 5 --objects 100
--
-- The script automatically creates the appropriate table structure based on --objects flag

-- Base Table Description (10 columns):
-- - id: Unique identifier for each record (TEXT, PRIMARY KEY)
-- - timestamp: When the event occurred (TIMESTAMP WITH TIME ZONE)
-- - region: Geographic region (TEXT) - 4 possible values
-- - product_category: Category of product (TEXT) - 5 possible values
-- - event_type: Type of user action (TEXT) - 5 possible values
-- - user_id: Numeric user identifier (INTEGER) - range 1-10,000
-- - user_segment: User classification (TEXT) - 4 possible values
-- - amount: Monetary amount (DOUBLE PRECISION) - range 1.0-1000.0
-- - quantity: Item quantity (INTEGER) - range 1-100
-- - metadata: JSON object with additional data (OBJECT DYNAMIC)

-- Object Columns (when using --objects flag):
-- - obj_0, obj_1, ... obj_N: Low cardinality TEXT fields
-- - Each object has 3-8 possible values (e.g., "val_0", "val_1", "val_2")
-- - Useful for testing wide tables and column performance

-- Performance Settings:
-- - number_of_replicas = 0: No replicas for faster writes (adjust for production)
-- - refresh_interval = 1000: Refresh every second for near real-time queries

-- Sample Data Structure (base table):
-- {
--   "id": "123e4567-e89b-12d3-a456-426614174000",
--   "timestamp": "2024-01-15T10:30:00.000Z",
--   "region": "us-east",
--   "product_category": "electronics",
--   "event_type": "purchase",
--   "user_id": 1234,
--   "user_segment": "premium",
--   "amount": 299.99,
--   "quantity": 2,
--   "metadata": {
--     "browser": "chrome",
--     "os": "macos",
--     "session_id": "987fcdeb-51a2-43d7-8f9e-123456789abc"
--   }
-- }

-- Sample Data Structure (with --objects 5):
-- {
--   ... (all base fields above) ...
--   "obj_0": "val_2",
--   "obj_1": "val_0",
--   "obj_2": "val_4",
--   "obj_3": "val_1",
--   "obj_4": "val_3"
-- }

-- Use Cases for Object Columns:
-- - Test wide table performance (100+ columns)
-- - Simulate real-world schemas with many dimensions
-- - Test CrateDB column storage efficiency
-- - Benchmark query performance on wide tables
