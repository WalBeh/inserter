-- CrateDB Table Creation Script
-- This file contains the SQL statement to create the test table for record generation

-- Create the main events table
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

-- Example usage:
-- Replace 'events' with your desired table name when running the script
--
-- The script automatically creates this table structure, but you can also
-- create it manually using this SQL statement in the CrateDB admin UI
-- or via any SQL client.

-- Table Description:
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

-- Performance Settings:
-- - number_of_replicas = 0: No replicas for faster writes (adjust for production)
-- - refresh_interval = 1000: Refresh every second for near real-time queries

-- Sample Data Structure:
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
