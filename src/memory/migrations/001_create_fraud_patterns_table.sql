-- Migration: 001_create_fraud_patterns_table.sql
-- Description: Create fraud_patterns table with pgvector support for persistent fraud pattern memory

BEGIN;

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS fraud_patterns (
    id SERIAL PRIMARY KEY,
    pattern_embedding vector(1536),
    advertiser_id VARCHAR(64),
    campaign_id VARCHAR(64),
    fraud_type VARCHAR(128),
    confidence_score FLOAT,
    pattern_description TEXT,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_seen TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fraud_patterns_embedding ON fraud_patterns
    USING ivfflat (pattern_embedding vector_cosine_ops) WITH (lists = 100);

CREATE INDEX IF NOT EXISTS idx_fraud_patterns_advertiser ON fraud_patterns (advertiser_id);
CREATE INDEX IF NOT EXISTS idx_fraud_patterns_fraud_type ON fraud_patterns (fraud_type);
CREATE INDEX IF NOT EXISTS idx_fraud_patterns_last_seen ON fraud_patterns (last_seen);

COMMIT;
