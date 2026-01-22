-- Migration: Create sync_jobs table for tracking historical synchronization jobs
-- Date: 2025-11-15
-- Description: Tracks background sync jobs for trades, orders, and positions

CREATE TABLE IF NOT EXISTS sync_jobs (
    id SERIAL PRIMARY KEY,

    -- Job metadata
    job_type VARCHAR(50) NOT NULL,  -- 'trade_sync', 'order_sync', 'position_sync'
    user_id INTEGER NOT NULL,
    trading_account_id INTEGER NOT NULL,

    -- Status tracking
    status VARCHAR(20) NOT NULL DEFAULT 'pending',  -- pending, running, completed, failed

    -- Date range (for historical syncs)
    start_date DATE NULL,
    end_date DATE NULL,

    -- Execution tracking
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    duration_seconds INTEGER NULL,

    -- Statistics
    records_fetched INTEGER DEFAULT 0,
    records_created INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_skipped INTEGER DEFAULT 0,
    errors_count INTEGER DEFAULT 0,

    -- Error details
    error_message TEXT NULL,
    error_details JSONB NULL,  -- List of error messages

    -- Trigger information
    triggered_by VARCHAR(50) NOT NULL,  -- 'manual', 'scheduled', 'api'
    trigger_metadata JSONB NULL,  -- Additional context

    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for efficient queries
CREATE INDEX idx_sync_jobs_user_id ON sync_jobs(user_id);
CREATE INDEX idx_sync_jobs_job_type ON sync_jobs(job_type);
CREATE INDEX idx_sync_jobs_status ON sync_jobs(status);
CREATE INDEX idx_sync_jobs_created_at ON sync_jobs(created_at DESC);
CREATE INDEX idx_sync_jobs_user_status ON sync_jobs(user_id, status);
CREATE INDEX idx_sync_jobs_user_type_status ON sync_jobs(user_id, job_type, status);

-- Comments
COMMENT ON TABLE sync_jobs IS 'Tracks background synchronization jobs for trades, orders, and positions';
COMMENT ON COLUMN sync_jobs.job_type IS 'Type of sync job: trade_sync, order_sync, position_sync';
COMMENT ON COLUMN sync_jobs.status IS 'Job status: pending, running, completed, failed';
COMMENT ON COLUMN sync_jobs.triggered_by IS 'Who triggered the job: manual, scheduled, api';
COMMENT ON COLUMN sync_jobs.error_details IS 'JSON array of error messages';
COMMENT ON COLUMN sync_jobs.trigger_metadata IS 'JSON object with additional context about the trigger';
