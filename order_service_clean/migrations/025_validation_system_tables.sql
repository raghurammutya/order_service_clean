-- Migration: Create validation system tables
-- Purpose: Support stored validation results and auto-fix tracking
-- Date: 2025-01-22

-- Create validation sessions table
CREATE TABLE IF NOT EXISTS order_service.validation_sessions (
    validation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id BIGINT NOT NULL,
    trading_account_id VARCHAR(50),
    symbol VARCHAR(50),
    validation_type VARCHAR(50) NOT NULL DEFAULT 'tagging_integrity',
    status VARCHAR(20) NOT NULL DEFAULT 'pending', -- pending, running, completed, failed
    total_issues INTEGER DEFAULT 0,
    critical_issues INTEGER DEFAULT 0,
    auto_fixable_issues INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Create validation issues table  
CREATE TABLE IF NOT EXISTS order_service.validation_issues (
    issue_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    validation_id UUID NOT NULL REFERENCES order_service.validation_sessions(validation_id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL,
    issue_type VARCHAR(50) NOT NULL, -- orphan_order, orphan_position, strategy_mismatch, etc.
    field_name VARCHAR(50),
    invalid_value TEXT,
    expected_value TEXT,
    severity VARCHAR(20) NOT NULL, -- critical, warning, info
    message TEXT,
    auto_fixable BOOLEAN DEFAULT false,
    order_id BIGINT,
    position_id BIGINT,
    trade_id BIGINT,
    suggested_strategy_id INTEGER,
    suggested_portfolio_id INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create validation fix history table
CREATE TABLE IF NOT EXISTS order_service.validation_fix_history (
    fix_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    validation_id UUID NOT NULL REFERENCES order_service.validation_sessions(validation_id),
    user_id BIGINT NOT NULL,
    fix_type VARCHAR(50) NOT NULL, -- auto_fix, manual_fix
    issues_fixed INTEGER DEFAULT 0,
    dry_run BOOLEAN DEFAULT false,
    status VARCHAR(20) NOT NULL, -- success, failed, partial
    error_message TEXT,
    fixed_items JSONB DEFAULT '[]'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_validation_sessions_user_id ON order_service.validation_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_validation_sessions_status ON order_service.validation_sessions(status);
CREATE INDEX IF NOT EXISTS idx_validation_sessions_created_at ON order_service.validation_sessions(created_at);

CREATE INDEX IF NOT EXISTS idx_validation_issues_validation_id ON order_service.validation_issues(validation_id);
CREATE INDEX IF NOT EXISTS idx_validation_issues_user_id ON order_service.validation_issues(user_id);
CREATE INDEX IF NOT EXISTS idx_validation_issues_severity ON order_service.validation_issues(severity);
CREATE INDEX IF NOT EXISTS idx_validation_issues_auto_fixable ON order_service.validation_issues(auto_fixable);

CREATE INDEX IF NOT EXISTS idx_validation_fix_history_validation_id ON order_service.validation_fix_history(validation_id);
CREATE INDEX IF NOT EXISTS idx_validation_fix_history_user_id ON order_service.validation_fix_history(user_id);

-- Add comments
COMMENT ON TABLE order_service.validation_sessions IS 'Tracks validation runs with metadata and results summary';
COMMENT ON TABLE order_service.validation_issues IS 'Stores detailed validation issues found during validation runs';  
COMMENT ON TABLE order_service.validation_fix_history IS 'Tracks auto-fix and manual fix operations applied to validation issues';

-- Grant permissions (adjust as needed for your setup)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON order_service.validation_sessions TO order_service_app;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON order_service.validation_issues TO order_service_app;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON order_service.validation_fix_history TO order_service_app;