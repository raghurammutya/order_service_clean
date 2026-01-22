-- Order State History Table for Audit Trail
-- Migration: 004_create_order_state_history.sql
-- Purpose: SEBI compliance - track all order state transitions
-- Date: 2025-11-22

-- Create order_state_history table
CREATE TABLE IF NOT EXISTS order_state_history (
    id BIGSERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,

    -- State transition
    old_status VARCHAR(20),  -- NULL for first state (order creation)
    new_status VARCHAR(20) NOT NULL,

    -- Actor (who made the change)
    changed_by_user_id INTEGER,  -- User ID if manual action
    changed_by_system VARCHAR(50),  -- 'order_service', 'broker_webhook', 'reconciliation', etc.

    -- Context
    reason TEXT,  -- "User cancelled", "Circuit breaker opened", "Broker rejected", etc.
    broker_response TEXT,  -- Broker error message if applicable

    -- Additional data
    metadata JSONB,  -- Extra context (IP address, order params changed, etc.)

    -- Timing
    changed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create indexes for common queries
CREATE INDEX idx_order_state_history_order_id ON order_state_history(order_id);
CREATE INDEX idx_order_state_history_changed_at ON order_state_history(changed_at);
CREATE INDEX idx_order_state_history_new_status ON order_state_history(new_status);
CREATE INDEX idx_order_state_history_changed_by_user ON order_state_history(changed_by_user_id)
WHERE changed_by_user_id IS NOT NULL;

-- Index for system actions (useful for operational queries)
CREATE INDEX idx_order_state_history_system ON order_state_history(changed_by_system)
WHERE changed_by_system IS NOT NULL;

-- Comments for documentation
COMMENT ON TABLE order_state_history IS 'Audit trail for all order state transitions (SEBI compliance requirement)';
COMMENT ON COLUMN order_state_history.order_id IS 'Reference to orders table';
COMMENT ON COLUMN order_state_history.old_status IS 'Previous status (NULL for order creation)';
COMMENT ON COLUMN order_state_history.new_status IS 'New status after transition';
COMMENT ON COLUMN order_state_history.changed_by_user_id IS 'User who triggered the change (for manual actions)';
COMMENT ON COLUMN order_state_history.changed_by_system IS 'System component that triggered change (order_service, broker_webhook, reconciliation)';
COMMENT ON COLUMN order_state_history.reason IS 'Human-readable reason for state change';
COMMENT ON COLUMN order_state_history.broker_response IS 'Broker response/error message if applicable';
COMMENT ON COLUMN order_state_history.metadata IS 'Additional context in JSON format';
COMMENT ON COLUMN order_state_history.changed_at IS 'When the state change occurred';

-- Grant permissions (adjust as needed for your setup)
-- GRANT SELECT, INSERT ON order_state_history TO order_service_user;
-- GRANT SELECT ON order_state_history TO order_service_readonly;

-- Example queries for verification:
--
-- Get complete history for an order:
-- SELECT * FROM order_state_history WHERE order_id = 123 ORDER BY changed_at DESC;
--
-- Find all user cancellations in last 24 hours:
-- SELECT * FROM order_state_history
-- WHERE new_status = 'CANCELLED'
--   AND changed_by_user_id IS NOT NULL
--   AND changed_at > NOW() - INTERVAL '24 hours';
--
-- Find drift corrections by reconciliation:
-- SELECT * FROM order_state_history
-- WHERE changed_by_system = 'reconciliation_worker'
--   AND reason LIKE '%drift%';
