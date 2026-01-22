-- Migration: Create GTT (Good-Till-Triggered) orders table
-- Date: 2025-11-15
-- Description: Tracks conditional orders that trigger when price conditions are met

CREATE TABLE IF NOT EXISTS gtt_orders (
    id SERIAL PRIMARY KEY,

    -- User & Account
    user_id INTEGER NOT NULL,
    trading_account_id INTEGER NOT NULL,

    -- Broker Information
    broker_gtt_id INTEGER NULL,  -- Kite's GTT ID (assigned after creation)

    -- GTT Configuration
    gtt_type VARCHAR(20) NOT NULL,  -- 'single' or 'two-leg' (OCO)
    status VARCHAR(20) NOT NULL DEFAULT 'active',  -- active, triggered, cancelled, expired, deleted

    -- Instrument Details
    symbol VARCHAR(50) NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    tradingsymbol VARCHAR(50) NOT NULL,  -- Broker's trading symbol

    -- Trigger Conditions
    condition JSONB NOT NULL,  -- Trigger conditions (price levels, type)

    -- Orders to Place When Triggered
    orders JSONB NOT NULL,  -- Array of order specifications

    -- Metadata
    expires_at TIMESTAMP NULL,  -- When this GTT expires (optional)

    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    triggered_at TIMESTAMP NULL,  -- When the GTT was triggered
    cancelled_at TIMESTAMP NULL,  -- When the GTT was cancelled

    -- Broker Metadata
    broker_metadata JSONB NULL,  -- Additional data from broker

    -- User Notes
    user_tag VARCHAR(100) NULL,  -- Custom user tag
    user_notes TEXT NULL  -- User notes about this GTT
);

-- Indexes for efficient queries
CREATE INDEX idx_gtt_orders_user_id ON gtt_orders(user_id);
CREATE INDEX idx_gtt_orders_trading_account_id ON gtt_orders(trading_account_id);
CREATE INDEX idx_gtt_orders_status ON gtt_orders(status);
CREATE INDEX idx_gtt_orders_broker_gtt_id ON gtt_orders(broker_gtt_id);
CREATE INDEX idx_gtt_orders_symbol ON gtt_orders(symbol);
CREATE INDEX idx_gtt_orders_created_at ON gtt_orders(created_at DESC);
CREATE INDEX idx_gtt_orders_user_status ON gtt_orders(user_id, status);

-- Comments
COMMENT ON TABLE gtt_orders IS 'Tracks Good-Till-Triggered (GTT) conditional orders';
COMMENT ON COLUMN gtt_orders.gtt_type IS 'single (stop-loss/target) or two-leg (OCO - One Cancels Other)';
COMMENT ON COLUMN gtt_orders.status IS 'active, triggered, cancelled, expired, deleted';
COMMENT ON COLUMN gtt_orders.condition IS 'JSON: {trigger_type: "single"/"two-leg", trigger_values: [price1, price2]}';
COMMENT ON COLUMN gtt_orders.orders IS 'JSON array: [{transaction_type, order_type, quantity, price, ...}]';
