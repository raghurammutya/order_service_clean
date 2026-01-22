-- Migration: Create position_subscriptions table for real-time P&L tracking
-- Date: 2025-01-27
-- Purpose: Track which instruments need real-time tick subscriptions for positions/holdings

-- Create position_subscriptions table in order_service schema
CREATE TABLE IF NOT EXISTS order_service.position_subscriptions (
    id SERIAL PRIMARY KEY,

    -- Instrument identification
    instrument_token BIGINT NOT NULL,
    tradingsymbol VARCHAR(100) NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    segment VARCHAR(20) NOT NULL DEFAULT 'NSE',  -- NSE, NFO, BSE, BFO, MCX, CDS

    -- Account tracking (which accounts need this subscription)
    trading_account_id VARCHAR(100) NOT NULL,

    -- Source tracking
    source VARCHAR(20) NOT NULL DEFAULT 'position',  -- position, holding

    -- Subscription state
    is_active BOOLEAN NOT NULL DEFAULT true,
    is_subscribable BOOLEAN NOT NULL DEFAULT true,  -- false for bonds, debt, etc.

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    -- Unique constraint: one subscription per instrument per account per source
    UNIQUE(instrument_token, trading_account_id, source)
);

-- Indexes for efficient lookups
CREATE INDEX IF NOT EXISTS idx_position_subs_active
    ON order_service.position_subscriptions(is_active) WHERE is_active = true;

CREATE INDEX IF NOT EXISTS idx_position_subs_token
    ON order_service.position_subscriptions(instrument_token);

CREATE INDEX IF NOT EXISTS idx_position_subs_account
    ON order_service.position_subscriptions(trading_account_id);

CREATE INDEX IF NOT EXISTS idx_position_subs_source
    ON order_service.position_subscriptions(source);

CREATE INDEX IF NOT EXISTS idx_position_subs_subscribable
    ON order_service.position_subscriptions(is_subscribable) WHERE is_subscribable = true;

-- Comment on table
COMMENT ON TABLE order_service.position_subscriptions IS
    'Tracks instrument subscriptions required for real-time P&L updates on positions and holdings';

COMMENT ON COLUMN order_service.position_subscriptions.is_subscribable IS
    'False for instruments that cannot be subscribed (bonds, debt, SGBs). These use polling instead.';

-- Add last_price column to positions table for caching
ALTER TABLE order_service.positions
    ADD COLUMN IF NOT EXISTS instrument_token BIGINT;

-- Add index for fast lookups by instrument_token
CREATE INDEX IF NOT EXISTS idx_positions_instrument_token
    ON order_service.positions(instrument_token) WHERE instrument_token IS NOT NULL;

-- Create view for all active subscriptions (union of position and rebalancer subscriptions)
-- This view will be used by ticker_service to determine what to subscribe
CREATE OR REPLACE VIEW public.all_active_subscriptions AS
SELECT DISTINCT
    instrument_token,
    tradingsymbol,
    segment,
    'position' as subscription_source
FROM order_service.position_subscriptions
WHERE is_active = true AND is_subscribable = true

UNION

SELECT DISTINCT
    instrument_token,
    tradingsymbol,
    segment,
    'rebalancer' as subscription_source
FROM public.instrument_subscriptions
WHERE status = 'active';

COMMENT ON VIEW public.all_active_subscriptions IS
    'Unified view of all active subscriptions from both position tracking and strike rebalancer';
