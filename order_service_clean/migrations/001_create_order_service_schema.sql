-- Migration 001: Create Order Service Schema and Tables
-- Description: Initialize order_service schema with orders, trades, and positions tables
-- Date: 2025-11-15
-- Phase: Phase 2 - Order Execution Service

-- =========================================
-- SCHEMA CREATION
-- =========================================

CREATE SCHEMA IF NOT EXISTS order_service;

-- =========================================
-- TABLE: orders
-- =========================================

CREATE TABLE IF NOT EXISTS order_service.orders (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- User & Account
    user_id INTEGER NOT NULL,
    trading_account_id INTEGER NOT NULL,

    -- Broker Information
    broker_order_id VARCHAR(100) UNIQUE,
    broker_tag VARCHAR(50),

    -- Order Details
    symbol VARCHAR(50) NOT NULL,
    exchange VARCHAR(10) NOT NULL,

    -- Order Specifications
    transaction_type VARCHAR(4) NOT NULL CHECK (transaction_type IN ('BUY', 'SELL')),
    order_type VARCHAR(10) NOT NULL CHECK (order_type IN ('MARKET', 'LIMIT', 'SL', 'SL-M')),
    product_type VARCHAR(10) NOT NULL CHECK (product_type IN ('CNC', 'MIS', 'NRML')),
    variety VARCHAR(20) NOT NULL DEFAULT 'regular' CHECK (variety IN ('regular', 'amo', 'iceberg', 'auction')),

    -- Quantity
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    filled_quantity INTEGER NOT NULL DEFAULT 0 CHECK (filled_quantity >= 0),
    pending_quantity INTEGER NOT NULL CHECK (pending_quantity >= 0),
    cancelled_quantity INTEGER NOT NULL DEFAULT 0 CHECK (cancelled_quantity >= 0),

    -- Price
    price NUMERIC(18, 2),
    trigger_price NUMERIC(18, 2),
    average_price NUMERIC(18, 2),

    -- Status
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING' CHECK (status IN (
        'PENDING', 'SUBMITTED', 'OPEN', 'COMPLETE', 'CANCELLED', 'REJECTED', 'TRIGGER_PENDING'
    )),
    status_message TEXT,

    -- Validity
    validity VARCHAR(10) NOT NULL DEFAULT 'DAY' CHECK (validity IN ('DAY', 'IOC')),
    disclosed_quantity INTEGER,

    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    submitted_at TIMESTAMP,
    exchange_timestamp TIMESTAMP,

    -- Risk Checks
    risk_check_passed BOOLEAN NOT NULL DEFAULT FALSE,
    risk_check_details TEXT,

    -- Parent Order (for bracket/cover orders)
    parent_order_id INTEGER,

    -- Additional Data
    order_metadata TEXT,

    -- Constraints
    CONSTRAINT fk_parent_order FOREIGN KEY (parent_order_id) REFERENCES order_service.orders(id)
);

-- Indexes for orders table
CREATE INDEX idx_orders_user_id ON order_service.orders(user_id);
CREATE INDEX idx_orders_trading_account_id ON order_service.orders(trading_account_id);
CREATE INDEX idx_orders_symbol ON order_service.orders(symbol);
CREATE INDEX idx_orders_status ON order_service.orders(status);
CREATE INDEX idx_orders_created_at ON order_service.orders(created_at DESC);
CREATE INDEX idx_orders_broker_order_id ON order_service.orders(broker_order_id);
CREATE INDEX idx_orders_user_status ON order_service.orders(user_id, status);
CREATE INDEX idx_orders_user_created ON order_service.orders(user_id, created_at DESC);

-- Trigger to auto-update updated_at
CREATE OR REPLACE FUNCTION order_service.update_orders_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_orders_updated_at
    BEFORE UPDATE ON order_service.orders
    FOR EACH ROW
    EXECUTE FUNCTION order_service.update_orders_updated_at();

-- =========================================
-- TABLE: trades
-- =========================================

CREATE TABLE IF NOT EXISTS order_service.trades (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- References
    order_id INTEGER NOT NULL REFERENCES order_service.orders(id),
    broker_order_id VARCHAR(100) NOT NULL,
    broker_trade_id VARCHAR(100) UNIQUE NOT NULL,

    -- User & Account
    user_id INTEGER NOT NULL,
    trading_account_id INTEGER NOT NULL,

    -- Trade Details
    symbol VARCHAR(50) NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    transaction_type VARCHAR(4) NOT NULL CHECK (transaction_type IN ('BUY', 'SELL')),
    product_type VARCHAR(10) NOT NULL CHECK (product_type IN ('CNC', 'MIS', 'NRML')),

    -- Execution Details
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    price NUMERIC(18, 2) NOT NULL CHECK (price > 0),
    trade_value NUMERIC(18, 2) NOT NULL CHECK (trade_value > 0),

    -- Timestamps
    trade_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Indexes for trades table
CREATE INDEX idx_trades_order_id ON order_service.trades(order_id);
CREATE INDEX idx_trades_user_id ON order_service.trades(user_id);
CREATE INDEX idx_trades_symbol ON order_service.trades(symbol);
CREATE INDEX idx_trades_trade_time ON order_service.trades(trade_time DESC);
CREATE INDEX idx_trades_broker_trade_id ON order_service.trades(broker_trade_id);
CREATE INDEX idx_trades_user_trade_time ON order_service.trades(user_id, trade_time DESC);

-- =========================================
-- TABLE: positions
-- =========================================

CREATE TABLE IF NOT EXISTS order_service.positions (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- User & Account
    user_id INTEGER NOT NULL,
    trading_account_id INTEGER NOT NULL,

    -- Position Details
    symbol VARCHAR(50) NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    product_type VARCHAR(10) NOT NULL CHECK (product_type IN ('CNC', 'MIS', 'NRML')),

    -- Quantity
    quantity INTEGER NOT NULL DEFAULT 0,
    overnight_quantity INTEGER NOT NULL DEFAULT 0,
    day_quantity INTEGER NOT NULL DEFAULT 0,

    -- Buy Side
    buy_quantity INTEGER NOT NULL DEFAULT 0 CHECK (buy_quantity >= 0),
    buy_value NUMERIC(18, 2) NOT NULL DEFAULT 0 CHECK (buy_value >= 0),
    buy_price NUMERIC(18, 2),

    -- Sell Side
    sell_quantity INTEGER NOT NULL DEFAULT 0 CHECK (sell_quantity >= 0),
    sell_value NUMERIC(18, 2) NOT NULL DEFAULT 0 CHECK (sell_value >= 0),
    sell_price NUMERIC(18, 2),

    -- P&L Calculation
    realized_pnl NUMERIC(18, 2) NOT NULL DEFAULT 0,
    unrealized_pnl NUMERIC(18, 2) NOT NULL DEFAULT 0,
    total_pnl NUMERIC(18, 2) NOT NULL DEFAULT 0,

    -- Market Data
    last_price NUMERIC(18, 2),
    close_price NUMERIC(18, 2),

    -- Status
    is_open BOOLEAN NOT NULL DEFAULT TRUE,

    -- Timestamps
    opened_at TIMESTAMP NOT NULL DEFAULT NOW(),
    closed_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    -- Trading Day
    trading_day DATE NOT NULL,

    -- Unique constraint: one position per user+symbol+product per trading day
    CONSTRAINT unique_position UNIQUE (user_id, symbol, product_type, trading_day)
);

-- Indexes for positions table
CREATE INDEX idx_positions_user_id ON order_service.positions(user_id);
CREATE INDEX idx_positions_trading_account_id ON order_service.positions(trading_account_id);
CREATE INDEX idx_positions_symbol ON order_service.positions(symbol);
CREATE INDEX idx_positions_updated_at ON order_service.positions(updated_at DESC);
CREATE INDEX idx_positions_user_symbol ON order_service.positions(user_id, symbol, product_type);
CREATE INDEX idx_positions_trading_day ON order_service.positions(trading_day DESC);
CREATE INDEX idx_positions_open ON order_service.positions(is_open) WHERE is_open = TRUE;

-- Trigger to auto-update updated_at
CREATE OR REPLACE FUNCTION order_service.update_positions_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_positions_updated_at
    BEFORE UPDATE ON order_service.positions
    FOR EACH ROW
    EXECUTE FUNCTION order_service.update_positions_updated_at();

-- =========================================
-- COMMENTS
-- =========================================

COMMENT ON SCHEMA order_service IS 'Order execution and position tracking service';

COMMENT ON TABLE order_service.orders IS 'All trading orders placed through the platform';
COMMENT ON TABLE order_service.trades IS 'Individual trade executions (fills)';
COMMENT ON TABLE order_service.positions IS 'Current and historical trading positions';

-- =========================================
-- GRANTS (adjust as needed for your setup)
-- =========================================

-- Grant permissions to application user
GRANT USAGE ON SCHEMA order_service TO stocksblitz;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA order_service TO stocksblitz;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA order_service TO stocksblitz;

-- =========================================
-- VERIFICATION
-- =========================================

-- Verify tables were created
SELECT
    schemaname,
    tablename,
    tableowner
FROM pg_tables
WHERE schemaname = 'order_service'
ORDER BY tablename;

-- Verify indexes
SELECT
    schemaname,
    tablename,
    indexname
FROM pg_indexes
WHERE schemaname = 'order_service'
ORDER BY tablename, indexname;
