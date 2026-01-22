-- Migration: Add strategy_id foreign keys to order_service tables
-- Purpose: Link orders, trades, and positions to strategies for P&L tracking
-- Created: 2025-11-24
-- Ticket: PHASE1-STRATEGY-LINKAGE

-- ============================================================================
-- STEP 1: Add strategy_id column to orders (NULLABLE first for existing data)
-- ============================================================================

ALTER TABLE order_service.orders
ADD COLUMN IF NOT EXISTS strategy_id BIGINT;

-- Add comment
COMMENT ON COLUMN order_service.orders.strategy_id IS 'Foreign key to public.strategies(id) - tracks which strategy placed this order';

-- ============================================================================
-- STEP 2: Backfill strategy_id for existing orders
-- ============================================================================
-- For existing orders without strategy_id, assign to a default "Manual Trading" strategy
-- This ensures we don't lose historical data

DO $$
DECLARE
    default_strategy_id BIGINT;
BEGIN
    -- Check if "Manual Trading" strategy exists
    SELECT id INTO default_strategy_id
    FROM public.strategies
    WHERE name = 'Manual Trading - Legacy Orders'
    LIMIT 1;

    -- If not, create it
    IF default_strategy_id IS NULL THEN
        INSERT INTO public.strategies (
            name,
            description,
            strategy_type,
            tags,
            state,
            mode,
            is_active,
            created_at,
            updated_at
        ) VALUES (
            'Manual Trading - Legacy Orders',
            'Default strategy for orders placed before strategy tracking was implemented',
            'manual',
            ARRAY['legacy', 'manual'],
            'active',
            'live',
            true,
            NOW(),
            NOW()
        )
        RETURNING id INTO default_strategy_id;

        RAISE NOTICE 'Created default strategy with ID: %', default_strategy_id;
    ELSE
        RAISE NOTICE 'Using existing default strategy with ID: %', default_strategy_id;
    END IF;

    -- Backfill existing orders
    UPDATE order_service.orders
    SET strategy_id = default_strategy_id
    WHERE strategy_id IS NULL;

    RAISE NOTICE 'Backfilled % orders with default strategy_id',
        (SELECT COUNT(*) FROM order_service.orders WHERE strategy_id = default_strategy_id);
END $$;

-- ============================================================================
-- STEP 3: Make strategy_id NOT NULL and add foreign key constraint
-- ============================================================================

ALTER TABLE order_service.orders
ALTER COLUMN strategy_id SET NOT NULL;

ALTER TABLE order_service.orders
ADD CONSTRAINT fk_orders_strategy_id
FOREIGN KEY (strategy_id)
REFERENCES public.strategies(id)
ON DELETE RESTRICT  -- Prevent strategy deletion if orders exist
ON UPDATE CASCADE;

-- ============================================================================
-- STEP 4: Create indexes for performance (critical for real-time queries)
-- ============================================================================

-- Index for: "Get all orders for strategy X"
CREATE INDEX IF NOT EXISTS idx_orders_strategy_id
ON order_service.orders(strategy_id);

-- Index for: "Get recent orders for strategy X"
CREATE INDEX IF NOT EXISTS idx_orders_strategy_created
ON order_service.orders(strategy_id, created_at DESC);

-- Index for: "Get active orders for strategy X"
CREATE INDEX IF NOT EXISTS idx_orders_strategy_status
ON order_service.orders(strategy_id, status)
WHERE status IN ('PENDING', 'SUBMITTED', 'OPEN', 'TRIGGER_PENDING');

-- ============================================================================
-- STEP 5: Add strategy_id to trades table
-- ============================================================================

ALTER TABLE order_service.trades
ADD COLUMN IF NOT EXISTS strategy_id BIGINT;

COMMENT ON COLUMN order_service.trades.strategy_id IS 'Foreign key to public.strategies(id) - tracks which strategy executed this trade';

-- Backfill trades.strategy_id from orders.strategy_id
UPDATE order_service.trades t
SET strategy_id = o.strategy_id
FROM order_service.orders o
WHERE t.order_id = o.id
  AND t.strategy_id IS NULL;

-- Make NOT NULL and add foreign key
ALTER TABLE order_service.trades
ALTER COLUMN strategy_id SET NOT NULL;

ALTER TABLE order_service.trades
ADD CONSTRAINT fk_trades_strategy_id
FOREIGN KEY (strategy_id)
REFERENCES public.strategies(id)
ON DELETE RESTRICT
ON UPDATE CASCADE;

-- Create indexes for trades
CREATE INDEX IF NOT EXISTS idx_trades_strategy_id
ON order_service.trades(strategy_id);

CREATE INDEX IF NOT EXISTS idx_trades_strategy_time
ON order_service.trades(strategy_id, trade_time DESC);

-- ============================================================================
-- STEP 6: Add strategy_id to positions table
-- ============================================================================

ALTER TABLE order_service.positions
ADD COLUMN IF NOT EXISTS strategy_id BIGINT;

COMMENT ON COLUMN order_service.positions.strategy_id IS 'Foreign key to public.strategies(id) - tracks which strategy owns this position';

-- For existing positions, we need to infer strategy_id from recent orders
-- This is best-effort - positions without clear attribution go to default strategy
DO $$
DECLARE
    default_strategy_id BIGINT;
BEGIN
    SELECT id INTO default_strategy_id
    FROM public.strategies
    WHERE name = 'Manual Trading - Legacy Orders'
    LIMIT 1;

    -- Try to infer strategy_id from most recent order for that symbol/product
    UPDATE order_service.positions p
    SET strategy_id = (
        SELECT o.strategy_id
        FROM order_service.orders o
        WHERE o.symbol = p.symbol
          AND o.product_type = p.product_type
          AND o.user_id = p.user_id
          AND o.trading_account_id = p.trading_account_id
          AND o.status = 'COMPLETE'
        ORDER BY o.created_at DESC
        LIMIT 1
    )
    WHERE p.strategy_id IS NULL;

    -- Fallback to default strategy for positions we couldn't infer
    UPDATE order_service.positions
    SET strategy_id = default_strategy_id
    WHERE strategy_id IS NULL;

    RAISE NOTICE 'Backfilled positions with strategy_id';
END $$;

-- Make NOT NULL and add foreign key
ALTER TABLE order_service.positions
ALTER COLUMN strategy_id SET NOT NULL;

ALTER TABLE order_service.positions
ADD CONSTRAINT fk_positions_strategy_id
FOREIGN KEY (strategy_id)
REFERENCES public.strategies(id)
ON DELETE RESTRICT
ON UPDATE CASCADE;

-- Create indexes for positions
CREATE INDEX IF NOT EXISTS idx_positions_strategy_id
ON order_service.positions(strategy_id);

CREATE INDEX IF NOT EXISTS idx_positions_strategy_symbol
ON order_service.positions(strategy_id, symbol);

CREATE INDEX IF NOT EXISTS idx_positions_strategy_open
ON order_service.positions(strategy_id, is_open)
WHERE is_open = true;

-- ============================================================================
-- STEP 7: Create helper function for real-time strategy order counts
-- ============================================================================

CREATE OR REPLACE FUNCTION get_strategy_order_count(p_strategy_id BIGINT)
RETURNS TABLE (
    total_orders BIGINT,
    pending_orders BIGINT,
    completed_orders BIGINT,
    cancelled_orders BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        COUNT(*) as total_orders,
        COUNT(*) FILTER (WHERE status IN ('PENDING', 'SUBMITTED', 'OPEN', 'TRIGGER_PENDING')) as pending_orders,
        COUNT(*) FILTER (WHERE status = 'COMPLETE') as completed_orders,
        COUNT(*) FILTER (WHERE status IN ('CANCELLED', 'REJECTED')) as cancelled_orders
    FROM order_service.orders
    WHERE strategy_id = p_strategy_id;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION get_strategy_order_count IS 'Get real-time order counts for a strategy (optimized for per-second usage)';

-- ============================================================================
-- STEP 8: Create helper function for real-time strategy position summary
-- ============================================================================

CREATE OR REPLACE FUNCTION get_strategy_position_summary(p_strategy_id BIGINT)
RETURNS TABLE (
    open_positions BIGINT,
    total_quantity BIGINT,
    realized_pnl NUMERIC,
    unrealized_pnl NUMERIC,
    total_pnl NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        COUNT(*) FILTER (WHERE is_open = true) as open_positions,
        COALESCE(SUM(ABS(quantity)) FILTER (WHERE is_open = true), 0) as total_quantity,
        COALESCE(SUM(realized_pnl), 0) as realized_pnl,
        COALESCE(SUM(unrealized_pnl) FILTER (WHERE is_open = true), 0) as unrealized_pnl,
        COALESCE(SUM(total_pnl), 0) as total_pnl
    FROM order_service.positions
    WHERE strategy_id = p_strategy_id;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION get_strategy_position_summary IS 'Get real-time position summary for a strategy (optimized for per-second usage)';

-- ============================================================================
-- STEP 9: Verify migration success
-- ============================================================================

-- Verification queries (commented out for production, uncomment for testing)
/*
-- Check orders have strategy_id
SELECT COUNT(*), COUNT(strategy_id)
FROM order_service.orders;

-- Check trades have strategy_id
SELECT COUNT(*), COUNT(strategy_id)
FROM order_service.trades;

-- Check positions have strategy_id
SELECT COUNT(*), COUNT(strategy_id)
FROM order_service.positions;

-- Check foreign keys exist
SELECT
    tc.table_schema,
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = 'order_service'
  AND kcu.column_name = 'strategy_id';

-- Test helper functions
SELECT * FROM get_strategy_order_count(1);
SELECT * FROM get_strategy_position_summary(1);
*/

-- ============================================================================
-- Migration Complete
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '====================================================================';
    RAISE NOTICE 'Migration 005: strategy_id foreign keys - COMPLETED';
    RAISE NOTICE '====================================================================';
    RAISE NOTICE 'Changes:';
    RAISE NOTICE '  1. Added strategy_id to order_service.orders (with FK to public.strategies)';
    RAISE NOTICE '  2. Added strategy_id to order_service.trades (with FK to public.strategies)';
    RAISE NOTICE '  3. Added strategy_id to order_service.positions (with FK to public.strategies)';
    RAISE NOTICE '  4. Created 9 indexes for real-time query performance';
    RAISE NOTICE '  5. Created helper functions for real-time strategy metrics';
    RAISE NOTICE '  6. Backfilled existing data to default strategy';
    RAISE NOTICE '';
    RAISE NOTICE 'Next Steps:';
    RAISE NOTICE '  1. Update order_service API schema to require strategy_id';
    RAISE NOTICE '  2. Update order_service models to include strategy_id';
    RAISE NOTICE '  3. Rebuild order_service Docker image';
    RAISE NOTICE '  4. Test order placement with strategy_id';
    RAISE NOTICE '====================================================================';
END $$;
