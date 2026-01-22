-- Migration 024: Default Strategy Auto-Tagging for External Orders/Positions
--
-- This migration adds support for automatically tagging external orders and positions
-- (placed via broker terminal, mobile app, etc.) to a Default Strategy per trading account.
--
-- Changes:
-- 1. Add is_default column to strategies table
-- 2. Add source column to positions and orders tables
-- 3. Create default strategies for existing trading accounts
-- 4. Add function to get_or_create default strategy
-- 5. Make orders.strategy_id nullable for initial creation

-- =============================================================================
-- 1. Add is_default column to strategies table
-- =============================================================================

ALTER TABLE public.strategies
ADD COLUMN IF NOT EXISTS is_default BOOLEAN DEFAULT FALSE;

-- Add unique constraint for default strategies per trading account
-- (only one default strategy per trading_account_id)
CREATE UNIQUE INDEX IF NOT EXISTS idx_strategies_default_per_account
ON public.strategies(trading_account_id)
WHERE is_default = TRUE;

-- Add index for quick lookup of default strategies
CREATE INDEX IF NOT EXISTS idx_strategies_is_default
ON public.strategies(is_default)
WHERE is_default = TRUE;

COMMENT ON COLUMN public.strategies.is_default IS
  'Default strategy contains all positions/orders not assigned to custom strategies';


-- =============================================================================
-- 2. Add source column to positions table
-- =============================================================================

ALTER TABLE order_service.positions
ADD COLUMN IF NOT EXISTS source VARCHAR(20) DEFAULT 'internal';

-- Add check constraint for valid source values
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'positions_source_check'
  ) THEN
    ALTER TABLE order_service.positions
    ADD CONSTRAINT positions_source_check
    CHECK (source IN ('internal', 'external', 'migrated'));
  END IF;
END $$;

-- Add index for source column
CREATE INDEX IF NOT EXISTS idx_positions_source
ON order_service.positions(source);

COMMENT ON COLUMN order_service.positions.source IS
  'Source of position: internal (created by system), external (from broker sync), migrated (historical)';


-- =============================================================================
-- 3. Add source column to orders table
-- =============================================================================

ALTER TABLE order_service.orders
ADD COLUMN IF NOT EXISTS source VARCHAR(20) DEFAULT 'internal';

-- Add check constraint for valid source values
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'orders_source_check'
  ) THEN
    ALTER TABLE order_service.orders
    ADD CONSTRAINT orders_source_check
    CHECK (source IN ('internal', 'external', 'migrated'));
  END IF;
END $$;

-- Add index for source column
CREATE INDEX IF NOT EXISTS idx_orders_source
ON order_service.orders(source);

COMMENT ON COLUMN order_service.orders.source IS
  'Source of order: internal (placed by system), external (from broker sync), migrated (historical)';


-- =============================================================================
-- 4. Make orders.strategy_id nullable
-- =============================================================================

-- Allow nullable strategy_id for initial order creation (before default strategy assignment)
ALTER TABLE order_service.orders
ALTER COLUMN strategy_id DROP NOT NULL;


-- =============================================================================
-- 5. Function to get or create default strategy for a trading account
-- =============================================================================

CREATE OR REPLACE FUNCTION get_or_create_default_strategy(
    p_trading_account_id TEXT,
    p_user_id BIGINT DEFAULT NULL
) RETURNS BIGINT AS $$
DECLARE
    v_strategy_id BIGINT;
BEGIN
    -- Try to get existing default strategy for this account
    SELECT id INTO v_strategy_id
    FROM public.strategies
    WHERE trading_account_id = p_trading_account_id
      AND is_default = TRUE
    LIMIT 1;

    -- If not found, create a new default strategy
    IF v_strategy_id IS NULL THEN
        INSERT INTO public.strategies (
            name,
            description,
            strategy_type,
            trading_account_id,
            is_default,
            is_active,
            state,
            mode,
            user_id,
            created_by,
            parameters,
            config,
            metadata
        ) VALUES (
            'Default Strategy',
            'Auto-created default strategy for tracking external orders and positions. This strategy does not execute trades - it only tracks external activity.',
            'passive',
            p_trading_account_id,
            TRUE,
            TRUE,
            'active',
            'live',
            p_user_id,
            'system',
            '{"is_tracking_only": true}'::jsonb,
            '{"auto_execute": false}'::jsonb,
            '{"source": "auto_created", "created_reason": "default_strategy_auto_tagging"}'::jsonb
        )
        RETURNING id INTO v_strategy_id;

        RAISE NOTICE 'Created default strategy % for trading account %', v_strategy_id, p_trading_account_id;
    END IF;

    RETURN v_strategy_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_or_create_default_strategy(TEXT, BIGINT) IS
  'Gets the default strategy for a trading account, creating one if it does not exist';


-- =============================================================================
-- 6. Function to tag orphan positions to default strategy
-- =============================================================================

CREATE OR REPLACE FUNCTION tag_orphan_positions_to_default_strategy(
    p_trading_account_id TEXT DEFAULT NULL
) RETURNS TABLE(positions_tagged INT, account_id TEXT) AS $$
DECLARE
    v_account RECORD;
    v_default_strategy_id BIGINT;
    v_tagged_count INT;
BEGIN
    -- If trading_account_id provided, only process that account
    -- Otherwise, process all accounts with orphan positions
    FOR v_account IN
        SELECT DISTINCT p.trading_account_id
        FROM order_service.positions p
        WHERE p.strategy_id IS NULL
          AND (p_trading_account_id IS NULL OR p.trading_account_id = p_trading_account_id)
    LOOP
        -- Get or create default strategy for this account
        SELECT get_or_create_default_strategy(v_account.trading_account_id) INTO v_default_strategy_id;

        -- Tag orphan positions to default strategy
        WITH updated AS (
            UPDATE order_service.positions
            SET strategy_id = v_default_strategy_id,
                source = CASE WHEN source = 'internal' THEN 'external' ELSE source END,
                updated_at = NOW()
            WHERE trading_account_id = v_account.trading_account_id
              AND strategy_id IS NULL
            RETURNING 1
        )
        SELECT COUNT(*) INTO v_tagged_count FROM updated;

        positions_tagged := v_tagged_count;
        account_id := v_account.trading_account_id;
        RETURN NEXT;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION tag_orphan_positions_to_default_strategy(TEXT) IS
  'Tags all positions without a strategy_id to the default strategy for their trading account';


-- =============================================================================
-- 7. Function to tag orphan orders to default strategy
-- =============================================================================

CREATE OR REPLACE FUNCTION tag_orphan_orders_to_default_strategy(
    p_trading_account_id TEXT DEFAULT NULL
) RETURNS TABLE(orders_tagged INT, account_id TEXT) AS $$
DECLARE
    v_account RECORD;
    v_default_strategy_id BIGINT;
    v_tagged_count INT;
BEGIN
    -- If trading_account_id provided, only process that account
    -- Otherwise, process all accounts with orphan orders
    FOR v_account IN
        SELECT DISTINCT o.trading_account_id
        FROM order_service.orders o
        WHERE o.strategy_id IS NULL
          AND (p_trading_account_id IS NULL OR o.trading_account_id = p_trading_account_id)
    LOOP
        -- Get or create default strategy for this account
        SELECT get_or_create_default_strategy(v_account.trading_account_id) INTO v_default_strategy_id;

        -- Tag orphan orders to default strategy
        WITH updated AS (
            UPDATE order_service.orders
            SET strategy_id = v_default_strategy_id,
                source = CASE WHEN source = 'internal' THEN 'external' ELSE source END,
                updated_at = NOW()
            WHERE trading_account_id = v_account.trading_account_id
              AND strategy_id IS NULL
            RETURNING 1
        )
        SELECT COUNT(*) INTO v_tagged_count FROM updated;

        orders_tagged := v_tagged_count;
        account_id := v_account.trading_account_id;
        RETURN NEXT;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION tag_orphan_orders_to_default_strategy(TEXT) IS
  'Tags all orders without a strategy_id to the default strategy for their trading account';


-- =============================================================================
-- 8. Create default strategies for existing trading accounts
-- =============================================================================

-- Create default strategies for all trading accounts that don't have one
INSERT INTO public.strategies (
    name,
    description,
    strategy_type,
    trading_account_id,
    is_default,
    is_active,
    state,
    mode,
    created_by,
    parameters,
    config,
    metadata
)
SELECT
    'Default Strategy',
    'Auto-created default strategy for tracking external orders and positions',
    'passive',
    ta.account_id,
    TRUE,
    TRUE,
    'active',
    'live',
    'system',
    '{"is_tracking_only": true}'::jsonb,
    '{"auto_execute": false}'::jsonb,
    '{"source": "migration_024", "created_reason": "backfill"}'::jsonb
FROM public.trading_account ta
WHERE NOT EXISTS (
    SELECT 1 FROM public.strategies s
    WHERE s.trading_account_id = ta.account_id
      AND s.is_default = TRUE
)
ON CONFLICT DO NOTHING;


-- =============================================================================
-- 9. Backfill: Tag existing orphan positions and orders to default strategies
-- =============================================================================

-- Tag orphan positions
SELECT * FROM tag_orphan_positions_to_default_strategy(NULL);

-- Tag orphan orders
SELECT * FROM tag_orphan_orders_to_default_strategy(NULL);


-- =============================================================================
-- 10. Add trades.source column (if trades table needs it)
-- =============================================================================

ALTER TABLE order_service.trades
ADD COLUMN IF NOT EXISTS source VARCHAR(20) DEFAULT 'internal';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'trades_source_check'
  ) THEN
    ALTER TABLE order_service.trades
    ADD CONSTRAINT trades_source_check
    CHECK (source IN ('internal', 'external', 'migrated'));
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_trades_source
ON order_service.trades(source);

COMMENT ON COLUMN order_service.trades.source IS
  'Source of trade: internal (from system order), external (from broker sync), migrated (historical)';


-- =============================================================================
-- Migration verification
-- =============================================================================

DO $$
DECLARE
    v_default_strategy_count INT;
    v_orphan_positions INT;
    v_orphan_orders INT;
BEGIN
    -- Count default strategies
    SELECT COUNT(*) INTO v_default_strategy_count
    FROM public.strategies
    WHERE is_default = TRUE;

    -- Count remaining orphan positions
    SELECT COUNT(*) INTO v_orphan_positions
    FROM order_service.positions
    WHERE strategy_id IS NULL;

    -- Count remaining orphan orders
    SELECT COUNT(*) INTO v_orphan_orders
    FROM order_service.orders
    WHERE strategy_id IS NULL;

    RAISE NOTICE 'Migration 024 completed successfully:';
    RAISE NOTICE '  - Default strategies created: %', v_default_strategy_count;
    RAISE NOTICE '  - Remaining orphan positions: %', v_orphan_positions;
    RAISE NOTICE '  - Remaining orphan orders: %', v_orphan_orders;

    IF v_orphan_positions > 0 OR v_orphan_orders > 0 THEN
        RAISE WARNING 'Some orphan items remain - check trading accounts without default strategies';
    END IF;
END $$;
