-- =====================================================
-- Remove Public Schema Dependencies - Migration 007
-- =====================================================
--
-- Date: 2025-01-22
-- Purpose: Remove all foreign key constraints and dependencies on public schema
--
-- This migration:
-- 1. Drops foreign key constraints to public.strategies
-- 2. Removes public schema table creation/modification
-- 3. Makes order_service schema-isolated
-- 4. Replaces DB constraints with API validation
--
-- CRITICAL: This enforces true service boundary isolation
-- =====================================================

-- =====================================================
-- STEP 1: Drop foreign key constraints to public schema
-- =====================================================

-- Drop FK constraint on orders.strategy_id -> public.strategies(id)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE table_schema = 'order_service' 
        AND table_name = 'orders' 
        AND constraint_name = 'fk_orders_strategy_id'
    ) THEN
        ALTER TABLE order_service.orders DROP CONSTRAINT fk_orders_strategy_id;
        RAISE NOTICE 'Dropped FK constraint: orders.strategy_id -> public.strategies(id)';
    ELSE
        RAISE NOTICE 'FK constraint fk_orders_strategy_id does not exist';
    END IF;
END $$;

-- Drop FK constraint on trades.strategy_id -> public.strategies(id)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE table_schema = 'order_service' 
        AND table_name = 'trades' 
        AND constraint_name = 'fk_trades_strategy_id'
    ) THEN
        ALTER TABLE order_service.trades DROP CONSTRAINT fk_trades_strategy_id;
        RAISE NOTICE 'Dropped FK constraint: trades.strategy_id -> public.strategies(id)';
    ELSE
        RAISE NOTICE 'FK constraint fk_trades_strategy_id does not exist';
    END IF;
END $$;

-- Drop FK constraint on positions.strategy_id -> public.strategies(id)  
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE table_schema = 'order_service' 
        AND table_name = 'positions' 
        AND constraint_name = 'fk_positions_strategy_id'
    ) THEN
        ALTER TABLE order_service.positions DROP CONSTRAINT fk_positions_strategy_id;
        RAISE NOTICE 'Dropped FK constraint: positions.strategy_id -> public.strategies(id)';
    ELSE
        RAISE NOTICE 'FK constraint fk_positions_strategy_id does not exist';
    END IF;
END $$;

-- =====================================================
-- STEP 2: Update column comments to reflect API validation
-- =====================================================

-- Update orders.strategy_id comment
COMMENT ON COLUMN order_service.orders.strategy_id IS 'Strategy ID - validated via Strategy Service API (NO database FK constraint)';

-- Update trades.strategy_id comment
COMMENT ON COLUMN order_service.trades.strategy_id IS 'Strategy ID - validated via Strategy Service API (NO database FK constraint)';

-- Update positions.strategy_id comment
COMMENT ON COLUMN order_service.positions.strategy_id IS 'Strategy ID - validated via Strategy Service API (NO database FK constraint)';

-- =====================================================
-- STEP 3: Remove any public schema table references
-- =====================================================

-- Note: Any existing public.* table references should be handled at application level
-- The database should NOT manage cross-schema dependencies

-- =====================================================
-- STEP 4: Create validation helper functions
-- =====================================================

-- Create a helper function to validate strategy_id at application level
-- This replaces the database FK constraint with API-based validation

CREATE OR REPLACE FUNCTION order_service.validate_strategy_id_comment()
RETURNS TEXT AS $$
BEGIN
    RETURN 'IMPORTANT: strategy_id values must be validated via Strategy Service API before insert/update. No database constraint enforces this - it is application responsibility.';
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION order_service.validate_strategy_id_comment() IS 'Reminder function: strategy_id validation is done via API calls, not database constraints';

-- =====================================================
-- STEP 5: Verification queries
-- =====================================================

-- Verify all FK constraints to public schema are gone
DO $$
DECLARE
    fk_count INTEGER;
    fk_record RECORD;
BEGIN
    -- Check for any remaining FK constraints pointing to public schema
    SELECT COUNT(*) INTO fk_count
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_schema = 'order_service'
      AND ccu.table_schema = 'public';

    IF fk_count = 0 THEN
        RAISE NOTICE '✅ SUCCESS: No foreign key constraints to public schema remain';
    ELSE
        RAISE WARNING '⚠️  WARNING: % FK constraints to public schema still exist:', fk_count;
        
        -- List remaining FK constraints
        FOR fk_record IN (
            SELECT 
                tc.table_name,
                tc.constraint_name,
                ccu.table_name AS foreign_table,
                ccu.column_name AS foreign_column
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = 'order_service'
              AND ccu.table_schema = 'public'
        ) LOOP
            RAISE WARNING '  - %: % -> public.%.%', 
                fk_record.table_name, 
                fk_record.constraint_name,
                fk_record.foreign_table,
                fk_record.foreign_column;
        END LOOP;
    END IF;
END $$;

-- Verify strategy_id columns still exist (they should, just without FK constraints)
DO $$
DECLARE
    strategy_id_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO strategy_id_count
    FROM information_schema.columns 
    WHERE table_schema = 'order_service' 
      AND column_name = 'strategy_id';

    IF strategy_id_count >= 3 THEN
        RAISE NOTICE '✅ SUCCESS: strategy_id columns preserved in % tables', strategy_id_count;
    ELSE
        RAISE WARNING '⚠️  WARNING: Expected 3 strategy_id columns, found %', strategy_id_count;
    END IF;
END $$;

-- =====================================================
-- STEP 6: Application layer recommendations
-- =====================================================

DO $$
BEGIN
    RAISE NOTICE '====================================================================';
    RAISE NOTICE 'Migration 007: Remove Public Schema Dependencies - COMPLETED';
    RAISE NOTICE '====================================================================';
    RAISE NOTICE 'CRITICAL CHANGES:';
    RAISE NOTICE '  1. ❌ REMOVED: All FK constraints to public.strategies';
    RAISE NOTICE '  2. ✅ KEPT: strategy_id columns (as simple integers)';
    RAISE NOTICE '  3. ⚡ REQUIRED: API validation in application layer';
    RAISE NOTICE '';
    RAISE NOTICE 'APPLICATION REQUIREMENTS:';
    RAISE NOTICE '  1. Validate strategy_id via Strategy Service API before insert';
    RAISE NOTICE '  2. Handle strategy validation errors gracefully';
    RAISE NOTICE '  3. Use service clients for all cross-service data access';
    RAISE NOTICE '  4. NO direct public.* schema access from order_service';
    RAISE NOTICE '';
    RAISE NOTICE 'DEPLOYMENT REQUIREMENTS:';
    RAISE NOTICE '  1. order_service can now deploy independently';
    RAISE NOTICE '  2. No dependency on public.strategies table existing';
    RAISE NOTICE '  3. Strategy Service must be available for validation';
    RAISE NOTICE '====================================================================';
END $$;