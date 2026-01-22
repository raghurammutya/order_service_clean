# Schema Isolation Plan - Correct Architecture

## Current Problem: Database-Level Coupling

order_service currently assumes these **public schema tables exist locally**:

- `public.strategy` → **Should NOT exist** (owned by backend/algo-engine)
- `public.portfolio` → **Should NOT exist** (owned by backend/algo-engine)
- `public.strategy_portfolio` → **Should NOT exist** (owned by backend/algo-engine)
- `public.kite_accounts` → **Should NOT exist** (owned by user service)
- `public.instrument_registry` → **Should NOT exist** (owned by market data service)
- `public.strategy_pnl_metrics` → **Should NOT exist** (owned by analytics service)

## Correct Architecture: True Service Isolation

### order_service Database Should ONLY Contain:
```
order_service_db/
├── order_service.orders          ✅ Own table
├── order_service.trades          ✅ Own table  
├── order_service.positions       ✅ Own table
├── order_service.gtt_orders      ✅ Own table
├── order_service.portfolios      ✅ Own table (if needed)
└── order_service.* (other order-specific tables)

❌ NO public.* tables at all
❌ NO foreign keys to external services
❌ NO cross-service database dependencies
```

### Cross-Service Data Access via APIs ONLY:
```
order_service → Strategy Service API → strategy_service_db.strategies
order_service → Portfolio Service API → portfolio_service_db.portfolios  
order_service → Account Service API → user_service_db.kite_accounts
order_service → Market Data API → market_data_db.instruments
order_service → Analytics Service API → analytics_db.pnl_metrics
```

---

## Required Changes

### 1. Database Schema Cleanup

**Remove Foreign Key Constraints:**
```sql
-- Migration 007 (DONE)
DROP CONSTRAINT fk_orders_strategy_id;     -- orders.strategy_id -> public.strategies(id)
DROP CONSTRAINT fk_trades_strategy_id;     -- trades.strategy_id -> public.strategies(id)  
DROP CONSTRAINT fk_positions_strategy_id;  -- positions.strategy_id -> public.strategies(id)
```

**Keep Strategy IDs as Simple Integers:**
```sql
-- strategy_id remains as BIGINT but NO database constraint
-- Validation happens at application layer via API calls
ALTER COLUMN strategy_id TYPE BIGINT; -- Simple integer, no FK
```

**Remove Public Schema Table References:**
- No migrations should create/modify `public.*` tables
- No application code should assume `public.*` tables exist locally

### 2. Application Layer Replacements

**Replace Direct SQL with API Calls:**

| Current Violation | Replacement |
|------------------|-------------|
| `SELECT * FROM public.strategy` | `strategy_client.get_strategy(id)` |
| `INSERT INTO public.portfolio` | `portfolio_client.create_portfolio(data)` |
| `SELECT * FROM public.kite_accounts` | `account_client.get_account(id)` |
| `SELECT * FROM public.instrument_registry` | `market_data_client.get_instrument(symbol)` |
| `INSERT INTO public.strategy_pnl_metrics` | `analytics_client.store_pnl_metrics(data)` |

### 3. Validation Strategy

**Database Constraints → API Validation:**
```python
# OLD: Database enforces constraint
INSERT INTO orders (strategy_id) VALUES (123);  -- FK constraint validates

# NEW: Application validates via API
strategy_client = await get_strategy_client()
is_valid = await strategy_client.validate_strategy(123)
if not is_valid:
    raise ValidationError("Invalid strategy_id")
await db.execute("INSERT INTO orders (strategy_id) VALUES (?)", [123])
```

### 4. Deployment Independence

**Before (Coupled):**
```
order_service deployment requires:
├── public.strategies table exists
├── public.portfolios table exists  
├── public.kite_accounts table exists
└── Cross-service database migrations coordinated
```

**After (Independent):**
```
order_service deployment requires:
├── order_service schema only
├── Strategy Service API available  
├── Portfolio Service API available
└── Network connectivity to service APIs
```

---

## Implementation Priority

### Phase 1: Remove Database Dependencies ⚠️ CRITICAL
1. ✅ **Migration 007**: Drop all FK constraints to public schema
2. **Remove public.* table creation** from all migrations
3. **Update models** to not reference public schema tables

### Phase 2: Replace SQL with API Calls 🔧 HIGH
1. **default_portfolio_service.py**: Replace `INSERT INTO public.portfolio`
2. **account_tier_service.py**: Replace `JOIN public.kite_accounts`  
3. **position_service.py**: Replace `FROM public.instrument_registry`
4. **transfer_instruction_generator.py**: Replace strategy lookups
5. **All remaining ~40+ violations**

### Phase 3: Service Integration Testing 🧪 MEDIUM
1. **Mock service responses** for unit testing
2. **Integration tests** with real service dependencies
3. **Fallback mechanisms** for service unavailability
4. **Circuit breakers** and retry logic

---

## Risk Mitigation

### Fallback Strategy (Temporary)
```python
async def get_strategy_info(strategy_id: int):
    try:
        # Primary: API call
        strategy_client = await get_strategy_client()
        return await strategy_client.get_strategy(strategy_id)
    except Exception as e:
        logger.warning(f"Strategy API failed: {e}")
        # Fallback: Direct DB (marked for removal)
        # NOTE: This requires public.strategy table to exist temporarily
        return await fallback_db_query(strategy_id)
```

### Migration Path
1. **Deploy with fallbacks enabled** (both API and DB access work)
2. **Monitor API success rates** 
3. **Gradually disable fallbacks** as APIs prove reliable
4. **Remove public.* tables** once 100% API-based

---

## Success Criteria

✅ **Schema Isolation Achieved When:**
- order_service database has NO public.* tables
- NO foreign key constraints to external services  
- ALL cross-service access via HTTP APIs
- order_service deploys independently
- Zero SQL queries to public schema (verified by CI)

This is true microservice architecture with proper service boundaries.