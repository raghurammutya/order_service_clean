# Implicit Public Schema Violations Report

This document catalogs all implicit references to the `public` schema that violate the "order_service only touches order_service schema" rule.

## Executive Summary

**Status: MAJOR VIOLATIONS FOUND** ❌

- **~50+ files** still have explicit public.* references
- **Migration files** create cross-schema foreign keys
- **SQLAlchemy models** reference tables without explicit schemas
- **Services** continue direct SQL access to public tables

---

## 1. Migration Files (Database Schema Level)

### Critical Violations in SQL Migrations:

#### `/migrations/005_add_strategy_id_foreign_keys.sql`
```sql
-- Lines 14, 28, 34, 81, 109, 123, 143, 152-153, 175, 184
REFERENCES public.strategies(id)    -- VIOLATION: Cross-schema FK
FROM public.strategies              -- VIOLATION: Cross-schema query
INSERT INTO public.strategies       -- VIOLATION: Cross-schema write
```

#### `/migrations/006_pnl_calculation_enhancements.sql`
```sql
-- Lines 16, 19, 169
ALTER TABLE public.strategy_pnl_metrics     -- VIOLATION: Cross-schema modification
CREATE INDEX ... ON public.strategy_pnl_metrics  -- VIOLATION: Cross-schema index
```

**Impact**: These migrations create **hard dependencies** between order_service and public schemas at the database constraint level.

---

## 2. SQLAlchemy Models (ORM Level)

### Implicit Schema References:

#### `/app/models/strategy.py`
```python
# Line 29: REMOVED schema but table still references public implicitly
__tablename__ = "strategies"  # IMPLICIT: Assumes public.strategies
```

#### `/app/models/portfolio.py` 
```python
# Lines 64, 89: Foreign keys reference order_service schema but strategies table is ambiguous
ForeignKey("order_service.portfolios.portfolio_id")  # OK
strategy_id = Column(Integer, primary_key=True, comment="Strategy ID (references order_service.strategies)")  # UNCLEAR: Which schema?
```

**Impact**: Models assume default schema behavior, which typically resolves to `public`.

---

## 3. Service Layer (Application Level)

### Direct Public Schema Access (Still Present):

#### `/app/services/default_strategy_service.py`
```python
# Line 683
SELECT is_default FROM public.strategy WHERE strategy_id = :strategy_id
```

#### `/app/services/default_portfolio_service.py`
```python  
# Lines 161, 227, 277
INSERT INTO public.portfolio (...)
INSERT INTO public.strategy_portfolio (...)
INSERT INTO public.strategy (...)
```

#### `/app/services/account_aggregation.py`
```python
# Lines 303, 569
FROM public.holdings
LEFT JOIN public.strategies s ON p.strategy_id = s.id
```

#### `/app/services/account_tier_service.py`
```python
# Lines 239, 263
JOIN public.kite_accounts ka ON ta.broker_account_id = ka.account_id
UPDATE public.kite_accounts
```

**Impact**: ~50+ files continue direct cross-schema database access.

---

## 4. Implicit Schema Resolution

### Database Connection Configuration:

When no schema is explicitly specified in SQLAlchemy table definitions:

```python
# This implicitly resolves to public.strategies in most PostgreSQL setups
__tablename__ = "strategies"  # Dangerous assumption
```

### Search Path Dependencies:

PostgreSQL resolves unqualified table names using `search_path`:
```sql
-- Typical search path puts public first
SET search_path TO public, order_service;
```

This means `strategies` → `public.strategies` by default.

---

## 5. Foreign Key Constraint Violations

### Database-Level Schema Coupling:

```sql
-- From migrations/005_add_strategy_id_foreign_keys.sql
ALTER TABLE order_service.orders
ADD CONSTRAINT fk_orders_strategy_id  
FOREIGN KEY (strategy_id) 
REFERENCES public.strategies(id);  -- HARD DEPENDENCY on public schema
```

**Critical Issue**: order_service tables have **foreign key constraints** pointing to public schema tables, creating:
- Hard coupling between schemas
- Migration dependencies  
- Cross-service data consistency requirements

---

## 6. Service Discovery Violations

While service clients were created, **actual integration** wasn't completed:

### Still Using Direct SQL:
```python
# Should be: strategy_client.validate_strategy(id)
# Actually is: SELECT FROM public.strategy WHERE id = ?
```

### Created But Not Integrated:
- `/app/clients/strategy_service_client.py` ✅ Created
- `/app/clients/portfolio_service_client.py` ✅ Created  
- `/app/clients/account_service_client.py` ✅ Created
- `/app/clients/analytics_service_client.py` ✅ Created

**But**: Services still use direct SQL instead of these clients.

---

## 7. Recommended Remediation

### Immediate Actions Required:

1. **Migration Rollback Strategy**
   - Remove cross-schema foreign key constraints
   - Replace with application-level reference validation
   - Use service APIs for data integrity

2. **Service Layer Replacement** 
   - Replace all ~50+ `public.*` SQL access with HTTP API calls
   - Integrate existing service clients into actual business logic
   - Add fallback mechanisms for service unavailability

3. **Model Schema Isolation**
   - Explicitly set `__table_args__ = {'schema': 'order_service'}` 
   - Remove strategy/portfolio models from order_service
   - Use API response DTOs instead of cross-schema ORM models

4. **Database Schema Separation**
   - Set PostgreSQL search_path to `order_service` only
   - Remove public schema from default resolution
   - Enforce schema-qualified table references

### Long-term Architecture:

```
order_service schema: ✅ Own tables only
├── orders, trades, positions (order_service.*)
├── No foreign keys to other schemas
└── API clients for cross-service data

public schema: ❌ No direct access  
├── strategies, portfolios → via backend/algo-engine APIs
├── kite_accounts → via account service APIs  
└── holdings → via user service APIs
```

---

## 8. Current State Assessment

| Component | Status | Violations | 
|-----------|--------|------------|
| SQL Migrations | ❌ Critical | Cross-schema FKs, direct writes |
| SQLAlchemy Models | ❌ Major | Implicit public schema resolution |
| Service Layer | ❌ Extensive | ~50+ files with direct public.* access |
| Service Clients | ✅ Ready | Created but not integrated |
| Documentation | ✅ Complete | Architecture guidelines documented |

**Conclusion**: The foundation for schema isolation is in place, but **actual implementation** requires systematic replacement of existing violations with API-based access patterns.