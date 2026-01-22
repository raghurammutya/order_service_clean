# Exception Handling Security Audit

## Risk Assessment Summary

### 🚨 **CRITICAL RISK** - Immediate Action Required
**HTTP Client Modules** (Swallow all exceptions in service communication):
- `app/clients/*_service_client.py` - 20+ `except Exception` blocks
- `app/services/kite_client.py` - 18 `except Exception` blocks  
- `app/services/kite_client_multi.py` - 13 `except Exception` blocks

**Order Processing APIs** (May hide order placement failures):
- `app/api/v1/endpoints/order_events.py` - 12 `except Exception` blocks
- `app/api/v1/endpoints/accounts.py` - 11 `except Exception` blocks  
- `app/api/v1/endpoints/capital_ledger.py` - 10 `except Exception` blocks

### ⚠️  **HIGH RISK** - Priority Fixes
**Background Workers** (May continue with corrupt state):
- `app/workers/sync_workers.py` - 27 `except Exception` blocks
- `app/workers/tick_listener.py` - Multiple blocks

**Core Services** (Business logic failures):
- `app/services/pnl_calculator.py` - 15 `except Exception` blocks
- `app/services/handoff_state_machine.py` - 14 `except Exception` blocks
- `app/services/handoff_concurrency_manager.py` - 12 `except Exception` blocks

### 📋 **MEDIUM RISK** - Review and Fix
**Portfolio/Strategy Services**:
- `app/services/default_portfolio_service.py` - 12 `except Exception` blocks

## Detailed Analysis by Category

### HTTP Client Risk Analysis
```python
# DANGEROUS PATTERN - Swallows all HTTP errors:
try:
    response = await client.get(url)
    return response.json()
except Exception:  # 🚨 HIDES: Network timeouts, HTTP 500, JSON decode errors
    return None      # 🚨 Caller can't distinguish between no data vs service failure
```

**Should be:**
```python
try:
    response = await client.get(url)  
    response.raise_for_status()
    return response.json()
except (httpx.HTTPError, httpx.TimeoutException, asyncio.TimeoutError) as e:
    logger.error(f"HTTP request failed: {e}")
    raise ServiceUnavailableError(f"Service unavailable: {e}")
except json.JSONDecodeError as e:
    logger.error(f"Invalid JSON response: {e}")
    raise DataFormatError(f"Invalid response format: {e}")
```

### Order API Risk Analysis  
```python
# DANGEROUS PATTERN - May hide order processing failures:
try:
    order = await process_order(order_data)
    return {"success": True, "order_id": order.id}
except Exception:  # 🚨 HIDES: Database failures, validation errors, broker API errors
    return {"success": False}  # 🚨 Client has no idea what went wrong
```

**Should be:**
```python
try:
    order = await process_order(order_data)
    return {"success": True, "order_id": order.id}
except ValidationError as e:
    logger.warning(f"Order validation failed: {e}")
    raise HTTPException(400, f"Invalid order: {e}")
except BrokerAPIError as e:
    logger.error(f"Broker API error: {e}")
    raise HTTPException(502, f"Broker unavailable: {e}")
except DatabaseError as e:
    logger.error(f"Database error: {e}")
    raise HTTPException(500, "Internal server error")
```

### Worker Risk Analysis
```python
# DANGEROUS PATTERN - Worker continues with corrupted state:
while True:
    try:
        batch = fetch_sync_jobs()
        process_batch(batch)
    except Exception:  # 🚨 HIDES: Database corruption, memory leaks, logic errors  
        continue       # 🚨 Worker keeps running with potentially corrupted state
```

**Should be:**
```python  
while True:
    try:
        batch = fetch_sync_jobs()
        process_batch(batch)
    except (ConnectionError, TimeoutError) as e:
        logger.warning(f"Temporary error, retrying: {e}")
        await asyncio.sleep(retry_delay)
        continue
    except (DatabaseError, ValidationError) as e:
        logger.error(f"Data error, skipping batch: {e}")
        continue  
    except Exception as e:
        logger.critical(f"Unexpected error, shutting down worker: {e}")
        raise  # Let supervisor restart worker
```

## Immediate Action Plan

### Phase 1: Critical HTTP Clients (Day 1)
1. Fix all `app/clients/*_service_client.py` files
2. Fix `app/services/kite_client*.py` files  
3. Replace `except Exception` with specific exception types

### Phase 2: Order APIs (Day 2)
1. Fix order processing endpoints
2. Ensure proper HTTP status codes for different failure types
3. Add structured error responses

### Phase 3: Workers (Day 3)  
1. Add proper exception handling with restart logic
2. Implement circuit breakers for repeated failures
3. Add health check endpoints

### Phase 4: Linting (Day 4)
1. Add Ruff configuration to ban `except Exception`
2. Add CI checks to prevent regressions
3. Update coding standards documentation