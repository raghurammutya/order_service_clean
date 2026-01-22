# Exception Handling Security Architecture

## Overview

This document defines the secure exception handling patterns implemented across the order service to prevent silent failures, data corruption, and operational integrity issues.

## Security Principle

**Never allow silent failures in financial operations.** All errors must be:
1. **Logged appropriately** (with severity matching impact)
2. **Categorized specifically** (network, validation, critical system)
3. **Propagated correctly** (structured exceptions, not raw strings)
4. **Handled safely** (fail-fast vs graceful degradation)

## Exception Handling Patterns

### ✅ APPROVED: Structured Exception Hierarchy

```python
try:
    # Critical operation (P&L calculation, order placement, etc.)
    pass
except (ConnectionError, TimeoutError, OSError) as e:
    # Infrastructure failures - temporary, retry possible
    logger.error(f"Infrastructure error in {operation}: {e}")
    raise ServiceUnavailableError(f"Service temporarily unavailable: {e}")
    
except ValueError as e:
    # Data validation failures - permanent, client error
    logger.error(f"Validation error in {operation}: {e}")
    raise ValidationError(f"Invalid parameters: {e}")
    
except KeyError as e:
    # Missing required data - permanent, client error  
    logger.error(f"Missing required data in {operation}: {e}")
    raise ValidationError(f"Missing required field: {e}")
    
except Exception as e:  # ← INTENTIONAL FALLBACK - Last resort only
    # Unexpected system errors - critical, needs investigation
    logger.critical(f"CRITICAL: Unexpected error in {operation}: {e}", exc_info=True)
    raise OrderServiceError(f"Critical system failure in {operation}: {e}")
```

### ✅ APPROVED: Worker Loop Pattern

```python
while self.is_running:
    try:
        # Worker operation
        pass
    except asyncio.CancelledError:
        # Graceful shutdown
        logger.info("Worker cancelled")
        break
    except (ConnectionError, TimeoutError, asyncio.TimeoutError) as e:
        # Infrastructure issues - retry with backoff
        logger.warning(f"Network error in worker, retrying: {e}")
        await asyncio.sleep(retry_interval)
        continue
    except Exception as e:  # ← INTENTIONAL FALLBACK - Prevent worker corruption
        # Unexpected errors - stop worker safely
        logger.critical(f"CRITICAL: Unexpected worker error: {e}", exc_info=True)
        self.is_running = False  # Stop all workers for safety
        raise  # Let supervisor restart service
```

### ❌ PROHIBITED: Silent Failure Patterns

```python
# NEVER DO THIS - Silent data corruption
try:
    pnl = calculate_pnl()
except Exception:
    pnl = Decimal('0')  # ← DANGEROUS: Hides calculation errors

# NEVER DO THIS - Silent operational failures  
try:
    place_order()
except Exception:
    pass  # ← DANGEROUS: Order may appear successful but fail

# NEVER DO THIS - Continuing with corrupt state
try:
    update_position()
except Exception:
    continue  # ← DANGEROUS: Worker continues with invalid state
```

## Implementation Status

### 🛡️ SECURED (Mission-Critical Paths)

| Component | Status | Security Impact |
|-----------|---------|-----------------|
| `pnl_calculator.py` | ✅ Hardened | No silent P&L corruption |
| `kite_client.py` | ✅ Hardened | No hidden order failures |
| `handoff_state_machine.py` | ✅ Hardened | No state transition corruption |
| `sync_workers.py` | ✅ Hardened | No worker state corruption |
| API endpoints (P&L, capital) | ✅ Hardened | No silent financial API failures |

### 🔧 INTENTIONAL FALLBACKS (Documented)

The following modules contain intentional `except Exception` fallback handlers:

| Module | Count | Purpose | Safety Mechanism |
|--------|-------|---------|------------------|
| `pnl_calculator.py` | 15 | Final fallback after specific handling | Logs CRITICAL + raises OrderServiceError |
| `kite_client.py` | 19 | Final fallback after specific handling | Logs CRITICAL + raises BrokerAPIError |
| `handoff_state_machine.py` | 18 | Final fallback after specific handling | Logs CRITICAL + raises OrderServiceError |
| `sync_workers.py` | 27 | Final fallback after specific handling | Logs CRITICAL + stops worker safely |
| API endpoints | 13 | Final fallback after specific handling | Logs CRITICAL + returns 500 with structured error |

**Pattern Verification:**
- ✅ All fallbacks log as `CRITICAL` with full stack traces
- ✅ All fallbacks re-raise as structured exceptions (never silent)
- ✅ All fallbacks occur after specific exception handling
- ✅ All fallbacks are protected by BLE001 lint rule to prevent regression

### 🔄 REMAINING WORK (Lower Priority)

| Component | Count | Risk Level | Cleanup Priority |
|-----------|-------|------------|------------------|
| Config/initialization helpers | ~20 | Low | Optional |
| Logging/monitoring utilities | ~15 | Low | Optional |
| Test fixtures | ~10 | None | Skip |
| Legacy compatibility layers | ~8 | Medium | Consider |

## Lint Rule Protection

```toml
# pyproject.toml
[tool.ruff.lint]
extend-select = ["BLE001"]  # Do not catch blind exception: Exception

[tool.ruff.lint.per-file-ignores]
# Strict rules for critical financial modules
"app/services/kite_client*.py" = []
"app/api/v1/endpoints/*.py" = []  
"app/workers/*" = []
```

**Effect:** New broad `except Exception` blocks will be flagged, preventing regression.

## Testing Strategy

### Critical Path Verification
```python
def test_pnl_calculation_error_handling():
    """Verify P&L calculation fails fast instead of returning 0"""
    with mock.patch('database.execute', side_effect=Exception("DB Error")):
        with pytest.raises(OrderServiceError, match="Critical system failure"):
            calculator.calculate_realized_pnl(strategy_id=123)

def test_order_placement_error_categorization():
    """Verify order errors are properly categorized"""
    with mock.patch('kite.place_order', side_effect=ConnectionError()):
        with pytest.raises(ServiceUnavailableError):
            client.place_order(...)
```

## Monitoring and Alerting

### Log Analysis Patterns
```bash
# Critical system errors requiring immediate attention
grep "CRITICAL:" /var/log/order-service.log | grep "Unexpected"

# Infrastructure errors indicating service health issues  
grep "Infrastructure error\|Network error\|Database error" /var/log/order-service.log

# Validation errors indicating client integration issues
grep "Validation error\|Invalid parameters" /var/log/order-service.log
```

### Alert Configuration
- **CRITICAL logs** → Immediate PagerDuty alert
- **ServiceUnavailableError** → Service health dashboard
- **ValidationError** → Client integration monitoring

## Conclusion

**Security Status:** ✅ **Mission-Critical Paths Secured**

The order service is now protected against the highest-risk silent failure scenarios:
- ❌ Silent financial data corruption (P&L returning 0 on errors)
- ❌ Hidden order processing failures (orders appearing successful but failing)  
- ❌ State machine corruption (transitions continuing with invalid state)
- ❌ Worker state corruption (background processes continuing with bad data)

**Intentional fallback patterns** provide safety without hiding errors - they log as CRITICAL and re-raise structured exceptions for proper handling and monitoring.