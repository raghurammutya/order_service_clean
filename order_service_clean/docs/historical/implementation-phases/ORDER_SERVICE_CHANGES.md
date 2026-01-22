# order_service.py - Exact Changes Made

## Change 1: Import Statement (Line 31)

### ADDED:
```python
from .lot_size_service import LotSizeService
```

### Location:
After the circuit_breaker import, before `logger = logging.getLogger(__name__)`

### Full Context:
```python
from .kite_client import get_kite_client
from .circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerError,
    RetryConfig,
    retry_with_backoff
)
from .lot_size_service import LotSizeService  # ← NEW

logger = logging.getLogger(__name__)
```

---

## Change 2: OrderService Initialization (Line 69)

### ADDED:
```python
self.lot_size_service = LotSizeService(db)
```

### Location:
In `OrderService.__init__()`, after `self.kite_client = get_kite_client()`

### Full Context:
```python
def __init__(self, db: AsyncSession, user_id: int, trading_account_id: int):
    """
    Initialize order service.

    Args:
        db: Database session
        user_id: User ID from JWT token
        trading_account_id: Trading account ID
    """
    self.db = db
    self.user_id = user_id
    self.trading_account_id = trading_account_id
    self.kite_client = get_kite_client()
    self.lot_size_service = LotSizeService(db)  # ← NEW
```

---

## Change 3: Validation Call in place_order() (Lines 128-133)

### ADDED:
```python
# Validate lot size for F&O orders
await self._validate_lot_size(
    symbol=symbol,
    exchange=exchange,
    quantity=quantity,
)
```

### Location:
In `place_order()` method, after `self._validate_order()` and before `_perform_risk_checks()`

### Full Context:
```python
# Validate order parameters
self._validate_order(
    symbol=symbol,
    exchange=exchange,
    transaction_type=transaction_type,
    quantity=quantity,
    order_type=order_type,
    product_type=product_type,
    price=price,
    trigger_price=trigger_price,
)

# Validate lot size for F&O orders  # ← NEW
await self._validate_lot_size(      # ← NEW
    symbol=symbol,                   # ← NEW
    exchange=exchange,               # ← NEW
    quantity=quantity,               # ← NEW
)                                    # ← NEW

# Perform risk checks
risk_check_passed, risk_check_details = await self._perform_risk_checks(
    symbol=symbol,
    transaction_type=transaction_type,
    quantity=quantity,
    price=price or 0,
)
```

---

## Change 4: New Method _validate_lot_size() (Lines 541-589)

### ADDED:
Complete new method (49 lines)

### Location:
After `_validate_order()` method, before `_perform_risk_checks()`

### Full Code:
```python
async def _validate_lot_size(
    self,
    symbol: str,
    exchange: str,
    quantity: int,
) -> None:
    """
    Validate lot size for F&O orders.

    F&O orders must be placed in multiples of the lot size.
    Example: If NIFTY lot size is 50, valid quantities are 50, 100, 150, etc.

    Args:
        symbol: Trading symbol
        exchange: Exchange code
        quantity: Order quantity

    Raises:
        HTTPException: If lot size validation fails
    """
    is_valid, error_msg, lot_size = await self.lot_size_service.validate_lot_size(
        tradingsymbol=symbol,
        exchange=exchange,
        quantity=quantity
    )

    if not is_valid:
        logger.error(
            f"Lot size validation failed: {symbol} on {exchange}, "
            f"quantity={quantity}, lot_size={lot_size}, error={error_msg}"
        )

        # Add helpful suggestion for valid quantities
        if lot_size:
            valid_quantities = self.lot_size_service.get_valid_quantities(
                lot_size=lot_size,
                max_lots=5
            )
            error_msg += f" Suggested quantities: {', '.join(map(str, valid_quantities))}"

        raise HTTPException(400, error_msg)

    # Log successful validation for F&O orders
    if lot_size is not None:
        num_lots = quantity // lot_size
        logger.info(
            f"Lot size validation passed: {symbol} on {exchange}, "
            f"quantity={quantity} ({num_lots} lots x {lot_size})"
        )
```

---

## Summary of Changes

**Total Lines Modified:** 52 lines
- Import: 1 line
- Initialization: 1 line
- Validation call: 6 lines (including comments)
- New method: 49 lines (including docstring)

**Impact:**
- All F&O orders now validated for lot size
- Invalid orders rejected with helpful error messages
- No breaking changes to API
- Backward compatible (equity orders unaffected)

**Testing:**
All existing tests still pass. New tests added in `tests/test_lot_size_validation.py`.

---
