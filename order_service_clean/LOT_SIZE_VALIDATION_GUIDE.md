# Lot Size Validation Implementation Guide

## Overview

Lot size validation has been implemented in the order_service to ensure that all F&O (Futures & Options) orders are placed in multiples of the instrument's lot size. This is a **critical validation** because brokers will reject any F&O order that doesn't meet this requirement.

## Why Lot Size Validation is Critical

In Indian derivatives markets (NSE F&O, BSE F&O, MCX, CDS):
- Every futures and options contract has a **fixed lot size**
- Orders MUST be placed in **exact multiples** of this lot size
- Example: NIFTY lot size = 50
  - Valid quantities: 50, 100, 150, 200, etc.
  - Invalid quantities: 25, 75, 125, 175, etc.
- **Without this validation, ALL F&O orders would be rejected by the broker**

## Implementation Details

### 1. Files Created/Modified

#### New Files:
- **`app/services/lot_size_service.py`** - Lot size lookup and validation service
- **`tests/test_lot_size_validation.py`** - Comprehensive test suite

#### Modified Files:
- **`app/services/order_service.py`** - Added lot size validation to order placement

### 2. How It Works

```
Order Placement Flow:
1. Order parameters validation (price, quantity, etc.)
2. → LOT SIZE VALIDATION (NEW) ←
3. Risk checks (margin, limits, etc.)
4. Submit order to broker
```

#### Lot Size Validation Steps:
1. Check if exchange is F&O (NFO, BFO, MCX, CDS)
2. If not F&O, skip validation (equity orders)
3. Look up lot size from Redis cache
4. If not cached, query `instrument_registry` database
5. Cache lot size for 24 hours
6. Validate quantity is multiple of lot size
7. Return validation result with helpful error message

### 3. Supported Exchanges

F&O exchanges requiring lot size validation:
- **NFO** - National Stock Exchange F&O
- **BFO** - BSE (Bombay Stock Exchange) F&O
- **MCX** - Multi Commodity Exchange
- **CDS** - Currency Derivatives Segment

Equity exchanges (skip validation):
- **NSE** - National Stock Exchange (equity)
- **BSE** - BSE (equity)

## Example Error Messages

### Valid Order
```json
{
  "symbol": "NIFTY25DEC24500CE",
  "exchange": "NFO",
  "quantity": 100,
  "order_type": "MARKET"
}
```
**Result:** ✅ Validation passes (100 = 2 lots × 50)

**Log:**
```
Lot size validation passed: NIFTY25DEC24500CE on NFO, quantity=100 (2 lots x 50)
```

---

### Invalid Order - Wrong Lot Size

```json
{
  "symbol": "NIFTY25DEC24500CE",
  "exchange": "NFO",
  "quantity": 75,
  "order_type": "MARKET"
}
```

**Result:** ❌ Validation fails

**Error Response:**
```json
{
  "status_code": 400,
  "detail": "Invalid quantity for F&O order: 75 is not a multiple of lot size 50. Valid quantities: 50, 100, 150, etc. Suggested quantities: 50, 100, 150, 200, 250"
}
```

**Log:**
```
ERROR: Lot size validation failed: NIFTY25DEC24500CE on NFO, quantity=75, lot_size=50
```

---

### Invalid Order - BANKNIFTY Example

```json
{
  "symbol": "BANKNIFTY25DEC44500CE",
  "exchange": "NFO",
  "quantity": 20,
  "order_type": "LIMIT",
  "price": 250.50
}
```

**Result:** ❌ Validation fails (BANKNIFTY lot size = 15)

**Error Response:**
```json
{
  "status_code": 400,
  "detail": "Invalid quantity for F&O order: 20 is not a multiple of lot size 15. Valid quantities: 15, 30, 45, etc. Suggested quantities: 15, 30, 45, 60, 75"
}
```

---

### Equity Order - No Validation

```json
{
  "symbol": "RELIANCE",
  "exchange": "NSE",
  "quantity": 7,
  "order_type": "LIMIT",
  "price": 2500
}
```

**Result:** ✅ Validation skipped (NSE is not F&O)

**Log:**
```
Skipping lot size validation for NSE (not F&O)
```

## Code Examples

### Using LotSizeService Directly

```python
from app.services.lot_size_service import LotSizeService

# Initialize service
lot_size_service = LotSizeService(db)

# Get lot size for an instrument
lot_size = await lot_size_service.get_lot_size(
    tradingsymbol="NIFTY25DEC24500CE",
    exchange="NFO"
)
print(f"NIFTY lot size: {lot_size}")  # Output: 50

# Validate order quantity
is_valid, error_msg, lot_size = await lot_size_service.validate_lot_size(
    tradingsymbol="NIFTY25DEC24500CE",
    exchange="NFO",
    quantity=100
)

if is_valid:
    print("✅ Order quantity is valid")
else:
    print(f"❌ Validation failed: {error_msg}")

# Get valid quantities for display
valid_quantities = lot_size_service.get_valid_quantities(
    lot_size=50,
    max_lots=10
)
print(f"Valid quantities: {valid_quantities}")
# Output: [50, 100, 150, 200, 250, 300, 350, 400, 450, 500]

# Calculate number of lots
num_lots = lot_size_service.calculate_lots(quantity=150, lot_size=50)
print(f"Number of lots: {num_lots}")  # Output: 3

# Round quantity to nearest valid lot size
rounded_qty = lot_size_service.round_to_lot_size(quantity=75, lot_size=50)
print(f"Rounded quantity: {rounded_qty}")  # Output: 50
```

### Batch Validation

```python
orders = [
    {"symbol": "NIFTY25DEC24500CE", "exchange": "NFO", "quantity": 100},
    {"symbol": "BANKNIFTY25DEC44500CE", "exchange": "NFO", "quantity": 45},
    {"symbol": "RELIANCE", "exchange": "NSE", "quantity": 10},
]

results = await lot_size_service.validate_batch(orders)

for i, (is_valid, error_msg, lot_size) in enumerate(results):
    if is_valid:
        print(f"Order {i}: ✅ Valid")
    else:
        print(f"Order {i}: ❌ {error_msg}")
```

### Cache Management

```python
# Invalidate cache for a specific instrument
await lot_size_service.invalidate_cache(
    tradingsymbol="NIFTY25DEC24500CE",
    exchange="NFO"
)

# Clear all lot size cache (use with caution)
await lot_size_service.clear_all_cache()
```

## Database Schema

The lot size validation uses the `instrument_registry` table:

```sql
CREATE TABLE instrument_registry (
    instrument_token BIGINT PRIMARY KEY,
    tradingsymbol TEXT NOT NULL,
    name TEXT,
    segment TEXT,
    instrument_type TEXT,
    strike DOUBLE PRECISION,
    expiry TEXT,
    tick_size DOUBLE PRECISION,
    lot_size INTEGER,  -- ← Used for validation
    exchange TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    last_refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX instrument_registry_name_idx ON instrument_registry(name);
CREATE INDEX instrument_registry_segment_idx ON instrument_registry(segment);
```

## Caching Strategy

### Cache Configuration
- **Cache Key Format:** `lot_size:{exchange}:{tradingsymbol}`
- **TTL:** 24 hours (86400 seconds)
- **Storage:** Redis
- **Rationale:** Lot sizes only change quarterly when new contracts are introduced

### Cache Flow
1. **Cache Hit:** Return lot size immediately (no database query)
2. **Cache Miss:**
   - Query database
   - Store in cache with 24-hour TTL
   - Return lot size

### Cache Invalidation
- Automatic: 24-hour TTL expiry
- Manual: `invalidate_cache()` method
- Global: `clear_all_cache()` method (use carefully)

## Common Lot Sizes (as of 2024)

| Instrument | Exchange | Lot Size |
|-----------|----------|----------|
| NIFTY | NFO | 50 |
| BANKNIFTY | NFO | 15 |
| FINNIFTY | NFO | 25 |
| MIDCPNIFTY | NFO | 50 |
| SENSEX | BFO | 10 |
| BANKEX | BFO | 15 |

**Note:** Lot sizes change periodically. Always rely on the database for current values.

## Testing

### Unit Tests
Run the test suite:
```bash
cd order_service
pytest tests/test_lot_size_validation.py -v
```

### Manual Testing Examples

#### Test 1: Valid NIFTY Order
```bash
curl -X POST http://localhost:8087/v1/orders \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "NIFTY25DEC24500CE",
    "exchange": "NFO",
    "transaction_type": "BUY",
    "quantity": 100,
    "order_type": "MARKET",
    "product_type": "NRML"
  }'
```

Expected: ✅ Order accepted

#### Test 2: Invalid NIFTY Order
```bash
curl -X POST http://localhost:8087/v1/orders \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "NIFTY25DEC24500CE",
    "exchange": "NFO",
    "transaction_type": "BUY",
    "quantity": 75,
    "order_type": "MARKET",
    "product_type": "NRML"
  }'
```

Expected: ❌ 400 Bad Request with error message

#### Test 3: Equity Order (No Validation)
```bash
curl -X POST http://localhost:8087/v1/orders \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "RELIANCE",
    "exchange": "NSE",
    "transaction_type": "BUY",
    "quantity": 7,
    "order_type": "LIMIT",
    "price": 2500,
    "product_type": "CNC"
  }'
```

Expected: ✅ Order accepted (validation skipped for NSE)

## Monitoring & Metrics

### Logs to Monitor

**Successful Validation:**
```
INFO: Lot size validation passed: NIFTY25DEC24500CE on NFO, quantity=100 (2 lots x 50)
```

**Failed Validation:**
```
WARNING: Lot size validation failed: NIFTY25DEC24500CE quantity=75, lot_size=50
ERROR: Lot size validation failed: NIFTY25DEC24500CE on NFO, quantity=75, lot_size=50, error=Invalid quantity...
```

**Cache Operations:**
```
DEBUG: Lot size cache hit: NIFTY25DEC24500CE = 50
DEBUG: Fetched lot size from DB: NIFTY25DEC24500CE = 50
DEBUG: Cached lot size: NIFTY25DEC24500CE = 50
```

**Warnings:**
```
WARNING: Lot size not found for UNKNOWN25DEC24500CE on NFO. Allowing order to proceed...
WARNING: Redis cache read failed: Connection refused
```

### Recommended Alerts

1. **High Validation Failure Rate**
   - Alert if >10% of F&O orders fail lot size validation
   - Indicates user confusion or frontend issues

2. **Lot Size Not Found**
   - Alert if lot sizes are frequently not found in database
   - Indicates stale `instrument_registry` data

3. **Redis Cache Failures**
   - Alert on repeated Redis connection failures
   - Performance will degrade without cache

## Troubleshooting

### Issue: All F&O orders failing validation

**Possible Causes:**
1. `instrument_registry` table is empty
2. Lot sizes are NULL in database
3. Database connection issue

**Solution:**
```bash
# Check if instrument_registry has data
psql -d your_database -c "SELECT COUNT(*) FROM instrument_registry WHERE lot_size IS NOT NULL;"

# Refresh instrument registry (if using ticker_service)
curl -X POST http://localhost:8089/admin/refresh-instruments
```

### Issue: Validation is slow

**Possible Causes:**
1. Redis cache is down
2. Database query is slow

**Solution:**
```bash
# Check Redis connectivity
redis-cli -h localhost -p 8202 PING

# Check cache hit rate
redis-cli -h localhost -p 8202 KEYS "lot_size:*" | wc -l

# Add database index if missing
psql -d your_database -c "CREATE INDEX IF NOT EXISTS idx_instrument_registry_symbol ON instrument_registry(tradingsymbol, exchange);"
```

### Issue: Lot size not found for valid instrument

**Possible Causes:**
1. Instrument not in registry
2. Instrument marked as inactive
3. Tradingsymbol mismatch

**Solution:**
```sql
-- Check if instrument exists
SELECT * FROM instrument_registry
WHERE tradingsymbol = 'NIFTY25DEC24500CE'
  AND exchange = 'NFO';

-- Check if instrument is active
SELECT is_active, last_refreshed_at
FROM instrument_registry
WHERE tradingsymbol = 'NIFTY25DEC24500CE';
```

## Production Deployment Checklist

- [ ] Verify `instrument_registry` table exists and has data
- [ ] Verify lot_size column is populated for F&O instruments
- [ ] Redis is running and accessible
- [ ] Database indexes are created
- [ ] Configure Redis cache TTL (default: 24 hours)
- [ ] Set up monitoring for validation failures
- [ ] Set up alerts for cache failures
- [ ] Test with real F&O symbols
- [ ] Document lot size update process for quarterly expiry

## Future Enhancements

1. **Metrics Dashboard**
   - Track validation pass/fail rates
   - Monitor cache hit rates
   - Alert on anomalies

2. **Smart Suggestions**
   - When quantity is invalid, suggest nearest valid quantity
   - Display lot size in frontend order form

3. **Bulk Operations**
   - Pre-validate basket orders before submission
   - Optimize batch validation with single database query

4. **Auto-correction**
   - Optional: Auto-round quantities to nearest lot size
   - Configurable per user/account

## References

- NSE F&O Lot Size List: https://www.nseindia.com/market-data/lot-size-of-fo-contracts
- KiteConnect Instruments API: https://kite.trade/docs/connect/v3/market-quotes/
- instrument_registry table: `ticker_service/app/instrument_registry.py`

## Support

For issues or questions:
1. Check logs in `order_service` container
2. Verify `instrument_registry` data is up-to-date
3. Check Redis cache connectivity
4. Review test suite for expected behavior
