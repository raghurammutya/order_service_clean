# Lot Size Validation - Quick Reference Card

## What is it?
Validates that F&O orders are in multiples of the lot size (50, 100, 150 for NIFTY, etc.)

## Why is it needed?
**Brokers reject ALL F&O orders that aren't multiples of lot size.**

## Which exchanges?
- ✅ NFO (F&O)
- ✅ BFO (BSE F&O)
- ✅ MCX (Commodity)
- ✅ CDS (Currency)
- ❌ NSE (Equity - no validation)
- ❌ BSE (Equity - no validation)

## Common Lot Sizes
```
NIFTY       = 50
BANKNIFTY   = 15
FINNIFTY    = 25
SENSEX      = 10
```

## Example Errors

### ✅ VALID (100 is multiple of 50)
```json
{
  "symbol": "NIFTY25DEC24500CE",
  "exchange": "NFO",
  "quantity": 100
}
```
→ Accepted (2 lots × 50)

### ❌ INVALID (75 is NOT multiple of 50)
```json
{
  "symbol": "NIFTY25DEC24500CE",
  "exchange": "NFO",
  "quantity": 75
}
```
→ **400 Error:** "Invalid quantity: 75 is not a multiple of lot size 50. Suggested quantities: 50, 100, 150, 200, 250"

## Code Usage

```python
from app.services.lot_size_service import LotSizeService

# Initialize
service = LotSizeService(db)

# Get lot size
lot_size = await service.get_lot_size("NIFTY25DEC24500CE", "NFO")
# Returns: 50

# Validate quantity
is_valid, error, lot_size = await service.validate_lot_size(
    "NIFTY25DEC24500CE", "NFO", 100
)
# Returns: (True, None, 50)

# Calculate lots
lots = service.calculate_lots(quantity=150, lot_size=50)
# Returns: 3

# Round to valid quantity
valid_qty = service.round_to_lot_size(quantity=75, lot_size=50)
# Returns: 50 (rounds down)
```

## Monitoring

### Check validation failures
```sql
SELECT COUNT(*) FROM orders
WHERE status = 'REJECTED'
  AND status_message LIKE '%lot size%'
  AND created_at > NOW() - INTERVAL '1 day';
```

### Check cache
```bash
redis-cli KEYS "lot_size:*" | wc -l
```

## Troubleshooting

**Problem:** All F&O orders failing
**Solution:** Check if `instrument_registry` has data
```sql
SELECT COUNT(*) FROM instrument_registry WHERE lot_size IS NOT NULL;
```

**Problem:** Slow validation
**Solution:** Check Redis cache
```bash
redis-cli PING
```

## Files
- Service: `app/services/lot_size_service.py`
- Tests: `tests/test_lot_size_validation.py`
- Full guide: `LOT_SIZE_VALIDATION_GUIDE.md`
