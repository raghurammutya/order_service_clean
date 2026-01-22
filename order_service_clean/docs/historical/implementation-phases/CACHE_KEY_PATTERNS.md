# Redis Cache Key Patterns - Order Service

## Overview

This document describes the Redis cache key patterns used in order_service for summary endpoints. All cache keys are designed to be filter-aware to prevent cache collisions and ensure accurate responses.

---

## Cache Key Format

### General Pattern
```
{endpoint}:summary:{required_params}:{optional_filter_1|all}:{optional_filter_2|all}:...
```

### Placeholder Rules
- When an optional filter is **not provided**: use `"all"` as placeholder
- When an optional filter **is provided**: use the actual value
- All values are converted to strings for consistency
- Parts are joined with `:` delimiter

---

## Endpoint-Specific Patterns

### 1. Orders Summary
**Endpoint:** `/api/v1/orders/summary`

**Cache Key Pattern:**
```
orders:summary:{trading_account_id}:{position_id|all}:{status|all}:{symbol|all}
```

**Examples:**
```
# No filters
orders:summary:1:all:all:all

# Symbol filter only
orders:summary:1:all:all:RELIANCE

# Status filter only
orders:summary:1:all:COMPLETE:all

# Position ID filter only
orders:summary:1:123:all:all

# Multiple filters
orders:summary:1:123:COMPLETE:RELIANCE
```

**Code:**
```python
cache_key_parts = [
    "orders:summary",
    str(trading_account_id),
    str(position_id or "all"),
    status or "all",
    symbol or "all"
]
cache_key = ":".join(cache_key_parts)
```

---

### 2. Positions Summary
**Endpoint:** `/api/v1/positions/summary`

**Cache Key Pattern:**
```
positions:summary:{trading_account_id}:{symbol|all}:{strategy_id|all}:{segment|all}
```

**Examples:**
```
# No filters
positions:summary:1:all:all:all

# Symbol filter only
positions:summary:1:RELIANCE:all:all

# Strategy ID filter only
positions:summary:1:all:5:all

# Segment filter only
positions:summary:1:all:all:NSE

# Multiple filters
positions:summary:1:RELIANCE:5:NSE
```

**Code:**
```python
cache_key_parts = [
    "positions:summary",
    str(trading_account_id),
    symbol or "all",
    str(strategy_id or "all"),
    segment or "all"
]
cache_key = ":".join(cache_key_parts)
```

---

### 3. Trades Summary
**Endpoint:** `/api/v1/trades/summary`

**Cache Key Pattern:**
```
trades:summary:{trading_account_id}:{start_date|today}:{end_date|today}:{order_id|all}:{symbol|all}
```

**Examples:**
```
# No filters (today's trades)
trades:summary:1:today:today:all:all

# Date range only
trades:summary:1:2025-12-01:2025-12-03:all:all

# Order ID filter only
trades:summary:1:today:today:123:all

# Symbol filter only
trades:summary:1:today:today:all:RELIANCE

# Multiple filters
trades:summary:1:2025-12-01:2025-12-03:123:RELIANCE
```

**Code:**
```python
cache_key_parts = [
    "trades:summary",
    str(trading_account_id),
    str(start_date or "today"),
    str(end_date or "today"),
    str(order_id or "all"),
    symbol or "all"
]
cache_key = ":".join(cache_key_parts)
```

---

## Cache Configuration

### TTL (Time To Live)
- **All summary endpoints:** 300 seconds (5 minutes)

### Storage Format
- **Value:** JSON string (serialized with `json.dumps()`)
- **Special handling:** `default=str` for datetime/date objects

### Example Redis Commands

```bash
# View cache keys
redis-cli KEYS "orders:summary:*"
redis-cli KEYS "positions:summary:*"
redis-cli KEYS "trades:summary:*"

# View specific cache entry
redis-cli GET "orders:summary:1:all:all:all"

# View TTL
redis-cli TTL "orders:summary:1:all:all:all"

# Manually delete cache
redis-cli DEL "orders:summary:1:all:all:all"

# Delete all orders summary caches
redis-cli KEYS "orders:summary:*" | xargs redis-cli DEL

# Count cached entries
redis-cli KEYS "orders:summary:*" | wc -l
```

---

## Cache Invalidation

### Current Invalidation Logic

The cache keys are automatically invalidated in the following scenarios:

1. **Position Changes:**
   - When position is created/updated/closed
   - Invalidates: `positions:summary:*` for that trading account

2. **Order Changes:**
   - When order status changes
   - When order is placed/modified/cancelled
   - Invalidates: `orders:summary:*` for that trading account

3. **Trade Changes:**
   - When new trade is recorded
   - Invalidates: `trades:summary:*` for that trading account

### Manual Invalidation

```python
from order_service.app.database.redis_client import get_redis

# Invalidate all summary caches for a trading account
redis = get_redis()
patterns = [
    f"orders:summary:{trading_account_id}:*",
    f"positions:summary:{trading_account_id}:*",
    f"trades:summary:{trading_account_id}:*"
]
for pattern in patterns:
    keys = await redis.keys(pattern)
    if keys:
        await redis.delete(*keys)
```

---

## Best Practices

1. **Always use consistent string conversion:**
   ```python
   str(value or "all")  # For numeric values
   value or "all"       # For string values
   ```

2. **Maintain order of filter parameters:**
   - Order matters for cache key matching
   - Always use the same order as defined in the pattern

3. **Handle None values consistently:**
   - None values should always map to "all"
   - Never use empty strings or "None" string

4. **Use descriptive placeholders:**
   - "all" is clear and indicates "no filter"
   - "today" for default date filters

5. **Log cache operations:**
   ```python
   logger.info(f"Returning cached data for key {cache_key}")
   logger.info(f"Cached data for key {cache_key}")
   ```

6. **Graceful degradation:**
   ```python
   try:
       cached_data = await redis.get(cache_key)
   except Exception as e:
       logger.warning(f"Redis cache failed: {e}")
       # Continue with database query
   ```

---

## Monitoring

### Useful Metrics to Track

1. **Cache Hit Rate:**
   - Hits: Requests served from cache
   - Misses: Requests that hit database
   - Formula: `hits / (hits + misses) * 100`

2. **Cache Size:**
   - Number of cached entries per endpoint
   - Total memory usage

3. **Cache Invalidation Rate:**
   - How often caches are invalidated
   - May indicate high trading activity

4. **Filter Usage:**
   - Which filters are most commonly used
   - Helps optimize caching strategy

---

## Troubleshooting

### Cache Not Working

```bash
# 1. Check Redis connection
redis-cli PING

# 2. Check if cache keys exist
redis-cli KEYS "*:summary:*"

# 3. Verify TTL is being set
redis-cli TTL "orders:summary:1:all:all:all"

# 4. Check order service logs
docker logs sb-order-service-prod | grep -i cache
```

### Cache Stale Data

```bash
# Clear all summary caches
redis-cli --scan --pattern "*:summary:*" | xargs redis-cli DEL

# Or use the Redis FLUSHDB command (WARNING: clears entire database)
# redis-cli FLUSHDB
```

### Cache Key Mismatch

```bash
# List all keys to identify pattern issues
redis-cli KEYS "*:summary:*" | sort

# Check for keys with unexpected format
redis-cli KEYS "*:summary:*" | grep -v "^[a-z_]*:summary:[0-9]*:"
```

---

**Last Updated:** 2025-12-03
**Related Issue:** #426
