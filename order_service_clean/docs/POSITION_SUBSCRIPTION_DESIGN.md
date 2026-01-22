# Position & Holding Real-Time Subscription System

## Overview

This system ensures all open positions and holdings across all trading accounts receive real-time price updates via the ticker service WebSocket feeds.

## Requirements

### Core Requirements
1. **Subscribe on position open** - Immediately subscribe when a new position is taken
2. **Subscribe on holding acquired** - Subscribe when a new holding is acquired (CNC orders)
3. **Real-time LTP updates** - Update position/holding LTP and P&L in real-time
4. **Unsubscribe on exit** - Unsubscribe when position/holding is fully exited
5. **Multi-account awareness** - Only unsubscribe if NO trading account needs the instrument
6. **Strike rebalancer coordination** - Don't unsubscribe if rebalancer needs the instrument

### Edge Cases
1. **Non-subscribable instruments** - Bonds, debt, mutual funds, SGBs → Poll every 5 mins
2. **Service restart** - Re-subscribe all open positions/holdings on startup
3. **Partial exits** - Don't unsubscribe until fully exited (quantity = 0)
4. **Multiple accounts same instrument** - Track all accounts, unsubscribe only when all exit

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           ORDER SERVICE                                  │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                  SubscriptionManager                             │   │
│  │  - subscribe_position(position)                                  │   │
│  │  - unsubscribe_position(position)                               │   │
│  │  - subscribe_holding(holding)                                    │   │
│  │  - unsubscribe_holding(holding)                                 │   │
│  │  - sync_all_subscriptions() [startup]                           │   │
│  │  - is_subscribable(instrument) → bool                           │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                              │                                          │
│                              ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                  TickListener (Background Worker)                │   │
│  │  - Subscribes to Redis: ticks:{token} channels                  │   │
│  │  - Updates position/holding LTP + P&L in real-time              │   │
│  │  - Handles reconnection and missed ticks                        │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                              │                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                  NonSubscribablePoller                           │   │
│  │  - Polls prices every 5 mins for bonds, debt, MF, etc.          │   │
│  │  - Uses Kite API quote endpoint                                  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              │ Database: instrument_subscriptions
                              │ Redis: subscription:requests channel
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         TICKER SERVICE V2                               │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                  SubscriptionStore                               │   │
│  │  - Loads subscriptions from database                            │   │
│  │  - Listens to Redis for real-time subscription requests         │   │
│  │  - Checks requested_by before unsubscribing                     │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                              │                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                  StrikeRebalancer                                │   │
│  │  - Before unsubscribe: check if positions/holdings need it      │   │
│  │  - Coordinates with SubscriptionStore                           │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                              │                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                  SessionManager + Publisher                      │   │
│  │  - Manages Kite WebSocket connections                           │   │
│  │  - Publishes ticks to Redis: ticks:{token}                      │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

## Database Schema Changes

### instrument_subscriptions table (modify)

```sql
-- Add columns to track subscription source and accounts
ALTER TABLE instrument_subscriptions
ADD COLUMN IF NOT EXISTS requested_by VARCHAR(50)[] DEFAULT '{}';
-- Array of sources: 'position', 'holding', 'rebalancer', 'manual', 'strategy'

ALTER TABLE instrument_subscriptions
ADD COLUMN IF NOT EXISTS position_account_ids TEXT[] DEFAULT '{}';
-- Trading account IDs that have open positions

ALTER TABLE instrument_subscriptions
ADD COLUMN IF NOT EXISTS holding_account_ids TEXT[] DEFAULT '{}';
-- Trading account IDs that have holdings

-- Index for efficient lookups
CREATE INDEX IF NOT EXISTS idx_instrument_subscriptions_requested_by
ON instrument_subscriptions USING GIN (requested_by);
```

## Subscription Logic

### 1. Subscribe Flow (Position Opened)

```python
async def subscribe_position(position: Position):
    """Subscribe to real-time feed when position is opened."""

    # 1. Get instrument token
    token = await get_instrument_token(position.symbol, position.exchange)

    # 2. Check if subscribable
    if not is_subscribable(position.symbol, position.exchange):
        await add_to_polling_list(position)
        return

    # 3. Update instrument_subscriptions table
    await db.execute("""
        INSERT INTO instrument_subscriptions
            (instrument_token, tradingsymbol, exchange, status,
             requested_by, position_account_ids)
        VALUES ($1, $2, $3, 'active',
                ARRAY['position'], ARRAY[$4])
        ON CONFLICT (instrument_token) DO UPDATE SET
            status = 'active',
            requested_by = array_append(
                array_remove(instrument_subscriptions.requested_by, 'position'),
                'position'
            ),
            position_account_ids = array_append(
                array_remove(instrument_subscriptions.position_account_ids, $4),
                $4
            ),
            updated_at = NOW()
    """, token, position.symbol, position.exchange, position.trading_account_id)

    # 4. Signal ticker service via Redis (immediate subscription)
    await redis.publish('subscription:requests', json.dumps({
        'action': 'subscribe',
        'token': token,
        'symbol': position.symbol,
        'requested_by': 'position'
    }))
```

### 2. Unsubscribe Flow (Position Closed)

```python
async def unsubscribe_position(position: Position):
    """Check if we can unsubscribe when position is closed."""

    token = await get_instrument_token(position.symbol, position.exchange)

    # 1. Remove this account from position_account_ids
    await db.execute("""
        UPDATE instrument_subscriptions SET
            position_account_ids = array_remove(position_account_ids, $1),
            updated_at = NOW()
        WHERE instrument_token = $2
    """, position.trading_account_id, token)

    # 2. Check if anyone still needs this subscription
    result = await db.fetchrow("""
        SELECT
            position_account_ids,
            holding_account_ids,
            'rebalancer' = ANY(requested_by) as needed_by_rebalancer
        FROM instrument_subscriptions
        WHERE instrument_token = $1
    """, token)

    # 3. Determine if we can unsubscribe
    can_unsubscribe = (
        len(result['position_account_ids']) == 0 and
        len(result['holding_account_ids']) == 0 and
        not result['needed_by_rebalancer']
    )

    if can_unsubscribe:
        # 4. Mark as inactive
        await db.execute("""
            UPDATE instrument_subscriptions SET
                status = 'inactive',
                requested_by = array_remove(requested_by, 'position'),
                updated_at = NOW()
            WHERE instrument_token = $1
        """, token)

        # 5. Signal ticker service
        await redis.publish('subscription:requests', json.dumps({
            'action': 'unsubscribe',
            'token': token,
            'symbol': position.symbol
        }))
```

### 3. Strike Rebalancer Coordination

```python
# In ticker_service_v2/app/subscription/strike_rebalancer.py

async def unsubscribe_tokens(self, tokens: List[int]):
    """Unsubscribe tokens, but only if positions/holdings don't need them."""

    safe_to_unsubscribe = []

    for token in tokens:
        # Check if positions or holdings still need this
        result = await self.db.fetchrow("""
            SELECT
                COALESCE(array_length(position_account_ids, 1), 0) as position_count,
                COALESCE(array_length(holding_account_ids, 1), 0) as holding_count
            FROM instrument_subscriptions
            WHERE instrument_token = $1
        """, token)

        if result and (result['position_count'] > 0 or result['holding_count'] > 0):
            logger.info(f"Skipping unsubscribe for {token}: still needed by positions/holdings")
            continue

        safe_to_unsubscribe.append(token)

    if safe_to_unsubscribe:
        await self._do_unsubscribe(safe_to_unsubscribe)
```

## Non-Subscribable Instruments

### Detection Logic

```python
NON_SUBSCRIBABLE_EXCHANGES = {'MUTUALFUND', 'AMC'}
NON_SUBSCRIBABLE_SEGMENTS = {'MF', 'BOND', 'DEBT'}
NON_SUBSCRIBABLE_TYPES = {'SGB', 'GSEC', 'TBILL', 'SDL', 'MF', 'BOND'}

def is_subscribable(symbol: str, exchange: str, instrument_type: str = None) -> bool:
    """Check if instrument can be subscribed to real-time feed."""

    # Check exchange
    if exchange.upper() in NON_SUBSCRIBABLE_EXCHANGES:
        return False

    # Check segment
    segment = get_segment(exchange)
    if segment in NON_SUBSCRIBABLE_SEGMENTS:
        return False

    # Check instrument type
    if instrument_type and instrument_type.upper() in NON_SUBSCRIBABLE_TYPES:
        return False

    # Subscribable exchanges: NSE, BSE, NFO, BFO, MCX, CDS
    return exchange.upper() in {'NSE', 'BSE', 'NFO', 'BFO', 'MCX', 'CDS'}
```

### Polling Fallback

```python
class NonSubscribablePoller:
    """Polls prices for instruments that can't be subscribed."""

    POLL_INTERVAL = 300  # 5 minutes

    async def start(self):
        """Start the polling loop."""
        while True:
            await self._poll_prices()
            await asyncio.sleep(self.POLL_INTERVAL)

    async def _poll_prices(self):
        """Fetch prices for all non-subscribable instruments."""

        # Get all instruments in polling list
        instruments = await self._get_polling_list()

        if not instruments:
            return

        # Batch fetch quotes from Kite API
        # Kite allows max 500 instruments per call
        for batch in chunk(instruments, 500):
            symbols = [f"{i['exchange']}:{i['symbol']}" for i in batch]
            quotes = await kite_client.quote(symbols)

            # Update positions/holdings with new prices
            for instrument in batch:
                key = f"{instrument['exchange']}:{instrument['symbol']}"
                if key in quotes:
                    ltp = quotes[key]['last_price']
                    await self._update_ltp(instrument, ltp)
```

## Tick Listener (Real-time Updates)

```python
class TickListener:
    """Listens to Redis tick channels and updates positions/holdings."""

    def __init__(self, redis_client, db_session):
        self.redis = redis_client
        self.db = db_session
        self.pubsub = None
        self.subscribed_tokens: Set[int] = set()

    async def start(self):
        """Start listening for ticks."""
        self.pubsub = self.redis.pubsub()

        # Subscribe to all open position/holding tokens
        tokens = await self._get_all_tokens()

        for token in tokens:
            await self.pubsub.subscribe(f"ticks:{token}")
            self.subscribed_tokens.add(token)

        # Also subscribe to subscription requests channel
        await self.pubsub.subscribe("subscription:updates")

        # Listen for messages
        async for message in self.pubsub.listen():
            await self._handle_message(message)

    async def _handle_message(self, message):
        """Handle incoming tick or subscription update."""

        if message['type'] != 'message':
            return

        channel = message['channel'].decode()
        data = json.loads(message['data'])

        if channel.startswith('ticks:'):
            await self._handle_tick(data)
        elif channel == 'subscription:updates':
            await self._handle_subscription_update(data)

    async def _handle_tick(self, tick: dict):
        """Update position/holding LTP and P&L from tick."""

        token = tick['instrument_token']
        ltp = tick['ltp']
        timestamp = tick.get('timestamp')

        # Update all positions with this token
        await self.db.execute("""
            UPDATE order_service.positions SET
                last_price = $1,
                unrealized_pnl = CASE
                    WHEN quantity > 0 THEN (($1 - buy_price) * quantity)
                    WHEN quantity < 0 THEN ((sell_price - $1) * ABS(quantity))
                    ELSE 0
                END,
                total_pnl = realized_pnl + CASE
                    WHEN quantity > 0 THEN (($1 - buy_price) * quantity)
                    WHEN quantity < 0 THEN ((sell_price - $1) * ABS(quantity))
                    ELSE 0
                END,
                updated_at = NOW()
            WHERE instrument_token = $2 AND is_open = true
        """, ltp, token)

        # Update all holdings with this token
        await self.db.execute("""
            UPDATE order_service.holdings SET
                last_price = $1,
                unrealized_pnl = ($1 - average_price) * quantity,
                updated_at = NOW()
            WHERE instrument_token = $2 AND quantity > 0
        """, ltp, token)
```

## Startup Recovery

```python
async def sync_all_subscriptions():
    """Ensure all open positions/holdings are subscribed on startup."""

    logger.info("Starting subscription sync for all open positions/holdings...")

    # 1. Get all open positions
    positions = await db.fetch("""
        SELECT DISTINCT symbol, exchange, trading_account_id, instrument_token
        FROM order_service.positions
        WHERE is_open = true AND quantity != 0
    """)

    # 2. Get all holdings
    holdings = await db.fetch("""
        SELECT DISTINCT symbol, exchange, trading_account_id, instrument_token
        FROM order_service.holdings
        WHERE quantity > 0
    """)

    # 3. Combine and dedupe
    required_tokens = {}

    for pos in positions:
        token = pos['instrument_token']
        if token not in required_tokens:
            required_tokens[token] = {
                'symbol': pos['symbol'],
                'exchange': pos['exchange'],
                'position_accounts': set(),
                'holding_accounts': set()
            }
        required_tokens[token]['position_accounts'].add(pos['trading_account_id'])

    for holding in holdings:
        token = holding['instrument_token']
        if token not in required_tokens:
            required_tokens[token] = {
                'symbol': holding['symbol'],
                'exchange': holding['exchange'],
                'position_accounts': set(),
                'holding_accounts': set()
            }
        required_tokens[token]['holding_accounts'].add(holding['trading_account_id'])

    # 4. Sync to instrument_subscriptions table
    for token, info in required_tokens.items():
        if not is_subscribable(info['symbol'], info['exchange']):
            await add_to_polling_list(token, info)
            continue

        await db.execute("""
            INSERT INTO instrument_subscriptions
                (instrument_token, tradingsymbol, exchange, status,
                 requested_by, position_account_ids, holding_account_ids)
            VALUES ($1, $2, $3, 'active',
                    ARRAY['position']::varchar[], $4, $5)
            ON CONFLICT (instrument_token) DO UPDATE SET
                status = 'active',
                position_account_ids = $4,
                holding_account_ids = $5,
                updated_at = NOW()
        """, token, info['symbol'], info['exchange'],
             list(info['position_accounts']),
             list(info['holding_accounts']))

    logger.info(f"Subscription sync complete: {len(required_tokens)} instruments")
```

## API Endpoints (Optional)

```python
# order_service/app/api/v1/endpoints/subscriptions.py

@router.post("/subscriptions/sync")
async def sync_subscriptions():
    """Manually trigger subscription sync."""
    await subscription_manager.sync_all_subscriptions()
    return {"status": "ok", "message": "Subscription sync triggered"}

@router.get("/subscriptions/status")
async def get_subscription_status():
    """Get current subscription status."""
    return {
        "subscribed_positions": await subscription_manager.get_subscribed_count('position'),
        "subscribed_holdings": await subscription_manager.get_subscribed_count('holding'),
        "polling_instruments": await subscription_manager.get_polling_count(),
        "total_subscriptions": await subscription_manager.get_total_count()
    }
```

## Conditions Summary

### Subscribe When:
- [x] New position opened (quantity != 0)
- [x] New holding acquired (CNC order completed)
- [x] Service starts (sync all open positions/holdings)
- [x] Position transferred between accounts (re-subscribe for new account)

### Unsubscribe When (ALL must be true):
- [x] Position fully closed (quantity = 0)
- [x] No other trading accounts have open positions for this instrument
- [x] No holdings exist for this instrument across any account
- [x] Strike rebalancer is not actively using this instrument
- [x] No manual/strategy subscriptions for this instrument

### Skip Subscription (Use Polling):
- [x] Exchange is MUTUALFUND or AMC
- [x] Instrument type is SGB, GSEC, TBILL, SDL, MF, BOND
- [x] Segment is not NSE, BSE, NFO, BFO, MCX, CDS

## Files to Create/Modify

### New Files:
1. `order_service/app/services/subscription_manager.py` - Core subscription logic
2. `order_service/app/workers/tick_listener.py` - Redis tick consumer
3. `order_service/app/workers/price_poller.py` - Non-subscribable instrument poller
4. `order_service/migrations/xxx_add_subscription_tracking.sql` - DB migration

### Modified Files:
1. `order_service/app/services/position_service.py` - Trigger subscribe/unsubscribe
2. `order_service/app/services/holding_service.py` - Trigger subscribe/unsubscribe
3. `order_service/app/main.py` - Start tick listener and poller
4. `ticker_service_v2/app/subscription/strike_rebalancer.py` - Coordinate unsubscribe
5. `ticker_service_v2/app/subscription/store.py` - Handle real-time subscription requests
