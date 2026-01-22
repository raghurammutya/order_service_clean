"""
Tick Listener Worker

Subscribes to Redis tick channels and updates position P&L in real-time.
This is the key component that solves the P&L variance issue by providing
real-time LTP updates instead of 5-minute polling.
"""
import asyncio
import json
import logging
import os
from typing import Dict, Set, Optional, Any
from datetime import datetime
from decimal import Decimal
import redis.asyncio as redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)


class TickListener:
    """
    Listens for tick updates and updates position P&L in real-time.

    Architecture:
    - Subscribes to Redis pub/sub channels for subscribed instruments
    - On each tick, updates position.last_price and recalculates unrealized_pnl
    - Batches updates to reduce database writes
    """

    def __init__(
        self,
        redis_url: str = None,
        database_url: str = None,
        batch_size: int = 100,
        batch_interval_ms: int = 500
    ):
        """
        Initialize tick listener.

        Args:
            redis_url: Redis connection URL
            database_url: Database connection URL
            batch_size: Maximum number of updates to batch
            batch_interval_ms: Maximum time (ms) to wait before flushing batch
        """
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.database_url = database_url or os.getenv("DATABASE_URL")
        self.batch_size = batch_size
        self.batch_interval_ms = batch_interval_ms

        self.redis_client: Optional[redis.Redis] = None
        self.pubsub: Optional[redis.client.PubSub] = None
        self.engine = None
        self.async_session = None

        # Pending updates: {instrument_token: last_tick_data}
        self._pending_updates: Dict[int, Dict[str, Any]] = {}
        self._update_lock = asyncio.Lock()

        # Active subscriptions
        self._subscribed_tokens: Set[int] = set()

        # Running state
        self._running = False
        self._listen_task: Optional[asyncio.Task] = None
        self._flush_task: Optional[asyncio.Task] = None

        logger.info("TickListener initialized")

    async def connect(self):
        """Establish Redis and database connections."""
        # Connect to Redis
        self.redis_client = await redis.from_url(
            self.redis_url,
            encoding="utf-8",
            decode_responses=True
        )
        await self.redis_client.ping()
        logger.info("TickListener connected to Redis")

        # Create pubsub
        self.pubsub = self.redis_client.pubsub()

        # Connect to database
        if self.database_url:
            self.engine = create_async_engine(
                self.database_url,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10
            )
            self.async_session = sessionmaker(
                self.engine,
                class_=AsyncSession,
                expire_on_commit=False
            )
            logger.info("TickListener connected to database")

    async def disconnect(self):
        """Close all connections."""
        self._running = False

        # Cancel tasks
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass

        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        # Flush any pending updates
        await self._flush_updates()

        # Close pubsub
        if self.pubsub:
            await self.pubsub.unsubscribe()
            await self.pubsub.close()

        # Close Redis
        if self.redis_client:
            await self.redis_client.close()

        # Close database
        if self.engine:
            await self.engine.dispose()

        logger.info("TickListener disconnected")

    async def start(self):
        """Start listening for ticks."""
        if self._running:
            logger.warning("TickListener already running")
            return

        self._running = True

        # Load subscriptions from database
        await self._load_subscriptions()

        # Subscribe to channels
        await self._subscribe_to_channels()

        # Start listener task
        self._listen_task = asyncio.create_task(self._listen_loop())

        # Start flush task
        self._flush_task = asyncio.create_task(self._flush_loop())

        logger.info(
            f"TickListener started, listening to {len(self._subscribed_tokens)} tokens"
        )

    async def stop(self):
        """Stop listening for ticks."""
        await self.disconnect()
        logger.info("TickListener stopped")

    async def _load_subscriptions(self):
        """Load active position subscriptions from database."""
        if not self.async_session:
            logger.warning("No database session - cannot load subscriptions")
            return

        async with self.async_session() as session:
            result = await session.execute(text("""
                SELECT DISTINCT instrument_token
                FROM order_service.position_subscriptions
                WHERE is_active = true AND is_subscribable = true
            """))

            self._subscribed_tokens = {row[0] for row in result.fetchall()}
            logger.info(f"Loaded {len(self._subscribed_tokens)} token subscriptions")

    async def _subscribe_to_channels(self):
        """Subscribe to Redis channels for all tokens."""
        if not self.pubsub:
            return

        if not self._subscribed_tokens:
            # Subscribe to a dummy channel to keep pubsub connection alive
            # This prevents errors when no tokens are subscribed yet
            await self.pubsub.subscribe("__tick_listener_keepalive__")
            logger.info("No tokens to subscribe, subscribed to keepalive channel")
            return

        # Subscribe to all tick channels
        channels = [f"ticks:{token}" for token in self._subscribed_tokens]

        if channels:
            await self.pubsub.subscribe(*channels)
            logger.info(f"Subscribed to {len(channels)} Redis channels")

    async def refresh_subscriptions(self):
        """Refresh subscriptions from database."""
        old_tokens = self._subscribed_tokens.copy()

        # Load new subscriptions
        await self._load_subscriptions()

        # Calculate changes
        added = self._subscribed_tokens - old_tokens
        removed = old_tokens - self._subscribed_tokens

        if self.pubsub:
            # Subscribe to new channels
            if added:
                channels = [f"ticks:{token}" for token in added]
                await self.pubsub.subscribe(*channels)
                logger.info(f"Added {len(added)} channel subscriptions")

            # Unsubscribe from old channels
            if removed:
                channels = [f"ticks:{token}" for token in removed]
                await self.pubsub.unsubscribe(*channels)
                logger.info(f"Removed {len(removed)} channel subscriptions")

    async def _listen_loop(self):
        """Main loop for listening to tick messages."""
        logger.info("Tick listener loop started")

        try:
            while self._running:
                try:
                    message = await self.pubsub.get_message(
                        ignore_subscribe_messages=True,
                        timeout=1.0
                    )

                    if message and message['type'] == 'message':
                        await self._handle_tick_message(message)

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in listen loop: {e}")
                    await asyncio.sleep(1)

        except asyncio.CancelledError:
            pass

        logger.info("Tick listener loop stopped")

    async def _handle_tick_message(self, message: dict):
        """Handle incoming tick message."""
        try:
            channel = message['channel']
            data = json.loads(message['data'])

            # Extract instrument token from channel (format: ticks:{token})
            token_str = channel.split(':')[1]
            instrument_token = int(token_str)

            # Handle list of ticks or single tick
            ticks = data if isinstance(data, list) else [data]

            # Take the latest tick
            if ticks:
                latest_tick = ticks[-1]

                async with self._update_lock:
                    self._pending_updates[instrument_token] = {
                        'instrument_token': instrument_token,
                        'last_price': latest_tick.get('last_price'),
                        'timestamp': latest_tick.get('timestamp') or datetime.utcnow().isoformat(),
                        'volume': latest_tick.get('volume'),
                        'oi': latest_tick.get('oi'),
                    }

                # Flush if batch is full
                if len(self._pending_updates) >= self.batch_size:
                    await self._flush_updates()

        except Exception as e:
            logger.error(f"Error handling tick message: {e}")

    async def _flush_loop(self):
        """Periodically flush pending updates."""
        while self._running:
            try:
                await asyncio.sleep(self.batch_interval_ms / 1000.0)
                await self._flush_updates()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in flush loop: {e}")

    async def _flush_updates(self):
        """Flush pending updates to database."""
        async with self._update_lock:
            if not self._pending_updates:
                return

            updates = list(self._pending_updates.values())
            self._pending_updates.clear()

        if not updates or not self.async_session:
            return

        try:
            async with self.async_session() as session:
                for update in updates:
                    token = update['instrument_token']
                    last_price = update.get('last_price')

                    if last_price is None:
                        continue

                    # Update position with new LTP and recalculate P&L
                    await session.execute(text("""
                        UPDATE order_service.positions
                        SET
                            last_price = :last_price,
                            unrealized_pnl = CASE
                                WHEN quantity > 0 THEN
                                    ((:last_price - buy_price) * quantity)
                                WHEN quantity < 0 THEN
                                    ((sell_price - :last_price) * ABS(quantity))
                                ELSE 0
                            END,
                            total_pnl = realized_pnl + CASE
                                WHEN quantity > 0 THEN
                                    ((:last_price - buy_price) * quantity)
                                WHEN quantity < 0 THEN
                                    ((sell_price - :last_price) * ABS(quantity))
                                ELSE 0
                            END,
                            net_pnl = realized_pnl + CASE
                                WHEN quantity > 0 THEN
                                    ((:last_price - buy_price) * quantity)
                                WHEN quantity < 0 THEN
                                    ((sell_price - :last_price) * ABS(quantity))
                                ELSE 0
                            END - total_charges,
                            updated_at = NOW()
                        WHERE instrument_token = :token
                          AND is_open = true
                          AND quantity != 0
                    """), {
                        "token": token,
                        "last_price": Decimal(str(last_price))
                    })

                await session.commit()
                logger.debug(f"Flushed {len(updates)} position updates")

        except Exception as e:
            logger.error(f"Error flushing position updates: {e}")


async def create_tick_listener(
    redis_url: str = None,
    database_url: str = None
) -> TickListener:
    """
    Create and configure a TickListener instance.

    Args:
        redis_url: Redis URL (defaults to settings)
        database_url: Database URL (defaults to settings)

    Returns:
        Configured TickListener
    """
    from ..config.settings import settings

    # IMPORTANT: Use ticker_redis_url for tick subscriptions (db 0)
    # The ticker_service publishes ticks to db 0, order_service uses db 1 for its own data
    redis_url = redis_url or getattr(settings, 'ticker_redis_url', None) or getattr(settings, 'redis_url', 'redis://localhost:6379/0')
    database_url = database_url or getattr(settings, 'database_url', None)

    # Convert sync URL to async URL if needed
    if database_url and 'postgresql://' in database_url and '+asyncpg' not in database_url:
        database_url = database_url.replace('postgresql://', 'postgresql+asyncpg://')

    listener = TickListener(
        redis_url=redis_url,
        database_url=database_url
    )

    return listener


# Singleton instance
_tick_listener: Optional[TickListener] = None


async def get_tick_listener() -> TickListener:
    """Get or create the tick listener singleton."""
    global _tick_listener

    if _tick_listener is None:
        _tick_listener = await create_tick_listener()
        await _tick_listener.connect()

    return _tick_listener


async def start_tick_listener():
    """Start the tick listener (called on service startup)."""
    listener = await get_tick_listener()
    await listener.start()
    logger.info("Tick listener started successfully")


async def stop_tick_listener():
    """Stop the tick listener (called on service shutdown)."""
    global _tick_listener

    if _tick_listener:
        await _tick_listener.stop()
        _tick_listener = None
        logger.info("Tick listener stopped")