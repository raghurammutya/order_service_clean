"""
Background Sync Workers

Periodic tasks for synchronizing orders, positions, and trades from broker.

Supports two modes:
- WebSocket mode (during market hours): Real-time order updates via Redis pub/sub
- REST polling mode (after market hours): Periodic polling from broker API
"""
import logging
import asyncio
import json
from datetime import datetime
from typing import Optional, List, Set
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as aioredis

from ..config.settings import settings
from ..database.connection import get_async_session
from ..services.position_service import PositionService
from ..services.trade_service import TradeService
from ..services.holding_service import HoldingService
from ..services.margin_service import MarginService
from ..services.pnl_calculator import PnLCalculator
from ..services.kite_client_multi import get_kite_client_for_account, get_all_trading_accounts
from ..services.market_hours import MarketHoursService, MarketSegment
from ..services.default_strategy_service import get_or_create_default_strategy
from ..models.order import OrderSource

logger = logging.getLogger(__name__)

# Map broker status (Kite) to database status
# Kite uses "TRIGGER PENDING" with space, but DB constraint expects "TRIGGER_PENDING" with underscore
BROKER_STATUS_MAP = {
    "TRIGGER PENDING": "TRIGGER_PENDING",
    "TRIGGER PENDING → CANCELLED": "CANCELLED",  # Handle transition status
    # Standard mappings (no change needed)
    "PENDING": "PENDING",
    "SUBMITTED": "SUBMITTED",
    "OPEN": "OPEN",
    "COMPLETE": "COMPLETE",
    "CANCELLED": "CANCELLED",
    "REJECTED": "REJECTED",
}


def normalize_broker_status(status: str) -> str:
    """Normalize broker status to database-compatible format."""
    if not status:
        return status
    return BROKER_STATUS_MAP.get(status, status)


class SyncWorkerManager:
    """Manages all background sync workers"""

    def __init__(self):
        self.is_running = False
        self.tasks = []
        self.order_sync_task: Optional[asyncio.Task] = None
        self.position_sync_task: Optional[asyncio.Task] = None
        self.trade_sync_task: Optional[asyncio.Task] = None
        self.websocket_listener_task: Optional[asyncio.Task] = None
        self.position_validation_task: Optional[asyncio.Task] = None
        self.holdings_sync_task: Optional[asyncio.Task] = None
        self.margin_polling_task: Optional[asyncio.Task] = None
        self.tier_worker_task: Optional[asyncio.Task] = None

        # Redis connection for WebSocket order updates
        self.redis_client: Optional[aioredis.Redis] = None
        self.redis_pubsub: Optional[aioredis.client.PubSub] = None
        self._subscribed_channels: Set[str] = set()

        # Tier worker instance
        self._tier_worker = None

        # Metrics
        self.websocket_updates_received = 0
        self.rest_polls_executed = 0
        self.position_validations = 0
        self.holdings_syncs = 0
        self.margin_polls = 0

    async def _fetch_active_account_broker_ids(self) -> List[str]:
        """Fetch broker_user_id for each active account for Redis subscriptions."""
        from ..clients.user_service_client import UserServiceClient, UserServiceClientError

        try:
            async with UserServiceClient() as client:
                accounts = await client.list_active_trading_accounts(status_filter="ACTIVE")
        except UserServiceClientError as exc:
            logger.error("Failed to fetch active accounts from user_service: %s", exc)
            return []

        return [
            str(acc["broker_user_id"])
            for acc in accounts
            if acc.get("broker_user_id")
        ]

    async def _build_subscribe_channels(self) -> Set[str]:
        """Build the set of Redis channels to subscribe to."""
        channels = set()
        broker_ids = await self._fetch_active_account_broker_ids()
        for broker_id in broker_ids:
            channels.add(f"orders:{broker_id}:all")

        legacy_channel = f"orders:{settings.kite_account_id}:all"
        channels.add(legacy_channel)
        return channels

    async def refresh_subscriptions(self) -> Set[str]:
        """Refresh Redis pub/sub subscriptions based on active accounts."""
        if not self.redis_pubsub:
            return set()

        desired_channels = await self._build_subscribe_channels()

        to_unsubscribe = self._subscribed_channels - desired_channels
        to_subscribe = desired_channels - self._subscribed_channels

        if to_unsubscribe:
            await self.redis_pubsub.unsubscribe(*to_unsubscribe)
        if to_subscribe:
            await self.redis_pubsub.subscribe(*to_subscribe)

        self._subscribed_channels = desired_channels
        return desired_channels

    async def start(self):
        """Start all background workers"""
        if self.is_running:
            logger.warning("Sync workers already running")
            return

        self.is_running = True
        logger.info("=" * 60)
        logger.info("Starting background sync workers")
        logger.info("=" * 60)

        # Initialize Redis connection for WebSocket order updates
        try:
            self.redis_client = aioredis.from_url(
                settings.redis_url,
                encoding="utf-8",
                decode_responses=True
            )
            await self.redis_client.ping()
            logger.info("✓ Redis connection established for WebSocket order updates")
        except (ConnectionError, TimeoutError, OSError) as e:
            logger.error(f"Failed to connect to Redis due to network error: {e}")
            logger.warning("WebSocket order updates will be disabled due to Redis connectivity issues")
        except Exception as e:
            logger.error(f"Failed to connect to Redis due to configuration error: {e}")
            logger.warning("WebSocket order updates will be disabled due to Redis configuration issues")

        # Start WebSocket listener (for real-time order updates during market hours)
        if self.redis_client:
            self.websocket_listener_task = asyncio.create_task(
                self._websocket_order_listener()
            )
            self.tasks.append(self.websocket_listener_task)
            logger.info("✓ WebSocket order listener started (real-time via Redis)")

        # Start order status sync worker (smart polling based on market hours)
        self.order_sync_task = asyncio.create_task(
            self._order_status_sync_worker()
        )
        self.tasks.append(self.order_sync_task)
        logger.info("✓ Order status sync worker started (smart polling)")

        # Position sync worker - DISABLED (replaced by real-time updates + validation)
        # Positions are now updated in real-time from order completions
        # Validation runs every 5 minutes via _position_validation_worker()
        # if settings.enable_position_tracking:
        #     self.position_sync_task = asyncio.create_task(
        #         self._position_sync_worker()
        #     )
        #     self.tasks.append(self.position_sync_task)
        #     logger.info(f"✓ Position sync worker started (interval: {settings.position_sync_interval}s)")
        logger.info("✓ Position sync disabled (using real-time updates + validation)")

        # Start trade sync worker
        self.trade_sync_task = asyncio.create_task(
            self._trade_sync_worker()
        )
        self.tasks.append(self.trade_sync_task)
        logger.info("✓ Trade sync worker started (interval: 30s)")

        # Start position validation worker (5-minute interval)
        self.position_validation_task = asyncio.create_task(
            self._position_validation_worker()
        )
        self.tasks.append(self.position_validation_task)
        logger.info("✓ Position validation worker started (interval: 5 minutes)")

        # Start holdings daily sync worker
        self.holdings_sync_task = asyncio.create_task(
            self._holdings_daily_sync_worker()
        )
        self.tasks.append(self.holdings_sync_task)
        logger.info("✓ Holdings daily sync worker started (4:30 PM daily)")

        # Start margin polling worker (activity-based)
        self.margin_polling_task = asyncio.create_task(
            self._margin_polling_worker()
        )
        self.tasks.append(self.margin_polling_task)
        logger.info("✓ Margin polling worker started (activity-based)")

        # Start tier calculation worker (recalculates tiers every 5 minutes)
        self.tier_worker_task = asyncio.create_task(
            self._tier_calculation_worker()
        )
        self.tasks.append(self.tier_worker_task)
        logger.info("✓ Tier calculation worker started (interval: 5 minutes)")

        logger.info("=" * 60)
        logger.info("All background workers started successfully")
        logger.info("=" * 60)

    async def stop(self):
        """Stop all background workers"""
        if not self.is_running:
            logger.warning("Sync workers not running")
            return

        logger.info("Stopping background sync workers...")
        self.is_running = False

        # Unsubscribe from Redis channels
        if self.redis_pubsub:
            try:
                await self.redis_pubsub.unsubscribe()
                await self.redis_pubsub.close()
                logger.info("Unsubscribed from Redis channels")
            except Exception as e:
                logger.error(f"Error unsubscribing from Redis: {e}")

        # Close Redis connection
        if self.redis_client:
            try:
                await self.redis_client.close()
                logger.info("Redis connection closed")
            except Exception as e:
                logger.error(f"Error closing Redis connection: {e}")

        # Cancel all tasks
        for task in self.tasks:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self.tasks.clear()
        logger.info("All background workers stopped")

    async def _order_status_sync_worker(self):
        """
        Background worker to sync order statuses from broker.

        Smart polling based on market hours:
        - During market hours (9:15 AM - 3:30 PM IST): Poll every 60 seconds (backup for WebSocket)
        - After market hours: Poll every 10 seconds
        """
        logger.info("Order status sync worker started (smart polling enabled)")

        while self.is_running:
            try:
                # Determine polling interval based on market hours
                is_market_open = MarketHoursService.is_market_open(
                    MarketSegment.EQUITY_DERIVATIVES
                )

                if is_market_open:
                    # During market hours: 60 second polling (backup for WebSocket)
                    polling_interval = 60
                else:
                    # After market hours: 10 second polling
                    polling_interval = 10

                await asyncio.sleep(polling_interval)

                if not self.is_running:
                    break

                # Log polling mode
                if is_market_open:
                    logger.debug("REST polling (market hours backup mode)")
                else:
                    logger.debug("REST polling (after hours mode)")

                # Get all active orders from database
                async for session in get_async_session():
                    try:
                        await self._sync_active_orders(session)
                        self.rest_polls_executed += 1
                    finally:
                        pass  # Session cleanup handled by context manager

            except asyncio.CancelledError:
                logger.info("Order status sync worker cancelled")
                break
            except (ConnectionError, TimeoutError, asyncio.TimeoutError) as e:
                logger.warning(f"Network/connection error in order sync, retrying: {e}")
                await asyncio.sleep(10)  # Wait before retrying
                continue
            except Exception as e:
                # INTENTIONAL FALLBACK: Final safety net to prevent worker corruption
                # After handling specific errors (network, timeout), this catches truly unexpected system errors
                # and safely shuts down the worker to prevent data corruption
                logger.critical(f"CRITICAL: Unexpected error in order status sync worker: {e}", exc_info=True)
                logger.critical("This indicates a serious bug - worker shutting down for safety")
                self.is_running = False  # Stop all workers to prevent state corruption
                raise  # Let supervisor restart service

    async def _websocket_order_listener(self):
        """
        WebSocket listener for real-time order updates via Redis pub/sub.

        Subscribes to: orders:{account_id}:all for ALL configured accounts
        Processes order updates from ticker_service_v2 in real-time.
        """
        logger.info("WebSocket order listener started")

        if not self.redis_client:
            logger.error("Redis client not initialized - WebSocket listener cannot start")
            return

        try:
            # Create pub/sub instance
            self.redis_pubsub = self.redis_client.pubsub()

            subscribed_channels = await self.refresh_subscriptions()
            for channel in sorted(subscribed_channels):
                logger.info(f"Subscribed to Redis channel: {channel}")

            # Listen for messages
            while self.is_running:
                try:
                    # Get message with timeout
                    message = await self.redis_pubsub.get_message(
                        ignore_subscribe_messages=True,
                        timeout=1.0
                    )

                    if message and message["type"] == "message":
                        # Process order update
                        await self._process_websocket_order_update(message["data"])
                        self.websocket_updates_received += 1

                    # Small sleep to prevent busy-waiting
                    await asyncio.sleep(0.01)

                except asyncio.CancelledError:
                    logger.info("WebSocket order listener cancelled")
                    break
                except Exception as e:
                    logger.error(f"Error processing WebSocket message: {e}", exc_info=True)
                    await asyncio.sleep(1)  # Brief pause before continuing

        except Exception as e:
            logger.error(f"Fatal error in WebSocket order listener: {e}", exc_info=True)
        finally:
            logger.info("WebSocket order listener stopped")

    async def _process_websocket_order_update(self, message_data: str):
        """
        Process a real-time order update from WebSocket (via Redis).

        Args:
            message_data: JSON string containing order update data
        """
        try:
            # Parse JSON data
            order_data = json.loads(message_data)

            # Extract order details
            order_id = order_data.get("order_id")
            broker_order_id = order_data.get("order_id")  # Kite uses order_id as broker_order_id
            status = order_data.get("status")
            symbol = order_data.get("tradingsymbol", "UNKNOWN")

            logger.debug(
                f"WebSocket order update: {broker_order_id} ({symbol}) - {status}"
            )

            # Update order in database
            async for session in get_async_session():
                try:
                    await self._update_order_from_websocket(session, order_data)
                finally:
                    pass  # Session cleanup handled by context manager

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse WebSocket order data: {e}")
        except (ConnectionError, TimeoutError, OSError) as e:
            logger.error(f"WebSocket order processing failed due to database error: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in WebSocket order update: {e}")
        except ValueError as e:
            logger.error(f"Invalid data in WebSocket order update: {e}")
        except Exception as e:
            logger.critical(f"CRITICAL: Unexpected error processing WebSocket order update: {e}", exc_info=True)

    async def _update_order_from_websocket(
        self,
        session: AsyncSession,
        order_data: dict
    ):
        """
        Update order in database from WebSocket data.

        Args:
            session: Database session
            order_data: Order data from Kite WebSocket
        """
        from ..models.order import Order
        from sqlalchemy import select

        try:
            broker_order_id = order_data.get("order_id")
            if not broker_order_id:
                logger.warning("Order update missing order_id")
                return

            # Find order in database
            result = await session.execute(
                select(Order).where(Order.broker_order_id == broker_order_id)
            )
            order = result.scalars().first()

            if not order:
                logger.debug(f"Order {broker_order_id} not found in database (may be from another source)")
                return

            # Update order fields
            old_status = order.status
            new_status = order_data.get("status", order.status)
            order.status = normalize_broker_status(new_status)
            order.filled_quantity = order_data.get("filled_quantity", order.filled_quantity)
            order.pending_quantity = order_data.get("pending_quantity", order.pending_quantity)
            order.cancelled_quantity = order_data.get("cancelled_quantity", order.cancelled_quantity)
            order.average_price = order_data.get("average_price", order.average_price)
            order.status_message = order_data.get("status_message", order.status_message)
            order.exchange_timestamp = order_data.get("exchange_timestamp", order.exchange_timestamp)
            order.exchange_order_id = order_data.get("exchange_order_id", order.exchange_order_id)

            # Update modified timestamp
            order.updated_at = datetime.utcnow()

            # Commit order update first
            await session.commit()

            # Log status change
            if old_status != order.status:
                logger.info(
                    f"[WebSocket] Order {order.id} status changed: {old_status} → {order.status} "
                    f"({order_data.get('tradingsymbol', 'N/A')})"
                )
            else:
                logger.debug(
                    f"[WebSocket] Order {order.id} updated: {order.status} "
                    f"(filled: {order.filled_quantity}/{order.quantity})"
                )

            # If order completed, update positions/holdings
            if order.status == "COMPLETE" and order.filled_quantity > 0:
                logger.info(f"Order completed - updating positions/holdings for {order.symbol}")

                # Update positions (for MIS/NRML) or holdings (for CNC)
                product_type = order_data.get("product")

                if product_type in ["MIS", "NRML"]:
                    # Update position - pass strategy_id from order
                    try:
                        position_service = PositionService(session, order.user_id, order.trading_account_id)
                        position = await position_service.update_position_from_order(
                            order_data,
                            strategy_id=order.strategy_id
                        )

                        if position:
                            logger.info(
                                f"Position updated from order: {position.symbol} "
                                f"qty={position.quantity} pnl={position.total_pnl} "
                                f"strategy_id={position.strategy_id}"
                            )
                    except (ConnectionError, TimeoutError, OSError) as e:
                        logger.error(f"Failed to update position from order due to database error: {e}")
                    except ValueError as e:
                        logger.error(f"Failed to update position from order due to data validation error: {e}")
                    except Exception as e:
                        logger.critical(f"CRITICAL: Unexpected error updating position from order: {e}", exc_info=True)

                elif product_type == "CNC":
                    # Update holding
                    try:
                        holding_service = HoldingService(session, order.user_id, order.trading_account_id)
                        holding_result = await holding_service.update_holding_from_order(order_data)

                        if holding_result:
                            logger.info(
                                f"Holding updated from order: {holding_result['symbol']} "
                                f"action={holding_result['action']}"
                            )
                    except (ConnectionError, TimeoutError, OSError) as e:
                        logger.error(f"Failed to update holding from order due to database error: {e}")
                    except ValueError as e:
                        logger.error(f"Failed to update holding from order due to data validation error: {e}")
                    except Exception as e:
                        logger.critical(f"CRITICAL: Unexpected error updating holding from order: {e}", exc_info=True)

                # Phase 2: Update P&L metrics for strategy when order completes
                try:
                    pnl_calculator = PnLCalculator(session)
                    pnl_updated = await pnl_calculator.update_strategy_pnl_metrics(
                        strategy_id=order.strategy_id
                    )
                    if pnl_updated:
                        logger.info(f"✅ P&L metrics updated for strategy {order.strategy_id}")
                    else:
                        logger.warning(f"⚠️  Failed to update P&L metrics for strategy {order.strategy_id}")
                except (ConnectionError, TimeoutError, OSError) as e:
                    logger.error(f"Failed to update P&L metrics for strategy {order.strategy_id} due to database error: {e}")
                except Exception as e:
                    logger.critical(f"CRITICAL: Unexpected error updating P&L metrics for strategy {order.strategy_id}: {e}", exc_info=True)

                # Create trade record from completed order
                try:
                    await self._create_trade_from_order(session, order, order_data)
                except (ConnectionError, TimeoutError, OSError) as e:
                    logger.error(f"Failed to create trade from order due to database error: {e}")
                except ValueError as e:
                    logger.error(f"Failed to create trade from order due to data validation error: {e}")
                except Exception as e:
                    logger.critical(f"CRITICAL: Unexpected error creating trade from order: {e}", exc_info=True)

        except (ConnectionError, TimeoutError, OSError) as e:
            logger.error(f"Failed to update order from websocket due to database error: {e}")
            await session.rollback()
            from ..exceptions import DatabaseError
            raise DatabaseError(f"WebSocket order update failed due to database error: {e}")
        except ValueError as e:
            logger.error(f"Failed to update order from websocket due to validation error: {e}")
            await session.rollback()
            from ..exceptions import ValidationError
            raise ValidationError(f"WebSocket order update failed due to invalid data: {e}")
        except Exception as e:
            logger.critical(f"CRITICAL: Unexpected error updating order from websocket: {e}", exc_info=True)
            await session.rollback()
            from ..exceptions import OrderServiceError
            raise OrderServiceError(f"Critical websocket order update failure: {e}")

    async def _create_trade_from_order(
        self,
        session: AsyncSession,
        order,
        order_data: dict
    ):
        """
        Create trade record from completed order.

        This creates a 1:1 trade record when an order completes.
        For detailed per-fill trades, use on-demand trade sync via API.

        Args:
            session: Database session
            order: Order model instance
            order_data: Raw order data from broker
        """
        from ..models.trade import Trade
        from sqlalchemy import select
        from decimal import Decimal

        try:
            # Generate trade_id from order (since we don't have real trade_id)
            # Format: {broker_order_id}_fill to ensure uniqueness
            broker_trade_id = f"{order.broker_order_id}_fill"

            # Check if trade already exists (idempotent)
            result = await session.execute(
                select(Trade).where(Trade.broker_trade_id == broker_trade_id)
            )
            existing_trade = result.scalars().first()

            if existing_trade:
                logger.debug(f"Trade {broker_trade_id} already exists, skipping")
                return

            # Parse trade time from order data
            trade_time = order_data.get('exchange_timestamp') or order_data.get('order_timestamp')
            if isinstance(trade_time, str):
                from dateutil import parser
                try:
                    trade_time = parser.parse(trade_time)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Failed to parse trade timestamp, using current time: {e}")
                    trade_time = datetime.utcnow()
                except Exception as e:
                    logger.error(f"Unexpected error parsing trade timestamp: {e}")
                    trade_time = datetime.utcnow()
            elif trade_time is None:
                trade_time = datetime.utcnow()

            # Calculate trade value
            quantity = order.filled_quantity or 0
            price = Decimal(str(order.average_price or 0))
            trade_value = Decimal(str(quantity)) * price

            # Create trade record
            trade = Trade(
                order_id=order.id,
                broker_order_id=str(order.broker_order_id),
                broker_trade_id=broker_trade_id,
                user_id=order.user_id,
                trading_account_id=order.trading_account_id,
                strategy_id=order.strategy_id,
                symbol=order.symbol,
                exchange=order.exchange,
                transaction_type=order.transaction_type,
                product_type=order_data.get('product', order.product_type),
                quantity=quantity,
                price=price,
                trade_value=trade_value,
                trade_time=trade_time
            )

            session.add(trade)
            await session.commit()

            logger.info(
                f"✅ Trade created from order: {order.symbol} "
                f"qty={quantity} price={price} strategy={order.strategy_id}"
            )

        except Exception as e:
            logger.error(f"Failed to create trade from order: {e}", exc_info=True)
            await session.rollback()
            raise

    async def _detect_and_tag_external_orders(
        self,
        session: AsyncSession,
        trading_account_id: str,
        broker_orders: list
    ) -> int:
        """
        Detect orders from broker that don't exist in our database (external orders)
        and tag them to the default strategy.

        Args:
            session: Database session
            trading_account_id: Trading account ID
            broker_orders: List of orders from broker API

        Returns:
            Number of external orders detected and tagged
        """
        from ..models.order import Order
        from sqlalchemy import select

        external_orders_count = 0

        # Get all our broker_order_ids for this account
        result = await session.execute(
            select(Order.broker_order_id).where(
                Order.trading_account_id == trading_account_id
            )
        )
        our_order_ids = {str(row[0]) for row in result.fetchall() if row[0]}

        # Find broker orders that don't exist in our database
        for broker_order in broker_orders:
            broker_order_id = str(broker_order.get('order_id', ''))
            if not broker_order_id:
                continue

            if broker_order_id not in our_order_ids:
                # This is an external order - tag to default strategy
                logger.info(
                    f"Detected external order: {broker_order_id} "
                    f"({broker_order.get('tradingsymbol', 'N/A')})"
                )

                try:
                    # GAP-REC-15: Use idempotency service to prevent duplicate external order processing
                    from ..services.external_order_tagging_idempotency import ExternalOrderTaggingIdempotencyService
                    
                    idempotency_service = ExternalOrderTaggingIdempotencyService(session)
                    
                    # Prepare operation data for idempotency check
                    operation_data = {
                        "trading_account_id": trading_account_id,
                        "broker_order_id": broker_order_id,
                        "order_data": {
                            "symbol": broker_order.get('tradingsymbol'),
                            "transaction_type": broker_order.get('transaction_type'),
                            "quantity": broker_order.get('quantity'),
                            "price": broker_order.get('price'),
                            "order_type": broker_order.get('order_type'),
                            "status": broker_order.get('status'),
                            "order_timestamp": broker_order.get('order_timestamp', datetime.utcnow().isoformat())
                        }
                    }
                    
                    # Check if this operation has already been processed
                    idempotency_key = f"external_order_tag_{trading_account_id}_{broker_order_id}"
                    
                    idempotency_result = await idempotency_service.ensure_idempotent_operation(
                        operation_type="external_order_tagging",
                        client_id=f"sync_worker_{trading_account_id}",
                        idempotency_key=idempotency_key,
                        request_data=operation_data
                    )
                    
                    if not idempotency_result.should_proceed:
                        logger.info(
                            f"External order {broker_order_id} already processed, skipping "
                            f"(operation_id: {idempotency_result.operation_id})"
                        )
                        continue
                    
                    logger.info(
                        f"Processing external order {broker_order_id} with idempotency "
                        f"(operation_id: {idempotency_result.operation_id})"
                    )
                    
                    # Get default strategy and user-managed execution for this account
                    default_strategy_id, default_execution_id = await get_or_create_default_strategy(
                        session, trading_account_id
                    )
                    
                    # Use DefaultPortfolioService to get or create appropriate portfolio
                    from order_service.app.services.default_portfolio_service import DefaultPortfolioService
                    portfolio_service = DefaultPortfolioService(session)
                    
                    # Get default portfolio for external orders (returns tuple: portfolio_id, strategy_id)
                    portfolio_id, _ = await portfolio_service.get_or_create_default_portfolio(
                        trading_account_id=trading_account_id,
                        user_id=settings.system_user_id
                    )

                    # Create order record for tracking
                    new_order = Order(
                        user_id=settings.system_user_id,
                        trading_account_id=trading_account_id,
                        broker_order_id=broker_order_id,
                        strategy_id=default_strategy_id,
                        execution_id=default_execution_id,  # Set execution_id
                        portfolio_id=portfolio_id,  # NEW: Set portfolio_id via DefaultPortfolioService
                        source=OrderSource.EXTERNAL,
                        symbol=broker_order.get('tradingsymbol', ''),
                        exchange=broker_order.get('exchange', ''),
                        transaction_type=broker_order.get('transaction_type', 'BUY'),
                        order_type=broker_order.get('order_type', 'MARKET'),
                        product_type=broker_order.get('product', 'MIS'),
                        variety=broker_order.get('variety', 'regular'),
                        quantity=broker_order.get('quantity', 0),
                        filled_quantity=broker_order.get('filled_quantity', 0),
                        pending_quantity=broker_order.get('pending_quantity', 0),
                        cancelled_quantity=broker_order.get('cancelled_quantity', 0),
                        price=broker_order.get('price'),
                        trigger_price=broker_order.get('trigger_price'),
                        average_price=broker_order.get('average_price'),
                        status=normalize_broker_status(broker_order.get('status', 'OPEN')),
                        status_message=broker_order.get('status_message'),
                        validity=broker_order.get('validity', 'DAY'),
                    )
                    session.add(new_order)
                    external_orders_count += 1

                    # Mark idempotent operation as successful
                    await idempotency_service.mark_operation_complete(
                        operation_id=idempotency_result.operation_id,
                        success=True,
                        result_data={
                            "order_id": str(new_order.id) if hasattr(new_order, 'id') else None,
                            "default_strategy_id": default_strategy_id,
                            "execution_id": str(default_execution_id),
                            "portfolio_id": portfolio_id
                        }
                    )

                    logger.info(
                        f"Tagged external order {broker_order_id} to default strategy {default_strategy_id}, "
                        f"execution {default_execution_id}, and portfolio {portfolio_id} "
                        f"(idempotency: {idempotency_result.operation_id})"
                    )

                except Exception as e:
                    logger.error(f"Failed to tag external order {broker_order_id}: {e}")
                    
                    # Mark idempotent operation as failed if idempotency was started
                    if 'idempotency_result' in locals():
                        try:
                            await idempotency_service.mark_operation_complete(
                                operation_id=idempotency_result.operation_id,
                                success=False,
                                result_data={"error": str(e)}
                            )
                        except Exception as idempotency_error:
                            logger.error(f"Failed to mark idempotent operation as failed: {idempotency_error}")

        if external_orders_count > 0:
            await session.commit()
            logger.info(f"Tagged {external_orders_count} external orders to default strategy")

        return external_orders_count

    async def _sync_active_orders(self, session: AsyncSession):
        """Sync all active orders for all accounts AND detect external orders"""
        from ..models.order import Order
        from sqlalchemy import select
        from collections import defaultdict

        try:
            # Get all active orders (not in terminal state)
            result = await session.execute(
                select(Order).where(
                    Order.status.in_(['PENDING', 'SUBMITTED', 'OPEN', 'TRIGGER_PENDING'])
                ).limit(100)  # Limit to prevent overload
            )
            active_orders = result.scalars().all()

            # Group orders by trading_account_id
            orders_by_account = defaultdict(list)
            for order in active_orders:
                orders_by_account[order.trading_account_id].append(order)

            # IMPORTANT: Always fetch orders for ALL accounts to detect external orders
            # Even if we have no active orders in our DB, broker may have external orders
            account_mapping = await get_all_trading_accounts()
            total_synced = 0
            total_external = 0

            for trading_account_id, account_info in account_mapping.items():
                account_nickname = account_info.get('nickname', f'account_{trading_account_id}')
                try:
                    # Get kite client for this account
                    kite_client = get_kite_client_for_account(trading_account_id)
                    broker_orders = await kite_client.get_orders()

                    # Detect and tag external orders (orders not in our database)
                    external_count = await self._detect_and_tag_external_orders(
                        session, str(trading_account_id), broker_orders
                    )
                    total_external += external_count

                    # Get account's active orders to update
                    account_orders = orders_by_account.get(str(trading_account_id), [])

                    if not account_orders:
                        logger.debug(f"No active orders to update for {account_nickname} (checked {len(broker_orders)} broker orders)")
                        continue

                    # Create lookup dict by broker_order_id
                    broker_orders_dict = {
                        str(order['order_id']): order
                        for order in broker_orders
                    }

                    logger.debug(f"Fetched {len(broker_orders)} broker orders for {account_nickname}")

                    # Update each order
                    for order in account_orders:
                        if not order.broker_order_id:
                            continue

                        broker_order = broker_orders_dict.get(str(order.broker_order_id))
                        if not broker_order:
                            logger.warning(f"Order {order.id} not found in broker API for {account_nickname}")
                            continue

                        # Update order status
                        old_status = order.status
                        new_broker_status = broker_order['status']
                        order.status = normalize_broker_status(new_broker_status)
                        order.filled_quantity = broker_order.get('filled_quantity', 0)
                        order.pending_quantity = broker_order.get('pending_quantity', 0)
                        order.cancelled_quantity = broker_order.get('cancelled_quantity', 0)
                        order.average_price = broker_order.get('average_price')
                        order.status_message = broker_order.get('status_message')
                        order.exchange_timestamp = broker_order.get('exchange_timestamp')

                        if old_status != order.status:
                            logger.info(
                                f"Order {order.id} status changed: {old_status} → {order.status} ({account_nickname})"
                            )
                        total_synced += 1

                except Exception as e:
                    logger.error(f"Failed to sync orders for {account_nickname}: {e}")
                    # Continue with other accounts

            # Commit all changes
            await session.commit()
            if total_synced > 0 or total_external > 0:
                logger.info(f"Order sync complete: synced={total_synced} external={total_external} across {len(account_mapping)} accounts")

        except Exception as e:
            logger.error(f"Failed to sync orders: {e}", exc_info=True)
            await session.rollback()
            raise

    # DEPRECATED: Old position sync worker
    # Positions are now updated in real-time from order completions via:
    # - _update_order_from_websocket() -> PositionService.update_position_from_order()
    # - Validation via _position_validation_worker() every 5 minutes
    #
    # This method is kept for reference but is NO LONGER CALLED
    async def _position_sync_worker_DEPRECATED(self):
        """
        DEPRECATED: Old background worker to sync positions from broker.

        REPLACED BY:
        - Real-time updates: PositionService.update_position_from_order()
        - Validation: _position_validation_worker() (every 5 minutes)

        This worker is NO LONGER USED.
        """
        logger.warning("DEPRECATED: _position_sync_worker called - this should not happen")
        logger.warning("Positions are now updated in real-time from orders")

        # Old implementation removed - see git history if needed
        pass

    async def _trade_sync_worker(self):
        """
        Background worker to sync trades from broker.

        Runs every 60 seconds to fetch new trades for all configured accounts.
        Only syncs during market hours + 30 minutes after close.
        """
        logger.info("Trade sync worker started")

        while self.is_running:
            try:
                await asyncio.sleep(60)  # Poll every 60 seconds

                if not self.is_running:
                    break

                # Check if market is open or recently closed (sync trades after market close too)
                is_market_open = MarketHoursService.is_market_open(
                    MarketSegment.EQUITY_DERIVATIVES
                )

                # Also sync for 30 minutes after market close to catch final trades
                from zoneinfo import ZoneInfo
                IST = ZoneInfo("Asia/Kolkata")
                now = datetime.now(IST)
                is_post_market = (now.hour == 15 and now.minute <= 59) or (now.hour == 16 and now.minute <= 30)

                if not is_market_open and not is_post_market:
                    # Skip sync outside trading hours
                    logger.debug("Trade sync skipped - outside market hours")
                    continue

                # Sync trades for all configured trading accounts
                account_mapping = await get_all_trading_accounts()
                user_id = settings.system_user_id

                total_synced = 0
                total_created = 0

                async for session in get_async_session():
                    try:
                        for trading_account_id, account_info in account_mapping.items():
                            account_nickname = account_info.get('nickname', f'account_{trading_account_id}')
                            try:
                                trade_service = TradeService(session, user_id, trading_account_id)
                                stats = await trade_service.sync_trades_from_broker()

                                total_synced += stats.get('trades_synced', 0)
                                total_created += stats.get('trades_created', 0)

                                if stats.get('trades_synced', 0) > 0:
                                    logger.info(
                                        f"Trade sync for {account_nickname}: "
                                        f"synced={stats['trades_synced']}"
                                    )
                            except Exception as e:
                                logger.error(f"Error syncing trades for {account_nickname}: {e}")

                        if total_synced > 0:
                            logger.info(
                                f"Trade sync complete: synced={total_synced} "
                                f"across {len(account_mapping)} accounts"
                            )
                    finally:
                        pass  # Session cleanup handled by context manager

            except asyncio.CancelledError:
                logger.info("Trade sync worker cancelled")
                break
            except (ConnectionError, TimeoutError, asyncio.TimeoutError) as e:
                logger.warning(f"Network error in trade sync, retrying: {e}")
                await asyncio.sleep(60)
            except Exception as e:
                # INTENTIONAL FALLBACK: Final safety net to prevent worker corruption
                # After handling specific errors (network, timeout), this catches truly unexpected system errors
                # and safely shuts down the worker to prevent trade data corruption
                logger.critical(f"CRITICAL: Unexpected error in trade sync worker: {e}", exc_info=True)
                logger.critical("Trade sync worker shutting down for safety")
                self.is_running = False  # Stop all workers to prevent state corruption
                raise  # Let supervisor restart service

    async def _position_validation_worker(self):
        """
        Background worker to validate positions every 5 minutes.

        Detects drift between calculated positions and broker API.
        Validates positions for ALL configured trading accounts.
        """
        logger.info("Position validation worker started")

        while self.is_running:
            try:
                await asyncio.sleep(300)  # 5 minutes

                if not self.is_running:
                    break

                logger.debug("Position validation triggered")

                # Get all configured trading accounts
                account_mapping = await get_all_trading_accounts()
                user_id = settings.system_user_id  # Configurable via SYSTEM_USER_ID env var

                async for session in get_async_session():
                    try:
                        total_checked = 0
                        total_corrected = 0

                        # Validate positions for each trading account
                        for trading_account_id, account_info in account_mapping.items():
                            account_nickname = account_info.get('nickname', f'account_{trading_account_id}')
                            try:
                                position_service = PositionService(session, user_id, trading_account_id)
                                stats = await position_service.validate_positions()

                                total_checked += stats.get('positions_checked', 0)
                                total_corrected += stats.get('positions_corrected', 0)

                                # Alert on drift for this account
                                if stats.get('positions_corrected', 0) > 0:
                                    logger.warning(
                                        f"Position drift detected for {account_nickname}: "
                                        f"quantity_drifts={len(stats.get('quantity_drifts', []))} "
                                        f"pnl_drifts={len(stats.get('pnl_drifts', []))}"
                                    )

                            except Exception as e:
                                logger.error(f"Error validating positions for {account_nickname}: {e}")

                        self.position_validations += 1

                        logger.info(
                            f"Position validation complete: "
                            f"checked={total_checked} corrected={total_corrected} "
                            f"across {len(account_mapping)} accounts"
                        )

                    finally:
                        pass  # Session cleanup handled by context manager

            except asyncio.CancelledError:
                logger.info("Position validation worker cancelled")
                break
            except (ConnectionError, TimeoutError, asyncio.TimeoutError) as e:
                logger.warning(f"Network error in position validation, retrying: {e}")
                await asyncio.sleep(300)  # Wait before retrying
            except Exception as e:
                logger.critical(f"CRITICAL: Unexpected error in position validation worker: {e}", exc_info=True)
                logger.critical("Position validation worker shutting down for safety")
                self.is_running = False
                raise

    async def _holdings_daily_sync_worker(self):
        """
        Background worker to sync holdings once daily at 4:30 PM.

        Catches external buys, corporate actions, etc.
        Syncs holdings for ALL configured trading accounts.
        """
        logger.info("Holdings daily sync worker started")

        while self.is_running:
            try:
                from zoneinfo import ZoneInfo
                IST = ZoneInfo("Asia/Kolkata")
                now = datetime.now(IST)

                # Check if it's 4:30 PM
                if now.hour == 16 and now.minute == 30:
                    logger.info("Daily holdings sync triggered (4:30 PM)")

                    # Get all configured trading accounts
                    account_mapping = await get_all_trading_accounts()
                    user_id = settings.system_user_id  # Configurable via SYSTEM_USER_ID env var

                    async for session in get_async_session():
                        try:
                            total_synced = 0

                            # Sync holdings for each trading account
                            for trading_account_id, account_info in account_mapping.items():
                                account_nickname = account_info.get('nickname', f'account_{trading_account_id}')
                                try:
                                    holding_service = HoldingService(session, user_id, trading_account_id)
                                    stats = await holding_service.sync_holdings_daily()

                                    total_synced += stats.get('holdings_synced', 0)

                                    logger.info(
                                        f"Holdings sync for {account_nickname}: "
                                        f"synced={stats.get('holdings_synced', 0)}"
                                    )

                                except Exception as e:
                                    logger.error(f"Error syncing holdings for {account_nickname}: {e}")

                            self.holdings_syncs += 1

                            logger.info(
                                f"Holdings sync complete: synced={total_synced} "
                                f"across {len(account_mapping)} accounts"
                            )

                        finally:
                            pass  # Session cleanup handled by context manager

                    # Wait 1 hour to avoid re-triggering
                    await asyncio.sleep(3600)

                else:
                    # Check every minute
                    await asyncio.sleep(60)

                if not self.is_running:
                    break

            except asyncio.CancelledError:
                logger.info("Holdings daily sync worker cancelled")
                break
            except Exception as e:
                logger.error(f"Error in holdings daily sync worker: {e}", exc_info=True)
                await asyncio.sleep(60)

    async def _margin_polling_worker(self):
        """
        Background worker for activity-based margin polling.

        Polling interval adjusts based on market activity:
        - Active orders: 30 seconds
        - Open positions (market hours): 60 seconds
        - Market open, no activity: 5 minutes
        - Market closed: 30 minutes

        Polls margins for ALL configured trading accounts.
        """
        logger.info("Margin polling worker started (activity-based)")

        while self.is_running:
            try:
                # Get all configured trading accounts
                account_mapping = await get_all_trading_accounts()
                user_id = settings.system_user_id  # Configurable via SYSTEM_USER_ID env var

                # Use primary account to determine polling interval
                # (market hours are the same for all accounts)
                primary_account_id = 1

                async for session in get_async_session():
                    try:
                        margin_service = MarginService(session, user_id, primary_account_id)

                        # Get dynamic polling interval
                        polling_interval = await margin_service.get_polling_interval()

                        if polling_interval == 0:
                            # Weekend - no polling
                            logger.debug("Weekend detected - skipping margin poll")
                            await asyncio.sleep(3600)  # Check again in 1 hour
                            continue

                        # Wait for polling interval
                        await asyncio.sleep(polling_interval)

                        if not self.is_running:
                            break

                        # Fetch and cache margins for each trading account
                        for trading_account_id, account_info in account_mapping.items():
                            account_nickname = account_info.get('nickname', f'account_{trading_account_id}')
                            try:
                                account_margin_service = MarginService(session, user_id, trading_account_id)
                                await account_margin_service.fetch_and_cache_margins()

                                # Check for low margin alert for this account
                                alert = await account_margin_service.check_low_margin_alert(threshold=10000.0)
                                if alert.get("alert"):
                                    logger.warning(f"Low margin alert for {account_nickname}: {alert}")

                            except Exception as e:
                                logger.error(f"Error polling margin for {account_nickname}: {e}")

                        self.margin_polls += 1

                        logger.debug(
                            f"Margin polled for {len(account_mapping)} accounts "
                            f"(interval={polling_interval}s)"
                        )

                    finally:
                        pass  # Session cleanup handled by context manager

            except asyncio.CancelledError:
                logger.info("Margin polling worker cancelled")
                break
            except (ConnectionError, TimeoutError, asyncio.TimeoutError) as e:
                logger.warning(f"Network error in margin polling, retrying: {e}")
                await asyncio.sleep(60)  # Wait before retrying
            except Exception as e:
                logger.critical(f"CRITICAL: Unexpected error in margin polling worker: {e}", exc_info=True)
                logger.critical("Margin polling worker shutting down for safety")
                self.is_running = False
                raise

    async def _tier_calculation_worker(self):
        """
        Background worker for tier recalculation.

        Runs every 5 minutes to update account tiers based on activity.
        Uses TierWorker from tier_worker.py module.
        """
        from .tier_worker import TierWorker

        self._tier_worker = TierWorker()

        try:
            await self._tier_worker.start()
        except Exception as e:
            logger.error(f"Tier worker failed: {e}", exc_info=True)

    def get_metrics(self) -> dict:
        """
        Get sync worker metrics.

        Returns:
            Dictionary with WebSocket and REST polling statistics
        """
        tier_metrics = {}
        if self._tier_worker:
            tier_metrics = self._tier_worker.get_status()

        return {
            "websocket_updates_received": self.websocket_updates_received,
            "rest_polls_executed": self.rest_polls_executed,
            "position_validations": self.position_validations,
            "holdings_syncs": self.holdings_syncs,
            "margin_polls": self.margin_polls,
            "is_running": self.is_running,
            "active_tasks": len([t for t in self.tasks if t and not t.done()]),
            "websocket_connected": self.redis_client is not None and self.redis_pubsub is not None,
            "tier_worker": tier_metrics
        }


# Global worker manager instance
_worker_manager: Optional[SyncWorkerManager] = None


async def start_workers():
    """Start all background workers"""
    global _worker_manager

    if _worker_manager is None:
        _worker_manager = SyncWorkerManager()

    await _worker_manager.start()


async def stop_workers():
    """Stop all background workers"""
    global _worker_manager

    if _worker_manager:
        await _worker_manager.stop()


def get_worker_manager() -> Optional[SyncWorkerManager]:
    """Get the global worker manager instance"""
    return _worker_manager
