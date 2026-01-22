"""
Account Event Handler for Order Service - Production Ready

Handles account lifecycle events to maintain data consistency
and clean up resources when accounts are deleted or deactivated.
Implements production-grade async patterns and proper error handling.
"""

import logging
from typing import Dict, Any
from sqlalchemy import select

# Import from common module with proper path
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../common'))
from common.event_listener.account_events import AccountEvent, AccountEventType, AccountEventListener

from ..database.connection import get_db
from ..models.position import Position
from ..models.order import Order
from ..models.trade import Trade
from ..config.settings import settings

logger = logging.getLogger(__name__)

class OrderServiceAccountEventHandler:
    """Production-ready account event handler for order service"""
    
    def __init__(self):
        self.event_listener = None
        self.running = False
        
    async def initialize(self):
        """Initialize the event listener and register handlers"""
        try:
            # SECURITY: Use Redis URL from settings with proper auth
            if not settings.redis_url:
                raise ValueError("Redis URL not configured in settings")
                
            self.event_listener = AccountEventListener(
                redis_url=settings.redis_url, 
                service_name="order_service"
            )
            await self.event_listener.connect()
            
            # Register event handlers
            self.event_listener.register_handler(
                AccountEventType.ACCOUNT_DELETED, 
                self.handle_account_deleted
            )
            self.event_listener.register_handler(
                AccountEventType.ACCOUNT_DEACTIVATED,
                self.handle_account_deactivated  
            )
            self.event_listener.register_handler(
                AccountEventType.ACCOUNT_MEMBERSHIP_REVOKED,
                self.handle_membership_revoked
            )
            self.event_listener.register_handler(
                AccountEventType.ACCOUNT_CREATED,
                self.handle_account_created
            )
            
            logger.info("Order Service account event handler initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize account event handler: {e}")
            raise
    
    async def start_listening(self):
        """Start listening to account events in background"""
        if self.event_listener:
            self.running = True
            try:
                await self.event_listener.start_listening()
            except Exception as e:
                logger.error(f"Account event listener crashed: {e}")
                self.running = False
                raise
        else:
            logger.error("Event listener not initialized")
            raise RuntimeError("Event listener not initialized - call initialize() first")
    
    async def stop_listening(self):
        """Stop listening and cleanup"""
        self.running = False
        if self.event_listener:
            await self.event_listener.disconnect()
            self.event_listener = None

    async def handle_account_deleted(self, event: AccountEvent):
        """Handle account deletion - cleanup all related data with proper async patterns"""
        try:
            logger.info(
                f"Handling account deletion for account {event.trading_account_id}",
                extra={
                    "correlation_id": event.correlation_id,
                    "event_id": event.event_id,
                    "trading_account_id": event.trading_account_id
                }
            )
            
            # Use async database session properly
            async for db_session in get_db():
                try:
                    account_id = str(event.trading_account_id)
                    
                    # Close all open positions using async queries
                    open_positions_result = await db_session.execute(
                        select(Position).where(
                            Position.trading_account_id == account_id,
                            Position.is_open == True
                        )
                    )
                    open_positions = open_positions_result.scalars().all()
                    
                    for position in open_positions:
                        position.is_open = False
                        position.closed_at = event.timestamp
                        logger.info(
                            f"Closed position {position.id} for deleted account",
                            extra={"correlation_id": event.correlation_id}
                        )
                    
                    # Cancel all pending orders using async queries
                    pending_orders_result = await db_session.execute(
                        select(Order).where(
                            Order.trading_account_id == account_id,
                            Order.status.in_(['PENDING', 'OPEN', 'TRIGGER_PENDING'])
                        )
                    )
                    pending_orders = pending_orders_result.scalars().all()
                    
                    for order in pending_orders:
                        order.status = 'CANCELLED'
                        order.updated_at = event.timestamp
                        if hasattr(order, 'cancellation_reason'):
                            order.cancellation_reason = 'Account deleted'
                        logger.info(
                            f"Cancelled order {order.id} for deleted account",
                            extra={"correlation_id": event.correlation_id}
                        )
                    
                    # Archive trades (don't delete - for audit trail)
                    trades_result = await db_session.execute(
                        select(Trade).where(Trade.trading_account_id == account_id)
                    )
                    trades = trades_result.scalars().all()
                    
                    for trade in trades:
                        # Add a flag or move to archived status
                        if hasattr(trade, 'archived'):
                            trade.archived = True
                            trade.archived_at = event.timestamp
                    
                    await db_session.commit()
                    
                    logger.info(
                        f"Cleaned up {len(open_positions)} positions, "
                        f"{len(pending_orders)} orders, {len(trades)} trades "
                        f"for deleted account {event.trading_account_id}",
                        extra={
                            "correlation_id": event.correlation_id,
                            "positions_closed": len(open_positions),
                            "orders_cancelled": len(pending_orders),
                            "trades_archived": len(trades)
                        }
                    )
                    break  # Exit the async generator loop
                    
                except Exception as db_error:
                    await db_session.rollback()
                    logger.error(
                        f"Database error during account deletion cleanup: {db_error}",
                        extra={"correlation_id": event.correlation_id}
                    )
                    raise
                
        except Exception as e:
            logger.error(
                f"Error handling account deletion for {event.trading_account_id}: {e}",
                extra={"correlation_id": event.correlation_id}
            )

    async def handle_account_deactivated(self, event: AccountEvent):
        """Handle account deactivation - stop trading but keep data"""
        try:
            logger.info(
                f"Handling account deactivation for account {event.trading_account_id}",
                extra={
                    "correlation_id": event.correlation_id,
                    "event_id": event.event_id,
                    "trading_account_id": event.trading_account_id
                }
            )
            
            async for db_session in get_db():
                try:
                    account_id = str(event.trading_account_id)
                    
                    # Cancel all pending orders (but keep closed orders and trades)
                    pending_orders_result = await db_session.execute(
                        select(Order).where(
                            Order.trading_account_id == account_id,
                            Order.status.in_(['PENDING', 'OPEN', 'TRIGGER_PENDING'])
                        )
                    )
                    pending_orders = pending_orders_result.scalars().all()
                    
                    for order in pending_orders:
                        order.status = 'CANCELLED'
                        order.updated_at = event.timestamp
                        if hasattr(order, 'cancellation_reason'):
                            order.cancellation_reason = 'Account deactivated'
                        logger.info(
                            f"Cancelled order {order.id} for deactivated account",
                            extra={"correlation_id": event.correlation_id}
                        )
                    
                    await db_session.commit()
                    
                    logger.info(
                        f"Cancelled {len(pending_orders)} pending orders "
                        f"for deactivated account {event.trading_account_id}",
                        extra={
                            "correlation_id": event.correlation_id,
                            "orders_cancelled": len(pending_orders)
                        }
                    )
                    break
                    
                except Exception as db_error:
                    await db_session.rollback()
                    logger.error(
                        f"Database error during account deactivation: {db_error}",
                        extra={"correlation_id": event.correlation_id}
                    )
                    raise
                
        except Exception as e:
            logger.error(
                f"Error handling account deactivation for {event.trading_account_id}: {e}",
                extra={"correlation_id": event.correlation_id}
            )

    async def handle_membership_revoked(self, event: AccountEvent):
        """Handle membership revocation - user loses access with proper validation"""
        try:
            # Get member user ID from event data with validation
            revoked_user_id = event.data.get("member_user_id")
            if not revoked_user_id:
                logger.warning(
                    f"No member_user_id in membership revoked event for account {event.trading_account_id}",
                    extra={"correlation_id": event.correlation_id}
                )
                return
                
            account_id = str(event.trading_account_id)
            
            logger.info(
                f"Handling membership revocation for user {revoked_user_id} "
                f"on account {event.trading_account_id}",
                extra={
                    "correlation_id": event.correlation_id,
                    "event_id": event.event_id,
                    "trading_account_id": event.trading_account_id,
                    "revoked_user_id": revoked_user_id
                }
            )
            
            # Cancel any pending orders placed by this user for this account
            async for db_session in get_db():
                try:
                    user_orders_result = await db_session.execute(
                        select(Order).where(
                            Order.trading_account_id == account_id,
                            Order.user_id == revoked_user_id,
                            Order.status.in_(['PENDING', 'OPEN', 'TRIGGER_PENDING'])
                        )
                    )
                    user_orders = user_orders_result.scalars().all()
                    
                    for order in user_orders:
                        order.status = 'CANCELLED'
                        order.updated_at = event.timestamp
                        if hasattr(order, 'cancellation_reason'):
                            order.cancellation_reason = 'Membership revoked'
                        logger.info(
                            f"Cancelled order {order.id} for user with revoked membership",
                            extra={"correlation_id": event.correlation_id}
                        )
                    
                    await db_session.commit()
                    
                    logger.info(
                        f"Cancelled {len(user_orders)} orders for user {revoked_user_id} "
                        f"with revoked access to account {event.trading_account_id}",
                        extra={
                            "correlation_id": event.correlation_id,
                            "orders_cancelled": len(user_orders),
                            "revoked_user_id": revoked_user_id
                        }
                    )
                    break
                    
                except Exception as db_error:
                    await db_session.rollback()
                    logger.error(
                        f"Database error during membership revocation: {db_error}",
                        extra={"correlation_id": event.correlation_id}
                    )
                    raise
                
        except Exception as e:
            logger.error(
                f"Error handling membership revocation: {e}",
                extra={"correlation_id": event.correlation_id}
            )

    async def handle_account_created(self, event: AccountEvent):
        """Handle account creation - setup initial state"""
        try:
            logger.info(
                f"Handling account creation for account {event.trading_account_id}",
                extra={
                    "correlation_id": event.correlation_id,
                    "event_id": event.event_id,
                    "trading_account_id": event.trading_account_id
                }
            )
            
            # Initialize account-specific settings, quotas, etc.
            # This is where we could set up default position limits, 
            # risk parameters, etc. for new accounts
            
            broker = event.data.get("broker")
            account_name = event.data.get("account_name")
            status = event.data.get("status")
            
            logger.info(
                f"New {broker} account '{account_name}' (status: {status}) created: {event.trading_account_id} "
                f"for user {event.user_id}",
                extra={
                    "correlation_id": event.correlation_id,
                    "broker": broker,
                    "account_name": account_name,
                    "status": status
                }
            )
            
            # Could initialize default risk parameters, position limits, etc.
            # For now, just log the event and confirm account readiness
            
        except Exception as e:
            logger.error(
                f"Error handling account creation for {event.trading_account_id}: {e}",
                extra={"correlation_id": event.correlation_id}
            )

    def get_health_status(self) -> Dict[str, Any]:
        """Get health status for monitoring"""
        if self.event_listener:
            listener_health = self.event_listener.get_health_status()
            return {
                "status": "healthy" if self.running else "stopped",
                "running": self.running,
                "listener_health": listener_health
            }
        else:
            return {
                "status": "not_initialized",
                "running": False,
                "listener_health": None
            }

# Global handler instance
_account_event_handler = None

async def get_account_event_handler() -> OrderServiceAccountEventHandler:
    """Get the global account event handler"""
    global _account_event_handler
    if _account_event_handler is None:
        _account_event_handler = OrderServiceAccountEventHandler()
        await _account_event_handler.initialize()
    return _account_event_handler

async def cleanup_account_event_handler():
    """Cleanup the global account event handler"""
    global _account_event_handler
    if _account_event_handler:
        await _account_event_handler.stop_listening()
        _account_event_handler = None