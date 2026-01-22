import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from typing import Optional

from ..auth.jwt_auth import verify_ws_token
from .manager import ConnectionManager

logger = logging.getLogger(__name__)
router = APIRouter()

# Global connection manager (initialized in main.py)
manager: Optional[ConnectionManager] = None


def set_connection_manager(cm: ConnectionManager):
    """Set the global connection manager instance."""
    global manager
    manager = cm


def get_connection_manager() -> ConnectionManager:
    """Dependency to get connection manager."""
    if manager is None:
        raise RuntimeError("Connection manager not initialized")
    return manager


async def authenticate_websocket(token: str) -> int:
    """Authenticate WebSocket connection and return user_id."""
    try:
        # Use the WebSocket-specific verify function from jwt_auth
        payload = await verify_ws_token(token)
        user_id = payload.get("user_id")

        if user_id:
            # If user_id is a string like "user:7", extract the integer
            if isinstance(user_id, str) and user_id.startswith("user:"):
                return int(user_id.split(":")[1])
            return int(user_id)

        # Fallback to sub claim
        sub = payload.get("sub")
        if sub:
            if isinstance(sub, str) and sub.startswith("user:"):
                return int(sub.split(":")[1])
            return int(sub)

        raise ValueError("No user_id in token")
    except Exception as e:
        logger.warning(f"WebSocket authentication failed: {e}")
        raise


@router.websocket("/ws/v1/positions")
async def positions_websocket(
    websocket: WebSocket,
    token: str = Query(..., description="JWT authentication token"),
    trading_account_id: Optional[int] = Query(None, description="Filter by trading account"),
):
    """
    WebSocket endpoint for real-time position updates.

    Query params:
    - token: JWT authentication token
    - trading_account_id: Optional filter for specific account

    Message format:
    {
        "type": "position_update",
        "trading_account_id": 1,
        "position": {
            "symbol": "RELIANCE",
            "quantity": 100,
            "pnl": 1250.50,
            ...
        },
        "timestamp": "2025-12-03T10:30:00Z"
    }
    """
    cm = get_connection_manager()
    user_id = None

    try:
        # Authenticate
        user_id = await authenticate_websocket(token)

        # Connect
        await cm.connect(websocket, user_id)

        # Send welcome message
        await websocket.send_json({
            "type": "connected",
            "user_id": user_id,
            "message": "Connected to position updates",
            "filters": {
                "trading_account_id": trading_account_id
            }
        })

        # Keep connection alive
        while True:
            try:
                # Receive ping/pong messages
                data = await websocket.receive_text()
                if data == "ping":
                    await websocket.send_text("pong")
            except WebSocketDisconnect:
                break

    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        try:
            await websocket.close(code=1008, reason=str(e))
        except:
            pass
    finally:
        if user_id:
            cm.disconnect(websocket, user_id)
