import asyncio
import json
import logging
from typing import Dict, Set, Optional
from fastapi import WebSocket
import redis.asyncio as redis

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages WebSocket connections and Redis pub/sub for multi-server support."""

    def __init__(self, redis_url: str):
        self.active_connections: Dict[int, Set[WebSocket]] = {}  # user_id -> set of WebSockets
        self.redis_url = redis_url
        self.redis_client: Optional[redis.Redis] = None
        self.pubsub: Optional[redis.client.PubSub] = None
        self._listen_task: Optional[asyncio.Task] = None

    async def startup(self):
        """Initialize Redis connection and start listening."""
        self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
        self.pubsub = self.redis_client.pubsub()
        await self.pubsub.subscribe("positions:updates")
        self._listen_task = asyncio.create_task(self._listen_redis())
        logger.info("WebSocket connection manager started")

    async def shutdown(self):
        """Clean up Redis connections."""
        if self._listen_task:
            self._listen_task.cancel()
        if self.pubsub:
            await self.pubsub.unsubscribe("positions:updates")
            await self.pubsub.close()
        if self.redis_client:
            await self.redis_client.close()
        logger.info("WebSocket connection manager shutdown")

    async def connect(self, websocket: WebSocket, user_id: int):
        """Register a new WebSocket connection."""
        await websocket.accept()
        if user_id not in self.active_connections:
            self.active_connections[user_id] = set()
        self.active_connections[user_id].add(websocket)
        logger.info(f"User {user_id} connected. Total connections: {self.get_connection_count()}")

    def disconnect(self, websocket: WebSocket, user_id: int):
        """Remove a WebSocket connection."""
        if user_id in self.active_connections:
            self.active_connections[user_id].discard(websocket)
            if not self.active_connections[user_id]:
                del self.active_connections[user_id]
        logger.info(f"User {user_id} disconnected. Total connections: {self.get_connection_count()}")

    async def send_personal_message(self, message: dict, user_id: int):
        """Send message to all connections for a specific user."""
        if user_id not in self.active_connections:
            return

        message_str = json.dumps(message)
        disconnected = set()

        for connection in self.active_connections[user_id]:
            try:
                await connection.send_text(message_str)
            except Exception as e:
                logger.error(f"Error sending to user {user_id}: {e}")
                disconnected.add(connection)

        # Clean up disconnected clients
        for connection in disconnected:
            self.disconnect(connection, user_id)

    async def publish_position_update(self, user_id: int, trading_account_id: int, data: dict):
        """Publish position update to Redis (for multi-server support)."""
        if not self.redis_client:
            return

        message = {
            "user_id": user_id,
            "trading_account_id": trading_account_id,
            "data": data,
        }

        await self.redis_client.publish("positions:updates", json.dumps(message))

    async def _listen_redis(self):
        """Listen for Redis pub/sub messages and forward to WebSocket clients."""
        if not self.pubsub:
            return

        try:
            async for message in self.pubsub.listen():
                if message["type"] == "message":
                    try:
                        data = json.loads(message["data"])
                        user_id = data["user_id"]
                        await self.send_personal_message(data, user_id)
                    except Exception as e:
                        logger.error(f"Error processing Redis message: {e}")
        except asyncio.CancelledError:
            logger.info("Redis listener cancelled")
        except Exception as e:
            logger.error(f"Redis listener error: {e}")

    def get_connection_count(self) -> int:
        """Get total number of active connections."""
        return sum(len(conns) for conns in self.active_connections.values())
