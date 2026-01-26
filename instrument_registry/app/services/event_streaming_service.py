#!/usr/bin/env python3
"""
Event Streaming Service

Production-ready event streaming infrastructure with config service integration,
ordering guarantees, retry policies, and dead letter queue support.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, asdict
from enum import Enum

import redis.asyncio as redis
from prometheus_client import Counter, Histogram, Gauge

logger = logging.getLogger(__name__)


class OrderingGuarantee(Enum):
    NONE = "none"
    PARTITION = "partition" 
    GLOBAL = "global"


class EventStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DLQ = "dlq"


@dataclass
class StreamEvent:
    """Event structure for streaming"""
    event_id: str
    event_type: str
    payload: Dict[str, Any]
    partition_key: Optional[str] = None
    timestamp: Optional[datetime] = None
    retry_count: int = 0
    max_retries: int = 3
    status: EventStatus = EventStatus.PENDING
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for Redis storage"""
        data = asdict(self)
        data["timestamp"] = self.timestamp.isoformat()
        data["status"] = self.status.value
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StreamEvent':
        """Create from dictionary from Redis"""
        data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        data["status"] = EventStatus(data["status"])
        return cls(**data)


class EventStreamingService:
    """Production-ready event streaming service with config integration"""
    
    def __init__(self, config_client=None, monitoring_service=None):
        """
        Initialize event streaming service
        
        Args:
            config_client: ConfigClient instance for retrieving configuration
            monitoring_service: MonitoringService for metrics
        """
        self.config_client = config_client
        self.monitoring_service = monitoring_service
        self.redis_client: Optional[redis.Redis] = None
        
        # Config-driven settings (loaded from config service)
        self.broker_url: Optional[str] = None
        self.retry_attempts: int = 3
        self.batch_size: int = 100
        self.ordering_guarantee: OrderingGuarantee = OrderingGuarantee.PARTITION
        self.dlq_retention_hours: int = 72
        
        # Runtime state
        self._consumer_tasks: Dict[str, asyncio.Task] = {}
        self._running = False
        
    async def initialize(self) -> bool:
        """Initialize service with config-driven parameters"""
        try:
            # Load configuration from config service
            await self._load_configuration()
            
            # Initialize Redis connection
            await self._initialize_broker()
            
            # Set up monitoring metrics
            self._setup_metrics()
            
            self._running = True
            logger.info("Event streaming service initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize event streaming service: {e}")
            return False
    
    async def _load_configuration(self):
        """Load configuration from config service"""
        if not self.config_client:
            logger.warning("No config client provided, using defaults")
            self.broker_url = "redis://localhost:6379/0"
            return
        
        try:
            # Load event streaming configuration
            self.broker_url = self.config_client.get(
                'INSTRUMENT_REGISTRY_EVENT_BROKER_URL', 
                default='redis://localhost:6379/0'
            )
            
            self.retry_attempts = self.config_client.get_int(
                'INSTRUMENT_REGISTRY_EVENT_RETRY_ATTEMPTS',
                default=3
            )
            
            self.batch_size = self.config_client.get_int(
                'INSTRUMENT_REGISTRY_EVENT_BATCH_SIZE',
                default=100
            )
            
            ordering_str = self.config_client.get(
                'INSTRUMENT_REGISTRY_EVENT_ORDERING_GUARANTEE',
                default='partition'
            )
            self.ordering_guarantee = OrderingGuarantee(ordering_str)
            
            self.dlq_retention_hours = self.config_client.get_int(
                'INSTRUMENT_REGISTRY_DLQ_RETENTION_HOURS',
                default=72
            )
            
            logger.info(f"Loaded event streaming config: broker={self.broker_url[:20]}..., "
                       f"retries={self.retry_attempts}, batch_size={self.batch_size}, "
                       f"ordering={self.ordering_guarantee.value}, dlq_retention={self.dlq_retention_hours}h")
                       
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    async def _initialize_broker(self):
        """Initialize Redis broker connection"""
        if not self.broker_url:
            raise ValueError("Broker URL not configured")
        
        try:
            self.redis_client = redis.from_url(
                self.broker_url,
                encoding='utf-8',
                decode_responses=True,
                retry_on_timeout=True,
                socket_connect_timeout=10,
                socket_timeout=30
            )
            
            # Test connection
            await self.redis_client.ping()
            logger.info(f"Connected to event broker: {self.broker_url[:30]}...")
            
        except Exception as e:
            logger.error(f"Failed to connect to event broker: {e}")
            raise
    
    def _setup_metrics(self):
        """Set up Prometheus metrics for event streaming"""
        self.events_published_total = Counter(
            'instrument_registry_events_published_total',
            'Total events published to stream',
            ['event_type', 'partition', 'status']
        )
        
        self.events_consumed_total = Counter(
            'instrument_registry_events_consumed_total', 
            'Total events consumed from stream',
            ['event_type', 'status']
        )
        
        self.event_processing_duration_seconds = Histogram(
            'instrument_registry_event_processing_duration_seconds',
            'Event processing duration in seconds',
            ['event_type'],
            buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]
        )
        
        self.events_in_dlq_total = Gauge(
            'instrument_registry_events_in_dlq_total',
            'Current number of events in dead letter queue'
        )
        
        self.event_retry_attempts_total = Counter(
            'instrument_registry_event_retry_attempts_total',
            'Total event retry attempts',
            ['event_type', 'retry_count']
        )
    
    async def publish_event(self, event: StreamEvent) -> bool:
        """
        Publish event to stream with ordering guarantees
        
        Args:
            event: Event to publish
            
        Returns:
            bool: True if published successfully
        """
        if not self._running:
            logger.error("Event streaming service not running")
            return False
        
        start_time = time.time()
        
        try:
            # Set up event metadata
            event.max_retries = self.retry_attempts
            
            # Determine stream key based on ordering guarantee
            stream_key = self._get_stream_key(event)
            
            # Serialize event
            event_data = event.to_dict()
            
            # Publish to Redis Stream
            await self.redis_client.xadd(
                stream_key,
                event_data,
                maxlen=10000,  # Prevent unbounded growth
                approximate=True
            )
            
            # Update metrics
            duration = time.time() - start_time
            partition = event.partition_key or "default"
            
            self.events_published_total.labels(
                event_type=event.event_type,
                partition=partition,
                status="success"
            ).inc()
            
            self.event_processing_duration_seconds.labels(
                event_type=event.event_type
            ).observe(duration)
            
            # Record monitoring metrics
            if self.monitoring_service:
                self.monitoring_service.record_operation_duration(
                    f"event_publish_{event.event_type}", duration
                )
            
            logger.debug(f"Published event {event.event_id} to {stream_key}")
            return True
            
        except Exception as e:
            self.events_published_total.labels(
                event_type=event.event_type,
                partition=event.partition_key or "default",
                status="error"
            ).inc()
            
            logger.error(f"Failed to publish event {event.event_id}: {e}")
            return False
    
    def _get_stream_key(self, event: StreamEvent) -> str:
        """
        Get Redis stream key based on ordering guarantee
        
        Args:
            event: Event to determine stream for
            
        Returns:
            str: Redis stream key
        """
        base_key = f"instrument_registry:events:{event.event_type}"
        
        if self.ordering_guarantee == OrderingGuarantee.NONE:
            # Round-robin across multiple streams for parallel processing
            import hashlib
            hash_val = int(hashlib.md5(event.event_id.encode()).hexdigest(), 16)
            return f"{base_key}:{hash_val % 10}"
        
        elif self.ordering_guarantee == OrderingGuarantee.PARTITION:
            # Partition by key to maintain ordering within partition
            if event.partition_key:
                return f"{base_key}:partition:{event.partition_key}"
            else:
                return f"{base_key}:partition:default"
        
        else:  # GLOBAL
            # Single stream for global ordering
            return base_key
    
    async def start_consumer(self, event_type: str, handler_func: callable) -> bool:
        """
        Start event consumer for specific event type
        
        Args:
            event_type: Type of events to consume
            handler_func: Async function to handle events
            
        Returns:
            bool: True if consumer started successfully
        """
        if not self._running:
            logger.error("Event streaming service not running")
            return False
        
        consumer_id = f"consumer_{event_type}_{int(time.time())}"
        
        try:
            # Create consumer task
            task = asyncio.create_task(
                self._consumer_loop(event_type, handler_func, consumer_id)
            )
            
            self._consumer_tasks[consumer_id] = task
            
            logger.info(f"Started consumer {consumer_id} for event type {event_type}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start consumer for {event_type}: {e}")
            return False
    
    async def _consumer_loop(self, event_type: str, handler_func: callable, consumer_id: str):
        """Main consumer loop with retry logic and DLQ handling"""
        consumer_group = f"instrument_registry_{event_type}_group"
        stream_pattern = f"instrument_registry:events:{event_type}*"
        
        try:
            # Create consumer group if doesn't exist
            await self._create_consumer_group(stream_pattern, consumer_group)
            
            while self._running:
                try:
                    # Read from stream
                    messages = await self.redis_client.xreadgroup(
                        consumer_group,
                        consumer_id,
                        {stream_pattern: '>'},
                        count=self.batch_size,
                        block=1000  # 1 second timeout
                    )
                    
                    if not messages:
                        continue
                    
                    # Process messages
                    for stream_name, stream_messages in messages:
                        for message_id, fields in stream_messages:
                            await self._process_message(
                                stream_name, message_id, fields, 
                                handler_func, consumer_group, event_type
                            )
                
                except Exception as e:
                    logger.error(f"Consumer loop error for {consumer_id}: {e}")
                    await asyncio.sleep(5)  # Back off on errors
                    
        except Exception as e:
            logger.error(f"Fatal consumer error for {consumer_id}: {e}")
        finally:
            logger.info(f"Consumer {consumer_id} stopped")
    
    async def _create_consumer_group(self, stream_pattern: str, group_name: str):
        """Create Redis consumer group if it doesn't exist"""
        try:
            # Get all streams matching pattern
            streams = await self.redis_client.keys(stream_pattern)
            
            for stream in streams:
                try:
                    await self.redis_client.xgroup_create(
                        stream, group_name, id='0', mkstream=True
                    )
                except redis.RedisError as e:
                    if "BUSYGROUP" not in str(e):
                        raise
                        
        except Exception as e:
            logger.error(f"Failed to create consumer group {group_name}: {e}")
    
    async def _process_message(self, stream_name: str, message_id: str, 
                             fields: Dict, handler_func: callable, 
                             consumer_group: str, event_type: str):
        """Process individual message with retry and DLQ logic"""
        start_time = time.time()
        
        try:
            # Reconstruct event
            event = StreamEvent.from_dict(fields)
            event.status = EventStatus.PROCESSING
            
            # Call handler function
            success = await handler_func(event)
            
            if success:
                # Acknowledge successful processing
                await self.redis_client.xack(stream_name, consumer_group, message_id)
                
                event.status = EventStatus.COMPLETED
                duration = time.time() - start_time
                
                self.events_consumed_total.labels(
                    event_type=event_type,
                    status="success"
                ).inc()
                
                self.event_processing_duration_seconds.labels(
                    event_type=event_type
                ).observe(duration)
                
                logger.debug(f"Successfully processed event {event.event_id}")
                
            else:
                # Handle failure with retry logic
                await self._handle_processing_failure(
                    stream_name, message_id, event, consumer_group, event_type
                )
                
        except Exception as e:
            logger.error(f"Error processing message {message_id}: {e}")
            
            # Attempt to handle as processing failure
            try:
                event = StreamEvent.from_dict(fields)
                await self._handle_processing_failure(
                    stream_name, message_id, event, consumer_group, event_type
                )
            except:
                logger.error(f"Failed to handle processing failure for {message_id}")
    
    async def _handle_processing_failure(self, stream_name: str, message_id: str,
                                       event: StreamEvent, consumer_group: str,
                                       event_type: str):
        """Handle processing failure with retry logic and DLQ"""
        event.retry_count += 1
        
        self.event_retry_attempts_total.labels(
            event_type=event_type,
            retry_count=str(event.retry_count)
        ).inc()
        
        if event.retry_count >= event.max_retries:
            # Send to Dead Letter Queue
            await self._send_to_dlq(event, stream_name, message_id)
            
            # Acknowledge to remove from main stream
            await self.redis_client.xack(stream_name, consumer_group, message_id)
            
            self.events_consumed_total.labels(
                event_type=event_type,
                status="dlq"
            ).inc()
            
            logger.warning(f"Event {event.event_id} sent to DLQ after {event.retry_count} retries")
            
        else:
            # Requeue for retry (by not acknowledging)
            self.events_consumed_total.labels(
                event_type=event_type,
                status="retry"
            ).inc()
            
            logger.info(f"Event {event.event_id} scheduled for retry {event.retry_count}/{event.max_retries}")
    
    async def _send_to_dlq(self, event: StreamEvent, original_stream: str, message_id: str):
        """Send failed event to Dead Letter Queue"""
        dlq_key = f"instrument_registry:dlq:{event.event_type}"
        
        # Add DLQ metadata
        dlq_data = event.to_dict()
        dlq_data.update({
            "original_stream": original_stream,
            "original_message_id": message_id,
            "dlq_timestamp": datetime.utcnow().isoformat(),
            "status": EventStatus.DLQ.value
        })
        
        try:
            await self.redis_client.xadd(dlq_key, dlq_data)
            
            # Set expiration based on retention policy
            expire_seconds = self.dlq_retention_hours * 3600
            await self.redis_client.expire(dlq_key, expire_seconds)
            
            # Update DLQ metrics
            self.events_in_dlq_total.inc()
            
            logger.info(f"Event {event.event_id} added to DLQ with {self.dlq_retention_hours}h retention")
            
        except Exception as e:
            logger.error(f"Failed to send event {event.event_id} to DLQ: {e}")
    
    async def get_dlq_events(self, event_type: str, limit: int = 100) -> List[Dict]:
        """Retrieve events from Dead Letter Queue for manual processing"""
        dlq_key = f"instrument_registry:dlq:{event_type}"
        
        try:
            messages = await self.redis_client.xread({dlq_key: '0'}, count=limit)
            
            if not messages:
                return []
            
            events = []
            for stream_name, stream_messages in messages:
                for message_id, fields in stream_messages:
                    events.append({
                        "message_id": message_id,
                        "data": fields
                    })
            
            return events
            
        except Exception as e:
            logger.error(f"Failed to retrieve DLQ events: {e}")
            return []
    
    async def reprocess_dlq_event(self, event_type: str, message_id: str) -> bool:
        """Reprocess event from DLQ back to main stream"""
        dlq_key = f"instrument_registry:dlq:{event_type}"
        
        try:
            # Get event from DLQ
            messages = await self.redis_client.xread({dlq_key: message_id}, count=1)
            
            if not messages or not messages[0][1]:
                logger.error(f"Event {message_id} not found in DLQ")
                return False
            
            # Extract event data
            event_data = messages[0][1][0][1]
            
            # Reset retry count and status
            event_data["retry_count"] = "0"
            event_data["status"] = EventStatus.PENDING.value
            
            # Republish to main stream
            stream_key = f"instrument_registry:events:{event_type}"
            await self.redis_client.xadd(stream_key, event_data)
            
            # Remove from DLQ
            await self.redis_client.xdel(dlq_key, message_id)
            
            # Update metrics
            self.events_in_dlq_total.dec()
            
            logger.info(f"Event {message_id} reprocessed from DLQ")
            return True
            
        except Exception as e:
            logger.error(f"Failed to reprocess DLQ event {message_id}: {e}")
            return False
    
    async def shutdown(self):
        """Gracefully shutdown event streaming service"""
        logger.info("Shutting down event streaming service...")
        
        self._running = False
        
        # Cancel consumer tasks
        for consumer_id, task in self._consumer_tasks.items():
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            logger.info(f"Stopped consumer {consumer_id}")
        
        # Close Redis connection
        if self.redis_client:
            await self.redis_client.close()
        
        logger.info("Event streaming service shutdown complete")
    
    async def health_check(self) -> Dict[str, Any]:
        """Health check for event streaming service"""
        health = {
            "service": "event_streaming",
            "status": "healthy" if self._running else "stopped",
            "timestamp": datetime.utcnow().isoformat(),
            "configuration": {
                "broker_url": self.broker_url[:20] + "..." if self.broker_url else None,
                "retry_attempts": self.retry_attempts,
                "batch_size": self.batch_size,
                "ordering_guarantee": self.ordering_guarantee.value,
                "dlq_retention_hours": self.dlq_retention_hours
            },
            "active_consumers": len(self._consumer_tasks),
            "broker_connected": False
        }
        
        # Test broker connectivity
        if self.redis_client:
            try:
                await self.redis_client.ping()
                health["broker_connected"] = True
            except:
                health["status"] = "degraded"
                health["error"] = "Broker connection failed"
        
        return health