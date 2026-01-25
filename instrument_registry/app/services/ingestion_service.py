"""
Real implementation of instrument ingestion service
Replaces the 501 stub with actual job queue integration
"""

import json
import uuid
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
from enum import Enum

import redis.asyncio as redis
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..models import BrokerFeed
from .kite_ingest import KiteInstrumentIngestor

logger = logging.getLogger(__name__)


class IngestionStatus(str, Enum):
    """Ingestion job status"""
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class IngestionMode(str, Enum):
    """Ingestion mode"""
    FULL_CATALOG = "FULL_CATALOG"
    INCREMENTAL = "INCREMENTAL"
    REAL_TIME = "REAL_TIME"
    ON_DEMAND = "ON_DEMAND"


class IngestionService:
    """Service for managing instrument ingestion jobs"""
    
    def __init__(self, redis_url: str, queue_name: str = "instrument_ingestion"):
        """
        Initialize ingestion service
        
        Args:
            redis_url: Redis connection URL for job queue
            queue_name: Name of the Redis queue
        """
        self.redis_url = redis_url
        self.queue_name = queue_name
        self._redis_client = None
        self._ingestors = {}
        
        # Status tracking keys
        self.status_prefix = "ingestion:status:"
        self.result_prefix = "ingestion:result:"
        self.status_ttl = 86400  # 24 hours
    
    async def initialize(self):
        """Initialize Redis connection and ingestors"""
        self._redis_client = await redis.from_url(self.redis_url)
        
        # Initialize broker-specific ingestors
        # For now, only Kite is implemented
        # TODO: Add other brokers as they're implemented
        logger.info("Initialized ingestion service")
    
    async def queue_ingestion_job(self, 
                                broker_id: str,
                                mode: IngestionMode,
                                filters: Optional[Dict[str, Any]] = None,
                                priority: int = 0) -> str:
        """
        Queue an ingestion job
        
        Args:
            broker_id: Broker identifier
            mode: Ingestion mode
            filters: Optional filters for the ingestion
            priority: Job priority (higher = more important)
            
        Returns:
            Job ID for tracking
        """
        job_id = str(uuid.uuid4())
        timestamp = datetime.utcnow()
        
        # Create job payload
        job_data = {
            "job_id": job_id,
            "broker_id": broker_id,
            "mode": mode.value,
            "filters": filters or {},
            "priority": priority,
            "created_at": timestamp.isoformat(),
            "status": IngestionStatus.QUEUED.value
        }
        
        # Store job status
        status_key = f"{self.status_prefix}{job_id}"
        await self._redis_client.setex(
            status_key,
            self.status_ttl,
            json.dumps(job_data)
        )
        
        # Queue the job
        if priority > 0:
            # Use priority queue (sorted set)
            await self._redis_client.zadd(
                f"{self.queue_name}:priority",
                {json.dumps(job_data): -priority}  # Negative for high priority first
            )
        else:
            # Use regular queue (list)
            await self._redis_client.lpush(
                self.queue_name,
                json.dumps(job_data)
            )
        
        # Publish event for workers
        await self._redis_client.publish(
            f"{self.queue_name}:events",
            json.dumps({"event": "job_queued", "job_id": job_id})
        )
        
        logger.info(f"Queued ingestion job {job_id} for broker {broker_id}")
        return job_id
    
    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get status of an ingestion job"""
        status_key = f"{self.status_prefix}{job_id}"
        status_data = await self._redis_client.get(status_key)
        
        if status_data:
            return json.loads(status_data)
        
        # Check if we have a result
        result_key = f"{self.result_prefix}{job_id}"
        result_data = await self._redis_client.get(result_key)
        
        if result_data:
            return json.loads(result_data)
        
        return None
    
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a queued job"""
        status_key = f"{self.status_prefix}{job_id}"
        status_data = await self._redis_client.get(status_key)
        
        if not status_data:
            return False
        
        job_data = json.loads(status_data)
        
        # Only cancel if queued
        if job_data["status"] != IngestionStatus.QUEUED.value:
            return False
        
        # Update status
        job_data["status"] = IngestionStatus.CANCELLED.value
        job_data["cancelled_at"] = datetime.utcnow().isoformat()
        
        await self._redis_client.setex(
            status_key,
            self.status_ttl,
            json.dumps(job_data)
        )
        
        logger.info(f"Cancelled job {job_id}")
        return True
    
    async def process_job(self, job_data: Dict[str, Any], db: AsyncSession) -> Dict[str, Any]:
        """
        Process an ingestion job (called by workers)
        
        Args:
            job_data: Job data from queue
            db: Database session
            
        Returns:
            Job result
        """
        job_id = job_data["job_id"]
        broker_id = job_data["broker_id"]
        mode = IngestionMode(job_data["mode"])
        filters = job_data.get("filters", {})
        
        logger.info(f"Processing ingestion job {job_id} for broker {broker_id}")
        
        # Update status to running
        status_key = f"{self.status_prefix}{job_id}"
        job_data["status"] = IngestionStatus.RUNNING.value
        job_data["started_at"] = datetime.utcnow().isoformat()
        
        await self._redis_client.setex(
            status_key,
            self.status_ttl,
            json.dumps(job_data)
        )
        
        try:
            # Get broker configuration
            result = await db.execute(
                select(BrokerFeed).where(BrokerFeed.broker_id == broker_id)
            )
            broker = result.scalar_one_or_none()
            
            if not broker:
                raise ValueError(f"Broker {broker_id} not found")
            
            # Get appropriate ingestor
            ingestor = await self._get_ingestor(broker_id, broker, db)
            
            # Perform ingestion based on mode
            if mode == IngestionMode.FULL_CATALOG:
                stats = await ingestor.ingest_full_catalog()
            elif mode == IngestionMode.INCREMENTAL:
                stats = await ingestor.ingest_incremental()
            elif mode == IngestionMode.ON_DEMAND:
                # Apply filters for on-demand
                symbols = filters.get("symbols", [])
                exchanges = filters.get("exchanges", [])
                stats = await ingestor.ingest_filtered(symbols=symbols, exchanges=exchanges)
            else:
                raise ValueError(f"Unsupported ingestion mode: {mode}")
            
            # Update broker last sync
            broker.last_successful_sync = datetime.utcnow()
            await db.commit()
            
            # Create success result
            result = {
                "job_id": job_id,
                "broker_id": broker_id,
                "status": IngestionStatus.COMPLETED.value,
                "completed_at": datetime.utcnow().isoformat(),
                "duration_seconds": (
                    datetime.utcnow() - datetime.fromisoformat(job_data["started_at"])
                ).total_seconds(),
                "stats": stats
            }
            
        except Exception as e:
            logger.error(f"Ingestion job {job_id} failed: {e}")
            
            # Create failure result
            result = {
                "job_id": job_id,
                "broker_id": broker_id,
                "status": IngestionStatus.FAILED.value,
                "failed_at": datetime.utcnow().isoformat(),
                "error": str(e),
                "error_type": type(e).__name__
            }
        
        # Store result
        result_key = f"{self.result_prefix}{job_id}"
        await self._redis_client.setex(
            result_key,
            self.status_ttl,
            json.dumps(result)
        )
        
        # Remove status key
        await self._redis_client.delete(status_key)
        
        # Publish completion event
        await self._redis_client.publish(
            f"{self.queue_name}:events",
            json.dumps({
                "event": "job_completed",
                "job_id": job_id,
                "status": result["status"]
            })
        )
        
        return result
    
    async def _get_ingestor(self, broker_id: str, broker: BrokerFeed, db: AsyncSession):
        """Get or create ingestor for a broker"""
        if broker_id not in self._ingestors:
            if broker_id == "kite":
                # Use existing Kite ingestor
                self._ingestors[broker_id] = KiteInstrumentIngestor(db)
            else:
                raise ValueError(f"No ingestor implemented for broker: {broker_id}")
        
        return self._ingestors[broker_id]
    
    async def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue statistics"""
        # Regular queue length
        queue_length = await self._redis_client.llen(self.queue_name)
        
        # Priority queue length
        priority_length = await self._redis_client.zcard(f"{self.queue_name}:priority")
        
        # Count jobs by status
        status_keys = await self._redis_client.keys(f"{self.status_prefix}*")
        running_count = 0
        queued_count = 0
        
        if status_keys:
            # Get all statuses in batch
            statuses = await self._redis_client.mget(status_keys)
            for status_data in statuses:
                if status_data:
                    job = json.loads(status_data)
                    if job["status"] == IngestionStatus.RUNNING.value:
                        running_count += 1
                    elif job["status"] == IngestionStatus.QUEUED.value:
                        queued_count += 1
        
        return {
            "queue_length": queue_length,
            "priority_queue_length": priority_length,
            "total_pending": queue_length + priority_length,
            "running_jobs": running_count,
            "queued_jobs": queued_count,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    async def cleanup_old_jobs(self, older_than_hours: int = 24):
        """Clean up old job results"""
        # This is handled by Redis TTL, but we can force cleanup if needed
        cutoff_time = datetime.utcnow().timestamp() - (older_than_hours * 3600)
        
        # Get all result keys
        result_keys = await self._redis_client.keys(f"{self.result_prefix}*")
        cleaned = 0
        
        for key in result_keys:
            data = await self._redis_client.get(key)
            if data:
                job = json.loads(data)
                # Check if job is old enough
                if "completed_at" in job:
                    completed = datetime.fromisoformat(job["completed_at"]).timestamp()
                    if completed < cutoff_time:
                        await self._redis_client.delete(key)
                        cleaned += 1
        
        logger.info(f"Cleaned up {cleaned} old job results")
        return cleaned
    
    async def close(self):
        """Close Redis connection"""
        if self._redis_client:
            await self._redis_client.close()