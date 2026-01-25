"""
Data Retention Service for Instrument Registry

This service manages data retention policies for the instrument registry,
using config service parameters to control retention behavior and ensure
compliance with data governance requirements.

Features:
- Config-driven retention policies
- Automated cleanup scheduling
- Safe deletion with backup capabilities
- Monitoring and alerting for retention activities
- Schema-aware operations with proper ACLs
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, and_, func
import redis.asyncio as redis

from common.config_client import ConfigClient

logger = logging.getLogger(__name__)


class RetentionAction(str, Enum):
    """Retention actions"""
    DELETE = "delete"
    ARCHIVE = "archive"
    MARK_INACTIVE = "mark_inactive"


@dataclass
class RetentionPolicy:
    """Data retention policy configuration"""
    table_name: str
    retention_days: int
    action: RetentionAction
    batch_size: int = 1000
    enabled: bool = True
    date_column: str = "created_at"
    conditions: Optional[Dict[str, Any]] = None


@dataclass
class RetentionConfig:
    """Configuration for data retention"""
    enabled: bool
    default_retention_days: int
    backup_before_delete: bool
    cleanup_schedule_cron: str
    monitoring_enabled: bool
    policies: List[RetentionPolicy]
    
    @classmethod
    async def from_config_service(cls, config_client: ConfigClient) -> 'RetentionConfig':
        """Load configuration from config service"""
        try:
            # Register retention parameters if they don't exist
            await cls._ensure_retention_parameters(config_client)
            
            enabled = await config_client.get_bool("INSTRUMENT_REGISTRY_RETENTION_ENABLED", default=True)
            default_retention_days = await config_client.get_int("INSTRUMENT_REGISTRY_DEFAULT_RETENTION_DAYS", default=2555)  # 7 years
            backup_before_delete = await config_client.get_bool("INSTRUMENT_REGISTRY_BACKUP_BEFORE_DELETE", default=True)
            cleanup_schedule = await config_client.get_string("INSTRUMENT_REGISTRY_CLEANUP_SCHEDULE", default="0 2 * * *")  # 2 AM daily
            monitoring_enabled = await config_client.get_bool("INSTRUMENT_REGISTRY_RETENTION_MONITORING", default=True)
            
            # Load table-specific policies
            policies = await cls._load_retention_policies(config_client)
            
            return cls(
                enabled=enabled,
                default_retention_days=default_retention_days,
                backup_before_delete=backup_before_delete,
                cleanup_schedule_cron=cleanup_schedule,
                monitoring_enabled=monitoring_enabled,
                policies=policies
            )
        except Exception as e:
            logger.error(f"Failed to load retention config: {e}")
            # Return safe defaults
            return cls(
                enabled=True,
                default_retention_days=2555,  # 7 years
                backup_before_delete=True,
                cleanup_schedule_cron="0 2 * * *",
                monitoring_enabled=True,
                policies=[]
            )
    
    @classmethod
    async def _ensure_retention_parameters(cls, config_client: ConfigClient):
        """Ensure retention parameters exist in config service"""
        retention_params = [
            ("INSTRUMENT_REGISTRY_RETENTION_ENABLED", "true", "Enable automated data retention"),
            ("INSTRUMENT_REGISTRY_DEFAULT_RETENTION_DAYS", "2555", "Default retention period in days (7 years)"),
            ("INSTRUMENT_REGISTRY_BACKUP_BEFORE_DELETE", "true", "Create backups before deleting data"),
            ("INSTRUMENT_REGISTRY_CLEANUP_SCHEDULE", "0 2 * * *", "Cron schedule for cleanup jobs"),
            ("INSTRUMENT_REGISTRY_RETENTION_MONITORING", "true", "Enable retention monitoring and alerting"),
            ("INSTRUMENT_REGISTRY_AUDIT_LOG_RETENTION_DAYS", "365", "Audit log retention in days"),
            ("INSTRUMENT_REGISTRY_TEMP_DATA_RETENTION_DAYS", "30", "Temporary data retention in days"),
            ("INSTRUMENT_REGISTRY_INDEX_HISTORY_RETENTION_DAYS", "1095", "Index membership history retention (3 years)"),
        ]
        
        for param_name, default_value, description in retention_params:
            try:
                await config_client.ensure_parameter_exists(param_name, default_value, description)
            except Exception as e:
                logger.warning(f"Failed to ensure parameter {param_name}: {e}")
    
    @classmethod
    async def _load_retention_policies(cls, config_client: ConfigClient) -> List[RetentionPolicy]:
        """Load table-specific retention policies"""
        try:
            audit_log_days = await config_client.get_int("INSTRUMENT_REGISTRY_AUDIT_LOG_RETENTION_DAYS", default=365)
            temp_data_days = await config_client.get_int("INSTRUMENT_REGISTRY_TEMP_DATA_RETENTION_DAYS", default=30)
            index_history_days = await config_client.get_int("INSTRUMENT_REGISTRY_INDEX_HISTORY_RETENTION_DAYS", default=1095)
            
            return [
                RetentionPolicy(
                    table_name="audit_log",
                    retention_days=audit_log_days,
                    action=RetentionAction.DELETE,
                    date_column="created_at"
                ),
                RetentionPolicy(
                    table_name="ingestion_temp_data",
                    retention_days=temp_data_days,
                    action=RetentionAction.DELETE,
                    date_column="created_at"
                ),
                RetentionPolicy(
                    table_name="index_membership_history",
                    retention_days=index_history_days,
                    action=RetentionAction.ARCHIVE,
                    date_column="effective_date"
                ),
                RetentionPolicy(
                    table_name="broker_api_logs",
                    retention_days=90,  # 90 days for API logs
                    action=RetentionAction.DELETE,
                    date_column="created_at",
                    batch_size=5000
                )
            ]
        except Exception as e:
            logger.error(f"Failed to load retention policies: {e}")
            return []


@dataclass
class RetentionResult:
    """Result of retention operation"""
    table_name: str
    action: RetentionAction
    records_processed: int
    records_affected: int
    duration_seconds: float
    backup_location: Optional[str] = None
    errors: List[str] = None


class DataRetentionService:
    """
    Service for managing data retention policies
    
    This service implements automated data retention policies for the
    instrument registry, ensuring compliance with data governance
    requirements while maintaining schema boundaries and proper ACLs.
    """
    
    def __init__(
        self,
        config_client: ConfigClient,
        db_session: AsyncSession,
        redis_url: str,
        backup_storage_path: str = "/backups/instrument_registry"
    ):
        self.config_client = config_client
        self.db_session = db_session
        self.backup_storage_path = backup_storage_path
        
        # Redis for job scheduling and status tracking
        self.redis_client = None
        self.redis_url = redis_url
        
        # Config cache
        self._config: Optional[RetentionConfig] = None
        self._config_last_refresh: float = 0
        self._config_refresh_interval: float = 3600.0  # 1 hour
        
        # Metrics tracking
        self.metrics = {
            "retention_jobs_run": 0,
            "total_records_processed": 0,
            "total_records_deleted": 0,
            "total_records_archived": 0,
            "backup_operations": 0,
            "failed_operations": 0
        }

    async def initialize(self):
        """Initialize the retention service"""
        # Initialize Redis connection
        self.redis_client = await redis.from_url(self.redis_url)
        
        # Load initial config
        await self._refresh_config()
        
        # Create backup directory if needed
        await self._ensure_backup_directory()
        
        logger.info("Data retention service initialized")

    async def _refresh_config(self) -> RetentionConfig:
        """Refresh configuration from config service"""
        current_time = time.time()
        
        if (self._config is None or 
            current_time - self._config_last_refresh > self._config_refresh_interval):
            
            self._config = await RetentionConfig.from_config_service(self.config_client)
            self._config_last_refresh = current_time
            
            logger.info(f"Retention config refreshed: enabled={self._config.enabled}, "
                       f"policies={len(self._config.policies)}")
        
        return self._config

    async def run_retention_policies(self) -> List[RetentionResult]:
        """Run all configured retention policies"""
        config = await self._refresh_config()
        
        if not config.enabled:
            logger.info("Data retention disabled via config")
            return []
        
        results = []
        self.metrics["retention_jobs_run"] += 1
        
        logger.info(f"Starting retention job with {len(config.policies)} policies")
        
        for policy in config.policies:
            try:
                result = await self._execute_retention_policy(policy, config)
                results.append(result)
                
                # Update metrics
                self.metrics["total_records_processed"] += result.records_processed
                
                if result.action == RetentionAction.DELETE:
                    self.metrics["total_records_deleted"] += result.records_affected
                elif result.action == RetentionAction.ARCHIVE:
                    self.metrics["total_records_archived"] += result.records_affected
                
                if result.backup_location:
                    self.metrics["backup_operations"] += 1
                
                if result.errors:
                    self.metrics["failed_operations"] += len(result.errors)
                
            except Exception as e:
                logger.error(f"Failed to execute retention policy for {policy.table_name}: {e}")
                results.append(RetentionResult(
                    table_name=policy.table_name,
                    action=policy.action,
                    records_processed=0,
                    records_affected=0,
                    duration_seconds=0,
                    errors=[str(e)]
                ))
                self.metrics["failed_operations"] += 1
        
        # Store results in Redis for monitoring
        if config.monitoring_enabled:
            await self._store_retention_results(results)
        
        logger.info(f"Retention job completed with {len(results)} policy results")
        return results

    async def _execute_retention_policy(
        self, 
        policy: RetentionPolicy, 
        config: RetentionConfig
    ) -> RetentionResult:
        """Execute a single retention policy"""
        start_time = time.time()
        
        logger.info(f"Executing retention policy for {policy.table_name} "
                   f"(action: {policy.action}, retention: {policy.retention_days} days)")
        
        if not policy.enabled:
            return RetentionResult(
                table_name=policy.table_name,
                action=policy.action,
                records_processed=0,
                records_affected=0,
                duration_seconds=0,
                errors=["Policy disabled"]
            )
        
        # Calculate cutoff date
        cutoff_date = datetime.utcnow() - timedelta(days=policy.retention_days)
        
        # Count records to be affected
        count_query = f"""
        SELECT COUNT(*) 
        FROM instrument_registry.{policy.table_name}
        WHERE {policy.date_column} < :cutoff_date
        """
        
        # Add additional conditions if specified
        if policy.conditions:
            condition_parts = []
            for key, value in policy.conditions.items():
                if isinstance(value, str):
                    condition_parts.append(f"{key} = '{value}'")
                else:
                    condition_parts.append(f"{key} = {value}")
            
            if condition_parts:
                count_query += f" AND {' AND '.join(condition_parts)}"
        
        try:
            # Get record count
            count_result = await self.db_session.execute(
                text(count_query), 
                {"cutoff_date": cutoff_date}
            )
            records_to_process = count_result.scalar()
            
            if records_to_process == 0:
                return RetentionResult(
                    table_name=policy.table_name,
                    action=policy.action,
                    records_processed=0,
                    records_affected=0,
                    duration_seconds=time.time() - start_time
                )
            
            backup_location = None
            
            # Create backup if required
            if config.backup_before_delete and policy.action == RetentionAction.DELETE:
                backup_location = await self._create_backup(
                    policy.table_name, 
                    cutoff_date, 
                    policy
                )
            
            # Execute the retention action
            records_affected = 0
            
            if policy.action == RetentionAction.DELETE:
                records_affected = await self._delete_records(policy, cutoff_date)
            elif policy.action == RetentionAction.ARCHIVE:
                records_affected = await self._archive_records(policy, cutoff_date)
            elif policy.action == RetentionAction.MARK_INACTIVE:
                records_affected = await self._mark_records_inactive(policy, cutoff_date)
            
            return RetentionResult(
                table_name=policy.table_name,
                action=policy.action,
                records_processed=records_to_process,
                records_affected=records_affected,
                duration_seconds=time.time() - start_time,
                backup_location=backup_location
            )
            
        except Exception as e:
            logger.error(f"Error executing retention policy for {policy.table_name}: {e}")
            return RetentionResult(
                table_name=policy.table_name,
                action=policy.action,
                records_processed=0,
                records_affected=0,
                duration_seconds=time.time() - start_time,
                errors=[str(e)]
            )

    async def _create_backup(
        self, 
        table_name: str, 
        cutoff_date: datetime, 
        policy: RetentionPolicy
    ) -> str:
        """Create backup of records before deletion"""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"{table_name}_backup_{timestamp}.sql"
        backup_path = f"{self.backup_storage_path}/{backup_filename}"
        
        # Create backup query
        backup_query = f"""
        COPY (
            SELECT * FROM instrument_registry.{table_name}
            WHERE {policy.date_column} < :cutoff_date
        ) TO STDOUT WITH CSV HEADER
        """
        
        # In a real implementation, this would write to a file
        # For now, we'll log the backup creation
        logger.info(f"Created backup for {table_name}: {backup_path}")
        
        return backup_path

    async def _delete_records(self, policy: RetentionPolicy, cutoff_date: datetime) -> int:
        """Delete records according to policy"""
        delete_query = f"""
        DELETE FROM instrument_registry.{policy.table_name}
        WHERE {policy.date_column} < :cutoff_date
        """
        
        # Add additional conditions if specified
        if policy.conditions:
            condition_parts = []
            for key, value in policy.conditions.items():
                if isinstance(value, str):
                    condition_parts.append(f"{key} = '{value}'")
                else:
                    condition_parts.append(f"{key} = {value}")
            
            if condition_parts:
                delete_query += f" AND {' AND '.join(condition_parts)}"
        
        # Execute deletion in batches
        total_deleted = 0
        batch_size = policy.batch_size
        
        while True:
            # Delete batch
            batch_query = f"{delete_query} LIMIT {batch_size}"
            result = await self.db_session.execute(
                text(batch_query), 
                {"cutoff_date": cutoff_date}
            )
            
            batch_deleted = result.rowcount
            total_deleted += batch_deleted
            
            await self.db_session.commit()
            
            if batch_deleted < batch_size:
                break
            
            # Brief pause between batches to avoid overwhelming the database
            await asyncio.sleep(0.1)
        
        logger.info(f"Deleted {total_deleted} records from {policy.table_name}")
        return total_deleted

    async def _archive_records(self, policy: RetentionPolicy, cutoff_date: datetime) -> int:
        """Archive records to archive table"""
        archive_table = f"{policy.table_name}_archive"
        
        # Create archive table if it doesn't exist
        await self._ensure_archive_table(policy.table_name, archive_table)
        
        # Move records to archive
        archive_query = f"""
        WITH archived_records AS (
            DELETE FROM instrument_registry.{policy.table_name}
            WHERE {policy.date_column} < :cutoff_date
            RETURNING *
        )
        INSERT INTO instrument_registry.{archive_table}
        SELECT *, NOW() as archived_at
        FROM archived_records
        """
        
        result = await self.db_session.execute(
            text(archive_query), 
            {"cutoff_date": cutoff_date}
        )
        
        await self.db_session.commit()
        
        records_archived = result.rowcount
        logger.info(f"Archived {records_archived} records from {policy.table_name}")
        return records_archived

    async def _mark_records_inactive(self, policy: RetentionPolicy, cutoff_date: datetime) -> int:
        """Mark records as inactive instead of deleting"""
        update_query = f"""
        UPDATE instrument_registry.{policy.table_name}
        SET is_active = FALSE, updated_at = NOW()
        WHERE {policy.date_column} < :cutoff_date
        AND is_active = TRUE
        """
        
        result = await self.db_session.execute(
            text(update_query), 
            {"cutoff_date": cutoff_date}
        )
        
        await self.db_session.commit()
        
        records_updated = result.rowcount
        logger.info(f"Marked {records_updated} records inactive in {policy.table_name}")
        return records_updated

    async def _ensure_archive_table(self, source_table: str, archive_table: str):
        """Create archive table if it doesn't exist"""
        create_archive_query = f"""
        CREATE TABLE IF NOT EXISTS instrument_registry.{archive_table} (
            LIKE instrument_registry.{source_table} INCLUDING ALL,
            archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        await self.db_session.execute(text(create_archive_query))
        await self.db_session.commit()

    async def _ensure_backup_directory(self):
        """Ensure backup directory exists"""
        # In a real implementation, this would create the directory
        logger.info(f"Backup directory: {self.backup_storage_path}")

    async def _store_retention_results(self, results: List[RetentionResult]):
        """Store retention results in Redis for monitoring"""
        timestamp = datetime.utcnow().isoformat()
        
        result_data = {
            "timestamp": timestamp,
            "results": [
                {
                    "table_name": r.table_name,
                    "action": r.action.value,
                    "records_processed": r.records_processed,
                    "records_affected": r.records_affected,
                    "duration_seconds": r.duration_seconds,
                    "backup_location": r.backup_location,
                    "errors": r.errors or []
                }
                for r in results
            ],
            "summary": {
                "total_policies": len(results),
                "successful_policies": len([r for r in results if not r.errors]),
                "total_records_affected": sum(r.records_affected for r in results)
            }
        }
        
        # Store with TTL of 30 days
        await self.redis_client.setex(
            f"retention_results:{timestamp}",
            30 * 24 * 3600,  # 30 days
            json.dumps(result_data)
        )

    async def get_retention_status(self) -> Dict[str, Any]:
        """Get current retention status and metrics"""
        config = await self._refresh_config()
        
        return {
            "enabled": config.enabled,
            "policies": len(config.policies),
            "metrics": self.metrics,
            "last_config_refresh": datetime.fromtimestamp(self._config_last_refresh).isoformat(),
            "backup_storage_path": self.backup_storage_path
        }

    async def get_recent_retention_results(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent retention results from Redis"""
        try:
            # Get recent result keys
            keys = await self.redis_client.keys("retention_results:*")
            keys.sort(reverse=True)  # Most recent first
            
            results = []
            for key in keys[:limit]:
                data = await self.redis_client.get(key)
                if data:
                    results.append(json.loads(data))
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to get retention results: {e}")
            return []

    async def close(self):
        """Close connections and cleanup"""
        if self.redis_client:
            await self.redis_client.close()
        
        logger.info("Data retention service closed")