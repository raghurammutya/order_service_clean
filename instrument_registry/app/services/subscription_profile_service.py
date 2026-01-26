"""
Subscription Profile Service

Implements subscription profile management with production compliance:
- Config-driven validation and limits
- Comprehensive audit logging
- Conflict resolution strategies
- Lifecycle management with retention policies
"""

import logging
import asyncio
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass
from enum import Enum

import asyncpg
from fastapi import HTTPException

from common.config_client import ConfigClient
from app.services.monitoring_service import MonitoringService

logger = logging.getLogger(__name__)


class SubscriptionType(str, Enum):
    """Subscription types"""
    LIVE_FEED = "live_feed"
    HISTORICAL = "historical"
    ALERTS = "alerts"


class ConflictType(str, Enum):
    """Conflict types"""
    LIMIT_EXCEEDED = "limit_exceeded"
    DUPLICATE_SUBSCRIPTION = "duplicate_subscription"
    INVALID_INSTRUMENT = "invalid_instrument"


class AuditAction(str, Enum):
    """Audit actions"""
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    ACTIVATE = "activate"
    DEACTIVATE = "deactivate"


@dataclass
class SubscriptionProfile:
    """Subscription profile data model"""
    profile_id: str
    user_id: str
    profile_name: str
    subscription_type: SubscriptionType
    instruments: List[str]
    preferences: Optional[Dict[str, Any]] = None
    validation_rules: Optional[Dict[str, Any]] = None
    max_instruments: Optional[int] = None
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None


@dataclass
class SubscriptionConflict:
    """Subscription conflict data model"""
    conflict_id: str
    profile_id: str
    user_id: str
    conflict_type: ConflictType
    conflict_data: Dict[str, Any]
    resolution_strategy: Optional[str] = None
    status: str = "pending"
    resolution_data: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None


class SubscriptionProfileService:
    """Production-ready subscription profile management service"""
    
    def __init__(self, database_url: str, config_client: ConfigClient, monitoring: MonitoringService):
        self.database_url = database_url
        self.config_client = config_client
        self.monitoring = monitoring
        self._connection_pool = None
        
    async def _get_connection(self) -> asyncpg.Connection:
        """Get database connection from pool"""
        if self._connection_pool is None:
            self._connection_pool = await asyncpg.create_pool(
                self.database_url,
                min_size=2,
                max_size=10,
                command_timeout=30
            )
        return await self._connection_pool.acquire()
    
    async def _get_config_values(self) -> Dict[str, Any]:
        """Get all subscription-related config values from config service"""
        try:
            config_keys = [
                'INSTRUMENT_REGISTRY_SUBSCRIPTION_TIMEOUT',
                'INSTRUMENT_REGISTRY_MAX_SUBSCRIPTIONS_PER_USER', 
                'INSTRUMENT_REGISTRY_PROFILE_VALIDATION_STRICT',
                'INSTRUMENT_REGISTRY_AUDIT_RETENTION_DAYS',
                'INSTRUMENT_REGISTRY_CONFLICT_RESOLUTION_STRATEGY'
            ]
            
            config_values = {}
            for key in config_keys:
                try:
                    value = await self.config_client.get_secret(key, environment='prod')
                    # Convert string values to appropriate types
                    if 'TIMEOUT' in key or 'MAX_' in key or 'DAYS' in key:
                        config_values[key] = int(value)
                    elif 'STRICT' in key:
                        config_values[key] = value.lower() == 'true'
                    else:
                        config_values[key] = value
                except Exception as e:
                    logger.warning(f"Failed to get config {key}: {e}")
                    # Set sensible defaults
                    if 'TIMEOUT' in key:
                        config_values[key] = 30
                    elif 'MAX_SUBSCRIPTIONS' in key:
                        config_values[key] = 100
                    elif 'STRICT' in key:
                        config_values[key] = True
                    elif 'RETENTION_DAYS' in key:
                        config_values[key] = 365
                    elif 'STRATEGY' in key:
                        config_values[key] = 'latest_wins'
            
            return config_values
            
        except Exception as e:
            logger.error(f"Failed to get config values: {e}")
            # Return sensible defaults
            return {
                'INSTRUMENT_REGISTRY_SUBSCRIPTION_TIMEOUT': 30,
                'INSTRUMENT_REGISTRY_MAX_SUBSCRIPTIONS_PER_USER': 100,
                'INSTRUMENT_REGISTRY_PROFILE_VALIDATION_STRICT': True,
                'INSTRUMENT_REGISTRY_AUDIT_RETENTION_DAYS': 365,
                'INSTRUMENT_REGISTRY_CONFLICT_RESOLUTION_STRATEGY': 'latest_wins'
            }
    
    async def _create_audit_log(
        self, 
        conn: asyncpg.Connection,
        profile_id: Optional[str],
        user_id: str,
        action: AuditAction,
        entity_type: str,
        entity_id: Optional[str] = None,
        old_data: Optional[Dict[str, Any]] = None,
        new_data: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None
    ) -> str:
        """Create comprehensive audit log entry"""
        import json
        audit_id = f"audit_{uuid.uuid4().hex}"
        
        await conn.execute("""
            INSERT INTO instrument_registry.subscription_audit_log 
            (audit_id, profile_id, user_id, action, entity_type, entity_id, 
             old_data, new_data, metadata, ip_address, user_agent)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        """, 
            audit_id, profile_id, user_id, action.value, entity_type, 
            entity_id, 
            json.dumps(old_data) if old_data else None,
            json.dumps(new_data) if new_data else None, 
            json.dumps(metadata) if metadata else None,
            ip_address, user_agent
        )
        
        logger.info(f"Created audit log {audit_id} for user {user_id} action {action.value}")
        return audit_id
    
    async def _validate_subscription_limits(
        self, 
        conn: asyncpg.Connection,
        user_id: str,
        subscription_type: SubscriptionType,
        instruments_count: int,
        config: Dict[str, Any]
    ) -> List[SubscriptionConflict]:
        """Validate subscription against configured limits"""
        conflicts = []
        
        # Check global per-user limit
        max_subscriptions = config.get('INSTRUMENT_REGISTRY_MAX_SUBSCRIPTIONS_PER_USER', 100)
        
        current_count = await conn.fetchval("""
            SELECT COUNT(*) FROM instrument_registry.subscription_profiles 
            WHERE user_id = $1 AND is_active = true
        """, user_id)
        
        if current_count >= max_subscriptions:
            conflicts.append(SubscriptionConflict(
                conflict_id=f"conflict_{uuid.uuid4().hex}",
                profile_id="",  # Will be set when profile is created
                user_id=user_id,
                conflict_type=ConflictType.LIMIT_EXCEEDED,
                conflict_data={
                    "limit_type": "max_subscriptions_per_user",
                    "limit_value": max_subscriptions,
                    "current_value": current_count + 1,
                    "attempted_subscription_type": subscription_type.value
                },
                resolution_strategy=config.get('INSTRUMENT_REGISTRY_CONFLICT_RESOLUTION_STRATEGY', 'latest_wins')
            ))
        
        # Check per-subscription instrument limits (if strict validation is enabled)
        if config.get('INSTRUMENT_REGISTRY_PROFILE_VALIDATION_STRICT', True):
            # Reasonable per-subscription limits based on type
            type_limits = {
                SubscriptionType.LIVE_FEED: 50,
                SubscriptionType.HISTORICAL: 200,
                SubscriptionType.ALERTS: 100
            }
            
            type_limit = type_limits.get(subscription_type, 100)
            if instruments_count > type_limit:
                conflicts.append(SubscriptionConflict(
                    conflict_id=f"conflict_{uuid.uuid4().hex}",
                    profile_id="",
                    user_id=user_id,
                    conflict_type=ConflictType.LIMIT_EXCEEDED,
                    conflict_data={
                        "limit_type": "instruments_per_subscription",
                        "limit_value": type_limit,
                        "current_value": instruments_count,
                        "subscription_type": subscription_type.value
                    },
                    resolution_strategy=config.get('INSTRUMENT_REGISTRY_CONFLICT_RESOLUTION_STRATEGY', 'latest_wins')
                ))
        
        return conflicts
    
    async def _validate_instruments(
        self, 
        conn: asyncpg.Connection,
        instruments: List[str],
        config: Dict[str, Any]
    ) -> List[SubscriptionConflict]:
        """Validate instruments against registry"""
        conflicts = []
        
        if not config.get('INSTRUMENT_REGISTRY_PROFILE_VALIDATION_STRICT', True):
            return conflicts  # Skip validation if not strict
        
        # Check if instruments exist in registry
        existing_instruments = await conn.fetch("""
            SELECT instrument_key FROM instrument_registry.instrument_keys 
            WHERE instrument_key = ANY($1) AND is_active = true
        """, instruments)
        
        existing_keys = {row['instrument_key'] for row in existing_instruments}
        invalid_instruments = [inst for inst in instruments if inst not in existing_keys]
        
        if invalid_instruments:
            conflicts.append(SubscriptionConflict(
                conflict_id=f"conflict_{uuid.uuid4().hex}",
                profile_id="",
                user_id="",  # Will be set by caller
                conflict_type=ConflictType.INVALID_INSTRUMENT,
                conflict_data={
                    "invalid_instruments": invalid_instruments,
                    "validation_type": "instrument_registry_lookup"
                },
                resolution_strategy=config.get('INSTRUMENT_REGISTRY_CONFLICT_RESOLUTION_STRATEGY', 'latest_wins')
            ))
        
        return conflicts
    
    async def create_subscription_profile(
        self,
        profile_data: SubscriptionProfile,
        audit_metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create new subscription profile with full validation and audit logging"""
        import json  # Import at method start for all operations
        start_time = time.time()
        
        try:
            config = await self._get_config_values()
            conn = await self._get_connection()
            
            async with conn.transaction():
                # Validate subscription limits
                limit_conflicts = await self._validate_subscription_limits(
                    conn, profile_data.user_id, profile_data.subscription_type,
                    len(profile_data.instruments), config
                )
                
                # Validate instruments
                instrument_conflicts = await self._validate_instruments(
                    conn, profile_data.instruments, config
                )
                
                all_conflicts = limit_conflicts + instrument_conflicts
                
                # Handle conflicts based on strategy
                strategy = config.get('INSTRUMENT_REGISTRY_CONFLICT_RESOLUTION_STRATEGY', 'latest_wins')
                
                # Handle conflicts based on strategy
                if all_conflicts and strategy == 'fail_on_conflict':
                    # Create profile first, then store conflicts, then fail
                    await conn.execute("""
                        INSERT INTO instrument_registry.subscription_profiles
                        (profile_id, user_id, profile_name, subscription_type, instruments, 
                         preferences, validation_rules, max_instruments, is_active, expires_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    """, 
                        profile_data.profile_id,
                        profile_data.user_id,
                        profile_data.profile_name,
                        profile_data.subscription_type.value,
                        json.dumps(profile_data.instruments),
                        json.dumps(profile_data.preferences or {}),
                        json.dumps(profile_data.validation_rules or {}),
                        profile_data.max_instruments,
                        profile_data.is_active,
                        profile_data.expires_at
                    )
                    
                    # Now store conflicts with valid profile_id
                    for conflict in all_conflicts:
                        conflict.profile_id = profile_data.profile_id
                        conflict.user_id = profile_data.user_id
                        await self._store_conflict(conn, conflict)
                    
                    await self._create_audit_log(
                        conn, profile_data.profile_id, profile_data.user_id,
                        AuditAction.CREATE, "subscription_profile",
                        profile_data.profile_id, None, None,
                        {"conflicts": [c.conflict_data for c in all_conflicts], "strategy": strategy},
                        audit_metadata.get('ip_address') if audit_metadata else None,
                        audit_metadata.get('user_agent') if audit_metadata else None
                    )
                    
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "message": "Subscription validation failed",
                            "conflicts": [{"id": c.conflict_id, "type": c.conflict_type.value, "data": c.conflict_data} for c in all_conflicts],
                            "strategy": strategy
                        }
                    )
                
                elif all_conflicts and strategy == 'merge':
                    # Merge strategy: Remove invalid instruments but keep valid ones
                    merged_instruments = profile_data.instruments.copy()
                    for conflict in all_conflicts:
                        if conflict.conflict_type == ConflictType.INVALID_INSTRUMENT:
                            invalid_instruments = conflict.conflict_data.get('invalid_instruments', [])
                            # Remove invalid instruments from the list
                            merged_instruments = [inst for inst in merged_instruments if inst not in invalid_instruments]
                    
                    # Update profile with merged instrument list
                    profile_data.instruments = merged_instruments
                
                # For latest_wins and merge strategies, conflicts will be stored after profile creation
                conflicts_to_store = all_conflicts if all_conflicts and strategy in ['latest_wins', 'merge'] else []
                
                # Create subscription profile for latest_wins and merge (fail_on_conflict already created it)
                if not (all_conflicts and strategy == 'fail_on_conflict'):
                    await conn.execute("""
                        INSERT INTO instrument_registry.subscription_profiles
                        (profile_id, user_id, profile_name, subscription_type, instruments, 
                         preferences, validation_rules, max_instruments, is_active, expires_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    """, 
                        profile_data.profile_id,
                        profile_data.user_id,
                        profile_data.profile_name,
                        profile_data.subscription_type.value,
                        json.dumps(profile_data.instruments),  # Explicit JSON serialization
                        json.dumps(profile_data.preferences or {}),
                        json.dumps(profile_data.validation_rules or {}),
                        profile_data.max_instruments,
                        profile_data.is_active,
                        profile_data.expires_at
                    )
                
                # Store conflicts after profile creation for latest_wins and merge
                for conflict in conflicts_to_store:
                    conflict.profile_id = profile_data.profile_id
                    conflict.user_id = profile_data.user_id
                    conflict.status = "resolved"
                    conflict.resolved_at = datetime.now(timezone.utc)
                    conflict.resolution_data = {"auto_resolved": True, "strategy": strategy}
                    if strategy == 'merge':
                        conflict.resolution_data["merged_instruments"] = profile_data.instruments
                    await self._store_conflict(conn, conflict)
                
                # Create audit log with serializable data
                profile_dict = {
                    "profile_id": profile_data.profile_id,
                    "user_id": profile_data.user_id,
                    "profile_name": profile_data.profile_name,
                    "subscription_type": profile_data.subscription_type.value,
                    "instruments": profile_data.instruments,
                    "preferences": profile_data.preferences,
                    "validation_rules": profile_data.validation_rules,
                    "max_instruments": profile_data.max_instruments,
                    "is_active": profile_data.is_active,
                    "expires_at": profile_data.expires_at.isoformat() if profile_data.expires_at else None
                }
                await self._create_audit_log(
                    conn, profile_data.profile_id, profile_data.user_id,
                    AuditAction.CREATE, "subscription_profile",
                    profile_data.profile_id, None, profile_dict,
                    audit_metadata or {}
                )
                
                # Update user subscription limits tracking
                await self._update_user_limits(conn, profile_data.user_id, profile_data.subscription_type, 1)
                
                duration = time.time() - start_time
                self.monitoring.record_operation_duration("subscription_profile_create", duration)
                
                logger.info(f"Created subscription profile {profile_data.profile_id} for user {profile_data.user_id} in {duration:.3f}s")
                
                return {
                    "profile_id": profile_data.profile_id,
                    "user_id": profile_data.user_id,
                    "status": "created",
                    "conflicts_resolved": len(all_conflicts),
                    "resolution_strategy": strategy,
                    "duration_ms": int(duration * 1000)
                }
                
        except HTTPException:
            raise
        except Exception as e:
            duration = time.time() - start_time
            self.monitoring.record_operation_duration("subscription_profile_create_error", duration)
            logger.error(f"Error creating subscription profile: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create subscription profile: {str(e)}"
            )
        finally:
            if 'conn' in locals():
                await self._connection_pool.release(conn)
    
    async def _store_conflict(self, conn: asyncpg.Connection, conflict: SubscriptionConflict):
        """Store subscription conflict in database"""
        import json
        await conn.execute("""
            INSERT INTO instrument_registry.subscription_conflicts
            (conflict_id, profile_id, user_id, conflict_type, conflict_data,
             resolution_strategy, status, resolution_data, resolved_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """,
            conflict.conflict_id, conflict.profile_id, conflict.user_id,
            conflict.conflict_type.value, json.dumps(conflict.conflict_data),
            conflict.resolution_strategy, conflict.status,
            json.dumps(conflict.resolution_data) if conflict.resolution_data else None,
            conflict.resolved_at
        )
    
    async def _update_user_limits(
        self, 
        conn: asyncpg.Connection, 
        user_id: str, 
        subscription_type: SubscriptionType, 
        delta: int
    ):
        """Update user subscription limits tracking"""
        await conn.execute("""
            INSERT INTO instrument_registry.user_subscription_limits
            (user_id, subscription_type, max_subscriptions, max_instruments_per_subscription, current_count, is_active)
            VALUES ($1, $2, 100, 200, $3, true)
            ON CONFLICT (user_id, subscription_type)
            DO UPDATE SET 
                current_count = user_subscription_limits.current_count + $3,
                updated_at = now()
        """, user_id, subscription_type.value, delta)
    
    async def get_subscription_profile(self, profile_id: str) -> Optional[SubscriptionProfile]:
        """Get subscription profile by ID"""
        start_time = time.time()
        
        try:
            conn = await self._get_connection()
            
            row = await conn.fetchrow("""
                SELECT profile_id, user_id, profile_name, subscription_type, instruments,
                       preferences, validation_rules, max_instruments, is_active,
                       created_at, updated_at, expires_at
                FROM instrument_registry.subscription_profiles
                WHERE profile_id = $1
            """, profile_id)
            
            if not row:
                return None
            
            duration = time.time() - start_time
            self.monitoring.record_operation_duration("subscription_profile_get", duration)
            
            return SubscriptionProfile(
                profile_id=row['profile_id'],
                user_id=row['user_id'],
                profile_name=row['profile_name'],
                subscription_type=SubscriptionType(row['subscription_type']),
                instruments=row['instruments'],
                preferences=row['preferences'],
                validation_rules=row['validation_rules'],
                max_instruments=row['max_instruments'],
                is_active=row['is_active'],
                created_at=row['created_at'],
                updated_at=row['updated_at'],
                expires_at=row['expires_at']
            )
            
        except Exception as e:
            duration = time.time() - start_time
            self.monitoring.record_operation_duration("subscription_profile_get_error", duration)
            logger.error(f"Error getting subscription profile {profile_id}: {e}")
            return None
        finally:
            if 'conn' in locals():
                await self._connection_pool.release(conn)
    
    async def list_user_subscription_profiles(
        self, 
        user_id: str, 
        subscription_type: Optional[SubscriptionType] = None,
        is_active: Optional[bool] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """List subscription profiles for a user"""
        start_time = time.time()
        
        try:
            conn = await self._get_connection()
            
            # Build dynamic query
            where_clauses = ["user_id = $1"]
            params = [user_id]
            param_count = 1
            
            if subscription_type:
                param_count += 1
                where_clauses.append(f"subscription_type = ${param_count}")
                params.append(subscription_type.value)
            
            if is_active is not None:
                param_count += 1
                where_clauses.append(f"is_active = ${param_count}")
                params.append(is_active)
            
            where_clause = " AND ".join(where_clauses)
            
            rows = await conn.fetch(f"""
                SELECT profile_id, user_id, profile_name, subscription_type, instruments,
                       preferences, max_instruments, is_active, created_at, updated_at, expires_at
                FROM instrument_registry.subscription_profiles
                WHERE {where_clause}
                ORDER BY created_at DESC
                LIMIT ${param_count + 1} OFFSET ${param_count + 2}
            """, *params, limit, offset)
            
            total_count = await conn.fetchval(f"""
                SELECT COUNT(*) FROM instrument_registry.subscription_profiles
                WHERE {where_clause}
            """, *params)
            
            profiles = []
            for row in rows:
                profiles.append({
                    "profile_id": row['profile_id'],
                    "user_id": row['user_id'],
                    "profile_name": row['profile_name'],
                    "subscription_type": row['subscription_type'],
                    "instrument_count": len(row['instruments']),
                    "is_active": row['is_active'],
                    "created_at": row['created_at'].isoformat(),
                    "updated_at": row['updated_at'].isoformat(),
                    "expires_at": row['expires_at'].isoformat() if row['expires_at'] else None
                })
            
            duration = time.time() - start_time
            self.monitoring.record_operation_duration("subscription_profile_list", duration)
            
            return {
                "profiles": profiles,
                "pagination": {
                    "total": total_count,
                    "limit": limit,
                    "offset": offset,
                    "has_next": offset + limit < total_count
                },
                "duration_ms": int(duration * 1000)
            }
            
        except Exception as e:
            duration = time.time() - start_time
            self.monitoring.record_operation_duration("subscription_profile_list_error", duration)
            logger.error(f"Error listing subscription profiles for user {user_id}: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to list subscription profiles: {str(e)}"
            )
        finally:
            if 'conn' in locals():
                await self._connection_pool.release(conn)
    
    async def cleanup_expired_audit_logs(self) -> Dict[str, Any]:
        """Clean up expired audit logs based on retention policy"""
        start_time = time.time()
        
        try:
            config = await self._get_config_values()
            retention_days = config.get('INSTRUMENT_REGISTRY_AUDIT_RETENTION_DAYS', 365)
            
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)
            
            conn = await self._get_connection()
            
            # First count records to be deleted
            deleted_count = await conn.fetchval("""
                SELECT COUNT(*) FROM instrument_registry.subscription_audit_log
                WHERE created_at < $1
            """, cutoff_date)
            
            # Then delete them
            await conn.execute("""
                DELETE FROM instrument_registry.subscription_audit_log
                WHERE created_at < $1
            """, cutoff_date)
            
            duration = time.time() - start_time
            
            logger.info(f"Cleaned up {deleted_count} audit log entries older than {retention_days} days in {duration:.3f}s")
            
            return {
                "deleted_count": deleted_count,
                "retention_days": retention_days,
                "cutoff_date": cutoff_date.isoformat(),
                "duration_ms": int(duration * 1000)
            }
            
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Error cleaning up audit logs: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to cleanup audit logs: {str(e)}"
            )
        finally:
            if 'conn' in locals():
                await self._connection_pool.release(conn)