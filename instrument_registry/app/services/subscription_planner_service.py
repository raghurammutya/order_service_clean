"""
Subscription Planner Service

Implements subscription planning endpoints with production compliance:
- Config-driven optimization and filtering
- Performance-adaptive planning algorithms
- Comprehensive caching with TTL
- Integration with subscription profiles
"""

import logging
import asyncio
import time
import json
import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Union, Set
from dataclasses import dataclass
from enum import Enum
import hashlib

import asyncpg
from fastapi import HTTPException

from common.config_client import ConfigClient
from app.services.monitoring_service import MonitoringService
from app.services.subscription_profile_service import SubscriptionProfileService, SubscriptionType

# Import Prometheus metrics from metrics module
from app.metrics import (
    subscription_plans_created_total,
    subscription_plan_descriptions_generated_total, 
    subscription_plan_cache_operations_total,
    subscription_plan_conflicts_total,
    subscription_plan_optimization_duration_seconds,
    subscription_plans_active
)

logger = logging.getLogger(__name__)


class OptimizationLevel(str, Enum):
    """Optimization levels"""
    LOW = "low"
    MODERATE = "moderate"
    AGGRESSIVE = "aggressive"


class FilteringStrictness(str, Enum):
    """Filtering strictness levels"""
    LENIENT = "lenient"
    MODERATE = "moderate"
    STRICT = "strict"


class PlanStatus(str, Enum):
    """Plan status"""
    DRAFT = "draft"
    OPTIMIZED = "optimized"
    VALIDATED = "validated"
    ERROR = "error"


@dataclass
class SubscriptionPlan:
    """Subscription plan data model"""
    plan_id: str
    user_id: str
    plan_name: str
    description: Optional[str]
    subscription_type: SubscriptionType
    instruments: List[str]
    optimization_level: OptimizationLevel
    filtering_strictness: FilteringStrictness
    status: PlanStatus
    estimated_cost: Optional[float] = None
    performance_metrics: Optional[Dict[str, Any]] = None
    validation_results: Optional[Dict[str, Any]] = None
    cache_key: Optional[str] = None
    created_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class PlanDescription:
    """Plan description data model"""
    plan_id: str
    description_level: str
    instrument_analysis: Dict[str, Any]
    cost_breakdown: Dict[str, Any]
    performance_analysis: Dict[str, Any]
    recommendations: List[Dict[str, Any]]
    risk_analysis: Dict[str, Any]


class SubscriptionPlannerService:
    """Production-ready subscription planning service"""
    
    def __init__(self, 
                 database_url: str, 
                 config_client: ConfigClient, 
                 monitoring: MonitoringService,
                 profile_service: SubscriptionProfileService):
        self.database_url = database_url
        self.config_client = config_client
        self.monitoring = monitoring
        self.profile_service = profile_service
        self._connection_pool = None
        self._plan_cache = {}  # Simple in-memory cache
        
    async def _get_connection(self) -> asyncpg.Connection:
        """Get database connection from pool"""
        if self._connection_pool is None:
            self._connection_pool = await asyncpg.create_pool(
                self.database_url,
                min_size=2,
                max_size=10,
                command_timeout=60
            )
        return await self._connection_pool.acquire()
    
    async def _get_planner_config(self) -> Dict[str, Any]:
        """Get planner configuration from config service"""
        try:
            config_keys = [
                'INSTRUMENT_REGISTRY_PLANNER_OPTIMIZATION_LEVEL',
                'INSTRUMENT_REGISTRY_PLANNER_TIMEOUT',
                'INSTRUMENT_REGISTRY_MAX_INSTRUMENTS_PER_PLAN',
                'INSTRUMENT_REGISTRY_FILTERING_STRICTNESS',
                'INSTRUMENT_REGISTRY_PLAN_CACHE_TTL'
            ]
            
            config_values = {}
            for key in config_keys:
                try:
                    value = await self.config_client.get_secret(key, environment='prod')
                    config_values[key] = value
                except Exception as e:
                    logger.warning(f"Failed to get config {key}: {e}")
                    # Set sensible defaults
                    if 'TIMEOUT' in key:
                        config_values[key] = "30"
                    elif 'MAX_INSTRUMENTS' in key:
                        config_values[key] = "1000"
                    elif 'OPTIMIZATION_LEVEL' in key:
                        config_values[key] = "moderate"
                    elif 'FILTERING_STRICTNESS' in key:
                        config_values[key] = "moderate"
                    elif 'CACHE_TTL' in key:
                        config_values[key] = "300"
            
            return config_values
            
        except Exception as e:
            logger.error(f"Failed to get planner config: {e}")
            return {
                'INSTRUMENT_REGISTRY_PLANNER_OPTIMIZATION_LEVEL': 'moderate',
                'INSTRUMENT_REGISTRY_PLANNER_TIMEOUT': '30',
                'INSTRUMENT_REGISTRY_MAX_INSTRUMENTS_PER_PLAN': '1000',
                'INSTRUMENT_REGISTRY_FILTERING_STRICTNESS': 'moderate',
                'INSTRUMENT_REGISTRY_PLAN_CACHE_TTL': '300'
            }
    
    def _generate_cache_key(self, user_id: str, instruments: List[str], options: Dict[str, Any]) -> str:
        """Generate cache key for plan"""
        cache_data = {
            "user_id": user_id,
            "instruments": sorted(instruments),  # Sort for consistent keys
            "options": options
        }
        cache_str = json.dumps(cache_data, sort_keys=True)
        return hashlib.sha256(cache_str.encode()).hexdigest()
    
    def _is_cache_valid(self, cache_entry: Dict[str, Any], ttl_seconds: int) -> bool:
        """Check if cache entry is still valid"""
        if not cache_entry:
            return False
        
        cache_time = cache_entry.get('cached_at')
        if not cache_time:
            return False
        
        age = (datetime.now(timezone.utc) - cache_time).total_seconds()
        return age < ttl_seconds
    
    async def _filter_instruments_by_strictness(
        self, 
        conn: asyncpg.Connection,
        instruments: List[str], 
        strictness: FilteringStrictness
    ) -> List[str]:
        """Filter instruments based on strictness level"""
        if strictness == FilteringStrictness.LENIENT:
            return instruments  # Return all instruments
        
        # Get valid instruments from registry
        existing_instruments = await conn.fetch("""
            SELECT instrument_key, is_active FROM instrument_registry.instrument_keys 
            WHERE instrument_key = ANY($1)
        """, instruments)
        
        valid_keys = set()
        for row in existing_instruments:
            if strictness == FilteringStrictness.MODERATE:
                # Include all instruments in registry
                valid_keys.add(row['instrument_key'])
            elif strictness == FilteringStrictness.STRICT:
                # Only include active instruments
                if row['is_active']:
                    valid_keys.add(row['instrument_key'])
        
        # Filter original list maintaining order
        return [inst for inst in instruments if inst in valid_keys]
    
    async def _optimize_instrument_list(
        self, 
        instruments: List[str], 
        optimization_level: OptimizationLevel,
        max_instruments: int
    ) -> Dict[str, Any]:
        """Optimize instrument list based on optimization level"""
        start_time = time.time()
        
        if optimization_level == OptimizationLevel.LOW:
            # Simple truncation if over limit
            optimized = instruments[:max_instruments] if len(instruments) > max_instruments else instruments
            return {
                "optimized_instruments": optimized,
                "removed_count": len(instruments) - len(optimized),
                "optimization_strategy": "simple_truncation",
                "performance_impact": "minimal"
            }
        
        elif optimization_level == OptimizationLevel.MODERATE:
            # Remove duplicates and basic sorting
            unique_instruments = list(dict.fromkeys(instruments))  # Preserve order while removing duplicates
            
            # Sort by exchange and symbol for better organization
            sorted_instruments = sorted(unique_instruments, key=lambda x: (x.split(':')[0] if ':' in x else '', x))
            
            # Apply limit
            optimized = sorted_instruments[:max_instruments] if len(sorted_instruments) > max_instruments else sorted_instruments
            
            return {
                "optimized_instruments": optimized,
                "removed_count": len(instruments) - len(optimized),
                "deduplication_count": len(instruments) - len(unique_instruments),
                "optimization_strategy": "dedup_and_sort",
                "performance_impact": "moderate"
            }
        
        elif optimization_level == OptimizationLevel.AGGRESSIVE:
            # Advanced optimization simulation
            await asyncio.sleep(0.1)  # Simulate complex processing
            
            unique_instruments = list(dict.fromkeys(instruments))
            
            # Group by exchange for better performance
            exchange_groups = {}
            for inst in unique_instruments:
                exchange = inst.split(':')[0] if ':' in inst else 'UNKNOWN'
                if exchange not in exchange_groups:
                    exchange_groups[exchange] = []
                exchange_groups[exchange].append(inst)
            
            # Prioritize major exchanges and limit per exchange
            priority_exchanges = ['NSE', 'BSE', 'NFO']
            optimized = []
            
            for exchange in priority_exchanges:
                if exchange in exchange_groups:
                    # Take up to 1/3 of limit per major exchange
                    limit_per_exchange = max_instruments // 3
                    optimized.extend(exchange_groups[exchange][:limit_per_exchange])
                    if len(optimized) >= max_instruments:
                        break
            
            # Fill remaining slots with other exchanges
            remaining_slots = max_instruments - len(optimized)
            for exchange, instruments_list in exchange_groups.items():
                if exchange not in priority_exchanges:
                    take_count = min(len(instruments_list), remaining_slots)
                    optimized.extend(instruments_list[:take_count])
                    remaining_slots -= take_count
                    if remaining_slots <= 0:
                        break
            
            optimization_time = time.time() - start_time
            
            return {
                "optimized_instruments": optimized[:max_instruments],
                "removed_count": len(instruments) - len(optimized[:max_instruments]),
                "deduplication_count": len(instruments) - len(unique_instruments),
                "exchange_distribution": {ex: len([i for i in optimized[:max_instruments] if i.startswith(ex + ':')]) 
                                       for ex in exchange_groups.keys()},
                "optimization_strategy": "advanced_exchange_prioritization",
                "performance_impact": "significant",
                "optimization_time_ms": int(optimization_time * 1000)
            }
        
        return {
            "optimized_instruments": instruments,
            "removed_count": 0,
            "optimization_strategy": "none",
            "performance_impact": "none"
        }
    
    async def create_subscription_plan(
        self,
        user_id: str,
        plan_name: str,
        subscription_type: SubscriptionType,
        instruments: List[str],
        options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create subscription plan with config-driven optimization"""
        start_time = time.time()
        plan_id = f"plan_{uuid.uuid4().hex}"
        
        try:
            config = await self._get_planner_config()
            
            # Parse config values
            optimization_level = OptimizationLevel(config['INSTRUMENT_REGISTRY_PLANNER_OPTIMIZATION_LEVEL'])
            filtering_strictness = FilteringStrictness(config['INSTRUMENT_REGISTRY_FILTERING_STRICTNESS'])
            max_instruments = int(config['INSTRUMENT_REGISTRY_MAX_INSTRUMENTS_PER_PLAN'])
            cache_ttl = int(config['INSTRUMENT_REGISTRY_PLAN_CACHE_TTL'])
            timeout = int(config['INSTRUMENT_REGISTRY_PLANNER_TIMEOUT'])
            
            # Override with user options if provided
            if options:
                optimization_level = OptimizationLevel(options.get('optimization_level', optimization_level.value))
                filtering_strictness = FilteringStrictness(options.get('filtering_strictness', filtering_strictness.value))
                max_instruments = min(int(options.get('max_instruments', max_instruments)), max_instruments)
            
            # Check cache first
            cache_key = self._generate_cache_key(user_id, instruments, {
                'optimization_level': optimization_level.value,
                'filtering_strictness': filtering_strictness.value,
                'max_instruments': max_instruments,
                'subscription_type': subscription_type.value
            })
            
            if cache_key in self._plan_cache and self._is_cache_valid(self._plan_cache[cache_key], cache_ttl):
                cached_plan = self._plan_cache[cache_key]['plan']
                logger.info(f"Returning cached plan for user {user_id}")
                
                # Record cache hit metric
                subscription_plan_cache_operations_total.labels(
                    operation="hit",
                    status="success"
                ).inc()
                
                return {
                    "plan_id": cached_plan['plan_id'],
                    "status": "cached",
                    "cache_hit": True,
                    "plan_data": cached_plan
                }
            
            conn = await self._get_connection()
            
            # Apply timeout
            async def plan_with_timeout():
                # Filter instruments by strictness
                filtered_instruments = await self._filter_instruments_by_strictness(
                    conn, instruments, filtering_strictness
                )
                
                # Optimize instrument list with timing
                opt_start_time = time.time()
                optimization_result = await self._optimize_instrument_list(
                    filtered_instruments, optimization_level, max_instruments
                )
                opt_duration = time.time() - opt_start_time
                
                # Record optimization duration metric
                subscription_plan_optimization_duration_seconds.labels(
                    optimization_level=optimization_level.value
                ).observe(opt_duration)
                
                # Calculate estimated cost (mock implementation)
                estimated_cost = len(optimization_result['optimized_instruments']) * 0.05  # $0.05 per instrument
                
                # Performance metrics
                performance_metrics = {
                    "instrument_count": len(optimization_result['optimized_instruments']),
                    "optimization_level": optimization_level.value,
                    "filtering_strictness": filtering_strictness.value,
                    "removed_instruments": optimization_result['removed_count'],
                    "estimated_latency_ms": len(optimization_result['optimized_instruments']) * 2,  # 2ms per instrument
                    "memory_usage_mb": len(optimization_result['optimized_instruments']) * 0.1  # 0.1MB per instrument
                }
                
                # Validation results
                validation_results = {
                    "valid_instruments": len(optimization_result['optimized_instruments']),
                    "invalid_instruments": len(instruments) - len(filtered_instruments),
                    "optimization_applied": optimization_result['optimization_strategy'],
                    "within_limits": len(optimization_result['optimized_instruments']) <= max_instruments
                }
                
                plan = SubscriptionPlan(
                    plan_id=plan_id,
                    user_id=user_id,
                    plan_name=plan_name,
                    description=options.get('description', f"Optimized plan for {subscription_type.value}"),
                    subscription_type=subscription_type,
                    instruments=optimization_result['optimized_instruments'],
                    optimization_level=optimization_level,
                    filtering_strictness=filtering_strictness,
                    status=PlanStatus.OPTIMIZED,
                    estimated_cost=estimated_cost,
                    performance_metrics=performance_metrics,
                    validation_results=validation_results,
                    cache_key=cache_key,
                    created_at=datetime.now(timezone.utc),
                    expires_at=datetime.now(timezone.utc) + timedelta(seconds=cache_ttl),
                    metadata={
                        "original_instrument_count": len(instruments),
                        "optimization_result": optimization_result
                    }
                )
                
                return plan
            
            # Execute with timeout
            plan = await asyncio.wait_for(plan_with_timeout(), timeout=timeout)
            
            # Store plan in database
            plan_dict = {
                "plan_id": plan.plan_id,
                "user_id": plan.user_id,
                "plan_name": plan.plan_name,
                "description": plan.description,
                "subscription_type": plan.subscription_type.value,
                "instruments": json.dumps(plan.instruments),
                "optimization_level": plan.optimization_level.value,
                "filtering_strictness": plan.filtering_strictness.value,
                "status": plan.status.value,
                "estimated_cost": plan.estimated_cost,
                "performance_metrics": json.dumps(plan.performance_metrics),
                "validation_results": json.dumps(plan.validation_results),
                "cache_key": plan.cache_key,
                "expires_at": plan.expires_at,
                "metadata": json.dumps(plan.metadata)
            }
            
            await conn.execute("""
                INSERT INTO instrument_registry.subscription_plans
                (plan_id, user_id, plan_name, description, subscription_type, instruments,
                 optimization_level, filtering_strictness, status, estimated_cost,
                 performance_metrics, validation_results, cache_key, expires_at, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            """, 
                plan.plan_id, plan.user_id, plan.plan_name, plan.description, 
                plan.subscription_type.value, json.dumps(plan.instruments),
                plan.optimization_level.value, plan.filtering_strictness.value,
                plan.status.value, plan.estimated_cost,
                json.dumps(plan.performance_metrics), json.dumps(plan.validation_results),
                plan.cache_key, plan.expires_at, json.dumps(plan.metadata)
            )
            
            # Update cache
            self._plan_cache[cache_key] = {
                'plan': plan_dict,
                'cached_at': datetime.now(timezone.utc)
            }
            
            # Record cache miss metric
            subscription_plan_cache_operations_total.labels(
                operation="miss",
                status="success"
            ).inc()
            
            # Record successful plan creation metric
            subscription_plans_created_total.labels(
                optimization_level=optimization_level.value,
                filtering_strictness=filtering_strictness.value,
                status="success"
            ).inc()
            
            # Update active plans gauge
            subscription_plans_active.labels(
                user_id=user_id,
                subscription_type=subscription_type.value
            ).set(1)  # Simplified - in production would count actual active plans
            
            duration = time.time() - start_time
            self.monitoring.record_operation_duration("subscription_plan_create", duration)
            
            logger.info(f"Created subscription plan {plan_id} for user {user_id} in {duration:.3f}s")
            
            return {
                "plan_id": plan.plan_id,
                "status": "created",
                "cache_hit": False,
                "plan_data": plan_dict,
                "duration_ms": int(duration * 1000)
            }
            
        except asyncio.TimeoutError:
            # Record timeout metric
            subscription_plans_created_total.labels(
                optimization_level=optimization_level.value if 'optimization_level' in locals() else "unknown",
                filtering_strictness=filtering_strictness.value if 'filtering_strictness' in locals() else "unknown", 
                status="timeout"
            ).inc()
            
            logger.error(f"Subscription planning timeout for user {user_id} after {timeout}s")
            raise HTTPException(
                status_code=408,
                detail=f"Subscription planning timeout after {timeout} seconds"
            )
        except Exception as e:
            # Record error metric
            subscription_plans_created_total.labels(
                optimization_level=optimization_level.value if 'optimization_level' in locals() else "unknown",
                filtering_strictness=filtering_strictness.value if 'filtering_strictness' in locals() else "unknown",
                status="error"
            ).inc()
            
            duration = time.time() - start_time
            self.monitoring.record_operation_duration("subscription_plan_create_error", duration)
            logger.error(f"Error creating subscription plan: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create subscription plan: {str(e)}"
            )
        finally:
            if 'conn' in locals():
                await self._connection_pool.release(conn)
    
    async def describe_subscription_plan(
        self,
        plan_id: str,
        description_level: str = "detailed"
    ) -> Dict[str, Any]:
        """Generate detailed plan description with configurable detail levels"""
        start_time = time.time()
        
        try:
            config = await self._get_planner_config()
            conn = await self._get_connection()
            
            # Get plan from database
            plan_row = await conn.fetchrow("""
                SELECT * FROM instrument_registry.subscription_plans
                WHERE plan_id = $1
            """, plan_id)
            
            if not plan_row:
                raise HTTPException(
                    status_code=404,
                    detail=f"Subscription plan {plan_id} not found"
                )
            
            instruments = json.loads(plan_row['instruments'])
            performance_metrics = json.loads(plan_row['performance_metrics'])
            validation_results = json.loads(plan_row['validation_results'])
            metadata = json.loads(plan_row['metadata'])
            
            # Generate instrument analysis based on detail level
            instrument_analysis = {}
            if description_level in ["basic", "detailed", "comprehensive"]:
                instrument_analysis = {
                    "total_instruments": len(instruments),
                    "exchanges": {},
                    "instrument_types": {}
                }
                
                # Analyze instruments
                for inst in instruments:
                    parts = inst.split(':')
                    exchange = parts[0] if len(parts) > 1 else 'UNKNOWN'
                    instrument_analysis["exchanges"][exchange] = instrument_analysis["exchanges"].get(exchange, 0) + 1
                
                if description_level in ["detailed", "comprehensive"]:
                    # Add more detailed analysis
                    instrument_analysis.update({
                        "top_exchanges": sorted(instrument_analysis["exchanges"].items(), 
                                              key=lambda x: x[1], reverse=True)[:5],
                        "diversity_score": len(instrument_analysis["exchanges"]) / max(len(instruments), 1),
                        "optimization_efficiency": 1.0 - (metadata.get('optimization_result', {}).get('removed_count', 0) / 
                                                        max(metadata.get('original_instrument_count', 1), 1))
                    })
            
            # Generate cost breakdown
            cost_breakdown = {
                "base_cost": plan_row['estimated_cost'],
                "currency": "USD",
                "cost_per_instrument": plan_row['estimated_cost'] / max(len(instruments), 1)
            }
            
            if description_level in ["detailed", "comprehensive"]:
                cost_breakdown.update({
                    "monthly_estimate": plan_row['estimated_cost'] * 30,
                    "exchange_costs": {
                        exchange: count * (plan_row['estimated_cost'] / max(len(instruments), 1))
                        for exchange, count in instrument_analysis.get("exchanges", {}).items()
                    }
                })
            
            # Performance analysis
            performance_analysis = performance_metrics.copy()
            if description_level == "comprehensive":
                performance_analysis.update({
                    "scalability_assessment": {
                        "current_load": "optimal" if len(instruments) < 500 else "moderate" if len(instruments) < 800 else "high",
                        "recommended_max": int(config['INSTRUMENT_REGISTRY_MAX_INSTRUMENTS_PER_PLAN']),
                        "performance_grade": "A" if performance_metrics.get('estimated_latency_ms', 0) < 1000 else "B"
                    }
                })
            
            # Generate recommendations
            recommendations = []
            if len(instruments) > 500:
                recommendations.append({
                    "type": "performance",
                    "priority": "high",
                    "message": "Consider reducing instrument count for better performance",
                    "suggested_action": "Use more aggressive optimization"
                })
            
            if validation_results.get('invalid_instruments', 0) > 0:
                recommendations.append({
                    "type": "data_quality",
                    "priority": "medium", 
                    "message": f"{validation_results['invalid_instruments']} invalid instruments were removed",
                    "suggested_action": "Review and update instrument list"
                })
            
            # Risk analysis
            risk_analysis = {
                "data_staleness_risk": "low",
                "performance_risk": "low" if len(instruments) < 500 else "medium",
                "cost_risk": "low" if plan_row['estimated_cost'] < 10 else "medium"
            }
            
            if description_level == "comprehensive":
                risk_analysis.update({
                    "detailed_risks": {
                        "instrument_concentration": len(set(inst.split(':')[0] for inst in instruments if ':' in inst)) < 3,
                        "plan_complexity": len(instruments) > 800,
                        "optimization_impact": metadata.get('optimization_result', {}).get('removed_count', 0) > len(instruments) * 0.2
                    }
                })
            
            description = PlanDescription(
                plan_id=plan_id,
                description_level=description_level,
                instrument_analysis=instrument_analysis,
                cost_breakdown=cost_breakdown,
                performance_analysis=performance_analysis,
                recommendations=recommendations,
                risk_analysis=risk_analysis
            )
            
            duration = time.time() - start_time
            self.monitoring.record_operation_duration("subscription_plan_describe", duration)
            
            # Record successful description generation metric
            subscription_plan_descriptions_generated_total.labels(
                description_level=description_level,
                status="success"
            ).inc()
            
            return {
                "plan_id": description.plan_id,
                "description_level": description.description_level,
                "instrument_analysis": description.instrument_analysis,
                "cost_breakdown": description.cost_breakdown,
                "performance_analysis": description.performance_analysis,
                "recommendations": description.recommendations,
                "risk_analysis": description.risk_analysis,
                "duration_ms": int(duration * 1000)
            }
            
        except HTTPException:
            # Record HTTP exception metric
            subscription_plan_descriptions_generated_total.labels(
                description_level=description_level,
                status="not_found"
            ).inc()
            raise
        except Exception as e:
            # Record error metric
            subscription_plan_descriptions_generated_total.labels(
                description_level=description_level,
                status="error"
            ).inc()
            
            duration = time.time() - start_time
            self.monitoring.record_operation_duration("subscription_plan_describe_error", duration)
            logger.error(f"Error describing subscription plan: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to describe subscription plan: {str(e)}"
            )
        finally:
            if 'conn' in locals():
                await self._connection_pool.release(conn)
    
    async def cleanup_expired_plans(self) -> Dict[str, Any]:
        """Clean up expired subscription plans and cache entries"""
        start_time = time.time()
        
        try:
            conn = await self._get_connection()
            
            # Clean up database plans
            now = datetime.now(timezone.utc)
            deleted_count = await conn.fetchval("""
                SELECT COUNT(*) FROM instrument_registry.subscription_plans
                WHERE expires_at < $1
            """, now)
            
            await conn.execute("""
                DELETE FROM instrument_registry.subscription_plans
                WHERE expires_at < $1
            """, now)
            
            # Clean up memory cache
            expired_cache_keys = []
            for key, entry in self._plan_cache.items():
                if not self._is_cache_valid(entry, 0):  # TTL of 0 means immediate expiry check
                    expired_cache_keys.append(key)
            
            for key in expired_cache_keys:
                del self._plan_cache[key]
            
            duration = time.time() - start_time
            
            logger.info(f"Cleaned up {deleted_count} expired plans and {len(expired_cache_keys)} cache entries in {duration:.3f}s")
            
            return {
                "deleted_plans": deleted_count,
                "cleared_cache_entries": len(expired_cache_keys),
                "duration_ms": int(duration * 1000)
            }
            
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Error cleaning up expired plans: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to cleanup expired plans: {str(e)}"
            )
        finally:
            if 'conn' in locals():
                await self._connection_pool.release(conn)