"""
Subscription Planner API Routes

Production-ready endpoints for subscription planning with config-driven optimization.
Integrates with config service for all parameters and provides comprehensive
monitoring and error handling.
"""

import logging
import time
from typing import List, Optional, Dict, Any
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, validator

from common.auth_middleware import verify_internal_token
from common.config_client import ConfigClient
from app.services.monitoring_service import MonitoringService
from app.services.subscription_profile_service import SubscriptionProfileService, SubscriptionType
from app.services.subscription_planner_service import SubscriptionPlannerService

logger = logging.getLogger(__name__)

# Create router with authentication dependency
router = APIRouter(
    prefix="/api/v1/internal/instrument-registry",
    tags=["subscription-planner"],
    dependencies=[Depends(verify_internal_token)]
)

# =========================================
# PYDANTIC MODELS
# =========================================

class SubscriptionPlanRequest(BaseModel):
    """Subscription plan request model"""
    plan_name: str
    subscription_type: str
    instruments: List[str]
    description: Optional[str] = None
    optimization_level: Optional[str] = None  # low, moderate, aggressive
    filtering_strictness: Optional[str] = None  # lenient, moderate, strict
    max_instruments: Optional[int] = None
    
    @validator('subscription_type')
    def validate_subscription_type(cls, v):
        if v not in ['live_feed', 'historical', 'alerts']:
            raise ValueError('subscription_type must be one of: live_feed, historical, alerts')
        return v
    
    @validator('optimization_level')
    def validate_optimization_level(cls, v):
        if v is not None and v not in ['low', 'moderate', 'aggressive']:
            raise ValueError('optimization_level must be one of: low, moderate, aggressive')
        return v
    
    @validator('filtering_strictness')
    def validate_filtering_strictness(cls, v):
        if v is not None and v not in ['lenient', 'moderate', 'strict']:
            raise ValueError('filtering_strictness must be one of: lenient, moderate, strict')
        return v
    
    @validator('instruments')
    def validate_instruments(cls, v):
        if not v:
            raise ValueError('instruments list cannot be empty')
        if len(v) > 5000:  # Hard limit to prevent abuse
            raise ValueError('instruments list cannot exceed 5000 items')
        return v

class PlanDescriptionRequest(BaseModel):
    """Plan description request model"""
    description_level: Optional[str] = "detailed"
    
    @validator('description_level')
    def validate_description_level(cls, v):
        if v not in ['basic', 'detailed', 'comprehensive']:
            raise ValueError('description_level must be one of: basic, detailed, comprehensive')
        return v

# =========================================
# DEPENDENCY INJECTION
# =========================================

def get_config_client() -> ConfigClient:
    """Get shared global config client instance"""
    from main import config_client
    return config_client

def get_monitoring_service() -> MonitoringService:
    """Get shared global monitoring service instance"""
    from main import monitoring_service
    return monitoring_service

async def get_planner_service(
    config_client: ConfigClient = Depends(get_config_client),
    monitoring: MonitoringService = Depends(get_monitoring_service)
) -> SubscriptionPlannerService:
    """Get planner service instance"""
    database_url = "postgresql://instrument_registry_user:b4Gr60lYlbZVZz0ZRTcnf_YRkjO0sluNcwwJ-7lAfn4@localhost:5432/stocksblitz_unified_prod"
    
    # Create profile service dependency
    profile_service = SubscriptionProfileService(database_url, config_client, monitoring)
    
    return SubscriptionPlannerService(database_url, config_client, monitoring, profile_service)

# =========================================
# SUBSCRIPTION PLANNING ENDPOINTS
# =========================================

@router.post("/subscriptions/plan")
async def create_subscription_plan(
    request: Request,
    plan_request: SubscriptionPlanRequest,
    user_id: str = Query(..., description="User ID for the subscription plan"),
    planner_service: SubscriptionPlannerService = Depends(get_planner_service)
) -> Dict[str, Any]:
    """
    Create optimized subscription plan
    
    Creates a subscription plan with config-driven optimization. The plan will be
    optimized based on the current configuration parameters from the config service.
    
    Key features:
    - Config-driven optimization levels (low/moderate/aggressive)
    - Intelligent instrument filtering based on strictness settings
    - Performance metrics and cost estimation
    - Plan caching with configurable TTL
    - Comprehensive validation and conflict resolution
    """
    correlation_id = getattr(request.state, 'correlation_id', 'unknown')
    start_time = time.time()
    
    logger.info(f"Creating subscription plan [correlation_id: {correlation_id}] - user: {user_id}, type: {plan_request.subscription_type}")
    
    try:
        # Convert request to service parameters
        subscription_type = SubscriptionType(plan_request.subscription_type)
        
        options = {
            'description': plan_request.description
        }
        
        # Add optional parameters if provided
        if plan_request.optimization_level:
            options['optimization_level'] = plan_request.optimization_level
        if plan_request.filtering_strictness:
            options['filtering_strictness'] = plan_request.filtering_strictness
        if plan_request.max_instruments:
            options['max_instruments'] = plan_request.max_instruments
        
        # Create the plan
        result = await planner_service.create_subscription_plan(
            user_id=user_id,
            plan_name=plan_request.plan_name,
            subscription_type=subscription_type,
            instruments=plan_request.instruments,
            options=options
        )
        
        duration = time.time() - start_time
        logger.info(f"Subscription plan created in {duration:.3f}s [correlation_id: {correlation_id}] - plan_id: {result['plan_id']}")
        
        return {
            **result,
            "correlation_id": correlation_id,
            "response_time_ms": int(duration * 1000),
            "config_driven": True,
            "api_version": "v1"
        }
        
    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Validation error creating subscription plan [correlation_id: {correlation_id}]: {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Invalid request parameters: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error creating subscription plan [correlation_id: {correlation_id}]: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create subscription plan: {str(e)}"
        )

@router.post("/subscriptions/plan/{plan_id}/describe")
async def describe_subscription_plan(
    request: Request,
    plan_id: str,
    description_request: PlanDescriptionRequest,
    planner_service: SubscriptionPlannerService = Depends(get_planner_service)
) -> Dict[str, Any]:
    """
    Generate detailed subscription plan description
    
    Provides comprehensive analysis of a subscription plan with configurable
    detail levels. The analysis includes cost breakdown, performance metrics,
    risk assessment, and optimization recommendations.
    
    Description levels:
    - basic: Essential metrics and summary
    - detailed: Comprehensive analysis with recommendations
    - comprehensive: Full analysis with risk assessment and advanced metrics
    """
    correlation_id = getattr(request.state, 'correlation_id', 'unknown')
    start_time = time.time()
    
    logger.info(f"Describing subscription plan [correlation_id: {correlation_id}] - plan_id: {plan_id}, level: {description_request.description_level}")
    
    try:
        result = await planner_service.describe_subscription_plan(
            plan_id=plan_id,
            description_level=description_request.description_level
        )
        
        duration = time.time() - start_time
        logger.info(f"Subscription plan described in {duration:.3f}s [correlation_id: {correlation_id}]")
        
        return {
            **result,
            "correlation_id": correlation_id,
            "response_time_ms": int(duration * 1000),
            "api_version": "v1"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error describing subscription plan [correlation_id: {correlation_id}]: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to describe subscription plan: {str(e)}"
        )

@router.get("/subscriptions/plans/{plan_id}")
async def get_subscription_plan(
    request: Request,
    plan_id: str,
    planner_service: SubscriptionPlannerService = Depends(get_planner_service)
) -> Dict[str, Any]:
    """Get subscription plan by ID"""
    correlation_id = getattr(request.state, 'correlation_id', 'unknown')
    start_time = time.time()
    
    logger.info(f"Getting subscription plan [correlation_id: {correlation_id}] - plan_id: {plan_id}")
    
    try:
        conn = await planner_service._get_connection()
        
        plan_row = await conn.fetchrow("""
            SELECT plan_id, user_id, plan_name, description, subscription_type, instruments,
                   optimization_level, filtering_strictness, status, estimated_cost,
                   performance_metrics, validation_results, created_at, expires_at, metadata
            FROM instrument_registry.subscription_plans
            WHERE plan_id = $1
        """, plan_id)
        
        if not plan_row:
            raise HTTPException(
                status_code=404,
                detail=f"Subscription plan {plan_id} not found"
            )
        
        import json
        plan_data = {
            "plan_id": plan_row['plan_id'],
            "user_id": plan_row['user_id'],
            "plan_name": plan_row['plan_name'],
            "description": plan_row['description'],
            "subscription_type": plan_row['subscription_type'],
            "instruments": json.loads(plan_row['instruments']),
            "optimization_level": plan_row['optimization_level'],
            "filtering_strictness": plan_row['filtering_strictness'],
            "status": plan_row['status'],
            "estimated_cost": plan_row['estimated_cost'],
            "performance_metrics": json.loads(plan_row['performance_metrics']),
            "validation_results": json.loads(plan_row['validation_results']),
            "created_at": plan_row['created_at'].isoformat(),
            "expires_at": plan_row['expires_at'].isoformat() if plan_row['expires_at'] else None,
            "metadata": json.loads(plan_row['metadata'])
        }
        
        duration = time.time() - start_time
        logger.info(f"Subscription plan retrieved in {duration:.3f}s [correlation_id: {correlation_id}]")
        
        return {
            "plan": plan_data,
            "correlation_id": correlation_id,
            "response_time_ms": int(duration * 1000)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting subscription plan [correlation_id: {correlation_id}]: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get subscription plan: {str(e)}"
        )
    finally:
        if 'conn' in locals():
            await planner_service._connection_pool.release(conn)

@router.get("/subscriptions/plans")
async def list_subscription_plans(
    request: Request,
    user_id: Optional[str] = Query(None, description="Filter by user ID"),
    subscription_type: Optional[str] = Query(None, description="Filter by subscription type"),
    status: Optional[str] = Query(None, description="Filter by plan status"),
    limit: int = Query(50, le=200, ge=1, description="Maximum number of plans to return"),
    offset: int = Query(0, ge=0, description="Number of plans to skip"),
    planner_service: SubscriptionPlannerService = Depends(get_planner_service)
) -> Dict[str, Any]:
    """List subscription plans with optional filters"""
    correlation_id = getattr(request.state, 'correlation_id', 'unknown')
    start_time = time.time()
    
    logger.info(f"Listing subscription plans [correlation_id: {correlation_id}] - user_id: {user_id}")
    
    try:
        conn = await planner_service._get_connection()
        
        # Build dynamic query
        where_clauses = []
        params = []
        param_count = 0
        
        if user_id:
            param_count += 1
            where_clauses.append(f"user_id = ${param_count}")
            params.append(user_id)
        
        if subscription_type:
            param_count += 1
            where_clauses.append(f"subscription_type = ${param_count}")
            params.append(subscription_type)
        
        if status:
            param_count += 1
            where_clauses.append(f"status = ${param_count}")
            params.append(status)
        
        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        
        query = f"""
            SELECT plan_id, user_id, plan_name, subscription_type, status, 
                   estimated_cost, created_at, expires_at
            FROM instrument_registry.subscription_plans
            {where_clause}
            ORDER BY created_at DESC
            LIMIT ${param_count + 1} OFFSET ${param_count + 2}
        """
        
        rows = await conn.fetch(query, *params, limit, offset)
        
        total_query = f"""
            SELECT COUNT(*) FROM instrument_registry.subscription_plans
            {where_clause}
        """
        total_count = await conn.fetchval(total_query, *params)
        
        plans = []
        for row in rows:
            plans.append({
                "plan_id": row['plan_id'],
                "user_id": row['user_id'],
                "plan_name": row['plan_name'],
                "subscription_type": row['subscription_type'],
                "status": row['status'],
                "estimated_cost": row['estimated_cost'],
                "created_at": row['created_at'].isoformat(),
                "expires_at": row['expires_at'].isoformat() if row['expires_at'] else None
            })
        
        duration = time.time() - start_time
        logger.info(f"Listed {len(plans)} subscription plans in {duration:.3f}s [correlation_id: {correlation_id}]")
        
        return {
            "plans": plans,
            "pagination": {
                "total": total_count,
                "limit": limit,
                "offset": offset,
                "has_next": offset + limit < total_count
            },
            "filters_applied": {
                "user_id": user_id,
                "subscription_type": subscription_type,
                "status": status
            },
            "correlation_id": correlation_id,
            "response_time_ms": int(duration * 1000)
        }
        
    except Exception as e:
        logger.error(f"Error listing subscription plans [correlation_id: {correlation_id}]: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list subscription plans: {str(e)}"
        )
    finally:
        if 'conn' in locals():
            await planner_service._connection_pool.release(conn)

# =========================================
# CONFIGURATION AND MONITORING ENDPOINTS
# =========================================

@router.get("/subscriptions/planner/config")
async def get_planner_configuration(
    request: Request,
    config_client: ConfigClient = Depends(get_config_client)
) -> Dict[str, Any]:
    """Get current planner configuration from config service"""
    correlation_id = getattr(request.state, 'correlation_id', 'unknown')
    start_time = time.time()
    
    logger.info(f"Getting planner configuration [correlation_id: {correlation_id}]")
    
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
                value = await config_client.get_secret(key, environment='prod')
                config_values[key] = value
            except Exception as e:
                logger.warning(f"Failed to get config {key}: {e}")
                config_values[key] = "unavailable"
        
        duration = time.time() - start_time
        logger.info(f"Planner configuration retrieved in {duration:.3f}s [correlation_id: {correlation_id}]")
        
        return {
            "configuration": config_values,
            "last_updated": datetime.utcnow().isoformat(),
            "correlation_id": correlation_id,
            "response_time_ms": int(duration * 1000)
        }
        
    except Exception as e:
        logger.error(f"Error getting planner configuration [correlation_id: {correlation_id}]: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get planner configuration: {str(e)}"
        )

@router.post("/subscriptions/planner/cleanup")
async def cleanup_expired_plans(
    request: Request,
    planner_service: SubscriptionPlannerService = Depends(get_planner_service)
) -> Dict[str, Any]:
    """Clean up expired subscription plans and cache entries"""
    correlation_id = getattr(request.state, 'correlation_id', 'unknown')
    start_time = time.time()
    
    logger.info(f"Cleaning up expired plans [correlation_id: {correlation_id}]")
    
    try:
        result = await planner_service.cleanup_expired_plans()
        
        duration = time.time() - start_time
        logger.info(f"Cleanup completed in {duration:.3f}s [correlation_id: {correlation_id}]")
        
        return {
            **result,
            "correlation_id": correlation_id,
            "response_time_ms": int(duration * 1000)
        }
        
    except Exception as e:
        logger.error(f"Error during cleanup [correlation_id: {correlation_id}]: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to cleanup expired plans: {str(e)}"
        )