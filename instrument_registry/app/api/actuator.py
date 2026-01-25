"""
Actuator endpoints for manual service triggers and monitoring

These endpoints allow operations teams to manually trigger dual-write services
and get real-time status information for troubleshooting and maintenance.
"""

import logging
import time
from datetime import datetime
from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from common.auth_middleware import verify_internal_token

logger = logging.getLogger(__name__)

# Create router with authentication dependency
router = APIRouter(
    prefix="/actuator",
    tags=["actuator"],
    dependencies=[Depends(verify_internal_token)]
)

# Global service references (set by main.py)
dual_write_adapter = None
validation_service = None
retention_service = None
monitoring_service = None

def set_service_references(dw_adapter, val_service, ret_service, mon_service):
    """Set global service references from main.py"""
    global dual_write_adapter, validation_service, retention_service, monitoring_service
    dual_write_adapter = dw_adapter
    validation_service = val_service
    retention_service = ret_service
    monitoring_service = mon_service


# =========================================
# REQUEST/RESPONSE MODELS
# =========================================

class ValidationRequest(BaseModel):
    """Request model for validation operations"""
    validation_level: str = "detailed"  # basic, detailed, strict
    batch_size: int = None


class RetentionRequest(BaseModel):
    """Request model for retention operations"""
    dry_run: bool = True
    force_cleanup: bool = False


# =========================================
# DUAL-WRITE ACTUATOR ENDPOINTS
# =========================================

@router.get("/dual-write/status")
async def get_dual_write_status():
    """Get current dual-write adapter status and health"""
    if not dual_write_adapter:
        raise HTTPException(status_code=503, detail="Dual-write adapter not initialized")
    
    try:
        health_status = await dual_write_adapter.get_health_status()
        metrics = await dual_write_adapter.get_metrics()
        
        return {
            "service": "dual_write_adapter",
            "health": {
                "is_healthy": health_status.is_healthy,
                "details": health_status.details
            },
            "metrics": metrics,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get dual-write status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get status: {str(e)}")


@router.post("/dual-write/disable")
async def emergency_disable_dual_write():
    """Emergency disable dual-write mode (rollback to single-write)"""
    if not dual_write_adapter:
        raise HTTPException(status_code=503, detail="Dual-write adapter not initialized")
    
    try:
        success = await dual_write_adapter.disable_dual_write()
        
        if success:
            logger.info("Dual-write emergency disabled via actuator endpoint")
            return {
                "success": True,
                "message": "Dual-write disabled successfully",
                "timestamp": datetime.utcnow().isoformat()
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to disable dual-write")
            
    except Exception as e:
        logger.error(f"Failed to disable dual-write: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to disable dual-write: {str(e)}")


# =========================================
# VALIDATION ACTUATOR ENDPOINTS
# =========================================

@router.get("/validation/status")
async def get_validation_status():
    """Get current validation service status"""
    if not validation_service:
        raise HTTPException(status_code=503, detail="Validation service not initialized")
    
    try:
        metrics = await validation_service.get_validation_metrics()
        recent_validations = await validation_service.get_recent_validations(limit=5)
        
        return {
            "service": "data_validation",
            "metrics": metrics,
            "recent_validations": recent_validations,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get validation status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get status: {str(e)}")


@router.post("/validation/run")
async def trigger_validation(request: ValidationRequest):
    """Manually trigger data validation"""
    if not validation_service:
        raise HTTPException(status_code=503, detail="Validation service not initialized")
    
    try:
        from app.services.data_validation_service import ValidationLevel
        
        # Parse validation level
        validation_level_map = {
            "basic": ValidationLevel.BASIC,
            "detailed": ValidationLevel.DETAILED,
            "strict": ValidationLevel.STRICT
        }
        
        level = validation_level_map.get(request.validation_level, ValidationLevel.DETAILED)
        
        logger.info(f"Manual validation triggered via actuator - level: {request.validation_level}")
        
        # Run validation
        result = await validation_service.validate_index_memberships(
            validation_level=level,
            batch_size=request.batch_size
        )
        
        return {
            "validation_id": result.validation_id,
            "status": result.status.value,
            "validation_level": result.validation_level.value,
            "duration_seconds": result.duration_seconds,
            "summary": {
                "total_screener_records": result.total_records_screener,
                "total_registry_records": result.total_records_registry,
                "matched_records": result.matched_records,
                "missing_in_screener": result.missing_in_screener,
                "missing_in_registry": result.missing_in_registry,
                "passed_thresholds": result.passed_thresholds
            },
            "threshold_violations": result.threshold_violations,
            "field_mismatches": result.field_mismatch_summary,
            "timestamp": result.timestamp.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to run validation: {e}")
        raise HTTPException(status_code=500, detail=f"Validation failed: {str(e)}")


@router.get("/validation/result/{validation_id}")
async def get_validation_result(validation_id: str):
    """Get detailed validation result by ID"""
    if not validation_service:
        raise HTTPException(status_code=503, detail="Validation service not initialized")
    
    try:
        result = await validation_service.get_validation_result(validation_id)
        
        if not result:
            raise HTTPException(status_code=404, detail=f"Validation result not found: {validation_id}")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get validation result: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get result: {str(e)}")


# =========================================
# RETENTION ACTUATOR ENDPOINTS
# =========================================

@router.get("/retention/status")
async def get_retention_status():
    """Get current retention service status"""
    if not retention_service:
        raise HTTPException(status_code=503, detail="Retention service not initialized")
    
    try:
        status = await retention_service.get_retention_status()
        recent_results = await retention_service.get_recent_retention_results(limit=5)
        
        return {
            "service": "data_retention",
            "status": status,
            "recent_results": recent_results,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get retention status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get status: {str(e)}")


@router.post("/retention/run")
async def trigger_retention(request: RetentionRequest):
    """Manually trigger data retention policies"""
    if not retention_service:
        raise HTTPException(status_code=503, detail="Retention service not initialized")
    
    try:
        if request.dry_run:
            logger.info("Dry-run retention triggered via actuator")
            # For dry run, just return what would be done
            return {
                "dry_run": True,
                "message": "Dry run mode - no actual retention performed",
                "note": "In production, retention policies would be applied here",
                "timestamp": datetime.utcnow().isoformat()
            }
        else:
            if not request.force_cleanup:
                raise HTTPException(
                    status_code=400, 
                    detail="force_cleanup must be true for non-dry-run retention"
                )
            
            logger.warning("FORCED retention triggered via actuator - this will delete data!")
            
            # Run actual retention
            results = await retention_service.run_retention_policies()
            
            return {
                "dry_run": False,
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
                    "total_records_affected": sum(r.records_affected for r in results),
                    "total_duration_seconds": sum(r.duration_seconds for r in results)
                },
                "timestamp": datetime.utcnow().isoformat()
            }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to run retention: {e}")
        raise HTTPException(status_code=500, detail=f"Retention failed: {str(e)}")


# =========================================
# MONITORING ACTUATOR ENDPOINTS
# =========================================

@router.get("/monitoring/status")
async def get_monitoring_status():
    """Get current monitoring service status"""
    if not monitoring_service:
        raise HTTPException(status_code=503, detail="Monitoring service not initialized")
    
    try:
        metrics_summary = await monitoring_service.get_metrics_summary()
        recent_alerts = await monitoring_service.get_recent_alerts(limit=10)
        sla_metrics = await monitoring_service.get_sla_metrics()
        
        return {
            "service": "monitoring",
            "metrics_summary": metrics_summary,
            "recent_alerts": recent_alerts,
            "sla_metrics": sla_metrics,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get monitoring status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get status: {str(e)}")


@router.get("/monitoring/alerts")
async def get_alerts(limit: int = Query(50, ge=1, le=1000)):
    """Get recent alerts from monitoring service"""
    if not monitoring_service:
        raise HTTPException(status_code=503, detail="Monitoring service not initialized")
    
    try:
        alerts = await monitoring_service.get_recent_alerts(limit=limit)
        
        return {
            "alerts": alerts,
            "total_returned": len(alerts),
            "limit": limit,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get alerts: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get alerts: {str(e)}")


# =========================================
# COMPREHENSIVE STATUS ENDPOINT
# =========================================

@router.get("/status")
async def get_comprehensive_status():
    """Get comprehensive status of all dual-write services"""
    
    status = {
        "service": "dual_write_infrastructure", 
        "timestamp": datetime.utcnow().isoformat(),
        "services": {}
    }
    
    # Check each service
    services_info = [
        ("dual_write_adapter", dual_write_adapter),
        ("validation_service", validation_service),
        ("retention_service", retention_service),
        ("monitoring_service", monitoring_service)
    ]
    
    overall_healthy = True
    
    for service_name, service_instance in services_info:
        if service_instance is None:
            status["services"][service_name] = {
                "initialized": False,
                "healthy": False,
                "error": "Service not initialized"
            }
            overall_healthy = False
        else:
            try:
                if hasattr(service_instance, 'get_health_status'):
                    health = await service_instance.get_health_status()
                    status["services"][service_name] = {
                        "initialized": True,
                        "healthy": health.is_healthy,
                        "details": health.details
                    }
                    if not health.is_healthy:
                        overall_healthy = False
                else:
                    # Service doesn't have health check, assume healthy if initialized
                    status["services"][service_name] = {
                        "initialized": True,
                        "healthy": True,
                        "details": {"note": "Service initialized successfully"}
                    }
            except Exception as e:
                status["services"][service_name] = {
                    "initialized": True,
                    "healthy": False,
                    "error": str(e)
                }
                overall_healthy = False
    
    status["overall_healthy"] = overall_healthy
    status["summary"] = {
        "total_services": len(services_info),
        "healthy_services": len([s for s in status["services"].values() if s.get("healthy", False)]),
        "initialized_services": len([s for s in status["services"].values() if s.get("initialized", False)])
    }
    
    return status


# =========================================
# EMERGENCY ENDPOINTS
# =========================================

@router.post("/emergency/reset")
async def emergency_reset(
    confirm: bool = Query(False, description="Must be true to confirm reset")
):
    """Emergency reset of dual-write infrastructure (USE WITH CAUTION)"""
    
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail="Emergency reset requires explicit confirmation via ?confirm=true"
        )
    
    logger.critical("EMERGENCY RESET triggered via actuator - disabling dual-write mode")
    
    actions_taken = []
    errors = []
    
    # Disable dual-write
    if dual_write_adapter:
        try:
            success = await dual_write_adapter.disable_dual_write()
            if success:
                actions_taken.append("Dual-write mode disabled")
            else:
                errors.append("Failed to disable dual-write mode")
        except Exception as e:
            errors.append(f"Error disabling dual-write: {str(e)}")
    
    # Clear alert cooldowns if monitoring service available
    if monitoring_service:
        try:
            # Reset alert cooldowns
            monitoring_service.alert_cooldowns.clear()
            actions_taken.append("Alert cooldowns cleared")
        except Exception as e:
            errors.append(f"Error clearing alert cooldowns: {str(e)}")
    
    return {
        "emergency_reset": True,
        "actions_taken": actions_taken,
        "errors": errors,
        "timestamp": datetime.utcnow().isoformat(),
        "warning": "Emergency reset completed. Review system status before resuming normal operations."
    }