#!/usr/bin/env python3
"""
Production Readiness Verification Script

This script verifies that the dual-write services are properly integrated 
into the runtime and can execute in a production environment.

Run with: python3 verify_production_readiness.py
"""

import asyncio
import json
import logging
import sys
from datetime import datetime
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProductionReadinessChecker:
    """Verify production readiness of dual-write integration"""
    
    def __init__(self):
        self.checks = []
        self.results = {}
    
    def add_check(self, name: str, description: str):
        """Decorator to register a check"""
        def decorator(func):
            self.checks.append({
                "name": name,
                "description": description,
                "func": func
            })
            return func
        return decorator
    
    async def run_all_checks(self) -> Dict[str, Any]:
        """Run all registered checks"""
        logger.info("Starting production readiness verification...")
        
        results = {
            "timestamp": datetime.utcnow().isoformat(),
            "checks": {},
            "summary": {
                "total": len(self.checks),
                "passed": 0,
                "failed": 0,
                "warnings": 0
            }
        }
        
        for check in self.checks:
            check_name = check["name"]
            logger.info(f"Running check: {check_name}")
            
            try:
                result = await check["func"]()
                if result["status"] == "passed":
                    results["summary"]["passed"] += 1
                    logger.info(f"✅ {check_name}: PASSED")
                elif result["status"] == "warning":
                    results["summary"]["warnings"] += 1
                    logger.warning(f"⚠️ {check_name}: WARNING - {result.get('message', '')}")
                else:
                    results["summary"]["failed"] += 1
                    logger.error(f"❌ {check_name}: FAILED - {result.get('error', '')}")
                
                results["checks"][check_name] = result
                
            except Exception as e:
                results["summary"]["failed"] += 1
                error_result = {
                    "status": "failed",
                    "error": str(e),
                    "description": check["description"]
                }
                results["checks"][check_name] = error_result
                logger.error(f"❌ {check_name}: ERROR - {e}")
        
        return results


# Create global checker instance
checker = ProductionReadinessChecker()


@checker.add_check("service_imports", "All dual-write services can be imported")
async def check_service_imports():
    """Verify all services can be imported"""
    try:
        from app.services.dual_write_adapter import DualWriteAdapter
        from app.services.data_validation_service import DataValidationService
        from app.services.data_retention_service import DataRetentionService
        from app.services.monitoring_service import MonitoringService
        
        return {
            "status": "passed",
            "message": "All dual-write services imported successfully"
        }
    except ImportError as e:
        return {
            "status": "failed",
            "error": f"Import failed: {e}"
        }


@checker.add_check("main_integration", "Services are integrated into main.py")
async def check_main_integration():
    """Verify services are properly integrated in main.py"""
    try:
        # Read main.py and check for service imports
        with open('/home/stocksadmin/instrument_registry/main.py', 'r') as f:
            main_content = f.read()
        
        required_imports = [
            "from app.services.dual_write_adapter import DualWriteAdapter",
            "from app.services.data_validation_service import DataValidationService",
            "from app.services.data_retention_service import DataRetentionService", 
            "from app.services.monitoring_service import MonitoringService"
        ]
        
        missing_imports = []
        for import_line in required_imports:
            if import_line not in main_content:
                missing_imports.append(import_line)
        
        if missing_imports:
            return {
                "status": "failed",
                "error": f"Missing imports in main.py: {missing_imports}"
            }
        
        # Check for service initialization
        required_patterns = [
            "dual_write_adapter = DualWriteAdapter",
            "validation_service = DataValidationService", 
            "retention_service = DataRetentionService",
            "monitoring_service = MonitoringService"
        ]
        
        missing_patterns = []
        for pattern in required_patterns:
            if pattern not in main_content:
                missing_patterns.append(pattern)
        
        if missing_patterns:
            return {
                "status": "failed", 
                "error": f"Missing service initialization patterns: {missing_patterns}"
            }
        
        return {
            "status": "passed",
            "message": "All services properly integrated into main.py"
        }
        
    except Exception as e:
        return {
            "status": "failed",
            "error": f"Error checking main.py: {e}"
        }


@checker.add_check("background_tasks", "Background tasks are configured")
async def check_background_tasks():
    """Verify background tasks are properly configured"""
    try:
        with open('/home/stocksadmin/instrument_registry/main.py', 'r') as f:
            main_content = f.read()
        
        required_tasks = [
            "periodic_validation",
            "periodic_retention", 
            "periodic_health_monitoring"
        ]
        
        missing_tasks = []
        for task in required_tasks:
            if f"async def {task}" not in main_content:
                missing_tasks.append(task)
        
        if missing_tasks:
            return {
                "status": "failed",
                "error": f"Missing background task definitions: {missing_tasks}"
            }
        
        # Check that tasks are started
        if "asyncio.create_task" not in main_content:
            return {
                "status": "failed",
                "error": "Background tasks are not being started with asyncio.create_task"
            }
        
        return {
            "status": "passed",
            "message": "All background tasks properly configured"
        }
        
    except Exception as e:
        return {
            "status": "failed",
            "error": f"Error checking background tasks: {e}"
        }


@checker.add_check("actuator_endpoints", "Actuator endpoints are available") 
async def check_actuator_endpoints():
    """Verify actuator endpoints are properly configured"""
    try:
        from app.api.actuator import router
        
        # Check expected endpoints exist
        route_paths = [route.path for route in router.routes]
        
        required_endpoints = [
            "/actuator/dual-write/status",
            "/actuator/dual-write/disable", 
            "/actuator/validation/run",
            "/actuator/retention/run",
            "/actuator/monitoring/status",
            "/actuator/status"
        ]
        
        missing_endpoints = []
        for endpoint in required_endpoints:
            if endpoint not in route_paths:
                missing_endpoints.append(endpoint)
        
        if missing_endpoints:
            return {
                "status": "failed",
                "error": f"Missing actuator endpoints: {missing_endpoints}"
            }
        
        # Check that actuator is included in main app
        with open('/home/stocksadmin/instrument_registry/main.py', 'r') as f:
            main_content = f.read()
        
        if "from app.api.actuator import router as actuator_router" not in main_content:
            return {
                "status": "failed",
                "error": "Actuator router not imported in main.py"
            }
        
        if "app.include_router(actuator_router)" not in main_content:
            return {
                "status": "failed",
                "error": "Actuator router not included in main app"
            }
        
        return {
            "status": "passed",
            "message": f"All {len(required_endpoints)} actuator endpoints configured"
        }
        
    except Exception as e:
        return {
            "status": "failed",
            "error": f"Error checking actuator endpoints: {e}"
        }


@checker.add_check("config_service_integration", "Config service integration implemented")
async def check_config_service_integration():
    """Verify config service integration is implemented"""
    try:
        from app.services.dual_write_adapter import DualWriteConfig
        from app.services.data_retention_service import RetentionConfig
        from app.services.data_validation_service import ValidationThresholds
        from app.services.monitoring_service import MonitoringConfig
        
        # Check that all config classes have from_config_service methods
        config_classes = [DualWriteConfig, RetentionConfig, ValidationThresholds, MonitoringConfig]
        
        for config_class in config_classes:
            if not hasattr(config_class, 'from_config_service'):
                return {
                    "status": "failed",
                    "error": f"{config_class.__name__} missing from_config_service method"
                }
        
        return {
            "status": "passed",
            "message": "All config service integrations implemented"
        }
        
    except Exception as e:
        return {
            "status": "failed",
            "error": f"Error checking config service integration: {e}"
        }


@checker.add_check("error_handling", "Error handling and graceful degradation")
async def check_error_handling():
    """Verify error handling is implemented"""
    try:
        # Check that services handle config failures gracefully
        from app.services.dual_write_adapter import DualWriteConfig
        from unittest.mock import AsyncMock
        
        # Mock failing config client
        mock_config = AsyncMock()
        mock_config.get_bool.side_effect = Exception("Config unavailable")
        
        # Should return safe defaults
        config = await DualWriteConfig.from_config_service(mock_config)
        
        # Verify safe defaults are used
        if config.enabled != False:  # Should default to disabled for safety
            return {
                "status": "warning",
                "message": "Dual-write defaults to enabled on config failure (safety concern)"
            }
        
        return {
            "status": "passed",
            "message": "Error handling implemented with safe defaults"
        }
        
    except Exception as e:
        return {
            "status": "failed",
            "error": f"Error testing error handling: {e}"
        }


@checker.add_check("schema_enforcement", "Database schema boundaries enforced")
async def check_schema_enforcement():
    """Verify schema boundary enforcement is implemented"""
    try:
        # Check that schema enforcement patterns are used
        service_files = [
            '/home/stocksadmin/instrument_registry/app/services/dual_write_adapter.py',
            '/home/stocksadmin/instrument_registry/app/services/data_retention_service.py'
        ]
        
        for file_path in service_files:
            try:
                with open(file_path, 'r') as f:
                    content = f.read()
                
                # Check for schema-aware SQL operations
                if "instrument_registry." not in content:
                    return {
                        "status": "warning",
                        "message": f"{file_path} may not be using schema-qualified table names"
                    }
            except FileNotFoundError:
                continue
        
        return {
            "status": "passed",
            "message": "Schema enforcement patterns detected"
        }
        
    except Exception as e:
        return {
            "status": "failed",
            "error": f"Error checking schema enforcement: {e}"
        }


@checker.add_check("monitoring_integration", "Prometheus monitoring integration")
async def check_monitoring_integration():
    """Verify Prometheus monitoring is integrated"""
    try:
        from app.services.monitoring_service import MonitoringService
        
        # Create mock service to verify metrics structure
        from unittest.mock import AsyncMock
        mock_config = AsyncMock()
        
        monitoring = MonitoringService(
            config_client=mock_config,
            redis_url="redis://localhost:6379"
        )
        
        # Verify metrics registry exists
        if not hasattr(monitoring, 'registry'):
            return {
                "status": "failed",
                "error": "MonitoringService missing Prometheus registry"
            }
        
        # Verify key metrics are defined
        required_metrics = [
            "dual_write_operations_total",
            "validation_operations_total",
            "system_health_status"
        ]
        
        missing_metrics = []
        for metric_name in required_metrics:
            if metric_name not in monitoring.metrics:
                missing_metrics.append(metric_name)
        
        if missing_metrics:
            return {
                "status": "failed",
                "error": f"Missing required metrics: {missing_metrics}"
            }
        
        return {
            "status": "passed",
            "message": f"Prometheus integration with {len(monitoring.metrics)} metrics"
        }
        
    except Exception as e:
        return {
            "status": "failed",
            "error": f"Error checking monitoring integration: {e}"
        }


@checker.add_check("service_lifecycle", "Service lifecycle management")
async def check_service_lifecycle():
    """Verify service lifecycle is properly managed"""
    try:
        with open('/home/stocksadmin/instrument_registry/main.py', 'r') as f:
            main_content = f.read()
        
        # Check for proper initialization
        if "await dual_write_adapter.initialize()" not in main_content:
            return {
                "status": "failed",
                "error": "Dual-write adapter initialization missing"
            }
        
        if "await validation_service.initialize()" not in main_content:
            return {
                "status": "failed",
                "error": "Validation service initialization missing"
            }
        
        # Check for proper cleanup
        if "await dual_write_adapter.close()" not in main_content:
            return {
                "status": "failed",
                "error": "Dual-write adapter cleanup missing"
            }
        
        # Check for background task management
        if "task.cancel()" not in main_content:
            return {
                "status": "failed",
                "error": "Background task cancellation missing"
            }
        
        return {
            "status": "passed",
            "message": "Service lifecycle properly managed"
        }
        
    except Exception as e:
        return {
            "status": "failed",
            "error": f"Error checking service lifecycle: {e}"
        }


async def main():
    """Main verification routine"""
    print("🔍 Production Readiness Verification for Dual-Write Integration")
    print("=" * 70)
    
    # Run all checks
    results = await checker.run_all_checks()
    
    # Print summary
    print("\n" + "=" * 70)
    print("📊 VERIFICATION SUMMARY")
    print("=" * 70)
    
    summary = results["summary"]
    total = summary["total"]
    passed = summary["passed"]
    warnings = summary["warnings"]
    failed = summary["failed"]
    
    print(f"Total Checks: {total}")
    print(f"✅ Passed: {passed}")
    if warnings > 0:
        print(f"⚠️ Warnings: {warnings}")
    if failed > 0:
        print(f"❌ Failed: {failed}")
    
    # Calculate overall status
    if failed == 0 and warnings == 0:
        print("\n🎉 ALL CHECKS PASSED - PRODUCTION READY!")
        overall_status = "PRODUCTION_READY"
    elif failed == 0 and warnings > 0:
        print("\n⚠️ READY WITH WARNINGS - Review warnings before deployment")
        overall_status = "READY_WITH_WARNINGS"
    else:
        print("\n❌ NOT READY - Address failed checks before deployment")
        overall_status = "NOT_READY"
    
    # Print detailed results for failed/warning checks
    if failed > 0 or warnings > 0:
        print("\n📋 DETAILED RESULTS:")
        print("-" * 40)
        
        for check_name, check_result in results["checks"].items():
            if check_result["status"] in ["failed", "warning"]:
                status_icon = "❌" if check_result["status"] == "failed" else "⚠️"
                print(f"{status_icon} {check_name}:")
                if "error" in check_result:
                    print(f"   Error: {check_result['error']}")
                if "message" in check_result:
                    print(f"   Message: {check_result['message']}")
                print()
    
    # Save results
    results_file = f"production_readiness_verification_{int(datetime.utcnow().timestamp())}.json"
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"📄 Detailed results saved to: {results_file}")
    
    # Return appropriate exit code
    return failed == 0


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)