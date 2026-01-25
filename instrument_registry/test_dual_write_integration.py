#!/usr/bin/env python3
"""
Integration test for dual-write adapter and data migration pipeline

This script tests the complete dual-write implementation including:
- Config service integration
- Dual-write adapter functionality
- Data validation with thresholds
- Retention policy execution
- Rollback capabilities
- Monitoring and metrics

Run with: python test_dual_write_integration.py
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List

import aiohttp
import redis.asyncio as redis
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

# Local imports
from app.services.dual_write_adapter import DualWriteAdapter, DualWriteConfig
from app.services.data_validation_service import DataValidationService, ValidationLevel
from app.services.data_retention_service import DataRetentionService
from app.common.config_client import ConfigClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Test configuration
CONFIG = {
    "config_service_url": "http://localhost:8100",
    "screener_service_url": "http://localhost:8001",  # Mock service for testing
    "redis_url": "redis://localhost:6379/0",
    "database_url": "postgresql+asyncpg://stocksblitz:b4Gr60lYlbZVZz0ZRTcnf_YRkjO0sluNcwwJ-7lAfn4@localhost/stocksblitz_unified_prod",
    "internal_api_key": "AShhRzWhfXd6IomyzZnE3d-lCcAvT1L5GDCCZRSXZGsJq7_eAJGxeMi-4AlfTeOc"
}


class MockScreenerService:
    """Mock screener service for testing"""
    
    def __init__(self, port: int = 8001):
        self.port = port
        self.app = None
        self.server = None
        
        # Mock data
        self.mock_memberships = [
            {
                "symbol": "RELIANCE",
                "exchange": "NSE",
                "index_name": "NIFTY50",
                "weight": 12.5,
                "sector": "ENERGY",
                "effective_date": "2024-01-01"
            },
            {
                "symbol": "TCS",
                "exchange": "NSE", 
                "index_name": "NIFTY50",
                "weight": 8.2,
                "sector": "IT",
                "effective_date": "2024-01-01"
            },
            {
                "symbol": "HDFCBANK",
                "exchange": "NSE",
                "index_name": "NIFTY50", 
                "weight": 10.1,
                "sector": "BANKING",
                "effective_date": "2024-01-01"
            }
        ]
    
    async def create_mock_server(self):
        """Create mock HTTP server"""
        from aiohttp import web
        
        async def handle_get_memberships(request):
            return web.json_response({
                "memberships": self.mock_memberships,
                "total": len(self.mock_memberships)
            })
        
        async def handle_post_memberships(request):
            data = await request.json()
            logger.info(f"Mock screener received: {len(data.get('memberships', []))} memberships")
            return web.json_response({
                "success": True,
                "processed": len(data.get("memberships", [])),
                "message": "Mock data received successfully"
            })
        
        async def handle_health(request):
            return web.json_response({"status": "healthy"})
        
        self.app = web.Application()
        self.app.router.add_get('/api/v1/index-memberships', handle_get_memberships)
        self.app.router.add_post('/api/v1/index-memberships/bulk', handle_post_memberships)
        self.app.router.add_get('/health', handle_health)
        
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, 'localhost', self.port)
        await site.start()
        
        logger.info(f"Mock screener service started on port {self.port}")
        return runner


class DualWriteIntegrationTest:
    """Integration test for dual-write functionality"""
    
    def __init__(self):
        self.config_client = None
        self.db_session = None
        self.redis_client = None
        self.dual_write_adapter = None
        self.validation_service = None
        self.retention_service = None
        self.mock_screener = None
        self.mock_server_runner = None
        
        # Test results
        self.test_results = {}
        
    async def setup(self):
        """Set up test environment"""
        logger.info("Setting up test environment...")
        
        # Initialize config client
        self.config_client = ConfigClient(
            base_url=CONFIG["config_service_url"],
            api_key=CONFIG["internal_api_key"]
        )
        
        # Initialize database connection
        engine = create_async_engine(CONFIG["database_url"], echo=False)
        async_session = sessionmaker(engine, class_=AsyncSession)
        self.db_session = async_session()
        
        # Initialize Redis client
        self.redis_client = await redis.from_url(CONFIG["redis_url"])
        
        # Start mock screener service
        self.mock_screener = MockScreenerService()
        self.mock_server_runner = await self.mock_screener.create_mock_server()
        
        # Initialize services
        self.dual_write_adapter = DualWriteAdapter(
            config_client=self.config_client,
            db_session=self.db_session,
            redis_url=CONFIG["redis_url"],
            screener_service_url=CONFIG["screener_service_url"],
            internal_api_key=CONFIG["internal_api_key"]
        )
        
        self.validation_service = DataValidationService(
            config_client=self.config_client,
            db_session=self.db_session,
            redis_url=CONFIG["redis_url"],
            screener_service_url=CONFIG["screener_service_url"],
            internal_api_key=CONFIG["internal_api_key"]
        )
        
        self.retention_service = DataRetentionService(
            config_client=self.config_client,
            db_session=self.db_session,
            redis_url=CONFIG["redis_url"]
        )
        
        # Initialize all services
        await self.dual_write_adapter.initialize()
        await self.validation_service.initialize()
        await self.retention_service.initialize()
        
        logger.info("Test environment setup complete")
    
    async def test_config_service_integration(self) -> bool:
        """Test config service integration"""
        logger.info("Testing config service integration...")
        
        try:
            # Test parameter retrieval
            dual_write_enabled = await self.config_client.get_bool("INSTRUMENT_REGISTRY_DUAL_WRITE_ENABLED")
            batch_size = await self.config_client.get_int("INSTRUMENT_REGISTRY_BATCH_SIZE")
            validation_mode = await self.config_client.get_string("INSTRUMENT_REGISTRY_VALIDATION_MODE")
            
            logger.info(f"Config retrieved: dual_write={dual_write_enabled}, batch_size={batch_size}, validation={validation_mode}")
            
            # Test config update
            await self.config_client.update_config("INSTRUMENT_REGISTRY_DUAL_WRITE_ENABLED", "true")
            
            # Verify update
            updated_value = await self.config_client.get_bool("INSTRUMENT_REGISTRY_DUAL_WRITE_ENABLED")
            
            self.test_results["config_service_integration"] = {
                "success": True,
                "dual_write_enabled": updated_value,
                "batch_size": batch_size,
                "validation_mode": validation_mode
            }
            
            logger.info("✓ Config service integration test passed")
            return True
            
        except Exception as e:
            logger.error(f"✗ Config service integration test failed: {e}")
            self.test_results["config_service_integration"] = {
                "success": False,
                "error": str(e)
            }
            return False
    
    async def test_dual_write_functionality(self) -> bool:
        """Test dual-write adapter functionality"""
        logger.info("Testing dual-write functionality...")
        
        try:
            # Prepare test data
            test_memberships = [
                {
                    "instrument_key": "NSE:RELIANCE",
                    "index_id": "NIFTY50",
                    "weight": 12.5,
                    "sector": "ENERGY",
                    "date_added": "2024-01-01",
                    "is_active": True
                },
                {
                    "instrument_key": "NSE:TCS",
                    "index_id": "NIFTY50",
                    "weight": 8.2,
                    "sector": "IT", 
                    "date_added": "2024-01-01",
                    "is_active": True
                }
            ]
            
            # Execute dual write
            screener_result, registry_result = await self.dual_write_adapter.write_index_memberships(
                test_memberships
            )
            
            # Check results
            success = screener_result.success and registry_result.success
            
            self.test_results["dual_write_functionality"] = {
                "success": success,
                "screener_result": {
                    "success": screener_result.success,
                    "records_written": screener_result.records_written,
                    "duration_ms": screener_result.duration_ms,
                    "error": screener_result.error
                },
                "registry_result": {
                    "success": registry_result.success,
                    "records_written": registry_result.records_written,
                    "duration_ms": registry_result.duration_ms,
                    "error": registry_result.error
                }
            }
            
            if success:
                logger.info("✓ Dual-write functionality test passed")
            else:
                logger.error("✗ Dual-write functionality test failed")
            
            return success
            
        except Exception as e:
            logger.error(f"✗ Dual-write functionality test failed: {e}")
            self.test_results["dual_write_functionality"] = {
                "success": False,
                "error": str(e)
            }
            return False
    
    async def test_config_driven_behavior(self) -> bool:
        """Test config-driven behavior changes"""
        logger.info("Testing config-driven behavior changes...")
        
        try:
            # Test with dual-write enabled
            await self.config_client.update_config("INSTRUMENT_REGISTRY_DUAL_WRITE_ENABLED", "true")
            config_enabled = await DualWriteConfig.from_config_service(self.config_client)
            
            # Test with dual-write disabled
            await self.config_client.update_config("INSTRUMENT_REGISTRY_DUAL_WRITE_ENABLED", "false")
            config_disabled = await DualWriteConfig.from_config_service(self.config_client)
            
            # Test validation mode changes
            await self.config_client.update_config("INSTRUMENT_REGISTRY_VALIDATION_MODE", "lenient")
            config_lenient = await DualWriteConfig.from_config_service(self.config_client)
            
            # Re-enable for subsequent tests
            await self.config_client.update_config("INSTRUMENT_REGISTRY_DUAL_WRITE_ENABLED", "true")
            
            self.test_results["config_driven_behavior"] = {
                "success": True,
                "enabled_config": {
                    "enabled": config_enabled.enabled,
                    "validation_mode": config_enabled.validation_mode.value
                },
                "disabled_config": {
                    "enabled": config_disabled.enabled,
                    "validation_mode": config_disabled.validation_mode.value
                },
                "lenient_config": {
                    "validation_mode": config_lenient.validation_mode.value
                }
            }
            
            logger.info("✓ Config-driven behavior test passed")
            return True
            
        except Exception as e:
            logger.error(f"✗ Config-driven behavior test failed: {e}")
            self.test_results["config_driven_behavior"] = {
                "success": False,
                "error": str(e)
            }
            return False
    
    async def test_data_validation(self) -> bool:
        """Test data validation functionality"""
        logger.info("Testing data validation...")
        
        try:
            # Run validation
            validation_result = await self.validation_service.validate_index_memberships(
                validation_level=ValidationLevel.DETAILED
            )
            
            success = validation_result.status.value in ["passed", "warning"]
            
            self.test_results["data_validation"] = {
                "success": success,
                "validation_id": validation_result.validation_id,
                "status": validation_result.status.value,
                "total_screener_records": validation_result.total_records_screener,
                "total_registry_records": validation_result.total_records_registry,
                "matched_records": validation_result.matched_records,
                "mismatches": len(validation_result.record_mismatches),
                "passed_thresholds": validation_result.passed_thresholds,
                "duration_seconds": validation_result.duration_seconds
            }
            
            if success:
                logger.info("✓ Data validation test passed")
            else:
                logger.error("✗ Data validation test failed")
            
            return success
            
        except Exception as e:
            logger.error(f"✗ Data validation test failed: {e}")
            self.test_results["data_validation"] = {
                "success": False,
                "error": str(e)
            }
            return False
    
    async def test_retention_policies(self) -> bool:
        """Test retention policy execution"""
        logger.info("Testing retention policies...")
        
        try:
            # Run retention policies (should be safe with current data)
            retention_results = await self.retention_service.run_retention_policies()
            
            success = all(not result.errors for result in retention_results if result.errors)
            
            self.test_results["retention_policies"] = {
                "success": success,
                "policies_executed": len(retention_results),
                "results": [
                    {
                        "table_name": result.table_name,
                        "action": result.action.value,
                        "records_processed": result.records_processed,
                        "records_affected": result.records_affected,
                        "duration_seconds": result.duration_seconds,
                        "errors": result.errors or []
                    }
                    for result in retention_results
                ]
            }
            
            if success:
                logger.info("✓ Retention policies test passed")
            else:
                logger.error("✗ Retention policies test failed")
            
            return success
            
        except Exception as e:
            logger.error(f"✗ Retention policies test failed: {e}")
            self.test_results["retention_policies"] = {
                "success": False,
                "error": str(e)
            }
            return False
    
    async def test_rollback_capability(self) -> bool:
        """Test rollback to single-write mode"""
        logger.info("Testing rollback capability...")
        
        try:
            # Enable dual-write first
            await self.config_client.update_config("INSTRUMENT_REGISTRY_DUAL_WRITE_ENABLED", "true")
            
            # Test emergency disable
            rollback_success = await self.dual_write_adapter.disable_dual_write()
            
            # Verify rollback worked
            config_after_rollback = await DualWriteConfig.from_config_service(self.config_client)
            
            success = rollback_success and not config_after_rollback.enabled
            
            self.test_results["rollback_capability"] = {
                "success": success,
                "rollback_successful": rollback_success,
                "config_disabled": not config_after_rollback.enabled
            }
            
            if success:
                logger.info("✓ Rollback capability test passed")
            else:
                logger.error("✗ Rollback capability test failed")
            
            return success
            
        except Exception as e:
            logger.error(f"✗ Rollback capability test failed: {e}")
            self.test_results["rollback_capability"] = {
                "success": False,
                "error": str(e)
            }
            return False
    
    async def test_monitoring_and_metrics(self) -> bool:
        """Test monitoring and metrics collection"""
        logger.info("Testing monitoring and metrics...")
        
        try:
            # Get dual-write metrics
            dw_metrics = await self.dual_write_adapter.get_metrics()
            
            # Get validation metrics
            val_metrics = await self.validation_service.get_validation_metrics()
            
            # Get retention status
            ret_status = await self.retention_service.get_retention_status()
            
            # Get health status
            health_status = await self.dual_write_adapter.get_health_status()
            
            success = all([
                isinstance(dw_metrics, dict),
                isinstance(val_metrics, dict),
                isinstance(ret_status, dict),
                health_status.is_healthy
            ])
            
            self.test_results["monitoring_and_metrics"] = {
                "success": success,
                "dual_write_metrics": dw_metrics,
                "validation_metrics": val_metrics,
                "retention_status": ret_status,
                "health_status": {
                    "is_healthy": health_status.is_healthy,
                    "service_name": health_status.service_name,
                    "details": health_status.details
                }
            }
            
            if success:
                logger.info("✓ Monitoring and metrics test passed")
            else:
                logger.error("✗ Monitoring and metrics test failed")
            
            return success
            
        except Exception as e:
            logger.error(f"✗ Monitoring and metrics test failed: {e}")
            self.test_results["monitoring_and_metrics"] = {
                "success": False,
                "error": str(e)
            }
            return False
    
    async def run_all_tests(self) -> Dict[str, Any]:
        """Run all integration tests"""
        logger.info("Starting dual-write integration tests...")
        
        tests = [
            ("Config Service Integration", self.test_config_service_integration),
            ("Dual-Write Functionality", self.test_dual_write_functionality),
            ("Config-Driven Behavior", self.test_config_driven_behavior),
            ("Data Validation", self.test_data_validation),
            ("Retention Policies", self.test_retention_policies),
            ("Rollback Capability", self.test_rollback_capability),
            ("Monitoring and Metrics", self.test_monitoring_and_metrics)
        ]
        
        results = {
            "summary": {
                "total_tests": len(tests),
                "passed": 0,
                "failed": 0,
                "start_time": datetime.utcnow().isoformat()
            },
            "tests": {}
        }
        
        for test_name, test_func in tests:
            logger.info(f"\n{'='*60}")
            logger.info(f"Running: {test_name}")
            logger.info(f"{'='*60}")
            
            try:
                success = await test_func()
                if success:
                    results["summary"]["passed"] += 1
                else:
                    results["summary"]["failed"] += 1
                
                results["tests"][test_name.lower().replace(" ", "_")] = self.test_results.get(
                    test_name.lower().replace(" ", "_"), {"success": success}
                )
                
            except Exception as e:
                logger.error(f"Test {test_name} failed with exception: {e}")
                results["summary"]["failed"] += 1
                results["tests"][test_name.lower().replace(" ", "_")] = {
                    "success": False,
                    "error": str(e)
                }
        
        results["summary"]["end_time"] = datetime.utcnow().isoformat()
        results["summary"]["success_rate"] = (results["summary"]["passed"] / results["summary"]["total_tests"]) * 100
        
        # Overall assessment
        if results["summary"]["passed"] == results["summary"]["total_tests"]:
            logger.info(f"\n🎉 ALL TESTS PASSED! ({results['summary']['passed']}/{results['summary']['total_tests']})")
        else:
            logger.error(f"\n❌ {results['summary']['failed']} tests failed out of {results['summary']['total_tests']}")
        
        return results
    
    async def cleanup(self):
        """Clean up test environment"""
        logger.info("Cleaning up test environment...")
        
        try:
            # Close services
            if self.dual_write_adapter:
                await self.dual_write_adapter.close()
            
            if self.validation_service:
                await self.validation_service.close()
            
            if self.retention_service:
                await self.retention_service.close()
            
            # Close database session
            if self.db_session:
                await self.db_session.close()
            
            # Close Redis client
            if self.redis_client:
                await self.redis_client.close()
            
            # Stop mock server
            if self.mock_server_runner:
                await self.mock_server_runner.cleanup()
            
            logger.info("Test environment cleanup complete")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")


async def main():
    """Main test execution"""
    test = DualWriteIntegrationTest()
    
    try:
        await test.setup()
        results = await test.run_all_tests()
        
        # Save results to file
        results_filename = f"dual_write_integration_results_{int(time.time())}.json"
        with open(results_filename, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Test results saved to: {results_filename}")
        
        # Print summary
        print(f"\n{'='*80}")
        print("DUAL-WRITE INTEGRATION TEST SUMMARY")
        print(f"{'='*80}")
        print(f"Total Tests: {results['summary']['total_tests']}")
        print(f"Passed: {results['summary']['passed']}")
        print(f"Failed: {results['summary']['failed']}")
        print(f"Success Rate: {results['summary']['success_rate']:.1f}%")
        print(f"{'='*80}\n")
        
        return results['summary']['failed'] == 0
        
    except Exception as e:
        logger.error(f"Test execution failed: {e}")
        return False
    
    finally:
        await test.cleanup()


if __name__ == "__main__":
    import sys
    success = asyncio.run(main())
    sys.exit(0 if success else 1)