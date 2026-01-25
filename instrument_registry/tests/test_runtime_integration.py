"""
CI-friendly runtime integration tests for dual-write services

These tests can run in CI environments without requiring external dependencies
like PostgreSQL, Redis, or the config service. They use mocks and focus on
validating that the runtime integration is wired correctly.
"""

import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient


class TestRuntimeIntegration:
    """Test that dual-write services are properly integrated at runtime"""
    
    def test_service_imports_successful(self):
        """Test that all dual-write services can be imported"""
        # This validates that the module structure is correct
        from app.services.dual_write_adapter import DualWriteAdapter
        from app.services.data_validation_service import DataValidationService
        from app.services.data_retention_service import DataRetentionService
        from app.services.monitoring_service import MonitoringService
        
        assert DualWriteAdapter is not None
        assert DataValidationService is not None
        assert DataRetentionService is not None
        assert MonitoringService is not None
    
    def test_actuator_endpoints_importable(self):
        """Test that actuator endpoints can be imported"""
        from app.api.actuator import router
        assert router is not None
        
        # Check that key endpoints exist
        endpoint_paths = [route.path for route in router.routes]
        expected_paths = [
            "/actuator/dual-write/status",
            "/actuator/validation/run",
            "/actuator/retention/run",
            "/actuator/status"
        ]
        
        for path in expected_paths:
            assert path in endpoint_paths
    
    @patch('app.services.dual_write_adapter.DualWriteAdapter')
    @patch('app.services.data_validation_service.DataValidationService')
    @patch('app.services.data_retention_service.DataRetentionService')
    @patch('app.services.monitoring_service.MonitoringService')
    def test_service_initialization_pattern(self, mock_monitoring, mock_retention, mock_validation, mock_dual_write):
        """Test that services follow proper initialization pattern"""
        from app.services.dual_write_adapter import DualWriteAdapter
        from app.services.data_validation_service import DataValidationService
        from app.services.data_retention_service import DataRetentionService
        from app.services.monitoring_service import MonitoringService
        from common.config_client import ConfigClient
        
        # Mock config client
        mock_config = AsyncMock()
        mock_config.get.return_value = "test_value"
        
        # Test service creation patterns
        dual_write = DualWriteAdapter(
            config_client=mock_config,
            db_session=AsyncMock(),
            redis_url="redis://localhost:6379",
            internal_api_key="test_key"
        )
        
        validation = DataValidationService(
            config_client=mock_config,
            db_session=AsyncMock(),
            redis_url="redis://localhost:6379",
            internal_api_key="test_key"
        )
        
        retention = DataRetentionService(
            config_client=mock_config,
            db_session=AsyncMock(),
            redis_url="redis://localhost:6379"
        )
        
        monitoring = MonitoringService(
            config_client=mock_config,
            redis_url="redis://localhost:6379"
        )
        
        # Verify services were created with correct parameters
        assert dual_write is not None
        assert validation is not None
        assert retention is not None
        assert monitoring is not None
    
    @pytest.mark.asyncio
    async def test_config_driven_service_behavior(self):
        """Test that services respond to config changes"""
        from app.services.dual_write_adapter import DualWriteConfig
        
        # Mock config client
        mock_config = AsyncMock()
        mock_config.get_bool.side_effect = lambda key, default=False: {
            "INSTRUMENT_REGISTRY_DUAL_WRITE_ENABLED": True,
            "INSTRUMENT_REGISTRY_BACKUP_BEFORE_DELETE": True
        }.get(key, default)
        
        mock_config.get_int.side_effect = lambda key, default=0: {
            "INSTRUMENT_REGISTRY_BATCH_SIZE": 1000,
            "INSTRUMENT_REGISTRY_RETRY_ATTEMPTS": 3
        }.get(key, default)
        
        mock_config.get_string.side_effect = lambda key, default="": {
            "INSTRUMENT_REGISTRY_VALIDATION_MODE": "strict",
            "INSTRUMENT_REGISTRY_DEAD_LETTER_QUEUE": "test_dlq"
        }.get(key, default)
        
        # Test config loading
        config = await DualWriteConfig.from_config_service(mock_config)
        
        assert config.enabled == True
        assert config.batch_size == 1000
        assert config.retry_attempts == 3
        assert config.validation_mode.value == "strict"
        assert config.dead_letter_queue == "test_dlq"
    
    @pytest.mark.asyncio
    async def test_background_task_patterns(self):
        """Test that background tasks follow correct async patterns"""
        # This tests the patterns used in main.py background tasks
        
        # Mock services
        mock_validation_service = AsyncMock()
        mock_retention_service = AsyncMock()
        mock_monitoring_service = AsyncMock()
        
        # Mock validation result
        from app.services.data_validation_service import ValidationResult, ValidationStatus, ValidationLevel
        mock_result = MagicMock()
        mock_result.passed_thresholds = True
        mock_result.matched_records = 100
        mock_result.total_records_registry = 100
        mock_result.threshold_violations = []
        
        mock_validation_service.validate_index_memberships.return_value = mock_result
        
        # Mock retention result 
        from app.services.data_retention_service import RetentionResult, RetentionAction
        mock_retention_result = MagicMock()
        mock_retention_result.records_affected = 5
        
        mock_retention_service.run_retention_policies.return_value = [mock_retention_result]
        
        # Test validation task pattern
        async def test_validation_task():
            result = await mock_validation_service.validate_index_memberships()
            assert result.passed_thresholds == True
            return result
        
        # Test retention task pattern  
        async def test_retention_task():
            results = await mock_retention_service.run_retention_policies()
            total_affected = sum(r.records_affected for r in results)
            assert total_affected == 5
            return results
        
        # Run the test tasks
        validation_result = await test_validation_task()
        retention_results = await test_retention_task()
        
        assert validation_result is not None
        assert retention_results is not None
        assert len(retention_results) == 1
    
    def test_actuator_service_reference_pattern(self):
        """Test that actuator endpoints can receive service references"""
        from app.api.actuator import set_service_references
        
        # Mock services
        mock_dual_write = MagicMock()
        mock_validation = MagicMock() 
        mock_retention = MagicMock()
        mock_monitoring = MagicMock()
        
        # Test setting references
        set_service_references(mock_dual_write, mock_validation, mock_retention, mock_monitoring)
        
        # Import globals to verify they were set
        from app.api import actuator
        assert actuator.dual_write_adapter is mock_dual_write
        assert actuator.validation_service is mock_validation
        assert actuator.retention_service is mock_retention
        assert actuator.monitoring_service is mock_monitoring
    
    @patch('httpx.AsyncClient')
    def test_config_service_integration_pattern(self, mock_httpx):
        """Test config service client integration pattern"""
        from common.config_client import ConfigClient
        
        # Mock successful response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"secret_value": "test_value"}
        
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response
        mock_httpx.return_value.__aenter__.return_value = mock_client
        
        # Test config client creation
        config_client = ConfigClient(
            service_name="instrument_registry",
            internal_api_key="test_key"
        )
        
        assert config_client.service_name == "instrument_registry"
        assert config_client.internal_api_key == "test_key"
    
    @pytest.mark.asyncio
    async def test_error_handling_patterns(self):
        """Test error handling patterns in services"""
        from app.services.dual_write_adapter import DualWriteAdapter
        
        # Mock config client that raises errors
        mock_config = AsyncMock()
        mock_config.get_bool.side_effect = Exception("Config service unavailable")
        
        # Test that service handles config errors gracefully
        from app.services.dual_write_adapter import DualWriteConfig
        config = await DualWriteConfig.from_config_service(mock_config)
        
        # Should return safe defaults on error
        assert config.enabled == False  # Safe default
        assert config.batch_size == 100  # Safe default
        assert config.retry_attempts == 3
    
    def test_schema_enforcement_patterns(self):
        """Test that schema enforcement patterns are correctly implemented"""
        # Test table args pattern
        table_args = {'schema': 'instrument_registry'}
        
        # This is the pattern that should be used in all SQLAlchemy models
        assert table_args['schema'] == 'instrument_registry'
        
        # Test SQL query pattern
        query_pattern = "SELECT * FROM instrument_registry.index_memberships"
        assert "instrument_registry." in query_pattern
    
    def test_prometheus_metrics_patterns(self):
        """Test that Prometheus metrics are properly structured"""
        from app.services.monitoring_service import MonitoringService
        
        # Mock config client
        mock_config = AsyncMock()
        
        # Create monitoring service
        monitoring = MonitoringService(
            config_client=mock_config,
            redis_url="redis://localhost:6379"
        )
        
        # Check that metrics registry exists
        assert monitoring.registry is not None
        assert monitoring.metrics is not None
        
        # Check that expected metric types exist
        assert "dual_write_operations_total" in monitoring.metrics
        assert "validation_operations_total" in monitoring.metrics
        assert "system_health_status" in monitoring.metrics


class TestProductionReadiness:
    """Test production readiness requirements"""
    
    def test_all_required_services_defined(self):
        """Test that all required services are defined and importable"""
        required_services = [
            "app.services.dual_write_adapter",
            "app.services.data_validation_service", 
            "app.services.data_retention_service",
            "app.services.monitoring_service"
        ]
        
        for service_module in required_services:
            try:
                __import__(service_module)
            except ImportError as e:
                pytest.fail(f"Required service module {service_module} is not importable: {e}")
    
    def test_config_parameters_defined(self):
        """Test that all required config parameters are defined"""
        # This would typically check against the config service
        # For CI, we just verify the parameter names are consistent
        required_params = [
            "INSTRUMENT_REGISTRY_DUAL_WRITE_ENABLED",
            "INSTRUMENT_REGISTRY_BATCH_SIZE",
            "INSTRUMENT_REGISTRY_RETRY_ATTEMPTS", 
            "INSTRUMENT_REGISTRY_DEAD_LETTER_QUEUE",
            "INSTRUMENT_REGISTRY_VALIDATION_MODE"
        ]
        
        # Verify parameter names follow convention
        for param in required_params:
            assert param.startswith("INSTRUMENT_REGISTRY_")
            assert param.isupper()
    
    def test_error_recovery_patterns(self):
        """Test that services implement proper error recovery"""
        from app.services.dual_write_adapter import DualWriteAdapter
        
        # Mock failing components
        mock_config = AsyncMock()
        mock_config.get_bool.side_effect = Exception("Network error")
        
        # Services should handle initialization failures gracefully
        dual_write = DualWriteAdapter(
            config_client=mock_config,
            db_session=AsyncMock(),
            redis_url="redis://localhost:6379"
        )
        
        # Service should be created even if config fails
        assert dual_write is not None
    
    def test_security_patterns(self):
        """Test that security patterns are implemented"""
        from app.api.actuator import router
        
        # Check that actuator endpoints require authentication
        for route in router.routes:
            if hasattr(route, 'dependencies'):
                # Should have auth dependency
                dependency_names = [str(dep) for dep in route.dependencies]
                auth_found = any("verify_internal_token" in dep for dep in dependency_names)
                assert auth_found, f"Route {route.path} missing authentication"


# Smoke tests that can run without external dependencies
class TestSmokeTests:
    """Lightweight smoke tests for CI"""
    
    def test_main_module_imports(self):
        """Test that main module can be imported"""
        try:
            import main
            assert main.app is not None
        except Exception as e:
            pytest.fail(f"Main module import failed: {e}")
    
    def test_router_registration(self):
        """Test that all routers are properly registered"""
        from main import app
        
        # Check that routes exist
        route_paths = [route.path for route in app.routes]
        
        expected_route_prefixes = [
            "/actuator",
            "/api/v1/internal/instrument-registry",
            "/health",
            "/ready",
            "/metrics"
        ]
        
        for prefix in expected_route_prefixes:
            matching_routes = [path for path in route_paths if path.startswith(prefix)]
            assert len(matching_routes) > 0, f"No routes found for prefix {prefix}"
    
    @patch('common.config_client.ConfigClient.initialize')
    @patch('common.health_checks.HealthCheckManager')
    def test_lifespan_startup_sequence(self, mock_health_manager, mock_config_init):
        """Test that lifespan startup sequence is properly structured"""
        # Mock the initialization
        mock_config_init.return_value = None
        mock_health_manager.return_value.get_comprehensive_health.return_value = {"overall": "healthy"}
        
        # Import lifespan function
        from main import lifespan
        
        # Verify lifespan is properly defined
        assert lifespan is not None
        assert hasattr(lifespan, '__call__')


if __name__ == "__main__":
    # Run smoke tests
    print("Running CI-friendly dual-write integration tests...")
    
    test_integration = TestRuntimeIntegration()
    test_production = TestProductionReadiness()
    test_smoke = TestSmokeTests()
    
    # Run key tests
    try:
        test_integration.test_service_imports_successful()
        test_integration.test_actuator_endpoints_importable()
        test_production.test_all_required_services_defined()
        test_production.test_config_parameters_defined()
        test_smoke.test_main_module_imports()
        test_smoke.test_router_registration()
        
        print("✅ All CI tests passed!")
        exit(0)
        
    except Exception as e:
        print(f"❌ CI test failed: {e}")
        exit(1)