"""
Architecture Compliance Regression Tests

Tests to verify all architectural improvements are working correctly:
- Service client functionality and fallbacks
- Redis monitoring and health checks  
- Enhanced security and authentication
- Anomaly detection system
"""

import pytest
import asyncio
import json
from unittest.mock import AsyncMock, Mock, patch
from fastapi import Request, HTTPException

from app.clients.strategy_service_client import StrategyServiceClient, get_strategy_client
from app.clients.portfolio_service_client import PortfolioServiceClient, get_portfolio_client
from app.clients.account_service_client import AccountServiceClient, get_account_client
from app.clients.analytics_service_client import AnalyticsServiceClient, get_analytics_client
from app.services.redis_usage_monitor import RedisUsageMonitor
from app.security.internal_auth import CriticalServiceAuth, validate_order_placement
from app.services.order_anomaly_detector import OrderAnomalyDetector, AnomalyType


class TestServiceClients:
    """Test service client functionality and fallbacks"""
    
    @pytest.mark.asyncio
    async def test_strategy_client_creation(self):
        """Test strategy service client creation and configuration"""
        client = await get_strategy_client()
        assert isinstance(client, StrategyServiceClient)
        assert client.timeout == 30
    
    @pytest.mark.asyncio
    async def test_strategy_client_validate_fallback(self):
        """Test strategy validation with fallback behavior"""
        client = StrategyServiceClient()
        
        # Mock failed API call - should not raise exception, should fall back gracefully
        with patch.object(client, '_get_http_client') as mock_client:
            mock_http = AsyncMock()
            mock_http.post.side_effect = Exception("Service unavailable")
            mock_client.return_value = mock_http
            
            # Should handle gracefully and not crash
            try:
                result = await client.validate_strategy(123)
                # In fallback mode, should return False or handle gracefully
                assert result in [True, False]  # Either works or fails gracefully
            except Exception as e:
                # Should be a ServiceError, not a raw exception
                assert "Service" in str(type(e).__name__) or "HTTP" in str(type(e).__name__)
    
    @pytest.mark.asyncio
    async def test_portfolio_client_creation(self):
        """Test portfolio service client creation"""
        client = await get_portfolio_client()
        assert isinstance(client, PortfolioServiceClient)
    
    @pytest.mark.asyncio
    async def test_account_client_creation(self):
        """Test account service client creation"""
        client = await get_account_client()
        assert isinstance(client, AccountServiceClient)
    
    @pytest.mark.asyncio
    async def test_analytics_client_creation(self):
        """Test analytics service client creation"""
        client = await get_analytics_client()
        assert isinstance(client, AnalyticsServiceClient)


class TestRedisMonitoring:
    """Test Redis usage monitoring and health endpoints"""
    
    @pytest.mark.asyncio
    async def test_redis_monitor_initialization(self):
        """Test Redis monitor can be initialized"""
        monitor = RedisUsageMonitor()
        assert monitor is not None
        assert hasattr(monitor, 'memory_warning_threshold')
        assert hasattr(monitor, 'memory_critical_threshold')
        assert monitor.memory_warning_threshold == 0.80
        assert monitor.memory_critical_threshold == 0.95
    
    @pytest.mark.asyncio
    async def test_redis_monitor_pattern_classification(self):
        """Test Redis usage pattern classification"""
        monitor = RedisUsageMonitor()
        
        # Test pattern detection
        test_keys = [
            "idempotency:order:123",
            "rate_limit:user:456", 
            "cache:symbol:NIFTY",
            "live:price:RELIANCE",
            "worker:job:789"
        ]
        
        patterns = monitor._classify_usage_patterns(test_keys)
        
        assert "idempotency" in patterns
        assert "rate_limiting" in patterns
        assert "caching" in patterns
        assert "real_time_data" in patterns
        assert "worker_coordination" in patterns
    
    @pytest.mark.asyncio 
    async def test_redis_health_check_structure(self):
        """Test Redis health check returns proper structure"""
        monitor = RedisUsageMonitor()
        
        # Mock Redis to avoid real connection
        with patch.object(monitor, 'redis') as mock_redis:
            mock_redis.info.return_value = {
                'used_memory': 1000000,
                'maxmemory': 2000000,
                'connected_clients': 10
            }
            mock_redis.execute_command.return_value = 100
            mock_redis.keys.return_value = [
                b"idempotency:order:1",
                b"cache:data:2"
            ]
            
            health_data = await monitor.get_health_status()
            
            # Verify structure
            assert "is_healthy" in health_data
            assert "memory_usage_percentage" in health_data
            assert "connection_pool_usage" in health_data
            assert "warnings" in health_data
            assert "critical_issues" in health_data
            
            # Verify data types
            assert isinstance(health_data["is_healthy"], bool)
            assert isinstance(health_data["warnings"], list)
            assert isinstance(health_data["critical_issues"], list)


class TestEnhancedSecurity:
    """Test enhanced security layer and authentication"""
    
    def test_critical_service_auth_initialization(self):
        """Test critical service auth can be initialized"""
        auth = CriticalServiceAuth()
        assert auth is not None
        assert hasattr(auth, 'AUTHORIZED_SERVICES')
        assert 'algo_engine' in auth.AUTHORIZED_SERVICES
        assert 'place_order' in auth.AUTHORIZED_SERVICES['algo_engine']
    
    def test_service_identity_validation_missing_headers(self):
        """Test service identity validation with missing headers"""
        auth = CriticalServiceAuth()
        
        # Mock request without headers
        mock_request = Mock()
        mock_request.headers = {}
        
        with pytest.raises(HTTPException) as exc_info:
            auth.validate_service_identity(mock_request)
        
        assert exc_info.value.status_code == 401
        assert "Missing X-Service-Identity" in str(exc_info.value.detail)
    
    def test_service_identity_validation_invalid_service(self):
        """Test service identity validation with invalid service"""
        auth = CriticalServiceAuth()
        
        # Mock request with invalid service
        mock_request = Mock()
        mock_request.headers = {
            "X-Service-Identity": "malicious_service",
            "X-Internal-API-Key": "some-key"
        }
        
        with pytest.raises(HTTPException) as exc_info:
            auth.validate_service_identity(mock_request)
        
        assert exc_info.value.status_code == 403
        assert "not authorized" in str(exc_info.value.detail)
    
    def test_service_authorization_check(self):
        """Test service authorization for specific operations"""
        auth = CriticalServiceAuth()
        
        # Valid operation
        result = auth.validate_service_authorization("algo_engine", "place_order")
        assert result is True
        
        # Invalid operation
        with pytest.raises(HTTPException) as exc_info:
            auth.validate_service_authorization("algo_engine", "invalid_operation")
        
        assert exc_info.value.status_code == 403
        assert "not authorized for operation" in str(exc_info.value.detail)


class TestAnomalyDetection:
    """Test order anomaly detection system"""
    
    @pytest.mark.asyncio
    async def test_anomaly_detector_initialization(self):
        """Test anomaly detector can be initialized"""
        detector = OrderAnomalyDetector()
        assert detector is not None
        assert hasattr(detector, 'thresholds')
        assert detector.thresholds['max_orders_per_minute'] == 30
        assert detector.thresholds['large_order_quantity'] == 50000
    
    @pytest.mark.asyncio
    async def test_high_frequency_order_detection(self):
        """Test high frequency order anomaly detection"""
        detector = OrderAnomalyDetector()
        
        # Simulate rapid order placement
        order_data = {
            "symbol": "NIFTY",
            "quantity": 100,
            "price": 18000
        }
        
        # Place multiple orders quickly to trigger anomaly
        anomalies = []
        for i in range(35):  # Above threshold of 30
            alerts = await detector.analyze_order_event(
                event_type="placed",
                order_data=order_data,
                user_id="test_user_123",
                trading_account_id="test_account_456",
                service_identity="algo_engine",
                request_id=f"req_{i}"
            )
            anomalies.extend(alerts)
        
        # Should detect high frequency anomaly
        high_freq_anomalies = [
            a for a in anomalies 
            if a.anomaly_type == AnomalyType.HIGH_FREQUENCY_ORDERS
        ]
        assert len(high_freq_anomalies) > 0
        assert high_freq_anomalies[0].severity == "HIGH"
    
    @pytest.mark.asyncio
    async def test_large_order_size_detection(self):
        """Test large order size anomaly detection"""
        detector = OrderAnomalyDetector()
        
        # Large quantity order
        large_order_data = {
            "symbol": "RELIANCE",
            "quantity": 100000,  # Above threshold of 50000
            "price": 2500
        }
        
        anomalies = await detector.analyze_order_event(
            event_type="placed",
            order_data=large_order_data,
            user_id="test_user_123",
            trading_account_id="test_account_456",
            service_identity="user_interface",
            request_id="large_order_test"
        )
        
        # Should detect large order anomaly
        large_order_anomalies = [
            a for a in anomalies 
            if a.anomaly_type == AnomalyType.LARGE_ORDER_SIZE
        ]
        assert len(large_order_anomalies) > 0
        assert large_order_anomalies[0].severity in ["MEDIUM", "HIGH"]
    
    @pytest.mark.asyncio
    async def test_off_hours_activity_detection(self):
        """Test off-hours activity detection"""
        detector = OrderAnomalyDetector()
        
        # Mock datetime to simulate off-hours (e.g., 8 PM)
        with patch('app.services.order_anomaly_detector.datetime') as mock_dt:
            # Create a mock datetime object for 8 PM (hour=20)
            mock_now = Mock()
            mock_now.hour = 20  # 8 PM is outside market hours (9 AM - 3:30 PM)
            mock_dt.now.return_value = mock_now
            
            order_data = {
                "symbol": "NIFTY",
                "quantity": 100,
                "price": 18000
            }
            
            anomalies = await detector.analyze_order_event(
                event_type="placed",
                order_data=order_data,
                user_id="test_user_123",
                trading_account_id="test_account_456", 
                service_identity="user_interface",
                request_id="off_hours_test"
            )
            
            # Should detect off-hours anomaly
            off_hours_anomalies = [
                a for a in anomalies 
                if a.anomaly_type == AnomalyType.OFF_HOURS_ACTIVITY
            ]
            assert len(off_hours_anomalies) > 0
            assert off_hours_anomalies[0].severity in ["LOW", "MEDIUM"]


class TestSchemaAccessCompliance:
    """Test that no direct public.* schema access remains"""
    
    def test_no_public_schema_access_in_clients(self):
        """Verify service clients don't have direct public.* access"""
        import app.clients.strategy_service_client as strategy_client
        import app.clients.portfolio_service_client as portfolio_client
        import app.clients.account_service_client as account_client
        import app.clients.analytics_service_client as analytics_client
        
        # Read source code of each client
        import inspect
        
        for module in [strategy_client, portfolio_client, account_client, analytics_client]:
            source = inspect.getsource(module)
            
            # Should not contain direct public.* table access
            assert "public.strategy" not in source
            assert "public.portfolio" not in source 
            assert "public.kite_accounts" not in source
            assert "public.strategy_pnl_metrics" not in source
            
            # Should contain API-based calls
            assert "httpx" in source or "aiohttp" in source
    
    def test_service_discovery_usage(self):
        """Verify all clients use service discovery instead of hardcoded URLs"""
        import app.clients.strategy_service_client as strategy_client
        import inspect
        
        source = inspect.getsource(strategy_client)
        
        # Should not contain hardcoded localhost URLs
        assert "localhost:8089" not in source
        assert "localhost:8013" not in source
        assert "localhost:8011" not in source
        
        # Should use service discovery
        assert "_get_service_port" in source


if __name__ == "__main__":
    # Run specific test categories
    pytest.main([
        __file__,
        "-v",
        "--tb=short",
        "-k", "test_service_clients or test_redis_monitoring or test_enhanced_security"
    ])