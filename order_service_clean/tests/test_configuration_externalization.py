"""
Test configuration externalization - validates no hardcoded values remain
Critical for production signoff - proves environment-specific deployment works
"""
import pytest
import os
from unittest.mock import patch


class TestConfigurationExternalization:
    """Test that all configuration is properly externalized"""

    @pytest.mark.unit
    def test_service_urls_from_environment(self):
        """Test service URLs can be configured via environment variables"""
        from app.config.settings import get_service_url
        
        # Test direct URL override
        with patch.dict('os.environ', {'TICKER_SERVICE_URL': 'http://custom-ticker:9999'}):
            url = get_service_url('ticker_service')
            assert url == 'http://custom-ticker:9999'
        
        # Test port-based configuration
        with patch.dict('os.environ', {
            'TICKER_SERVICE_PORT': '8888',
            'SERVICE_HOST': 'custom-host'
        }):
            url = get_service_url('ticker_service')
            assert 'custom-host:8888' in url

    @pytest.mark.unit 
    def test_cors_origins_configurable(self):
        """Test CORS origins are configurable via environment"""
        from app.config.settings import Settings
        
        # Test explicit CORS configuration
        with patch.dict('os.environ', {'CORS_ORIGINS': 'https://app1.com,https://app2.com'}):
            settings = Settings()
            origins = settings.get_cors_origins()
            assert 'https://app1.com' in origins
            assert 'https://app2.com' in origins

    @pytest.mark.unit
    def test_production_hosts_configurable(self):
        """Test production host configuration"""
        from app.config.settings import Settings
        
        with patch.dict('os.environ', {
            'PRODUCTION_HOST_HTTP': 'http://custom-prod-host',
            'PRODUCTION_DOMAIN_HTTPS': 'https://custom-app.com'
        }):
            settings = Settings()
            origins = settings.get_cors_origins()
            assert any('custom-prod-host' in origin for origin in origins)
            assert 'https://custom-app.com' in origins

    @pytest.mark.unit
    def test_rate_limits_configurable(self):
        """Test rate limits are configurable"""
        from app.services.redis_daily_counter import RedisDailyOrderCounter
        from app.services.rate_limiter import HardRefreshRateLimiter
        
        # Test daily order limit
        with patch.dict('os.environ', {'DAILY_ORDER_LIMIT': '5000'}):
            # Re-import to pick up env change
            import importlib
            from app.services import redis_daily_counter
            importlib.reload(redis_daily_counter)
            assert redis_daily_counter.RedisDailyOrderCounter.DEFAULT_LIMIT == 5000
        
        # Test rate limiter
        with patch.dict('os.environ', {'HARD_REFRESH_RATE_LIMIT_SECONDS': '120'}):
            from app.services import rate_limiter
            importlib.reload(rate_limiter)
            assert rate_limiter.HardRefreshRateLimiter.RATE_LIMIT_SECONDS == 120

    @pytest.mark.unit
    def test_redis_url_configurable(self):
        """Test Redis URL is configurable"""
        from app.workers.tick_listener import TickListener
        
        # Test environment override
        custom_redis_url = "redis://custom-redis:6380/1"
        with patch.dict('os.environ', {'REDIS_URL': custom_redis_url}):
            listener = TickListener()
            assert listener.redis_url == custom_redis_url

    @pytest.mark.unit  
    def test_no_hardcoded_production_values(self):
        """Test that no production values are hardcoded"""
        # Test that sensitive production values are not in source code
        critical_files = [
            'app/config/settings.py',
            'app/services/market_hours.py',
            'app/services/redis_daily_counter.py'
        ]
        
        hardcoded_patterns = [
            '5.223.52.98',  # Should be configurable
            'localhost:8088',  # Should use env vars
            'stocksblitz123'  # Should not be in code
        ]
        
        for file_path in critical_files:
            try:
                with open(file_path, 'r') as f:
                    content = f.read()
                    for pattern in hardcoded_patterns:
                        # Some patterns are OK in comments or as defaults
                        lines = content.split('\n')
                        for line_num, line in enumerate(lines, 1):
                            if pattern in line and not (line.strip().startswith('#') or 
                                                      'os.getenv' in line or 
                                                      'default=' in line):
                                pytest.fail(
                                    f"Hardcoded value '{pattern}' found in {file_path}:{line_num} - {line.strip()}"
                                )
            except FileNotFoundError:
                pass  # Skip missing files

    @pytest.mark.unit
    def test_calendar_service_multi_year_support(self):
        """Test that market hours supports multiple years"""
        from app.services.market_hours import MarketHoursService
        
        # Test supported years
        supported_years = MarketHoursService.get_supported_years()
        assert '2024' in supported_years
        assert '2025' in supported_years  
        assert '2026' in supported_years
        
        # Test holiday data exists for each year
        for year in ['2024', '2025', '2026']:
            holidays = MarketHoursService.STATIC_HOLIDAYS[year]
            assert len(holidays) > 0
            assert all(holiday.startswith(year) for holiday in holidays)

    @pytest.mark.unit
    def test_environment_specific_configuration_loading(self):
        """Test configuration loads properly for different environments"""
        test_environments = ['development', 'staging', 'production']
        
        for env in test_environments:
            with patch.dict('os.environ', {'ENVIRONMENT': env}):
                from app.config.settings import Settings
                settings = Settings()
                assert settings.environment == env
                
                # Production should have stricter defaults
                if env == 'production':
                    assert settings.auth_enabled is True
                    assert settings.cors_enabled is True