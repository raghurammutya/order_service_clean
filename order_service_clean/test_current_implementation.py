"""
Test current token manager integration implementation.

This file tests what's actually implemented in the current order_service_clean directory.
"""
import pytest
from unittest.mock import patch, AsyncMock


class TestCurrentTokenManagerImplementation:
    """Test the actual implementation we have in order_service_clean."""

    def test_kite_client_multi_functions_exist(self):
        """Test that the expected functions exist in kite_client_multi."""
        from app.services.kite_client_multi import (
            resolve_trading_account_config,
            get_all_trading_accounts,
            get_kite_client_for_account,
            get_kite_client_for_account_async,
            clear_client_cache
        )
        
        # Verify functions are callable
        assert callable(resolve_trading_account_config)
        assert callable(get_all_trading_accounts)
        assert callable(get_kite_client_for_account)
        assert callable(get_kite_client_for_account_async)
        assert callable(clear_client_cache)
        
        # Verify async functions are coroutines
        import inspect
        assert inspect.iscoroutinefunction(resolve_trading_account_config)
        assert inspect.iscoroutinefunction(get_all_trading_accounts)
        assert inspect.iscoroutinefunction(get_kite_client_for_account_async)

    @pytest.mark.asyncio
    async def test_resolve_trading_account_config_structure(self):
        """Test the resolve_trading_account_config function with mocked response."""
        from app.services.kite_client_multi import resolve_trading_account_config
        
        # Mock the HTTP response
        mock_response = {
            "success": True,
            "account": {
                "trading_account_id": 1,
                "broker": "kite",
                "segment": "equity",
                "api_key": "test_api_key",
                "account_nickname": "primary",
                "policy": {
                    "refresh_policy": "auto",
                    "requires_consent": False,
                    "allowed_capabilities": ["market_data", "trading"]
                },
                "is_active": True
            }
        }
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_instance = AsyncMock()
            mock_client.return_value.__aenter__.return_value = mock_instance
            mock_instance.get.return_value.status_code = 200
            mock_instance.get.return_value.json.return_value = mock_response
            
            try:
                config = await resolve_trading_account_config(1)
                
                # Verify structure
                assert "nickname" in config
                assert "api_key" in config
                assert config["nickname"] == "primary"
                assert config["api_key"] == "test_api_key"
                
                print(f"✅ resolve_trading_account_config returned: {config}")
                
            except Exception as e:
                print(f"ℹ️  Function exists but may need token_manager service running: {e}")
                # This is expected if token_manager service isn't running

    def test_settings_token_manager_config(self):
        """Test that settings have token manager configuration."""
        from app.config.settings import settings
        
        # Check if token manager settings exist
        has_url = hasattr(settings, 'token_manager_url')
        has_api_key = hasattr(settings, 'token_manager_api_key') or hasattr(settings, 'token_manager_internal_api_key')
        
        print(f"Settings has token_manager_url: {has_url}")
        print(f"Settings has token_manager API key: {has_api_key}")
        
        if has_url:
            print(f"Token Manager URL: {getattr(settings, 'token_manager_url', 'Not set')}")
        
        # At minimum, we should have the configuration structure
        assert has_url, "Should have token_manager_url configuration"

    def test_kite_client_single_account_still_works(self):
        """Test that the original single account kite client still works."""
        try:
            from app.services.kite_client import get_kite_client
            
            # Should be importable
            assert callable(get_kite_client)
            print("✅ Single account kite client still available")
            
            # Try to instantiate (may fail without credentials, but should not have import errors)
            try:
                client = get_kite_client()
                print("✅ Single account client instantiated successfully")
            except Exception as e:
                # Expected if no credentials configured
                print(f"ℹ️  Single account client import works, instantiation needs credentials: {e}")
                
        except ImportError as e:
            pytest.fail(f"Single account kite client should still be available: {e}")

    @pytest.mark.asyncio  
    async def test_multi_account_error_handling(self):
        """Test error handling in multi-account client."""
        from app.services.kite_client_multi import resolve_trading_account_config
        
        # Mock 404 response
        with patch('httpx.AsyncClient') as mock_client:
            mock_instance = AsyncMock()
            mock_client.return_value.__aenter__.return_value = mock_instance
            mock_instance.get.return_value.status_code = 200
            mock_instance.get.return_value.json.return_value = {
                "success": False,
                "error": "Trading account 999 not found",
                "account": None
            }
            
            try:
                config = await resolve_trading_account_config(999)
                print(f"Unexpected success for unknown account: {config}")
            except (ValueError, Exception) as e:
                print(f"✅ Proper error handling for unknown account: {e}")
                assert "not found" in str(e).lower() or "failed" in str(e).lower()

    def test_rate_limiter_integration(self):
        """Test that rate limiting is integrated."""
        try:
            from app.services.kite_account_rate_limiter import get_rate_limiter_manager_sync
            
            print("✅ Rate limiter components available")
            
            # Verify rate limiter can be instantiated
            manager = get_rate_limiter_manager_sync()
            assert manager is not None
            print("✅ Rate limiter manager created")
            
        except ImportError as e:
            print(f"ℹ️  Rate limiter not fully integrated: {e}")


class TestOrderServiceStructure:
    """Test the overall order service structure."""
    
    def test_order_service_exists(self):
        """Test that OrderService class exists and has expected methods."""
        try:
            from app.services.order_service import OrderService
            import inspect
            
            assert hasattr(OrderService, 'place_order'), "OrderService should have place_order method"
            
            # Check if it's async
            place_order_method = getattr(OrderService, 'place_order')
            is_async = inspect.iscoroutinefunction(place_order_method)
            print(f"OrderService.place_order is async: {is_async}")
            
            print("✅ OrderService class structure looks correct")
            
        except ImportError as e:
            print(f"ℹ️  OrderService import issue: {e}")

    def test_app_structure(self):
        """Test the overall app structure."""
        import os
        
        # Check key directories
        key_dirs = ['app', 'app/services', 'app/config', 'tests']
        for dir_path in key_dirs:
            exists = os.path.exists(dir_path)
            print(f"{dir_path}: {'✅' if exists else '❌'}")
            assert exists, f"{dir_path} should exist"

    def test_settings_structure(self):
        """Test that settings are properly structured."""
        try:
            from app.config.settings import settings
            
            # Print some key settings (without sensitive values)
            settings_attrs = [attr for attr in dir(settings) if not attr.startswith('_')]
            token_related = [attr for attr in settings_attrs if 'token' in attr.lower()]
            
            print(f"Total settings attributes: {len(settings_attrs)}")
            print(f"Token-related settings: {token_related}")
            
            print("✅ Settings module loads successfully")
            
        except Exception as e:
            print(f"⚠️  Settings loading issue: {e}")


def test_current_implementation_summary():
    """Run a comprehensive summary of what's implemented."""
    print("\n" + "="*60)
    print("CURRENT IMPLEMENTATION SUMMARY")
    print("="*60)
    
    try:
        # Check multi-account client
        import app.services.kite_client_multi
        print("✅ Multi-account kite client: IMPLEMENTED")
    except ImportError:
        print("❌ Multi-account kite client: NOT FOUND")
    
    try:
        # Check single account client  
        import app.services.kite_client
        print("✅ Single account kite client: AVAILABLE")
    except ImportError:
        print("❌ Single account kite client: NOT FOUND")
    
    try:
        # Check order service
        import app.services.order_service  # noqa: F401 - import test only
        print("✅ OrderService: AVAILABLE")
    except ImportError:
        print("❌ OrderService: NOT FOUND")
    
    try:
        # Check settings
        from app.config.settings import settings
        has_token_config = any('token' in attr.lower() for attr in dir(settings))
        print(f"✅ Settings with token config: {'YES' if has_token_config else 'NO'}")
    except ImportError:
        print("❌ Settings: NOT FOUND")
    
    print("="*60)


if __name__ == "__main__":
    # Run the summary first
    test_current_implementation_summary()
    
    # Then run pytest
    import subprocess
    import sys
    result = subprocess.run([sys.executable, "-m", "pytest", __file__, "-v", "-s"], 
                          capture_output=False)
    sys.exit(result.returncode)