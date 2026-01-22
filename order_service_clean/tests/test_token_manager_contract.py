"""
Order Service -> Token Manager integration contract tests for Sprint 0.

These test stubs define the expected integration behavior for order_service
when using the new trading_account_id based token management endpoints.

These are stubs that will be implemented in Sprint 1 when endpoints are available.
"""
import ast
import pytest


def _load_settings_ast():
    settings_path = "/mnt/stocksblitz-data/Quantagro/order_service/app/config/settings.py"
    with open(settings_path, "r") as handle:
        return ast.parse(handle.read()), settings_path


def _settings_class_fields(module_ast: ast.Module) -> set[str]:
    for node in module_ast.body:
        if isinstance(node, ast.ClassDef) and node.name == "Settings":
            field_names: set[str] = set()
            for item in node.body:
                if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                    field_names.add(item.target.id)
                elif isinstance(item, ast.Assign):
                    for target in item.targets:
                        if isinstance(target, ast.Name):
                            field_names.add(target.id)
            return field_names
    return set()


def _module_has_function(module_ast: ast.Module, name: str) -> bool:
    return any(isinstance(node, ast.FunctionDef) and node.name == name for node in module_ast.body)


class TestOrderServiceTokenManagerIntegration:
    """
    Contract tests for order_service integration with new token_manager endpoints.
    
    Tests the order_service side of the trading_account_id based contract.
    """
    
    @pytest.mark.asyncio
    async def test_kite_client_resolves_trading_account_dynamically(self):
        """
        Contract test: MultiAccountKiteClient resolves trading_account_id dynamically.
        
        Expected behavior:
        - Replaces hardcoded get_account_mapping() with API call
        - Calls GET /api/v1/accounts/resolve/{trading_account_id}
        - Uses returned broker config for client initialization
        """
        from order_service.app.services.kite_client_multi import resolve_trading_account_config
        from unittest.mock import patch, AsyncMock
        
        # Mock the HTTP response from token_manager
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
            },
            "error": None,
            "timestamp": "2024-12-27T10:00:00Z"
        }
        
        # Mock httpx.AsyncClient to return our test response
        with patch('httpx.AsyncClient') as mock_client:
            mock_instance = AsyncMock()
            mock_client.return_value.__aenter__.return_value = mock_instance
            mock_instance.get.return_value.status_code = 200
            mock_instance.get.return_value.json.return_value = mock_response
            
            # Test the dynamic resolution
            config = await resolve_trading_account_config(1)
            
            # Verify the returned structure matches expected format
            assert config["nickname"] == "primary"
            assert config["api_key"] == "test_api_key"
            assert config["broker"] == "kite"
            assert config["segment"] == "equity"
            assert config["is_active"] is True
            
            # Verify the HTTP call was made to correct endpoint
            mock_instance.get.assert_called_once()
            call_args = mock_instance.get.call_args
            assert "/api/v1/accounts/resolve/1" in call_args[0][0]
    
    @pytest.mark.asyncio  
    async def test_token_fetch_uses_trading_account_id(self):
        """
        Contract test: Token fetching uses trading_account_id instead of nickname.
        
        Expected behavior:
        - Calls GET /api/v1/tokens/by-trading-account/{trading_account_id}
        - No longer uses nickname-based /tokens/{account_id} endpoint
        - Handles token resolution failures gracefully
        """
        from order_service.app.services.kite_client_multi import MultiAccountKiteClient
        from unittest.mock import patch, AsyncMock
        
        # Mock account resolution response
        resolve_response = {
            "success": True,
            "account": {
                "trading_account_id": 1,
                "broker": "kite",
                "segment": "equity", 
                "api_key": "test_api_key",
                "account_nickname": "primary",
                "policy": {"refresh_policy": "auto", "requires_consent": False, "allowed_capabilities": ["trading"]},
                "is_active": True
            }
        }
        
        # Mock token response
        token_response = {
            "account_id": "primary",
            "access_token": "test_access_token",
            "expires_at": "2024-12-27T18:00:00Z",
            "is_valid": True,
            "api_key": "test_api_key"
        }
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_instance = AsyncMock()
            mock_client.return_value.__aenter__.return_value = mock_instance
            
            # Mock both resolve and token endpoints
            def mock_get(url, **kwargs):
                response = AsyncMock()
                if "/resolve/" in url:
                    response.status_code = 200
                    response.json.return_value = resolve_response
                elif "/by-trading-account/" in url:
                    response.status_code = 200  
                    response.json.return_value = token_response
                return response
                
            mock_instance.get.side_effect = mock_get
            
            # Create client and fetch token
            client = await MultiAccountKiteClient.create(1)
            
            # Verify client has correct trading_account_id
            assert client.trading_account_id == 1
            assert client.account_nickname == "primary"
            
            # Test token fetching
            token = await client._fetch_access_token()
            
            # Verify token fetch used correct endpoint format
            assert token == "test_access_token"
            
            # Verify calls were made to correct endpoints
            call_urls = [call[0][0] for call in mock_instance.get.call_args_list]
            assert any("/resolve/1" in url for url in call_urls)
            assert any("/by-trading-account/1" in url for url in call_urls)
    
    def test_hardcoded_mapping_removed(self):
        """
        Contract test: Hardcoded account mapping is removed from order_service.
        
        Expected behavior:
        - get_account_mapping() function no longer exists
        - No hardcoded trading_account_id -> nickname mapping
        - Dynamic resolution via token_manager API calls
        """
        # Sprint 1: Verify that hardcoded mapping is removed and replaced
        from order_service.app.services import kite_client_multi
        import inspect
        
        # Verify old hardcoded function is gone
        assert not hasattr(kite_client_multi, 'get_account_mapping'), \
            "get_account_mapping() should be removed in Sprint 1"
        
        # Verify new dynamic functions exist
        assert hasattr(kite_client_multi, 'resolve_trading_account_config'), \
            "resolve_trading_account_config() should exist"
        assert hasattr(kite_client_multi, 'get_all_trading_accounts'), \
            "get_all_trading_accounts() should exist for sync workers"
        
        # Verify the new functions are actually async/callable
        assert inspect.iscoroutinefunction(kite_client_multi.resolve_trading_account_config)
        assert inspect.iscoroutinefunction(kite_client_multi.get_all_trading_accounts)
    
    @pytest.mark.asyncio
    async def test_error_handling_for_unknown_trading_account(self):
        """
        Contract test: order_service handles unknown trading_account_id gracefully.
        
        Expected behavior:
        - Unknown trading_account_id raises appropriate exception
        - Error message indicates account not found
        - No fallback to hardcoded values
        """
        from order_service.app.services.kite_client_multi import resolve_trading_account_config
        from unittest.mock import patch, AsyncMock
        
        # Mock the HTTP client to simulate token_manager response for unknown account
        with patch('httpx.AsyncClient') as mock_client:
            mock_instance = AsyncMock()
            mock_client.return_value.__aenter__.return_value = mock_instance
            
            # Mock 404 response for unknown account
            mock_response = AsyncMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "success": False,
                "error": "Trading account 999 not found",
                "account": None
            }
            mock_instance.get.return_value = mock_response
            
            # Test with non-existent trading account ID
            try:
                config = await resolve_trading_account_config(999)
                pytest.fail("Expected ValueError for unknown trading account")
            except ValueError as e:
                # Expected error for unknown trading account
                error_msg = str(e).lower()
                assert "not found" in error_msg or "failed" in error_msg, \
                    f"Error should indicate account not found: {e}"
                
                # Verify the HTTP call was made to correct endpoint
                mock_instance.get.assert_called_once()
                call_args = mock_instance.get.call_args[0][0]
                assert "/api/v1/accounts/resolve/999" in call_args
    
    def test_token_manager_url_configuration_exists(self):
        """
        Contract test: order_service has proper token_manager configuration.
        
        Expected behavior:
        - token_manager_url is configured
        - token_manager_api_key is available
        - Service discovery works for token_manager
        """
        module_ast, settings_path = _load_settings_ast()
        field_names = _settings_class_fields(module_ast)
        
        assert "token_manager_url" in field_names, f"token_manager_url missing in {settings_path}"
        assert "token_manager_api_key" in field_names, f"token_manager_api_key missing in {settings_path}"
    
    def test_internal_api_key_authentication(self):
        """
        Contract test: order_service uses correct authentication for token_manager.
        
        Expected behavior:
        - Uses X-Internal-API-Key header for authentication
        - Uses correct API key from settings
        - Handles authentication failures appropriately
        """
        import inspect
        from order_service.app.services.kite_client_multi import resolve_trading_account_config
        
        expected_header = "X-Internal-API-Key"
        
        # Contract expectations
        assert expected_header == "X-Internal-API-Key"
        
        # Verify the authentication is actually used in the code
        # Check the resolve_trading_account_config function uses the header
        source = inspect.getsource(resolve_trading_account_config)
        assert "X-Internal-API-Key" in source, "resolve_trading_account_config should use X-Internal-API-Key header"
        assert "token_manager_api_key" in source, "resolve_trading_account_config should use token_manager_api_key setting"


class TestBackwardCompatibilityContract:
    """
    Contract tests to ensure backward compatibility during Sprint 1 transition.
    """
    
    def test_existing_kite_client_still_functional(self):
        """
        Contract test: Existing KiteClient (non-multi) remains unchanged.
        
        Expected behavior:
        - Single account KiteClient continues to work
        - No breaking changes to existing single-account workflows
        - Legacy functionality preserved during transition
        """
        # Validate that single-account client is unaffected by Sprint 1 changes
        from order_service.app.services.kite_client import get_kite_client
        import inspect
        
        # Verify single-account client function still exists
        assert callable(get_kite_client), "get_kite_client() should still exist"
        
        # Verify it's not async (maintains sync interface)
        assert not inspect.iscoroutinefunction(get_kite_client), \
            "get_kite_client() should remain synchronous"
        
        # Verify basic instantiation works (may fail without credentials, but should be importable)
        try:
            client = get_kite_client()
            # If it succeeds, verify it has expected KiteConnect interface
            assert hasattr(client, 'place_order'), "KiteConnect interface should be preserved"
            assert hasattr(client, 'orders'), "KiteConnect orders() method should be preserved"
            assert hasattr(client, 'positions'), "KiteConnect positions() method should be preserved"
        except Exception as e:
            # Expected if credentials not configured in test environment
            # But verify it's a credentials error, not an import/interface error
            error_msg = str(e).lower()
            valid_errors = ["api_key", "token", "credential", "auth", "key", "secret"]
            if not any(err in error_msg for err in valid_errors):
                # Re-raise if it's not a credentials issue
                raise AssertionError(f"Unexpected error in get_kite_client(): {e}")
    
    def test_order_placement_flow_unchanged(self):
        """
        Contract test: High-level order placement flow remains the same.
        
        Expected behavior:
        - OrderService.place_order() API unchanged
        - Same input parameters and return types
        - Only internal token resolution mechanism changes
        """
        # Validate that public API remains stable
        from order_service.app.services.order_service import OrderService
        import inspect
        
        # Verify OrderService.place_order method signature unchanged
        assert hasattr(OrderService, 'place_order'), "OrderService.place_order should exist"
        
        # Get method signature
        place_order_method = getattr(OrderService, 'place_order')
        sig = inspect.signature(place_order_method)
        
        # Verify expected parameters exist (basic contract validation)
        param_names = list(sig.parameters.keys())
        expected_params = ['self', 'strategy_id', 'symbol', 'exchange', 'transaction_type', 'quantity', 'order_type', 'product_type']
        
        for param in expected_params:
            assert param in param_names, f"OrderService.place_order should have {param} parameter"
        
        # Verify it returns an async result (modern async pattern)
        assert inspect.iscoroutinefunction(place_order_method), \
            "OrderService.place_order should be async"
        
        # Verify OrderService still has get_kite_client_for_account reference
        try:
            from order_service.app.services.kite_client_multi import get_kite_client_for_account
            assert callable(get_kite_client_for_account), "get_kite_client_for_account should be accessible"
        except ImportError as e:
            pytest.fail(f"OrderService should still have access to multi-account client: {e}")


class TestContractSettings:
    """
    Contract tests for configuration and settings compliance.
    """
    
    def test_settings_names_match_contract(self):
        """
        Contract test: Settings use correct naming from Sprint 0 specification.
        
        Expected settings:
        - token_manager_url (for service URL)  
        - token_manager_api_key (for authentication)
        - No deprecated or incorrect setting names
        """
        module_ast, settings_path = _load_settings_ast()
        field_names = _settings_class_fields(module_ast)
        
        assert "token_manager_url" in field_names, f"token_manager_url missing in {settings_path}"
        assert "token_manager_api_key" in field_names, f"token_manager_api_key missing in {settings_path}"
    
    def test_service_discovery_integration(self):
        """
        Contract test: Service discovery works for token_manager.
        
        Expected behavior:
        - token_manager URL resolved via service registry
        - Falls back to default port if service registry unavailable
        - No hardcoded URLs in production code
        """
        module_ast, settings_path = _load_settings_ast()
        assert _module_has_function(module_ast, "get_service_url"), \
            f"get_service_url missing in {settings_path}"
