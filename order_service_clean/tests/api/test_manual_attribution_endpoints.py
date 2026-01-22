"""
Tests for Manual Attribution API Endpoints

Tests the REST API endpoints for manual attribution workflow,
including case creation, assignment, resolution, and application.
"""

import pytest
from decimal import Decimal
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient
from fastapi import HTTPException

from order_service.app.api.v1.endpoints.manual_attribution import router
from order_service.app.services.manual_attribution_service import (
    AttributionCase,
    AttributionDecision,
    AttributionStatus,
    AttributionPriority
)


class TestManualAttributionEndpoints:
    """Test cases for manual attribution API endpoints."""

    @pytest.fixture
    def mock_auth_context(self):
        """Mock authentication context."""
        auth_context = MagicMock()
        auth_context.user_id = "test_user_123"
        auth_context.trading_account_ids = ["acc_001", "acc_002"]
        return auth_context

    @pytest.fixture
    def sample_case(self):
        """Sample attribution case for testing."""
        return AttributionCase(
            case_id="case_123",
            trading_account_id="acc_001",
            symbol="AAPL",
            exit_quantity=Decimal("100"),
            exit_price=Decimal("170.00"),
            exit_timestamp=datetime.now(timezone.utc),
            affected_positions=[
                {
                    "position_id": "pos_1",
                    "symbol": "AAPL",
                    "quantity": 150,
                    "strategy_id": 1
                }
            ],
            suggested_allocation=None,
            status=AttributionStatus.PENDING,
            priority=AttributionPriority.NORMAL,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            assigned_to=None,
            resolution_data=None,
            audit_trail=[]
        )

    @pytest.fixture
    def sample_create_request(self):
        """Sample case creation request."""
        return {
            "trading_account_id": "acc_001",
            "symbol": "AAPL",
            "exit_quantity": "100",
            "exit_price": "170.00",
            "exit_timestamp": datetime.now(timezone.utc).isoformat(),
            "affected_positions": [
                {
                    "position_id": "pos_1",
                    "symbol": "AAPL",
                    "quantity": 150,
                    "strategy_id": 1
                }
            ],
            "priority": "normal"
        }

    async def test_create_attribution_case_success(self, mock_auth_context, sample_create_request):
        """Test successful attribution case creation."""
        with patch('order_service.app.api.v1.endpoints.manual_attribution.require_permissions') as mock_auth:
            mock_auth.return_value = lambda: mock_auth_context
            
            with patch('order_service.app.api.v1.endpoints.manual_attribution.get_session') as mock_get_db:
                mock_db = AsyncMock()
                mock_get_db.return_value = mock_db
                
                with patch('order_service.app.api.v1.endpoints.manual_attribution.ManualAttributionService') as mock_service:
                    mock_service_instance = mock_service.return_value
                    mock_service_instance.create_attribution_case.return_value = "case_new_123"
                    
                    from order_service.app.api.v1.endpoints.manual_attribution import create_attribution_case
                    
                    # Simulate request object
                    request_obj = MagicMock()
                    for key, value in sample_create_request.items():
                        setattr(request_obj, key, value)
                    
                    response = await create_attribution_case(request_obj, mock_auth_context, mock_db)
                    
                    assert response["case_id"] == "case_new_123"
                    assert response["status"] == "created"
                    mock_service_instance.create_attribution_case.assert_called_once()

    async def test_create_attribution_case_service_error(self, mock_auth_context, sample_create_request):
        """Test case creation with service error."""
        with patch('order_service.app.api.v1.endpoints.manual_attribution.require_permissions') as mock_auth:
            mock_auth.return_value = lambda: mock_auth_context
            
            with patch('order_service.app.api.v1.endpoints.manual_attribution.get_session') as mock_get_db:
                mock_db = AsyncMock()
                mock_get_db.return_value = mock_db
                
                with patch('order_service.app.api.v1.endpoints.manual_attribution.ManualAttributionService') as mock_service:
                    mock_service_instance = mock_service.return_value
                    mock_service_instance.create_attribution_case.side_effect = Exception("Service error")
                    
                    from order_service.app.api.v1.endpoints.manual_attribution import create_attribution_case
                    
                    request_obj = MagicMock()
                    for key, value in sample_create_request.items():
                        setattr(request_obj, key, value)
                    
                    with pytest.raises(HTTPException) as exc_info:
                        await create_attribution_case(request_obj, mock_auth_context, mock_db)
                    
                    assert exc_info.value.status_code == 500
                    assert "Failed to create case" in str(exc_info.value.detail)

    async def test_get_attribution_case_success(self, mock_auth_context, sample_case):
        """Test successful attribution case retrieval."""
        case_id = "case_123"
        
        with patch('order_service.app.api.v1.endpoints.manual_attribution.require_permissions') as mock_auth:
            mock_auth.return_value = lambda: mock_auth_context
            
            with patch('order_service.app.api.v1.endpoints.manual_attribution.get_session') as mock_get_db:
                mock_db = AsyncMock()
                mock_get_db.return_value = mock_db
                
                with patch('order_service.app.api.v1.endpoints.manual_attribution.ManualAttributionService') as mock_service:
                    mock_service_instance = mock_service.return_value
                    mock_service_instance.get_attribution_case.return_value = sample_case
                    
                    from order_service.app.api.v1.endpoints.manual_attribution import get_attribution_case
                    
                    response = await get_attribution_case(case_id, mock_auth_context, mock_db)
                    
                    assert response.case_id == "case_123"
                    assert response.symbol == "AAPL"
                    assert response.exit_quantity == "100"
                    assert response.status == "pending"

    async def test_get_attribution_case_not_found(self, mock_auth_context):
        """Test attribution case retrieval when case not found."""
        case_id = "nonexistent_case"
        
        with patch('order_service.app.api.v1.endpoints.manual_attribution.require_permissions') as mock_auth:
            mock_auth.return_value = lambda: mock_auth_context
            
            with patch('order_service.app.api.v1.endpoints.manual_attribution.get_session') as mock_get_db:
                mock_db = AsyncMock()
                mock_get_db.return_value = mock_db
                
                with patch('order_service.app.api.v1.endpoints.manual_attribution.ManualAttributionService') as mock_service:
                    mock_service_instance = mock_service.return_value
                    mock_service_instance.get_attribution_case.return_value = None
                    
                    from order_service.app.api.v1.endpoints.manual_attribution import get_attribution_case
                    
                    with pytest.raises(HTTPException) as exc_info:
                        await get_attribution_case(case_id, mock_auth_context, mock_db)
                    
                    assert exc_info.value.status_code == 404
                    assert f"Case {case_id} not found" in str(exc_info.value.detail)

    async def test_list_attribution_cases_success(self, mock_auth_context, sample_case):
        """Test successful listing of attribution cases."""
        # Mock database count query
        mock_count_result = MagicMock()
        mock_count_result.fetchone.return_value = (1,)  # Total count = 1
        
        with patch('order_service.app.api.v1.endpoints.manual_attribution.require_permissions') as mock_auth:
            mock_auth.return_value = lambda: mock_auth_context
            
            with patch('order_service.app.api.v1.endpoints.manual_attribution.get_session') as mock_get_db:
                mock_db = AsyncMock()
                mock_db.return_value = mock_db
                mock_db.execute.return_value = mock_count_result
                
                with patch('order_service.app.api.v1.endpoints.manual_attribution.ManualAttributionService') as mock_service:
                    mock_service_instance = mock_service.return_value
                    mock_service_instance.list_pending_cases.return_value = [sample_case]
                    
                    from order_service.app.api.v1.endpoints.manual_attribution import list_attribution_cases
                    
                    response = await list_attribution_cases(
                        trading_account_id="acc_001",
                        symbol=None,
                        priority=None,
                        assigned_to=None,
                        status=None,
                        limit=50,
                        offset=0,
                        auth_context=mock_auth_context,
                        db=mock_db
                    )
                    
                    assert len(response.cases) == 1
                    assert response.cases[0].case_id == "case_123"
                    assert response.total_count == 1
                    assert response.has_more is False

    async def test_list_attribution_cases_access_denied(self, mock_auth_context):
        """Test access denied when requesting unauthorized trading account."""
        # User tries to access account not in their list
        unauthorized_account = "acc_999"
        mock_auth_context.trading_account_ids = ["acc_001", "acc_002"]  # Not acc_999
        
        with patch('order_service.app.api.v1.endpoints.manual_attribution.require_permissions') as mock_auth:
            mock_auth.return_value = lambda: mock_auth_context
            
            with patch('order_service.app.api.v1.endpoints.manual_attribution.get_session') as mock_get_db:
                mock_db = AsyncMock()
                mock_get_db.return_value = mock_db
                
                from order_service.app.api.v1.endpoints.manual_attribution import list_attribution_cases
                
                with pytest.raises(HTTPException) as exc_info:
                    await list_attribution_cases(
                        trading_account_id=unauthorized_account,
                        symbol=None,
                        priority=None,
                        assigned_to=None,
                        status=None,
                        limit=50,
                        offset=0,
                        auth_context=mock_auth_context,
                        db=mock_db
                    )
                
                assert exc_info.value.status_code == 403
                assert "Access denied" in str(exc_info.value.detail)

    async def test_assign_attribution_case_success(self, mock_auth_context):
        """Test successful case assignment."""
        case_id = "case_123"
        assigned_to = "user_456"
        
        with patch('order_service.app.api.v1.endpoints.manual_attribution.require_permissions') as mock_auth:
            mock_auth.return_value = lambda: mock_auth_context
            
            with patch('order_service.app.api.v1.endpoints.manual_attribution.get_session') as mock_get_db:
                mock_db = AsyncMock()
                mock_get_db.return_value = mock_db
                
                with patch('order_service.app.api.v1.endpoints.manual_attribution.ManualAttributionService') as mock_service:
                    mock_service_instance = mock_service.return_value
                    mock_service_instance.assign_case.return_value = True
                    
                    from order_service.app.api.v1.endpoints.manual_attribution import assign_attribution_case
                    
                    request_obj = MagicMock()
                    request_obj.assigned_to = assigned_to
                    
                    response = await assign_attribution_case(case_id, request_obj, mock_auth_context, mock_db)
                    
                    assert response["case_id"] == case_id
                    assert response["assigned_to"] == assigned_to
                    assert response["status"] == "assigned"

    async def test_assign_attribution_case_invalid_case(self, mock_auth_context):
        """Test case assignment with invalid case."""
        case_id = "invalid_case"
        
        with patch('order_service.app.api.v1.endpoints.manual_attribution.require_permissions') as mock_auth:
            mock_auth.return_value = lambda: mock_auth_context
            
            with patch('order_service.app.api.v1.endpoints.manual_attribution.get_session') as mock_get_db:
                mock_db = AsyncMock()
                mock_get_db.return_value = mock_db
                
                with patch('order_service.app.api.v1.endpoints.manual_attribution.ManualAttributionService') as mock_service:
                    mock_service_instance = mock_service.return_value
                    mock_service_instance.assign_case.side_effect = ValueError("Case not found")
                    
                    from order_service.app.api.v1.endpoints.manual_attribution import assign_attribution_case
                    
                    request_obj = MagicMock()
                    request_obj.assigned_to = "user_456"
                    
                    with pytest.raises(HTTPException) as exc_info:
                        await assign_attribution_case(case_id, request_obj, mock_auth_context, mock_db)
                    
                    assert exc_info.value.status_code == 400
                    assert "Case not found" in str(exc_info.value.detail)

    async def test_resolve_attribution_case_success(self, mock_auth_context):
        """Test successful case resolution."""
        case_id = "case_123"
        
        allocation_decisions = [
            {"position_id": "pos_1", "quantity": 75, "strategy_id": 1},
            {"position_id": "pos_2", "quantity": 25, "strategy_id": 2}
        ]
        decision_rationale = "Manual analysis determined best allocation"
        
        with patch('order_service.app.api.v1.endpoints.manual_attribution.require_permissions') as mock_auth:
            mock_auth.return_value = lambda: mock_auth_context
            
            with patch('order_service.app.api.v1.endpoints.manual_attribution.get_session') as mock_get_db:
                mock_db = AsyncMock()
                mock_get_db.return_value = mock_db
                
                with patch('order_service.app.api.v1.endpoints.manual_attribution.ManualAttributionService') as mock_service:
                    mock_service_instance = mock_service.return_value
                    mock_service_instance.resolve_case.return_value = True
                    
                    from order_service.app.api.v1.endpoints.manual_attribution import resolve_attribution_case
                    
                    request_obj = MagicMock()
                    request_obj.allocation_decisions = allocation_decisions
                    request_obj.decision_rationale = decision_rationale
                    
                    response = await resolve_attribution_case(case_id, request_obj, mock_auth_context, mock_db)
                    
                    assert response["case_id"] == case_id
                    assert response["status"] == "resolved"
                    assert response["decision_maker"] == mock_auth_context.user_id

    async def test_resolve_attribution_case_invalid_decision(self, mock_auth_context):
        """Test case resolution with invalid decision."""
        case_id = "case_123"
        
        with patch('order_service.app.api.v1.endpoints.manual_attribution.require_permissions') as mock_auth:
            mock_auth.return_value = lambda: mock_auth_context
            
            with patch('order_service.app.api.v1.endpoints.manual_attribution.get_session') as mock_get_db:
                mock_db = AsyncMock()
                mock_get_db.return_value = mock_db
                
                with patch('order_service.app.api.v1.endpoints.manual_attribution.ManualAttributionService') as mock_service:
                    mock_service_instance = mock_service.return_value
                    mock_service_instance.resolve_case.side_effect = ValueError("Invalid allocation")
                    
                    from order_service.app.api.v1.endpoints.manual_attribution import resolve_attribution_case
                    
                    request_obj = MagicMock()
                    request_obj.allocation_decisions = [{"invalid": "decision"}]
                    request_obj.decision_rationale = "Test"
                    
                    with pytest.raises(HTTPException) as exc_info:
                        await resolve_attribution_case(case_id, request_obj, mock_auth_context, mock_db)
                    
                    assert exc_info.value.status_code == 400
                    assert "Invalid allocation" in str(exc_info.value.detail)

    async def test_apply_attribution_resolution_success(self, mock_auth_context):
        """Test successful application of resolution."""
        case_id = "case_123"
        
        with patch('order_service.app.api.v1.endpoints.manual_attribution.require_permissions') as mock_auth:
            mock_auth.return_value = lambda: mock_auth_context
            
            with patch('order_service.app.api.v1.endpoints.manual_attribution.get_session') as mock_get_db:
                mock_db = AsyncMock()
                mock_get_db.return_value = mock_db
                
                with patch('order_service.app.api.v1.endpoints.manual_attribution.ManualAttributionService') as mock_service:
                    mock_service_instance = mock_service.return_value
                    mock_service_instance.apply_resolution.return_value = True
                    
                    from order_service.app.api.v1.endpoints.manual_attribution import apply_attribution_resolution
                    
                    response = await apply_attribution_resolution(case_id, mock_auth_context, mock_db)
                    
                    assert response["case_id"] == case_id
                    assert response["status"] == "applied"
                    assert response["applied_by"] == mock_auth_context.user_id

    async def test_apply_attribution_resolution_not_resolved(self, mock_auth_context):
        """Test application failure when case is not resolved."""
        case_id = "case_123"
        
        with patch('order_service.app.api.v1.endpoints.manual_attribution.require_permissions') as mock_auth:
            mock_auth.return_value = lambda: mock_auth_context
            
            with patch('order_service.app.api.v1.endpoints.manual_attribution.get_session') as mock_get_db:
                mock_db = AsyncMock()
                mock_get_db.return_value = mock_db
                
                with patch('order_service.app.api.v1.endpoints.manual_attribution.ManualAttributionService') as mock_service:
                    mock_service_instance = mock_service.return_value
                    mock_service_instance.apply_resolution.side_effect = ValueError("Case not resolved")
                    
                    from order_service.app.api.v1.endpoints.manual_attribution import apply_attribution_resolution
                    
                    with pytest.raises(HTTPException) as exc_info:
                        await apply_attribution_resolution(case_id, mock_auth_context, mock_db)
                    
                    assert exc_info.value.status_code == 400
                    assert "Case not resolved" in str(exc_info.value.detail)

    async def test_get_attribution_stats_success(self, mock_auth_context):
        """Test successful attribution statistics retrieval."""
        # Mock database stats query
        mock_stats_result = MagicMock()
        mock_stats_result.fetchall.return_value = [
            ("pending", "high", 2),
            ("pending", "normal", 5),
            ("resolved", "high", 1),
            ("applied", "normal", 3)
        ]
        
        with patch('order_service.app.api.v1.endpoints.manual_attribution.require_permissions') as mock_auth:
            mock_auth.return_value = lambda: mock_auth_context
            
            with patch('order_service.app.api.v1.endpoints.manual_attribution.get_session') as mock_get_db:
                mock_db = AsyncMock()
                mock_db.return_value = mock_db
                mock_db.execute.return_value = mock_stats_result
                
                from order_service.app.api.v1.endpoints.manual_attribution import get_attribution_stats
                
                response = await get_attribution_stats(
                    trading_account_id="acc_001",
                    auth_context=mock_auth_context,
                    db=mock_db
                )
                
                assert "by_status" in response
                assert "by_priority" in response
                assert "total_cases" in response
                assert response["by_status"]["pending"] == 7  # 2 + 5
                assert response["by_priority"]["high"] == 3   # 2 + 1
                assert response["total_cases"] == 11         # Sum of all

    async def test_get_attribution_stats_unauthorized_account(self, mock_auth_context):
        """Test stats retrieval with unauthorized trading account."""
        unauthorized_account = "acc_999"
        mock_auth_context.trading_account_ids = ["acc_001", "acc_002"]
        
        with patch('order_service.app.api.v1.endpoints.manual_attribution.require_permissions') as mock_auth:
            mock_auth.return_value = lambda: mock_auth_context
            
            with patch('order_service.app.api.v1.endpoints.manual_attribution.get_session') as mock_get_db:
                mock_db = AsyncMock()
                mock_get_db.return_value = mock_db
                
                from order_service.app.api.v1.endpoints.manual_attribution import get_attribution_stats
                
                with pytest.raises(HTTPException) as exc_info:
                    await get_attribution_stats(
                        trading_account_id=unauthorized_account,
                        auth_context=mock_auth_context,
                        db=mock_db
                    )
                
                assert exc_info.value.status_code == 403
                assert "Access denied" in str(exc_info.value.detail)

    async def test_pagination_with_correct_total_count(self, mock_auth_context, sample_case):
        """Test that pagination returns correct total count."""
        # Mock the specific pagination fix from the API
        mock_count_result = MagicMock()
        mock_count_result.fetchone.return_value = (15,)  # Total 15 cases
        
        with patch('order_service.app.api.v1.endpoints.manual_attribution.require_permissions') as mock_auth:
            mock_auth.return_value = lambda: mock_auth_context
            
            with patch('order_service.app.api.v1.endpoints.manual_attribution.get_session') as mock_get_db:
                mock_db = AsyncMock()
                mock_db.return_value = mock_db
                mock_db.execute.return_value = mock_count_result
                
                with patch('order_service.app.api.v1.endpoints.manual_attribution.ManualAttributionService') as mock_service:
                    mock_service_instance = mock_service.return_value
                    mock_service_instance.list_pending_cases.return_value = [sample_case] * 10  # Return 10 cases
                    
                    from order_service.app.api.v1.endpoints.manual_attribution import list_attribution_cases
                    
                    response = await list_attribution_cases(
                        trading_account_id="acc_001",
                        symbol=None,
                        priority=None,
                        assigned_to=None,
                        status=None,
                        limit=10,
                        offset=5,
                        auth_context=mock_auth_context,
                        db=mock_db
                    )
                    
                    assert len(response.cases) == 10    # Current page size
                    assert response.total_count == 15   # Actual total from database
                    assert response.has_more is True    # 5 + 10 < 15

    @pytest.mark.parametrize("priority_filter,expected_priority", [
        ("high", AttributionPriority.HIGH),
        ("normal", AttributionPriority.NORMAL),
        ("low", AttributionPriority.LOW),
        ("urgent", AttributionPriority.URGENT),
        (None, None)
    ])
    async def test_list_cases_priority_filtering(self, mock_auth_context, sample_case, priority_filter, expected_priority):
        """Test case listing with different priority filters."""
        mock_count_result = MagicMock()
        mock_count_result.fetchone.return_value = (1,)
        
        with patch('order_service.app.api.v1.endpoints.manual_attribution.require_permissions') as mock_auth:
            mock_auth.return_value = lambda: mock_auth_context
            
            with patch('order_service.app.api.v1.endpoints.manual_attribution.get_session') as mock_get_db:
                mock_db = AsyncMock()
                mock_db.return_value = mock_db
                mock_db.execute.return_value = mock_count_result
                
                with patch('order_service.app.api.v1.endpoints.manual_attribution.ManualAttributionService') as mock_service:
                    mock_service_instance = mock_service.return_value
                    mock_service_instance.list_pending_cases.return_value = [sample_case]
                    
                    from order_service.app.api.v1.endpoints.manual_attribution import list_attribution_cases
                    
                    response = await list_attribution_cases(
                        trading_account_id="acc_001",
                        symbol=None,
                        priority=priority_filter,
                        assigned_to=None,
                        status=None,
                        limit=50,
                        offset=0,
                        auth_context=mock_auth_context,
                        db=mock_db
                    )
                    
                    # Verify service was called with correct priority filter
                    call_args = mock_service_instance.list_pending_cases.call_args[1]
                    assert call_args["priority"] == expected_priority

    async def test_error_handling_service_exception(self, mock_auth_context):
        """Test generic error handling for service exceptions."""
        case_id = "case_123"
        
        with patch('order_service.app.api.v1.endpoints.manual_attribution.require_permissions') as mock_auth:
            mock_auth.return_value = lambda: mock_auth_context
            
            with patch('order_service.app.api.v1.endpoints.manual_attribution.get_session') as mock_get_db:
                mock_db = AsyncMock()
                mock_get_db.return_value = mock_db
                
                with patch('order_service.app.api.v1.endpoints.manual_attribution.ManualAttributionService') as mock_service:
                    mock_service_instance = mock_service.return_value
                    mock_service_instance.get_attribution_case.side_effect = Exception("Unexpected error")
                    
                    from order_service.app.api.v1.endpoints.manual_attribution import get_attribution_case
                    
                    with pytest.raises(HTTPException) as exc_info:
                        await get_attribution_case(case_id, mock_auth_context, mock_db)
                    
                    assert exc_info.value.status_code == 500
                    assert "Failed to get case" in str(exc_info.value.detail)


class TestManualAttributionPermissions:
    """Test permission-based access control."""

    async def test_create_case_permission_required(self):
        """Test that create_case requires manual_attribution:create permission."""
        # This would test the actual permission decorator in integration
        pass

    async def test_read_permission_required(self):
        """Test that get/list operations require manual_attribution:read permission."""
        pass

    async def test_assign_permission_required(self):
        """Test that assign_case requires manual_attribution:assign permission."""
        pass

    async def test_resolve_permission_required(self):
        """Test that resolve_case requires manual_attribution:resolve permission."""
        pass

    async def test_apply_permission_required(self):
        """Test that apply_resolution requires manual_attribution:apply permission."""
        pass


class TestManualAttributionValidation:
    """Test request validation and data conversion."""

    async def test_create_request_validation(self):
        """Test validation of case creation request."""
        # Test missing required fields
        # Test invalid data types
        # Test field constraints
        pass

    async def test_decision_request_validation(self):
        """Test validation of attribution decision request."""
        # Test allocation decision structure
        # Test quantity validation
        # Test rationale requirements
        pass

    async def test_datetime_parsing(self):
        """Test datetime parsing in requests."""
        # Test ISO format parsing
        # Test timezone handling
        pass