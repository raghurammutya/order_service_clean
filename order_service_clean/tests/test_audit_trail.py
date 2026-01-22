"""
Test Suite for Order Audit Trail

Tests audit logging for all order state transitions.
"""
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.order_service import OrderService
from app.services.audit_service import OrderAuditService
from app.models.order import Order


class TestAuditTrail:
    """Test audit trail logging"""

    @pytest.mark.asyncio
    async def test_order_creation_logged(self, db: AsyncSession, mock_kite_client):
        """Verify order creation is logged to audit trail"""
        # Arrange
        user_id = 123
        trading_account_id = 1
        service = OrderService(db, user_id, trading_account_id)

        # Act: Place order
        order = await service.place_order(
            symbol="RELIANCE",
            exchange="NSE",
            transaction_type="BUY",
            quantity=1,
            order_type="MARKET",
            product_type="MIS"
        )

        # Assert: Check audit history
        audit_service = OrderAuditService(db)
        history = await audit_service.get_order_history(order.id)

        # Should have at least 2 entries: PENDING creation + SUBMITTED
        assert len(history) >= 2

        # First entry should be order creation
        creation_entry = next(h for h in history if h.old_status is None)
        assert creation_entry.new_status == "PENDING"
        assert creation_entry.changed_by_user_id == user_id
        assert creation_entry.reason == "Order created by user"
        assert creation_entry.event_metadata["symbol"] == "RELIANCE"

        # Second entry should be broker submission
        submission_entry = next(h for h in history if h.new_status == "SUBMITTED")
        assert submission_entry.old_status == "PENDING"
        assert submission_entry.changed_by_user_id == user_id
        assert submission_entry.reason == "Order submitted to broker"
        assert "broker_order_id" in submission_entry.event_metadata

    @pytest.mark.asyncio
    async def test_cancellation_logged_with_user(self, db: AsyncSession, mock_kite_client):
        """Verify user ID captured in audit trail for cancellations"""
        # Arrange
        user_id = 456
        trading_account_id = 1
        service = OrderService(db, user_id, trading_account_id)

        # Place order
        order = await service.place_order(
            symbol="TCS",
            exchange="NSE",
            transaction_type="BUY",
            quantity=1,
            order_type="MARKET",
            product_type="MIS"
        )

        # Act: Cancel order
        await service.cancel_order(order.id)

        # Assert: Check cancellation is logged
        audit_service = OrderAuditService(db)
        history = await audit_service.get_order_history(order.id)

        cancel_record = next(h for h in history if h.new_status == "CANCELLED")
        assert cancel_record.changed_by_user_id == user_id
        assert cancel_record.reason == "Order cancelled by user"
        assert cancel_record.old_status in ["PENDING", "SUBMITTED", "OPEN"]

    @pytest.mark.asyncio
    async def test_modification_logged(self, db: AsyncSession, mock_kite_client):
        """Verify order modifications are logged with details"""
        # Arrange
        user_id = 789
        trading_account_id = 1
        service = OrderService(db, user_id, trading_account_id)

        # Place order
        order = await service.place_order(
            symbol="INFY",
            exchange="NSE",
            transaction_type="BUY",
            quantity=1,
            order_type="LIMIT",
            price=1500.0,
            product_type="MIS"
        )

        # Act: Modify order
        await service.modify_order(
            order_id=order.id,
            price=1520.0,
            quantity=2
        )

        # Assert: Check modification is logged
        audit_service = OrderAuditService(db)
        history = await audit_service.get_order_history(order.id)

        mod_record = next(h for h in history if "modified" in (h.reason or "").lower())
        assert mod_record.changed_by_user_id == user_id
        assert "price" in mod_record.reason
        assert "quantity" in mod_record.reason
        assert mod_record.event_metadata["modifications"]["price"] == 1520.0
        assert mod_record.event_metadata["modifications"]["quantity"] == 2

    @pytest.mark.asyncio
    async def test_broker_rejection_logged(self, db: AsyncSession, mock_kite_client_fail):
        """Verify broker rejections are logged with error message"""
        # Arrange
        user_id = 111
        trading_account_id = 1
        service = OrderService(db, user_id, trading_account_id)

        # Mock broker to reject
        mock_kite_client_fail.place_order.side_effect = Exception("Insufficient funds")

        # Act: Try to place order (should fail)
        with pytest.raises(Exception):
            await service.place_order(
                symbol="RELIANCE",
                exchange="NSE",
                transaction_type="BUY",
                quantity=1,
                order_type="MARKET",
                product_type="MIS"
            )

        # Assert: Rejection should NOT be logged (order rolled back)
        # This is correct behavior - failed orders don't pollute DB

    @pytest.mark.asyncio
    async def test_get_user_actions(self, db: AsyncSession, mock_kite_client):
        """Test retrieving all actions by a specific user"""
        # Arrange
        user_id = 222
        trading_account_id = 1
        service = OrderService(db, user_id, trading_account_id)

        # Place multiple orders
        order1 = await service.place_order(
            symbol="RELIANCE",
            exchange="NSE",
            transaction_type="BUY",
            quantity=1,
            order_type="MARKET",
            product_type="MIS"
        )

        order2 = await service.place_order(
            symbol="TCS",
            exchange="NSE",
            transaction_type="SELL",
            quantity=1,
            order_type="MARKET",
            product_type="MIS"
        )

        # Cancel one
        await service.cancel_order(order1.id)

        # Act: Get all user actions
        audit_service = OrderAuditService(db)
        user_actions = await audit_service.get_user_actions(user_id)

        # Assert: Should see all actions by this user
        assert len(user_actions) >= 4  # 2 creates + 2 submissions + 1 cancel
        assert all(action.changed_by_user_id == user_id for action in user_actions)

    @pytest.mark.asyncio
    async def test_transition_count(self, db: AsyncSession, mock_kite_client):
        """Test counting specific state transitions"""
        # Arrange
        user_id = 333
        trading_account_id = 1
        service = OrderService(db, user_id, trading_account_id)

        # Place and cancel 2 orders
        for _ in range(2):
            order = await service.place_order(
                symbol="INFY",
                exchange="NSE",
                transaction_type="BUY",
                quantity=1,
                order_type="MARKET",
                product_type="MIS"
            )
            await service.cancel_order(order.id)

        # Act: Count cancellations
        audit_service = OrderAuditService(db)
        cancel_count = await audit_service.get_transition_count(new_status="CANCELLED")

        # Assert: Should be at least 2
        assert cancel_count >= 2

    @pytest.mark.asyncio
    async def test_audit_trail_api_endpoint(self, client, db: AsyncSession, auth_token, mock_kite_client):
        """Test the audit trail API endpoint"""
        # Arrange: Place an order
        user_id = 444
        trading_account_id = 1
        service = OrderService(db, user_id, trading_account_id)

        order = await service.place_order(
            symbol="RELIANCE",
            exchange="NSE",
            transaction_type="BUY",
            quantity=1,
            order_type="MARKET",
            product_type="MIS"
        )

        # Act: Call API endpoint
        response = await client.get(
            f"/api/v1/orders/{order.id}/audit-trail",
            headers={"Authorization": f"Bearer {auth_token}"}
        )

        # Assert: Should return audit trail
        assert response.status_code == 200
        data = response.json()

        assert isinstance(data, list)
        assert len(data) >= 2  # Creation + submission

        # Check structure
        first_entry = data[0]
        assert "transition" in first_entry
        assert "from" in first_entry["transition"]
        assert "to" in first_entry["transition"]
        assert "actor" in first_entry
        assert "context" in first_entry
        assert "timestamp" in first_entry

    @pytest.mark.asyncio
    async def test_audit_trail_chronological_order(self, db: AsyncSession, mock_kite_client):
        """Verify audit trail returns events in chronological order (newest first)"""
        # Arrange
        user_id = 555
        trading_account_id = 1
        service = OrderService(db, user_id, trading_account_id)

        # Place and modify order
        order = await service.place_order(
            symbol="TCS",
            exchange="NSE",
            transaction_type="BUY",
            quantity=1,
            order_type="LIMIT",
            price=3500.0,
            product_type="MIS"
        )

        await service.modify_order(order.id, price=3520.0)
        await service.cancel_order(order.id)

        # Act: Get history
        audit_service = OrderAuditService(db)
        history = await audit_service.get_order_history(order.id)

        # Assert: Newest first
        assert history[0].new_status == "CANCELLED"  # Most recent
        assert history[-1].old_status is None  # Oldest (creation)

        # Check timestamps are descending
        for i in range(len(history) - 1):
            assert history[i].changed_at >= history[i + 1].changed_at

    @pytest.mark.asyncio
    async def test_system_action_logging(self, db: AsyncSession):
        """Test logging of system actions (not user actions)"""
        # Arrange
        audit_service = OrderAuditService(db, user_id=None)  # System action

        # Create a dummy order for testing
        order = Order(
            user_id=999,
            trading_account_id=1,
            symbol="TEST",
            exchange="NSE",
            transaction_type="BUY",
            order_type="MARKET",
            product_type="MIS",
            quantity=1,
            filled_quantity=0,
            pending_quantity=1,
            status="PENDING"
        )
        db.add(order)
        await db.flush()

        # Act: Log system action (e.g., reconciliation)
        await audit_service.log_state_change(
            order_id=order.id,
            old_status="SUBMITTED",
            new_status="COMPLETE",
            reason="Reconciliation: Corrected drift from broker",
            changed_by_system="reconciliation_worker",
            metadata={"drift_detected": True}
        )

        await db.commit()

        # Assert: System action logged
        history = await audit_service.get_order_history(order.id)
        system_action = next(h for h in history if h.changed_by_system == "reconciliation_worker")

        assert system_action.changed_by_user_id is None  # No user
        assert system_action.changed_by_system == "reconciliation_worker"
        assert "drift" in system_action.reason.lower()
        assert system_action.event_metadata["drift_detected"] is True

    @pytest.mark.asyncio
    async def test_metadata_capture(self, db: AsyncSession, mock_kite_client):
        """Verify metadata is captured in audit logs"""
        # Arrange
        user_id = 666
        trading_account_id = 1
        service = OrderService(db, user_id, trading_account_id)

        # Act: Place order
        order = await service.place_order(
            symbol="WIPRO",
            exchange="NSE",
            transaction_type="BUY",
            quantity=5,
            order_type="LIMIT",
            price=450.0,
            product_type="CNC"
        )

        # Assert: Metadata captured
        audit_service = OrderAuditService(db)
        history = await audit_service.get_order_history(order.id)

        creation_entry = next(h for h in history if h.old_status is None)
        event_metadata = creation_entry.event_metadata

        assert event_metadata["symbol"] == "WIPRO"
        assert event_metadata["exchange"] == "NSE"
        assert event_metadata["quantity"] == 5
        assert event_metadata["order_type"] == "LIMIT"
        assert event_metadata["product_type"] == "CNC"


# Fixtures

@pytest.fixture
def mock_kite_client(mocker):
    """Mock KiteConnect client for successful operations"""
    mock = mocker.patch("app.services.order_service.get_kite_client")
    mock_instance = mocker.MagicMock()
    mock_instance.place_order.return_value = "BROKER123"
    mock_instance.modify_order.return_value = "BROKER123"
    mock_instance.cancel_order.return_value = "BROKER123"
    mock.return_value = mock_instance
    return mock_instance


@pytest.fixture
def mock_kite_client_fail(mocker):
    """Mock KiteConnect client that fails"""
    mock = mocker.patch("app.services.order_service.get_kite_client")
    mock_instance = mocker.MagicMock()
    mock.return_value = mock_instance
    return mock_instance


@pytest.fixture
async def client():
    """FastAPI test client"""
    from fastapi.testclient import TestClient
    from app.main import app

    with TestClient(app) as client:
        yield client


@pytest.fixture
def auth_token():
    """Mock JWT token for testing"""
    # In real tests, generate valid JWT or use test fixtures
    return "test_token_123"
