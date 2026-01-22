"""
Test Suite for Order Reconciliation

Tests reconciliation service and worker functionality.
"""
import pytest
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.reconciliation_service import ReconciliationService
from app.services.audit_service import OrderAuditService
from app.models.order import Order


class TestReconciliationService:
    """Test reconciliation service"""

    @pytest.mark.asyncio
    async def test_no_drift_detected(self, db: AsyncSession, mock_kite_client):
        """Verify no action taken when database matches broker"""
        # Arrange: Create order in database
        order = Order(
            user_id=123,
            trading_account_id=1,
            symbol="RELIANCE",
            exchange="NSE",
            transaction_type="BUY",
            order_type="MARKET",
            product_type="MIS",
            quantity=1,
            filled_quantity=0,
            pending_quantity=1,
            status="OPEN",
            broker_order_id="BROKER123",
            created_at=datetime.utcnow()
        )
        db.add(order)
        await db.flush()

        # Mock broker to return same status
        mock_kite_client.orders.return_value = [
            {
                "order_id": "BROKER123",
                "status": "OPEN",
                "filled_quantity": 0
            }
        ]

        # Act: Run reconciliation
        reconciliation = ReconciliationService(db)
        result = await reconciliation.reconcile_pending_orders(max_age_hours=24, batch_size=100)

        # Assert: No drift detected
        assert result["total_checked"] == 1
        assert result["drift_count"] == 0
        assert result["corrected"] == 0
        assert result["errors"] == 0

    @pytest.mark.asyncio
    async def test_drift_detected_and_corrected(self, db: AsyncSession, mock_kite_client):
        """Verify drift is detected and corrected"""
        # Arrange: Create order in OPEN state in database
        order = Order(
            user_id=456,
            trading_account_id=1,
            symbol="TCS",
            exchange="NSE",
            transaction_type="BUY",
            order_type="MARKET",
            product_type="MIS",
            quantity=1,
            filled_quantity=0,
            pending_quantity=1,
            status="OPEN",  # DB says OPEN
            broker_order_id="BROKER456",
            created_at=datetime.utcnow()
        )
        db.add(order)
        await db.flush()

        # Mock broker to return COMPLETE status
        mock_kite_client.orders.return_value = [
            {
                "order_id": "BROKER456",
                "status": "COMPLETE",  # Broker says COMPLETE
                "filled_quantity": 1,
                "average_price": 3500.0
            }
        ]

        # Act: Run reconciliation
        reconciliation = ReconciliationService(db)
        result = await reconciliation.reconcile_pending_orders(max_age_hours=24, batch_size=100)

        # Assert: Drift detected and corrected
        assert result["total_checked"] == 1
        assert result["drift_count"] == 1
        assert result["corrected"] == 1
        assert result["errors"] == 0

        # Verify correction details
        correction = result["corrections"][0]
        assert correction["order_id"] == order.id
        assert correction["db_status"] == "OPEN"
        assert correction["broker_status"] == "COMPLETE"
        assert correction["corrected"] is True

        # Verify order was updated in database
        await db.refresh(order)
        assert order.status == "COMPLETE"
        assert order.filled_quantity == 1
        assert order.average_price == 3500.0

    @pytest.mark.asyncio
    async def test_audit_trail_logged_on_correction(self, db: AsyncSession, mock_kite_client):
        """Verify audit trail is logged when drift is corrected"""
        # Arrange: Create order with drift
        order = Order(
            user_id=789,
            trading_account_id=1,
            symbol="INFY",
            exchange="NSE",
            transaction_type="BUY",
            order_type="MARKET",
            product_type="MIS",
            quantity=1,
            filled_quantity=0,
            pending_quantity=1,
            status="SUBMITTED",
            broker_order_id="BROKER789",
            created_at=datetime.utcnow()
        )
        db.add(order)
        await db.flush()

        # Mock broker to return CANCELLED status
        mock_kite_client.orders.return_value = [
            {
                "order_id": "BROKER789",
                "status": "CANCELLED",
                "filled_quantity": 0
            }
        ]

        # Act: Run reconciliation
        reconciliation = ReconciliationService(db)
        await reconciliation.reconcile_pending_orders(max_age_hours=24, batch_size=100)

        # Assert: Audit trail logged
        audit_service = OrderAuditService(db)
        history = await audit_service.get_order_history(order.id)

        # Should have reconciliation entry
        reconciliation_entry = next(
            (h for h in history if h.changed_by_system == "reconciliation_worker"),
            None
        )

        assert reconciliation_entry is not None
        assert reconciliation_entry.old_status == "SUBMITTED"
        assert reconciliation_entry.new_status == "CANCELLED"
        assert "reconciliation" in reconciliation_entry.reason.lower()
        assert reconciliation_entry.event_metadata["drift_detected"] is True

    @pytest.mark.asyncio
    async def test_terminal_orders_not_reconciled(self, db: AsyncSession, mock_kite_client):
        """Verify terminal orders are not reconciled"""
        # Arrange: Create order in terminal state
        order = Order(
            user_id=111,
            trading_account_id=1,
            symbol="WIPRO",
            exchange="NSE",
            transaction_type="BUY",
            order_type="MARKET",
            product_type="MIS",
            quantity=1,
            filled_quantity=1,
            pending_quantity=0,
            status="COMPLETE",  # Terminal state
            broker_order_id="BROKER111",
            created_at=datetime.utcnow()
        )
        db.add(order)
        await db.flush()

        # Mock broker (shouldn't be called)
        mock_kite_client.orders.return_value = []

        # Act: Run reconciliation
        reconciliation = ReconciliationService(db)
        result = await reconciliation.reconcile_pending_orders(max_age_hours=24, batch_size=100)

        # Assert: Order not checked (terminal state)
        assert result["total_checked"] == 0

    @pytest.mark.asyncio
    async def test_old_orders_not_reconciled(self, db: AsyncSession, mock_kite_client):
        """Verify orders older than max_age are not reconciled"""
        # Arrange: Create old order
        old_time = datetime.utcnow() - timedelta(hours=48)  # 2 days old
        order = Order(
            user_id=222,
            trading_account_id=1,
            symbol="RELIANCE",
            exchange="NSE",
            transaction_type="BUY",
            order_type="MARKET",
            product_type="MIS",
            quantity=1,
            filled_quantity=0,
            pending_quantity=1,
            status="OPEN",
            broker_order_id="BROKER222",
            created_at=old_time
        )
        db.add(order)
        await db.flush()

        # Act: Run reconciliation with max_age=24 hours
        reconciliation = ReconciliationService(db)
        result = await reconciliation.reconcile_pending_orders(max_age_hours=24, batch_size=100)

        # Assert: Order not checked (too old)
        assert result["total_checked"] == 0

    @pytest.mark.asyncio
    async def test_batch_size_limit(self, db: AsyncSession, mock_kite_client):
        """Verify batch_size parameter limits orders checked"""
        # Arrange: Create 5 orders
        for i in range(5):
            order = Order(
                user_id=333,
                trading_account_id=1,
                symbol=f"STOCK{i}",
                exchange="NSE",
                transaction_type="BUY",
                order_type="MARKET",
                product_type="MIS",
                quantity=1,
                filled_quantity=0,
                pending_quantity=1,
                status="OPEN",
                broker_order_id=f"BROKER{i}",
                created_at=datetime.utcnow()
            )
            db.add(order)
        await db.flush()

        # Mock broker
        mock_kite_client.orders.return_value = []

        # Act: Run reconciliation with batch_size=3
        reconciliation = ReconciliationService(db)
        result = await reconciliation.reconcile_pending_orders(max_age_hours=24, batch_size=3)

        # Assert: Only 3 orders checked
        assert result["total_checked"] == 3

    @pytest.mark.asyncio
    async def test_broker_api_error_handled(self, db: AsyncSession, mock_kite_client_fail):
        """Verify broker API errors are handled gracefully"""
        # Arrange: Create order
        order = Order(
            user_id=444,
            trading_account_id=1,
            symbol="RELIANCE",
            exchange="NSE",
            transaction_type="BUY",
            order_type="MARKET",
            product_type="MIS",
            quantity=1,
            filled_quantity=0,
            pending_quantity=1,
            status="OPEN",
            broker_order_id="BROKER444",
            created_at=datetime.utcnow()
        )
        db.add(order)
        await db.flush()

        # Mock broker to fail
        mock_kite_client_fail.orders.side_effect = Exception("Broker API down")

        # Act: Run reconciliation
        reconciliation = ReconciliationService(db)
        result = await reconciliation.reconcile_pending_orders(max_age_hours=24, batch_size=100)

        # Assert: Error tracked but service didn't crash
        assert result["total_checked"] == 1
        assert result["errors"] >= 1

    @pytest.mark.asyncio
    async def test_reconcile_single_order_by_id(self, db: AsyncSession, mock_kite_client):
        """Test manual reconciliation of single order"""
        # Arrange: Create order with drift
        order = Order(
            user_id=555,
            trading_account_id=1,
            symbol="TCS",
            exchange="NSE",
            transaction_type="BUY",
            order_type="MARKET",
            product_type="MIS",
            quantity=1,
            filled_quantity=0,
            pending_quantity=1,
            status="OPEN",
            broker_order_id="BROKER555",
            created_at=datetime.utcnow()
        )
        db.add(order)
        await db.flush()

        # Mock broker to return COMPLETE
        mock_kite_client.orders.return_value = [
            {
                "order_id": "BROKER555",
                "status": "COMPLETE",
                "filled_quantity": 1,
                "average_price": 3500.0
            }
        ]

        # Act: Reconcile single order
        reconciliation = ReconciliationService(db)
        result = await reconciliation.reconcile_single_order_by_id(order.id)

        # Assert: Drift detected and corrected
        assert result["success"] is True
        assert result["drift_detected"] is True
        assert result["correction"]["corrected"] is True

        # Verify order updated
        await db.refresh(order)
        assert order.status == "COMPLETE"

    @pytest.mark.asyncio
    async def test_reconcile_nonexistent_order(self, db: AsyncSession, mock_kite_client):
        """Test reconciling non-existent order returns error"""
        # Act: Try to reconcile non-existent order
        reconciliation = ReconciliationService(db)
        result = await reconciliation.reconcile_single_order_by_id(999999)

        # Assert: Error returned
        assert result["success"] is False
        assert "not found" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_multiple_accounts_reconciled(self, db: AsyncSession, mock_kite_client):
        """Test reconciliation works across multiple trading accounts"""
        # Arrange: Create orders for 2 different accounts
        order1 = Order(
            user_id=666,
            trading_account_id=1,
            symbol="RELIANCE",
            exchange="NSE",
            transaction_type="BUY",
            order_type="MARKET",
            product_type="MIS",
            quantity=1,
            filled_quantity=0,
            pending_quantity=1,
            status="OPEN",
            broker_order_id="BROKER666",
            created_at=datetime.utcnow()
        )

        order2 = Order(
            user_id=777,
            trading_account_id=2,  # Different account
            symbol="TCS",
            exchange="NSE",
            transaction_type="BUY",
            order_type="MARKET",
            product_type="MIS",
            quantity=1,
            filled_quantity=0,
            pending_quantity=1,
            status="OPEN",
            broker_order_id="BROKER777",
            created_at=datetime.utcnow()
        )

        db.add_all([order1, order2])
        await db.flush()

        # Mock broker for both accounts
        mock_kite_client.orders.return_value = [
            {"order_id": "BROKER666", "status": "OPEN", "filled_quantity": 0},
            {"order_id": "BROKER777", "status": "OPEN", "filled_quantity": 0}
        ]

        # Act: Run reconciliation
        reconciliation = ReconciliationService(db)
        result = await reconciliation.reconcile_pending_orders(max_age_hours=24, batch_size=100)

        # Assert: Both orders checked
        assert result["total_checked"] == 2


class TestAdminEndpoints:
    """Test admin reconciliation endpoints"""

    @pytest.mark.asyncio
    async def test_manual_reconciliation_endpoint(
        self, client, db: AsyncSession, auth_token, mock_kite_client
    ):
        """Test POST /admin/reconciliation/run"""
        # Arrange: Create order with drift
        order = Order(
            user_id=888,
            trading_account_id=1,
            symbol="WIPRO",
            exchange="NSE",
            transaction_type="BUY",
            order_type="MARKET",
            product_type="MIS",
            quantity=1,
            filled_quantity=0,
            pending_quantity=1,
            status="OPEN",
            broker_order_id="BROKER888",
            created_at=datetime.utcnow()
        )
        db.add(order)
        await db.flush()

        # Mock broker
        mock_kite_client.orders.return_value = [
            {"order_id": "BROKER888", "status": "COMPLETE", "filled_quantity": 1}
        ]

        # Act: Call endpoint
        response = await client.post(
            "/api/v1/admin/reconciliation/run",
            headers={"Authorization": f"Bearer {auth_token}"}
        )

        # Assert: Success
        assert response.status_code == 200
        data = response.json()
        assert data["total_checked"] >= 1
        assert data["drift_count"] >= 1

    @pytest.mark.asyncio
    async def test_reconcile_single_order_endpoint(
        self, client, db: AsyncSession, auth_token, mock_kite_client
    ):
        """Test POST /admin/reconciliation/order/{order_id}"""
        # Arrange: Create order
        order = Order(
            user_id=999,
            trading_account_id=1,
            symbol="INFY",
            exchange="NSE",
            transaction_type="BUY",
            order_type="MARKET",
            product_type="MIS",
            quantity=1,
            filled_quantity=0,
            pending_quantity=1,
            status="OPEN",
            broker_order_id="BROKER999",
            created_at=datetime.utcnow()
        )
        db.add(order)
        await db.flush()

        # Mock broker
        mock_kite_client.orders.return_value = [
            {"order_id": "BROKER999", "status": "COMPLETE", "filled_quantity": 1}
        ]

        # Act: Call endpoint
        response = await client.post(
            f"/api/v1/admin/reconciliation/order/{order.id}",
            headers={"Authorization": f"Bearer {auth_token}"}
        )

        # Assert: Success
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["drift_detected"] is True


# Fixtures

@pytest.fixture
def mock_kite_client(mocker):
    """Mock KiteConnect client for successful operations"""
    mock = mocker.patch("app.services.broker_client.get_kite_client")
    mock_instance = mocker.MagicMock()
    mock_instance.orders.return_value = []
    mock.return_value = mock_instance
    return mock_instance


@pytest.fixture
def mock_kite_client_fail(mocker):
    """Mock KiteConnect client that fails"""
    mock = mocker.patch("app.services.broker_client.get_kite_client")
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
    return "test_token_123"
