"""
Pytest Configuration and Shared Fixtures

Provides common test fixtures and configuration for Sprint 7A test suite.
"""

import pytest
import asyncio
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Dict, List, Any

# Configure asyncio for testing
@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def mock_db_session():
    """Mock database session for unit tests."""
    db = AsyncMock(spec=AsyncSession)
    db.execute = AsyncMock()
    db.commit = AsyncMock()
    db.rollback = AsyncMock()
    db.close = AsyncMock()
    
    # Mock transaction behavior
    db.begin = AsyncMock()
    db.refresh = AsyncMock()
    db.flush = AsyncMock()
    
    return db


@pytest.fixture
def sample_trading_account():
    """Sample trading account data for testing."""
    return {
        "trading_account_id": "test_acc_001",
        "user_id": "test_user_123",
        "account_name": "Test Trading Account",
        "broker": "test_broker",
        "status": "active",
        "created_at": datetime.now(timezone.utc)
    }


@pytest.fixture
def sample_strategies():
    """Sample strategy configurations for testing."""
    return [
        {
            "strategy_id": 1,
            "name": "Test Momentum Strategy",
            "description": "Test momentum-based trading strategy",
            "execution_id": "exec_momentum_001",
            "status": "active"
        },
        {
            "strategy_id": 2,
            "name": "Test Value Strategy", 
            "description": "Test value-based trading strategy",
            "execution_id": "exec_value_001",
            "status": "active"
        },
        {
            "strategy_id": 3,
            "name": "Test Arbitrage Strategy",
            "description": "Test arbitrage trading strategy", 
            "execution_id": "exec_arb_001",
            "status": "paused"
        }
    ]


@pytest.fixture
def sample_positions(sample_trading_account, sample_strategies):
    """Sample position data for testing."""
    base_time = datetime.now(timezone.utc) - timedelta(days=10)
    
    return [
        {
            "position_id": "pos_aapl_001",
            "trading_account_id": sample_trading_account["trading_account_id"],
            "symbol": "AAPL",
            "exchange": "NASDAQ",
            "product_type": "EQUITY",
            "quantity": 100,
            "strategy_id": sample_strategies[0]["strategy_id"],
            "execution_id": sample_strategies[0]["execution_id"],
            "portfolio_id": "port_momentum_001",
            "source": "script",
            "buy_price": Decimal("150.00"),
            "current_price": Decimal("170.00"),
            "created_at": base_time,
            "updated_at": base_time,
            "is_open": True,
            "metadata": {"entry_reason": "momentum_signal"}
        },
        {
            "position_id": "pos_aapl_002",
            "trading_account_id": sample_trading_account["trading_account_id"],
            "symbol": "AAPL",
            "exchange": "NASDAQ", 
            "product_type": "EQUITY",
            "quantity": 50,
            "strategy_id": sample_strategies[1]["strategy_id"],
            "execution_id": sample_strategies[1]["execution_id"],
            "portfolio_id": "port_value_001",
            "source": "script",
            "buy_price": Decimal("160.00"),
            "current_price": Decimal("170.00"),
            "created_at": base_time + timedelta(hours=2),
            "updated_at": base_time + timedelta(hours=2),
            "is_open": True,
            "metadata": {"entry_reason": "value_signal"}
        },
        {
            "position_id": "pos_msft_001",
            "trading_account_id": sample_trading_account["trading_account_id"],
            "symbol": "MSFT",
            "exchange": "NASDAQ",
            "product_type": "EQUITY", 
            "quantity": 75,
            "strategy_id": sample_strategies[0]["strategy_id"],
            "execution_id": sample_strategies[0]["execution_id"],
            "portfolio_id": "port_momentum_001",
            "source": "script",
            "buy_price": Decimal("300.00"),
            "current_price": Decimal("320.00"),
            "created_at": base_time + timedelta(hours=4),
            "updated_at": base_time + timedelta(hours=4),
            "is_open": True,
            "metadata": {"entry_reason": "momentum_signal"}
        },
        {
            "position_id": "pos_googl_001", 
            "trading_account_id": sample_trading_account["trading_account_id"],
            "symbol": "GOOGL",
            "exchange": "NASDAQ",
            "product_type": "EQUITY",
            "quantity": 30,
            "strategy_id": sample_strategies[1]["strategy_id"],
            "execution_id": sample_strategies[1]["execution_id"],
            "portfolio_id": None,  # Orphan position for testing
            "source": "external",
            "buy_price": Decimal("2800.00"),
            "current_price": Decimal("2850.00"),
            "created_at": base_time + timedelta(hours=6),
            "updated_at": base_time + timedelta(hours=6),
            "is_open": True,
            "metadata": {"external_order_id": "ext_ord_googl_001"}
        }
    ]


@pytest.fixture
def sample_orders(sample_trading_account, sample_strategies):
    """Sample order data for testing."""
    return [
        {
            "id": "ord_001",
            "order_id": "broker_ord_001",
            "trading_account_id": sample_trading_account["trading_account_id"],
            "symbol": "AAPL",
            "side": "BUY",
            "order_type": "LIMIT",
            "quantity": 25,
            "price": Decimal("165.00"),
            "status": "PENDING",
            "source": "script",
            "strategy_id": sample_strategies[0]["strategy_id"],
            "execution_id": sample_strategies[0]["execution_id"],
            "created_at": datetime.now(timezone.utc) - timedelta(minutes=30)
        },
        {
            "id": "ord_002",
            "order_id": "broker_ord_002",
            "trading_account_id": sample_trading_account["trading_account_id"],
            "symbol": "MSFT", 
            "side": "SELL",
            "order_type": "STOP",
            "quantity": 10,
            "price": Decimal("315.00"),
            "status": "SUBMITTED",
            "source": "manual",
            "strategy_id": sample_strategies[0]["strategy_id"],
            "execution_id": None,
            "created_at": datetime.now(timezone.utc) - timedelta(minutes=15)
        },
        {
            "id": "ord_003",
            "order_id": "ext_ord_003",
            "trading_account_id": sample_trading_account["trading_account_id"],
            "symbol": "GOOGL",
            "side": "BUY",
            "order_type": "MARKET",
            "quantity": 5,
            "price": None,
            "average_price": Decimal("2835.00"),
            "status": "COMPLETE",
            "source": "external", 
            "strategy_id": None,
            "execution_id": None,
            "created_at": datetime.now(timezone.utc) - timedelta(hours=2),
            "updated_at": datetime.now(timezone.utc) - timedelta(hours=2)
        }
    ]


@pytest.fixture
def sample_entry_trades():
    """Sample entry trades for attribution testing."""
    base_time = datetime.now(timezone.utc) - timedelta(days=5)
    
    return [
        {
            "trade_id": "trade_001",
            "position_id": "pos_aapl_001",
            "symbol": "AAPL",
            "side": "BUY",
            "quantity": 100,
            "price": Decimal("150.00"),
            "timestamp": base_time,
            "strategy_id": 1
        },
        {
            "trade_id": "trade_002",
            "position_id": "pos_aapl_002", 
            "symbol": "AAPL",
            "side": "BUY",
            "quantity": 50,
            "price": Decimal("160.00"),
            "timestamp": base_time + timedelta(hours=2),
            "strategy_id": 2
        },
        {
            "trade_id": "trade_003",
            "position_id": "pos_msft_001",
            "symbol": "MSFT",
            "side": "BUY", 
            "quantity": 75,
            "price": Decimal("300.00"),
            "timestamp": base_time + timedelta(hours=4),
            "strategy_id": 1
        }
    ]


@pytest.fixture
def mock_auth_context():
    """Mock authentication context for API testing."""
    context = MagicMock()
    context.user_id = "test_user_123"
    context.username = "test_user"
    context.trading_account_ids = ["test_acc_001", "test_acc_002"]
    context.permissions = [
        "manual_attribution:read",
        "manual_attribution:create", 
        "manual_attribution:assign",
        "manual_attribution:resolve",
        "manual_attribution:apply"
    ]
    context.is_admin = False
    return context


@pytest.fixture
def admin_auth_context():
    """Mock admin authentication context."""
    context = MagicMock()
    context.user_id = "admin_user_456"
    context.username = "admin_user"
    context.trading_account_ids = ["test_acc_001", "test_acc_002", "test_acc_003"]
    context.permissions = ["*"]  # Admin has all permissions
    context.is_admin = True
    return context


class DatabaseMockHelper:
    """Helper class for mocking database operations in tests."""
    
    @staticmethod
    def mock_select_result(rows: List[tuple]) -> MagicMock:
        """Create mock database select result."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = rows
        mock_result.fetchone.return_value = rows[0] if rows else None
        return mock_result
    
    @staticmethod
    def mock_insert_result(inserted_id: str = None) -> MagicMock:
        """Create mock database insert result."""
        mock_result = MagicMock()
        if inserted_id:
            mock_result.inserted_primary_key = [inserted_id]
        return mock_result
    
    @staticmethod
    def mock_update_result(affected_rows: int = 1) -> MagicMock:
        """Create mock database update result."""
        mock_result = MagicMock()
        mock_result.rowcount = affected_rows
        return mock_result


@pytest.fixture
def db_mock_helper():
    """Database mock helper fixture."""
    return DatabaseMockHelper


class TestDataFactory:
    """Factory for creating test data with relationships."""
    
    @staticmethod
    def create_position_data(
        symbol: str,
        quantity: int,
        strategy_id: int, 
        execution_id: str = None,
        created_at: datetime = None
    ) -> Dict[str, Any]:
        """Create test position data."""
        if created_at is None:
            created_at = datetime.now(timezone.utc)
        
        return {
            "position_id": f"pos_{symbol.lower()}_{strategy_id}",
            "symbol": symbol,
            "quantity": quantity,
            "strategy_id": strategy_id,
            "execution_id": execution_id or f"exec_{strategy_id}",
            "buy_price": Decimal("100.00"),
            "created_at": created_at,
            "is_open": True
        }
    
    @staticmethod
    def create_attribution_case_data(
        trading_account_id: str,
        symbol: str,
        exit_quantity: Decimal,
        affected_positions: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Create test attribution case data."""
        return {
            "trading_account_id": trading_account_id,
            "symbol": symbol,
            "exit_quantity": exit_quantity,
            "exit_price": Decimal("150.00"),
            "exit_timestamp": datetime.now(timezone.utc),
            "affected_positions": affected_positions,
            "priority": "normal"
        }


@pytest.fixture
def test_data_factory():
    """Test data factory fixture."""
    return TestDataFactory


# Performance testing helpers
@pytest.fixture
def performance_timer():
    """Timer for measuring test performance."""
    class Timer:
        def __init__(self):
            self.start_time = None
            self.end_time = None
        
        def start(self):
            self.start_time = datetime.now()
        
        def stop(self):
            self.end_time = datetime.now()
        
        @property
        def elapsed_seconds(self):
            if self.start_time and self.end_time:
                return (self.end_time - self.start_time).total_seconds()
            return None
    
    return Timer()


# Integration test helpers
@pytest.fixture
def integration_test_config():
    """Configuration for integration tests."""
    return {
        "database_url": "postgresql://test:test@localhost:5432/test_db",
        "redis_url": "redis://localhost:6379/0",
        "test_trading_account": "integration_test_001",
        "test_timeout": 30,  # seconds
        "cleanup_after_test": True
    }


# Markers for test categorization
pytest.mark.unit = pytest.mark.unit
pytest.mark.integration = pytest.mark.integration  
pytest.mark.slow = pytest.mark.slow
pytest.mark.performance = pytest.mark.performance


# Skip conditions for different environments
@pytest.fixture
def skip_if_no_db(integration_test_config):
    """Skip test if database is not available."""
    # In real implementation, would check database connectivity
    return pytest.mark.skipif(
        False,  # Replace with actual connectivity check
        reason="Database not available for integration tests"
    )


@pytest.fixture
def skip_if_slow():
    """Skip slow tests unless explicitly requested."""
    import os
    return pytest.mark.skipif(
        not os.getenv("RUN_SLOW_TESTS"),
        reason="Slow tests disabled (set RUN_SLOW_TESTS=1 to enable)"
    )


# Custom assertions for Sprint 7A testing
class Sprint7AAssertions:
    """Custom assertions for Sprint 7A functionality."""
    
    @staticmethod
    def assert_allocation_result_valid(allocation_result):
        """Assert that allocation result is valid."""
        assert allocation_result.allocation_id is not None
        assert allocation_result.total_allocated_quantity >= 0
        assert allocation_result.unallocated_quantity >= 0
        assert len(allocation_result.allocations) >= 0
        
        # Total allocated should equal sum of individual allocations
        individual_sum = sum(a.allocated_quantity for a in allocation_result.allocations)
        assert allocation_result.total_allocated_quantity == individual_sum
    
    @staticmethod
    def assert_variance_resolution_complete(variance_result):
        """Assert that variance resolution is complete."""
        assert variance_result.variance_id is not None
        assert variance_result.resolution_type is not None
        assert variance_result.variance_resolved >= 0
        assert variance_result.variance_remaining >= 0
    
    @staticmethod
    def assert_handoff_transition_successful(transition_result):
        """Assert that handoff transition was successful."""
        assert transition_result.success is True
        assert transition_result.transition_id is not None
        assert transition_result.positions_transferred >= 0
        assert transition_result.orders_cancelled >= 0
        assert len(transition_result.errors) == 0


@pytest.fixture
def sprint7a_assertions():
    """Sprint 7A custom assertions fixture."""
    return Sprint7AAssertions