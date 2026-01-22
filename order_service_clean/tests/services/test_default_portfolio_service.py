"""
Tests for Default Portfolio Service

Tests the automatic portfolio mapping for external/manual trades to ensure
orphaned positions are properly tagged.
"""

import pytest
from decimal import Decimal
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession

from order_service.app.services.default_portfolio_service import (
    DefaultPortfolioService,
    DefaultPortfolioMapping
)


class TestDefaultPortfolioService:
    """Test cases for DefaultPortfolioService."""

    @pytest.fixture
    async def mock_db(self):
        """Mock database session."""
        db = AsyncMock(spec=AsyncSession)
        db.execute = AsyncMock()
        db.commit = AsyncMock()
        db.rollback = AsyncMock()
        return db

    @pytest.fixture
    def service(self, mock_db):
        """Default portfolio service instance."""
        return DefaultPortfolioService(mock_db)

    @pytest.fixture
    def sample_mapping(self):
        """Sample portfolio mapping data."""
        return DefaultPortfolioMapping(
            trading_account_id="acc_001",
            strategy_id=1,
            portfolio_id="portfolio_external",
            mapping_type="external_trades",
            created_at=datetime.now(timezone.utc),
            metadata={"reason": "automated_mapping"}
        )

    async def test_get_or_create_default_portfolio_existing(self, service, mock_db, sample_mapping):
        """Test retrieving existing default portfolio mapping."""
        # Mock database response
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (
            sample_mapping.trading_account_id,
            sample_mapping.strategy_id,
            sample_mapping.portfolio_id,
            sample_mapping.mapping_type,
            sample_mapping.created_at,
            sample_mapping.metadata
        )
        mock_db.execute.return_value = mock_result

        result = await service.get_or_create_default_portfolio(
            "acc_001", 1, "external_trades"
        )

        assert result.trading_account_id == "acc_001"
        assert result.strategy_id == 1
        assert result.portfolio_id == "portfolio_external"
        mock_db.execute.assert_called_once()

    async def test_get_or_create_default_portfolio_new(self, service, mock_db):
        """Test creating new default portfolio mapping."""
        # Mock no existing mapping
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_db.execute.return_value = mock_result

        result = await service.get_or_create_default_portfolio(
            "acc_001", 1, "manual_trades"
        )

        assert result.trading_account_id == "acc_001"
        assert result.strategy_id == 1
        assert result.portfolio_id == "acc_001_strategy_1_manual"
        assert result.mapping_type == "manual_trades"
        
        # Should have called execute twice (select + insert)
        assert mock_db.execute.call_count == 2
        mock_db.commit.assert_called_once()

    async def test_tag_orphan_position_with_portfolio(self, service, mock_db):
        """Test tagging orphan position with default portfolio."""
        position_id = "pos_123"
        portfolio_id = "portfolio_default"
        reason = "External trade detected"

        await service.tag_orphan_position_with_portfolio(
            position_id, portfolio_id, reason
        )

        mock_db.execute.assert_called_once()
        call_args = mock_db.execute.call_args[0]
        
        # Verify the SQL contains the position update
        assert "UPDATE order_service.positions" in str(call_args[0])
        assert "portfolio_id = :portfolio_id" in str(call_args[0])
        
        # Verify parameters
        params = mock_db.execute.call_args[1]
        assert params["position_id"] == position_id
        assert params["portfolio_id"] == portfolio_id
        assert "orphan_tagging" in params["metadata"]["reason"]

        mock_db.commit.assert_called_once()

    async def test_get_orphan_positions(self, service, mock_db):
        """Test retrieving orphan positions."""
        # Mock database response
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            ("pos_1", "AAPL", 100, "acc_001", 1, None),
            ("pos_2", "MSFT", 50, "acc_001", 1, None)
        ]
        mock_db.execute.return_value = mock_result

        orphans = await service.get_orphan_positions("acc_001", 1)

        assert len(orphans) == 2
        assert orphans[0]["id"] == "pos_1"
        assert orphans[0]["symbol"] == "AAPL"
        assert orphans[0]["quantity"] == 100
        assert orphans[1]["id"] == "pos_2"
        assert orphans[1]["symbol"] == "MSFT"

        mock_db.execute.assert_called_once()

    async def test_process_external_trade_orphan_detection(self, service, mock_db):
        """Test processing external trade with orphan detection."""
        # Mock orphan positions found
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            ("pos_1", "AAPL", 100, "acc_001", 1, None, "external", datetime.now(timezone.utc))
        ]
        mock_db.execute.return_value = mock_result

        result = await service.process_external_trade(
            trading_account_id="acc_001",
            strategy_id=1,
            symbol="AAPL",
            quantity=Decimal("100"),
            side="BUY",
            external_order_id="ext_123"
        )

        assert result["orphans_detected"] == 1
        assert result["orphans_tagged"] == 1
        assert result["portfolio_id"] is not None
        
        # Should call execute multiple times for detection and tagging
        assert mock_db.execute.call_count >= 2
        mock_db.commit.assert_called()

    async def test_bulk_tag_orphan_positions(self, service, mock_db):
        """Test bulk tagging of orphan positions."""
        position_ids = ["pos_1", "pos_2", "pos_3"]
        portfolio_id = "bulk_portfolio"
        reason = "Bulk orphan cleanup"

        result = await service.bulk_tag_orphan_positions(
            position_ids, portfolio_id, reason
        )

        assert result["tagged_count"] == 3
        assert result["portfolio_id"] == portfolio_id

        # Should have called execute for each position
        assert mock_db.execute.call_count == len(position_ids)
        mock_db.commit.assert_called_once()

    async def test_portfolio_naming_conventions(self, service, mock_db):
        """Test portfolio naming conventions for different types."""
        # Mock no existing mapping
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_db.execute.return_value = mock_result

        # Test external trades naming
        result_external = await service.get_or_create_default_portfolio(
            "acc_001", 1, "external_trades"
        )
        assert result_external.portfolio_id == "acc_001_strategy_1_external"

        # Test manual trades naming
        result_manual = await service.get_or_create_default_portfolio(
            "acc_002", 5, "manual_trades"
        )
        assert result_manual.portfolio_id == "acc_002_strategy_5_manual"

        # Test reconciliation naming
        result_recon = await service.get_or_create_default_portfolio(
            "acc_003", 10, "reconciliation"
        )
        assert result_recon.portfolio_id == "acc_003_strategy_10_reconciliation"

    async def test_error_handling_db_failure(self, service, mock_db):
        """Test error handling when database operations fail."""
        mock_db.execute.side_effect = Exception("Database connection failed")

        with pytest.raises(Exception) as exc_info:
            await service.get_or_create_default_portfolio("acc_001", 1, "external_trades")

        assert "Database connection failed" in str(exc_info.value)
        mock_db.rollback.assert_called_once()

    async def test_metadata_preservation(self, service, mock_db):
        """Test that position metadata is preserved during tagging."""
        # Mock position with existing metadata
        position_id = "pos_with_metadata"
        portfolio_id = "new_portfolio"
        reason = "Test tagging"

        await service.tag_orphan_position_with_portfolio(
            position_id, portfolio_id, reason
        )

        # Verify metadata merge in the SQL call
        call_args = mock_db.execute.call_args[1]
        metadata = call_args["metadata"]
        
        assert "reason" in metadata
        assert "tagged_at" in metadata
        assert "original_portfolio_id" in metadata

    @pytest.mark.parametrize("mapping_type,expected_suffix", [
        ("external_trades", "external"),
        ("manual_trades", "manual"),
        ("reconciliation", "reconciliation"),
        ("unknown_type", "unknown_type")
    ])
    async def test_portfolio_naming_variations(self, service, mock_db, mapping_type, expected_suffix):
        """Test portfolio naming for different mapping types."""
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_db.execute.return_value = mock_result

        result = await service.get_or_create_default_portfolio(
            "test_acc", 99, mapping_type
        )

        expected_name = f"test_acc_strategy_99_{expected_suffix}"
        assert result.portfolio_id == expected_name


@pytest.mark.integration
class TestDefaultPortfolioServiceIntegration:
    """Integration tests for DefaultPortfolioService with real database operations."""

    async def test_end_to_end_orphan_workflow(self, real_db_session, sample_trading_account):
        """Test complete orphan detection and tagging workflow."""
        service = DefaultPortfolioService(real_db_session)
        
        # Create an orphan position (no portfolio_id)
        # This would require actual database setup in a real integration test
        pass

    async def test_concurrent_portfolio_creation(self, real_db_session):
        """Test concurrent creation of default portfolios."""
        # Test race conditions and uniqueness constraints
        pass