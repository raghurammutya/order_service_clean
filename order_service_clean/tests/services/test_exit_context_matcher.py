"""
Test Exit Context Matcher (GAP-REC-10)
Tests robust exit context matching with multi-fill orders, tolerance-based algorithms, and delayed data
"""

import pytest
from decimal import Decimal
from datetime import datetime, timedelta

from order_service.app.services.exit_context_matcher import (
    ExitContextMatcher,
    ExitContextConfig,
    MatchQuality
)


@pytest.mark.asyncio
class TestExitContextMatcher:
    """Test exit context matching functionality."""

    @pytest.fixture
    def config(self):
        """Default configuration for testing."""
        return ExitContextConfig(
            quantity_tolerance=Decimal("0.01"),
            price_tolerance_percent=Decimal("1.0"),
            time_tolerance_minutes=5,
            max_matches_to_consider=10
        )

    @pytest.fixture
    def matcher(self, mock_db_session, config):
        """Create matcher instance."""
        return ExitContextMatcher(mock_db_session, config)

    @pytest.fixture
    def sample_exit_data(self):
        """Sample external exit data."""
        return {
            "broker_trade_id": "bt_12345",
            "quantity": "100",
            "price": "150.50",
            "timestamp": "2024-01-15T10:30:00Z",
            "symbol": "AAPL"
        }

    async def test_exact_match_high_quality(self, matcher, sample_exit_data, mock_db_session):
        """Test exact match returns high quality result."""
        # Mock exact matching trade
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", 100, Decimal("150.50"), datetime.now(), "bt_internal_123", "external")
        ]
        
        result = await matcher.match_exit_context(
            sample_exit_data, "acc_001", "AAPL"
        )
        
        assert result.match_quality == MatchQuality.HIGH
        assert result.confidence_score >= 0.9
        assert len(result.matched_trades) == 1
        assert result.primary_match is not None

    async def test_quantity_tolerance_match(self, matcher, sample_exit_data, mock_db_session):
        """Test matching within quantity tolerance."""
        # Mock trade with slight quantity difference (within tolerance)
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", 100, Decimal("150.60"), datetime.now(), "bt_internal_123", "external")  # Price slightly different
        ]
        
        result = await matcher.match_exit_context(
            sample_exit_data, "acc_001", "AAPL"
        )
        
        assert result.match_quality in [MatchQuality.HIGH, MatchQuality.MEDIUM]
        assert result.confidence_score >= 0.7
        assert len(result.matched_trades) == 1

    async def test_price_tolerance_match(self, matcher, sample_exit_data, mock_db_session):
        """Test matching within price tolerance."""
        # Mock trade with price within 1% tolerance
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", 100, Decimal("151.00"), datetime.now(), "bt_internal_123", "external")  # ~0.3% difference
        ]
        
        result = await matcher.match_exit_context(
            sample_exit_data, "acc_001", "AAPL"
        )
        
        assert result.match_quality in [MatchQuality.HIGH, MatchQuality.MEDIUM]
        assert result.confidence_score >= 0.7

    async def test_time_tolerance_match(self, matcher, sample_exit_data, mock_db_session):
        """Test matching within time tolerance."""
        # Mock trade within time tolerance
        base_time = datetime.fromisoformat("2024-01-15T10:30:00+00:00")
        close_time = base_time + timedelta(minutes=3)  # Within 5-minute tolerance
        
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", 100, Decimal("150.50"), close_time, "bt_internal_123", "external")
        ]
        
        result = await matcher.match_exit_context(
            sample_exit_data, "acc_001", "AAPL"
        )
        
        assert result.match_quality == MatchQuality.HIGH
        assert result.confidence_score >= 0.8

    async def test_multi_fill_aggregation(self, matcher, mock_db_session):
        """Test aggregation of multiple fills into single exit context."""
        exit_data = {
            "broker_trade_id": "bt_12345",
            "quantity": "200",  # Total of multiple fills
            "price": "150.25",  # Average price
            "timestamp": "2024-01-15T10:30:00Z",
            "symbol": "AAPL"
        }
        
        # Mock multiple fills that sum to exit quantity
        base_time = datetime.fromisoformat("2024-01-15T10:30:00+00:00")
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", 75, Decimal("150.00"), base_time, "bt_fill_1", "external"),
            (2, "AAPL", 50, Decimal("150.25"), base_time + timedelta(seconds=30), "bt_fill_2", "external"),
            (3, "AAPL", 75, Decimal("150.50"), base_time + timedelta(minutes=1), "bt_fill_3", "external")
        ]
        
        result = await matcher.match_exit_context(
            exit_data, "acc_001", "AAPL"
        )
        
        assert result.match_quality in [MatchQuality.HIGH, MatchQuality.MEDIUM]
        assert len(result.matched_trades) == 3
        assert result.aggregate_quantity == Decimal("200")

    async def test_no_match_low_quality(self, matcher, sample_exit_data, mock_db_session):
        """Test no matching trades returns low quality result."""
        # Mock no matching trades
        mock_db_session.execute.return_value.fetchall.return_value = []
        
        result = await matcher.match_exit_context(
            sample_exit_data, "acc_001", "AAPL"
        )
        
        assert result.match_quality == MatchQuality.LOW
        assert result.confidence_score == 0.0
        assert len(result.matched_trades) == 0
        assert result.primary_match is None

    async def test_partial_quantity_match(self, matcher, sample_exit_data, mock_db_session):
        """Test partial quantity match handling."""
        # Mock trade with different quantity (outside tolerance)
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", 50, Decimal("150.50"), datetime.now(), "bt_internal_123", "external")  # Half quantity
        ]
        
        result = await matcher.match_exit_context(
            sample_exit_data, "acc_001", "AAPL"
        )
        
        assert result.match_quality == MatchQuality.MEDIUM
        assert result.confidence_score < 0.7
        assert len(result.matched_trades) == 1
        assert result.unmatched_quantity == Decimal("50")

    async def test_delayed_data_handling(self, matcher, sample_exit_data, mock_db_session):
        """Test handling of delayed broker data."""
        # Exit data timestamp is newer than available trade data
        exit_data = sample_exit_data.copy()
        exit_data["timestamp"] = "2024-01-15T11:00:00Z"  # 30 minutes later
        
        # Mock trade from 30 minutes ago
        old_time = datetime.fromisoformat("2024-01-15T10:30:00+00:00")
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", 100, Decimal("150.50"), old_time, "bt_internal_123", "external")
        ]
        
        result = await matcher.match_exit_context(
            exit_data, "acc_001", "AAPL"
        )
        
        # Should still find match but with lower confidence
        assert result.match_quality == MatchQuality.MEDIUM
        assert result.confidence_score >= 0.5
        assert len(result.matched_trades) == 1

    async def test_confidence_scoring_algorithm(self, matcher, sample_exit_data, mock_db_session):
        """Test confidence score calculation algorithm."""
        # Mock exact match
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", 100, Decimal("150.50"), datetime.now(), "bt_internal_123", "external")
        ]
        
        result = await matcher.match_exit_context(
            sample_exit_data, "acc_001", "AAPL"
        )
        
        # Exact match should have very high confidence
        assert result.confidence_score >= 0.95
        
        # Test with slight differences
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", 99, Decimal("151.00"), datetime.now(), "bt_internal_123", "external")  # Small differences
        ]
        
        result = await matcher.match_exit_context(
            sample_exit_data, "acc_001", "AAPL"
        )
        
        # Should have lower but still good confidence
        assert 0.7 <= result.confidence_score < 0.95

    async def test_multiple_potential_matches_ranking(self, matcher, sample_exit_data, mock_db_session):
        """Test ranking of multiple potential matches."""
        base_time = datetime.fromisoformat("2024-01-15T10:30:00+00:00")
        
        # Mock multiple potential matches with varying quality
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", 100, Decimal("150.50"), base_time, "bt_exact", "external"),  # Exact match
            (2, "AAPL", 95, Decimal("150.00"), base_time + timedelta(minutes=2), "bt_close", "external"),  # Close match
            (3, "AAPL", 80, Decimal("155.00"), base_time + timedelta(minutes=10), "bt_far", "external")  # Far match
        ]
        
        result = await matcher.match_exit_context(
            sample_exit_data, "acc_001", "AAPL"
        )
        
        assert len(result.matched_trades) == 3
        # Primary match should be the best one (exact match)
        assert result.primary_match.broker_trade_id == "bt_exact"
        assert result.match_quality == MatchQuality.HIGH

    async def test_symbol_filtering(self, matcher, sample_exit_data, mock_db_session):
        """Test that matching is properly filtered by symbol."""
        # Mock trades for different symbols
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "TSLA", 100, Decimal("150.50"), datetime.now(), "bt_wrong_symbol", "external"),  # Wrong symbol
            (2, "AAPL", 100, Decimal("150.50"), datetime.now(), "bt_correct_symbol", "external")  # Correct symbol
        ]
        
        result = await matcher.match_exit_context(
            sample_exit_data, "acc_001", "AAPL"
        )
        
        # Should only match AAPL trade
        assert len(result.matched_trades) == 1
        assert result.primary_match.broker_trade_id == "bt_correct_symbol"

    async def test_trading_account_filtering(self, matcher, sample_exit_data, mock_db_session):
        """Test that matching is properly filtered by trading account."""
        # Mock query execution to verify account filtering
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", 100, Decimal("150.50"), datetime.now(), "bt_12345", "external")
        ]
        
        await matcher.match_exit_context(sample_exit_data, "acc_001", "AAPL")
        
        # Verify SQL query includes trading account filter
        mock_db_session.execute.assert_called()
        call_args = mock_db_session.execute.call_args[0]
        query_text = str(call_args[0])
        assert "trading_account_id" in query_text

    async def test_source_field_usage(self, matcher, sample_exit_data, mock_db_session):
        """Test that source field is properly used in matching."""
        # Mock query to include source field
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", 100, Decimal("150.50"), datetime.now(), "bt_12345", "external")
        ]
        
        result = await matcher.match_exit_context(
            sample_exit_data, "acc_001", "AAPL"
        )
        
        # Verify source is included in the match result
        assert result.matched_trades[0].source == "external"

    async def test_configuration_tolerance_usage(self, mock_db_session):
        """Test that configuration tolerances are properly applied."""
        # Create matcher with tight tolerances
        tight_config = ExitContextConfig(
            quantity_tolerance=Decimal("0.001"),  # Very tight
            price_tolerance_percent=Decimal("0.1"),  # Very tight
            time_tolerance_minutes=1  # Very tight
        )
        matcher = ExitContextMatcher(mock_db_session, tight_config)
        
        exit_data = {
            "broker_trade_id": "bt_12345",
            "quantity": "100",
            "price": "150.50",
            "timestamp": "2024-01-15T10:30:00Z",
            "symbol": "AAPL"
        }
        
        # Mock trade that would match with loose tolerances but not tight ones
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", 102, Decimal("151.50"), datetime.now(), "bt_12345", "external")  # Outside tight tolerances
        ]
        
        result = await matcher.match_exit_context(exit_data, "acc_001", "AAPL")
        
        # Should have lower confidence with tight tolerances
        assert result.confidence_score < 0.5

    async def test_database_error_handling(self, matcher, sample_exit_data, mock_db_session):
        """Test graceful handling of database errors."""
        # Mock database error
        mock_db_session.execute.side_effect = Exception("Database connection failed")
        
        with pytest.raises(Exception):
            await matcher.match_exit_context(sample_exit_data, "acc_001", "AAPL")

    async def test_malformed_exit_data_handling(self, matcher, mock_db_session):
        """Test handling of malformed exit data."""
        # Missing required fields
        malformed_data = {
            "broker_trade_id": "bt_12345",
            # Missing quantity, price, timestamp
            "symbol": "AAPL"
        }
        
        with pytest.raises(ValueError):
            await matcher.match_exit_context(malformed_data, "acc_001", "AAPL")

    async def test_empty_trading_account_handling(self, matcher, sample_exit_data, mock_db_session):
        """Test handling of empty/invalid trading account."""
        with pytest.raises(ValueError):
            await matcher.match_exit_context(sample_exit_data, "", "AAPL")
        
        with pytest.raises(ValueError):
            await matcher.match_exit_context(sample_exit_data, None, "AAPL")