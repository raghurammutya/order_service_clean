"""
Test Missing Trade History Handler (GAP-REC-14)
Tests detection and handling of missing trade history with reconstruction workflows
"""

import pytest
from decimal import Decimal
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch, MagicMock

from order_service.app.services.missing_trade_history_handler import (
    MissingTradeHistoryHandler,
    TradeGapDetectionConfig,
    TradeGap,
    GapType,
    ReconstructionStrategy,
    ReconstructionResult,
    TradeHistorySource
)


@pytest.mark.asyncio
class TestMissingTradeHistoryHandler:
    """Test missing trade history detection and handling."""

    @pytest.fixture
    def detection_config(self):
        """Default gap detection configuration."""
        return TradeGapDetectionConfig(
            max_gap_duration_minutes=30,
            min_gap_significance_threshold=Decimal("1000.00"),
            enable_cross_validation=True,
            broker_api_timeout_seconds=60
        )

    @pytest.fixture
    def handler(self, mock_db_session, detection_config):
        """Create handler instance."""
        return MissingTradeHistoryHandler(mock_db_session, detection_config)

    @pytest.fixture
    def sample_trade_sequence(self):
        """Sample trade sequence for testing."""
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        return [
            {
                "trade_id": 1, "symbol": "AAPL", "quantity": 100, "price": Decimal("150.00"),
                "trade_time": base_time, "broker_trade_id": "bt_001"
            },
            # Missing trade between 10:05 and 10:35 (30 minute gap)
            {
                "trade_id": 3, "symbol": "AAPL", "quantity": 50, "price": Decimal("155.00"),
                "trade_time": base_time + timedelta(minutes=35), "broker_trade_id": "bt_003"
            }
        ]

    async def test_detect_time_based_gaps(self, handler, sample_trade_sequence, mock_db_session):
        """Test detection of time-based gaps in trade history."""
        # Mock database query returning trade sequence with gap
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", 100, Decimal("150.00"), sample_trade_sequence[0]["trade_time"], "bt_001"),
            (3, "AAPL", 50, Decimal("155.00"), sample_trade_sequence[1]["trade_time"], "bt_003")
        ]
        
        gaps = await handler.detect_time_based_gaps(
            trading_account_id="acc_001",
            symbol="AAPL",
            start_date=datetime(2024, 1, 15),
            end_date=datetime(2024, 1, 15, 23, 59, 59)
        )
        
        assert len(gaps) == 1
        gap = gaps[0]
        assert gap.gap_type == GapType.TIME_SEQUENCE
        assert gap.duration_minutes == 35
        assert gap.before_trade_id == 1
        assert gap.after_trade_id == 3

    async def test_detect_sequence_number_gaps(self, handler, mock_db_session):
        """Test detection of sequence number gaps in trade history."""
        # Mock trades with missing sequence numbers
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "bt_001", 1, datetime.now()),   # Sequence 1
            (3, "bt_003", 3, datetime.now()),   # Sequence 3 (missing 2)
            (4, "bt_004", 4, datetime.now())    # Sequence 4
        ]
        
        gaps = await handler.detect_sequence_number_gaps(
            trading_account_id="acc_001",
            date_range=(datetime(2024, 1, 15), datetime(2024, 1, 15, 23, 59, 59))
        )
        
        assert len(gaps) >= 1
        sequence_gap = next((g for g in gaps if g.gap_type == GapType.SEQUENCE_NUMBER), None)
        assert sequence_gap is not None
        assert sequence_gap.missing_sequence_numbers == [2]

    async def test_detect_volume_inconsistencies(self, handler, mock_db_session):
        """Test detection of volume inconsistencies indicating missing trades."""
        # Mock position vs trade volume mismatch
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("AAPL", Decimal("1000"), Decimal("800"))  # Position 1000, trades total 800
        ]
        
        gaps = await handler.detect_volume_inconsistencies(
            trading_account_id="acc_001",
            symbols=["AAPL"],
            date_range=(datetime(2024, 1, 15), datetime(2024, 1, 15, 23, 59, 59))
        )
        
        assert len(gaps) >= 1
        volume_gap = next((g for g in gaps if g.gap_type == GapType.VOLUME_MISMATCH), None)
        assert volume_gap is not None
        assert volume_gap.expected_quantity == Decimal("1000")
        assert volume_gap.actual_quantity == Decimal("800")

    async def test_cross_validation_with_broker_api(self, handler, mock_db_session):
        """Test cross-validation of trades with broker API."""
        # Mock our trade data
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", 100, Decimal("150.00"), datetime.now(), "bt_001")
        ]
        
        # Mock broker API returning additional trades
        broker_trades = [
            {"trade_id": "bt_001", "symbol": "AAPL", "quantity": 100, "price": 150.00, "timestamp": datetime.now()},
            {"trade_id": "bt_002", "symbol": "AAPL", "quantity": 50, "price": 152.00, "timestamp": datetime.now()},  # Missing from our DB
        ]
        
        with patch.object(handler, '_fetch_broker_trades', return_value=broker_trades):
            validation_result = await handler.cross_validate_with_broker(
                trading_account_id="acc_001",
                date_range=(datetime(2024, 1, 15), datetime(2024, 1, 15, 23, 59, 59))
            )
        
        assert len(validation_result.missing_trades) == 1
        assert validation_result.missing_trades[0]["trade_id"] == "bt_002"

    async def test_reconstruct_missing_trades_from_broker(self, handler, mock_db_session):
        """Test reconstruction of missing trades from broker data."""
        # Mock gap with missing trade
        gap = TradeGap(
            gap_id="gap_123",
            gap_type=GapType.TIME_SEQUENCE,
            trading_account_id="acc_001",
            symbol="AAPL",
            detected_at=datetime.now(),
            gap_start_time=datetime(2024, 1, 15, 10, 5),
            gap_end_time=datetime(2024, 1, 15, 10, 35),
            before_trade_id=1,
            after_trade_id=3,
            significance_score=Decimal("5000.00")
        )
        
        # Mock broker trade data for reconstruction
        broker_trade_data = [
            {
                "trade_id": "bt_002",
                "symbol": "AAPL", 
                "quantity": 75,
                "price": 152.50,
                "timestamp": datetime(2024, 1, 15, 10, 20),
                "order_id": "order_456"
            }
        ]
        
        with patch.object(handler, '_fetch_broker_trades_for_gap', return_value=broker_trade_data):
            mock_db_session.execute.return_value = MagicMock()
            mock_db_session.commit = AsyncMock()
            
            result = await handler.reconstruct_missing_trades(
                gap=gap,
                strategy=ReconstructionStrategy.BROKER_API_BACKFILL
            )
        
        assert isinstance(result, ReconstructionResult)
        assert result.success is True
        assert result.reconstructed_trade_count == 1
        assert result.total_reconstructed_value == Decimal("11437.50")  # 75 * 152.50

    async def test_reconstruct_from_execution_logs(self, handler, mock_db_session):
        """Test reconstruction from execution service logs."""
        gap = TradeGap(
            gap_id="gap_124",
            gap_type=GapType.VOLUME_MISMATCH,
            trading_account_id="acc_001", 
            symbol="TSLA",
            detected_at=datetime.now()
        )
        
        # Mock execution logs
        execution_logs = [
            {
                "execution_id": "exec_789",
                "order_id": "order_123",
                "symbol": "TSLA",
                "quantity": 25,
                "price": Decimal("800.00"),
                "execution_time": datetime.now(),
                "broker_confirmation_id": "bt_missing"
            }
        ]
        
        with patch.object(handler, '_fetch_execution_logs_for_gap', return_value=execution_logs):
            mock_db_session.execute.return_value = MagicMock()
            mock_db_session.commit = AsyncMock()
            
            result = await handler.reconstruct_missing_trades(
                gap=gap,
                strategy=ReconstructionStrategy.EXECUTION_LOG_RECONSTRUCTION
            )
        
        assert result.success is True
        assert result.reconstructed_trade_count == 1

    async def test_intelligent_gap_prioritization(self, handler, mock_db_session):
        """Test intelligent prioritization of gaps for reconstruction."""
        # Mock multiple gaps with different characteristics
        gaps = [
            TradeGap(
                gap_id="gap_small", gap_type=GapType.TIME_SEQUENCE, 
                significance_score=Decimal("100.00"), duration_minutes=5
            ),
            TradeGap(
                gap_id="gap_large", gap_type=GapType.VOLUME_MISMATCH,
                significance_score=Decimal("50000.00"), duration_minutes=60
            ),
            TradeGap(
                gap_id="gap_recent", gap_type=GapType.SEQUENCE_NUMBER,
                significance_score=Decimal("5000.00"), 
                detected_at=datetime.now() - timedelta(minutes=10)
            )
        ]
        
        prioritized_gaps = await handler.prioritize_gaps_for_reconstruction(gaps)
        
        # Large value gap should be highest priority
        assert prioritized_gaps[0].gap_id == "gap_large"
        # Recent gap should be second priority
        assert prioritized_gaps[1].gap_id == "gap_recent"
        # Small gap should be lowest priority
        assert prioritized_gaps[2].gap_id == "gap_small"

    async def test_gap_resolution_tracking(self, handler, mock_db_session):
        """Test tracking of gap resolution attempts."""
        gap_id = "gap_123"
        
        # Mock gap resolution attempt
        mock_db_session.execute.return_value = MagicMock()
        mock_db_session.commit = AsyncMock()
        
        await handler.record_reconstruction_attempt(
            gap_id=gap_id,
            strategy=ReconstructionStrategy.BROKER_API_BACKFILL,
            success=True,
            trades_reconstructed=2,
            error_message=None
        )
        
        # Verify attempt was recorded
        mock_db_session.execute.assert_called()
        mock_db_session.commit.assert_called_once()

    async def test_multiple_source_reconciliation(self, handler, mock_db_session):
        """Test reconciliation across multiple trade history sources."""
        # Mock data from different sources
        db_trades = [
            {"trade_id": "bt_001", "quantity": 100, "source": "database"}
        ]
        broker_trades = [
            {"trade_id": "bt_001", "quantity": 100, "source": "broker_api"},
            {"trade_id": "bt_002", "quantity": 50, "source": "broker_api"}  # Only in broker
        ]
        execution_logs = [
            {"trade_id": "bt_001", "quantity": 100, "source": "execution_service"},
            {"trade_id": "bt_003", "quantity": 25, "source": "execution_service"}  # Only in execution logs
        ]
        
        reconciliation = await handler.reconcile_multiple_sources(
            trading_account_id="acc_001",
            sources={
                TradeHistorySource.DATABASE: db_trades,
                TradeHistorySource.BROKER_API: broker_trades,
                TradeHistorySource.EXECUTION_LOGS: execution_logs
            }
        )
        
        assert reconciliation.consensus_trades == 1  # bt_001 in all sources
        assert reconciliation.source_only_trades == 2  # bt_002 and bt_003
        assert len(reconciliation.reconstruction_candidates) == 2

    async def test_automated_gap_detection_workflow(self, handler, mock_db_session):
        """Test end-to-end automated gap detection workflow."""
        # Mock trade data with gaps
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", 100, Decimal("150.00"), datetime(2024, 1, 15, 10, 0), "bt_001"),
            # 30 minute gap here
            (3, "AAPL", 50, Decimal("155.00"), datetime(2024, 1, 15, 10, 35), "bt_003")
        ]
        
        # Mock broker validation
        with patch.object(handler, '_fetch_broker_trades', return_value=[
            {"trade_id": "bt_001", "timestamp": datetime(2024, 1, 15, 10, 0)},
            {"trade_id": "bt_002", "timestamp": datetime(2024, 1, 15, 10, 20)},  # Missing trade
            {"trade_id": "bt_003", "timestamp": datetime(2024, 1, 15, 10, 35)}
        ]):
            workflow_result = await handler.run_automated_detection_workflow(
                trading_account_id="acc_001",
                date_range=(datetime(2024, 1, 15), datetime(2024, 1, 15, 23, 59, 59))
            )
        
        assert workflow_result.total_gaps_detected >= 1
        assert workflow_result.high_priority_gaps >= 0
        assert workflow_result.reconstruction_success_rate >= 0.0

    async def test_gap_false_positive_filtering(self, handler, mock_db_session):
        """Test filtering of false positive gaps."""
        # Mock gaps that should be filtered out
        candidate_gaps = [
            TradeGap(
                gap_id="gap_insignificant", 
                significance_score=Decimal("10.00"),  # Below threshold
                gap_type=GapType.TIME_SEQUENCE
            ),
            TradeGap(
                gap_id="gap_weekend",
                gap_start_time=datetime(2024, 1, 13, 18, 0),  # Saturday
                gap_end_time=datetime(2024, 1, 15, 9, 0),    # Monday
                gap_type=GapType.TIME_SEQUENCE
            ),
            TradeGap(
                gap_id="gap_valid",
                significance_score=Decimal("5000.00"),  # Above threshold
                gap_start_time=datetime(2024, 1, 15, 10, 0),  # Weekday
                gap_end_time=datetime(2024, 1, 15, 10, 30),
                gap_type=GapType.TIME_SEQUENCE
            )
        ]
        
        filtered_gaps = await handler.filter_false_positive_gaps(candidate_gaps)
        
        # Only valid gap should remain
        assert len(filtered_gaps) == 1
        assert filtered_gaps[0].gap_id == "gap_valid"

    async def test_reconstruction_validation(self, handler, mock_db_session):
        """Test validation of reconstructed trade data."""
        reconstructed_trade = {
            "broker_trade_id": "bt_reconstructed",
            "symbol": "AAPL",
            "quantity": 100,
            "price": Decimal("150.00"),
            "trade_time": datetime.now(),
            "reconstruction_source": "broker_api"
        }
        
        # Mock validation checks
        with patch.object(handler, '_validate_trade_integrity', return_value=True):
            with patch.object(handler, '_check_duplicate_trade', return_value=False):
                validation_result = await handler.validate_reconstructed_trade(
                    reconstructed_trade,
                    original_gap_context={"symbol": "AAPL", "trading_account_id": "acc_001"}
                )
        
        assert validation_result.is_valid is True
        assert len(validation_result.validation_errors) == 0

    async def test_performance_monitoring_large_datasets(self, handler, mock_db_session):
        """Test performance with large datasets."""
        # Mock large dataset
        large_trade_dataset = [
            (i, "AAPL", 100, Decimal("150.00"), datetime.now() + timedelta(minutes=i), f"bt_{i}")
            for i in range(10000)
        ]
        mock_db_session.execute.return_value.fetchall.return_value = large_trade_dataset
        
        start_time = datetime.now()
        gaps = await handler.detect_time_based_gaps(
            trading_account_id="acc_001",
            symbol="AAPL",
            start_date=datetime(2024, 1, 15),
            end_date=datetime(2024, 1, 15, 23, 59, 59)
        )
        end_time = datetime.now()
        
        # Should complete processing within reasonable time
        processing_time = (end_time - start_time).total_seconds()
        assert processing_time < 10  # Should complete within 10 seconds
        
        # Should handle large datasets without errors
        assert isinstance(gaps, list)

    async def test_error_handling_broker_api_timeout(self, handler, mock_db_session):
        """Test handling of broker API timeouts during reconstruction."""
        gap = TradeGap(
            gap_id="gap_timeout_test",
            gap_type=GapType.TIME_SEQUENCE,
            trading_account_id="acc_001"
        )
        
        # Mock broker API timeout
        with patch.object(handler, '_fetch_broker_trades_for_gap', side_effect=asyncio.TimeoutError("Broker API timeout")):
            result = await handler.reconstruct_missing_trades(
                gap=gap,
                strategy=ReconstructionStrategy.BROKER_API_BACKFILL
            )
        
        assert result.success is False
        assert "timeout" in result.error_message.lower()
        assert result.fallback_strategy_used is True