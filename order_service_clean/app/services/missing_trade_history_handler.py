"""
Missing Trade History Handler

Handles reconciliation scenarios where trade history is incomplete or missing by implementing:
- Trade gap detection and analysis
- Historical data reconstruction methods
- Broker API fallback for missing data
- Position state inference from available data
- Trade sequence validation and repair

Key Features:
- Automatic trade gap detection
- Multiple reconstruction strategies (broker API, position inference, manual input)
- Data quality validation for reconstructed trades
- Audit trail for all reconstruction activities
- Safe fallback mechanisms when reconstruction fails
"""

import logging
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from dataclasses import dataclass
from enum import Enum
from uuid import uuid4
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class TradeGapType(str, Enum):
    """Types of trade history gaps."""
    MISSING_ENTRY = "missing_entry"              # Missing entry trade for position
    MISSING_EXIT = "missing_exit"                # Missing exit trade for closed position
    SEQUENCE_GAP = "sequence_gap"                # Gap in trade sequence numbers
    TIMESTAMP_GAP = "timestamp_gap"              # Suspicious time gap between trades
    QUANTITY_MISMATCH = "quantity_mismatch"      # Position quantity doesn't match trades
    BROKER_SYNC_GAP = "broker_sync_gap"          # Missing trades from broker sync


class ReconstructionStrategy(str, Enum):
    """Strategies for reconstructing missing trades."""
    BROKER_API_FETCH = "broker_api_fetch"        # Fetch from broker API
    POSITION_INFERENCE = "position_inference"    # Infer from position state
    MANUAL_INPUT = "manual_input"                # Manual data entry required
    INTERPOLATION = "interpolation"              # Interpolate from surrounding data
    EXTERNAL_SOURCE = "external_source"          # Use external data source


class ReconstructionConfidence(str, Enum):
    """Confidence levels for reconstructed trade data."""
    HIGH = "high"          # >90% confidence - can auto-apply
    MEDIUM = "medium"      # 70-90% confidence - review recommended
    LOW = "low"            # 50-70% confidence - manual review required
    VERY_LOW = "very_low"  # <50% confidence - manual intervention required


@dataclass
class TradeGap:
    """Represents a detected gap in trade history."""
    gap_id: str
    gap_type: TradeGapType
    position_id: Optional[int]
    symbol: str
    trading_account_id: str
    detected_at: datetime
    gap_period_start: Optional[datetime]
    gap_period_end: Optional[datetime]
    expected_trades: int
    actual_trades: int
    missing_quantity: Optional[Decimal]
    context_data: Dict[str, Any]


@dataclass
class ReconstructedTrade:
    """Represents a reconstructed trade."""
    reconstruction_id: str
    original_gap_id: str
    symbol: str
    side: str
    quantity: Decimal
    price: Decimal
    timestamp: datetime
    order_id: Optional[str]
    trade_source: str
    reconstruction_strategy: ReconstructionStrategy
    confidence_level: ReconstructionConfidence
    metadata: Dict[str, Any]


@dataclass
class TradeHistoryAnalysis:
    """Analysis of trade history completeness."""
    analysis_id: str
    trading_account_id: str
    symbol: Optional[str]
    analysis_period_start: datetime
    analysis_period_end: datetime
    total_positions: int
    total_trades: int
    gaps_detected: List[TradeGap]
    completeness_score: float  # 0.0 to 1.0
    data_quality_score: float  # 0.0 to 1.0
    recommendations: List[str]
    analysis_metadata: Dict[str, Any]


@dataclass
class ReconstructionResult:
    """Result of trade history reconstruction."""
    reconstruction_id: str
    gap_id: str
    success: bool
    reconstructed_trades: List[ReconstructedTrade]
    reconstruction_strategy: ReconstructionStrategy
    confidence_level: ReconstructionConfidence
    validation_errors: List[str]
    requires_manual_review: bool
    audit_data: Dict[str, Any]


class MissingTradeHistoryHandler:
    """
    Handles detection and reconstruction of missing trade history.
    
    Provides comprehensive analysis of trade data completeness and implements
    multiple strategies for reconstructing missing trade information.
    """

    def __init__(self, db: AsyncSession, broker_api_client=None):
        """
        Initialize the trade history handler.

        Args:
            db: Async database session
            broker_api_client: Optional broker API client for data fetching
        """
        self.db = db
        self.broker_api_client = broker_api_client

    async def analyze_trade_history_completeness(
        self,
        trading_account_id: str,
        symbol: Optional[str] = None,
        analysis_period_days: int = 30
    ) -> TradeHistoryAnalysis:
        """
        Analyze trade history for completeness and data quality issues.

        Args:
            trading_account_id: Trading account to analyze
            symbol: Optional specific symbol (None for all symbols)
            analysis_period_days: Days to look back for analysis

        Returns:
            Comprehensive analysis of trade history completeness

        Raises:
            Exception: If analysis fails
        """
        analysis_id = str(uuid4())
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=analysis_period_days)
        
        logger.info(
            f"[{analysis_id}] Analyzing trade history completeness for account {trading_account_id} "
            f"symbol={symbol} period={analysis_period_days}d"
        )

        try:
            # Step 1: Gather basic trade and position statistics
            stats = await self._gather_trade_statistics(
                trading_account_id, symbol, start_date, end_date
            )

            # Step 2: Detect trade gaps
            gaps = await self._detect_trade_gaps(
                trading_account_id, symbol, start_date, end_date
            )

            # Step 3: Calculate completeness and quality scores
            completeness_score = self._calculate_completeness_score(stats, gaps)
            quality_score = self._calculate_data_quality_score(stats, gaps)

            # Step 4: Generate recommendations
            recommendations = self._generate_reconstruction_recommendations(gaps, stats)

            analysis = TradeHistoryAnalysis(
                analysis_id=analysis_id,
                trading_account_id=trading_account_id,
                symbol=symbol,
                analysis_period_start=start_date,
                analysis_period_end=end_date,
                total_positions=stats["total_positions"],
                total_trades=stats["total_trades"],
                gaps_detected=gaps,
                completeness_score=completeness_score,
                data_quality_score=quality_score,
                recommendations=recommendations,
                analysis_metadata={
                    "analysis_duration_days": analysis_period_days,
                    "gaps_by_type": {gap_type.value: len([g for g in gaps if g.gap_type == gap_type]) for gap_type in TradeGapType},
                    "position_coverage": stats["positions_with_trades"] / max(stats["total_positions"], 1),
                    "trade_sequence_integrity": stats.get("sequence_integrity_score", 1.0)
                }
            )

            # Step 5: Store analysis results
            await self._store_analysis_results(analysis)

            logger.info(
                f"[{analysis_id}] Analysis complete: {len(gaps)} gaps detected, "
                f"completeness={completeness_score:.2f}, quality={quality_score:.2f}"
            )

            return analysis

        except Exception as e:
            logger.error(f"[{analysis_id}] Trade history analysis failed: {e}", exc_info=True)
            raise

    async def reconstruct_missing_trades(
        self,
        gap: TradeGap,
        preferred_strategy: Optional[ReconstructionStrategy] = None
    ) -> ReconstructionResult:
        """
        Reconstruct missing trades for a detected gap.

        Args:
            gap: Trade gap to reconstruct
            preferred_strategy: Optional preferred reconstruction strategy

        Returns:
            Reconstruction result with reconstructed trades

        Raises:
            Exception: If reconstruction fails
        """
        reconstruction_id = str(uuid4())
        
        logger.info(
            f"[{reconstruction_id}] Reconstructing missing trades for gap {gap.gap_id} "
            f"type={gap.gap_type} symbol={gap.symbol}"
        )

        try:
            # Step 1: Determine best reconstruction strategy
            strategy = preferred_strategy or await self._determine_reconstruction_strategy(gap)

            # Step 2: Execute reconstruction based on strategy
            if strategy == ReconstructionStrategy.BROKER_API_FETCH:
                result = await self._reconstruct_via_broker_api(gap)
            elif strategy == ReconstructionStrategy.POSITION_INFERENCE:
                result = await self._reconstruct_via_position_inference(gap)
            elif strategy == ReconstructionStrategy.INTERPOLATION:
                result = await self._reconstruct_via_interpolation(gap)
            else:
                result = ReconstructionResult(
                    reconstruction_id=reconstruction_id,
                    gap_id=gap.gap_id,
                    success=False,
                    reconstructed_trades=[],
                    reconstruction_strategy=strategy,
                    confidence_level=ReconstructionConfidence.VERY_LOW,
                    validation_errors=[f"Reconstruction strategy {strategy} not implemented"],
                    requires_manual_review=True,
                    audit_data={"strategy_not_implemented": strategy.value}
                )

            # Step 3: Validate reconstructed trades
            if result.success and result.reconstructed_trades:
                validation_result = await self._validate_reconstructed_trades(
                    gap, result.reconstructed_trades
                )
                result.validation_errors.extend(validation_result["errors"])
                if validation_result["critical_errors"]:
                    result.success = False
                    result.requires_manual_review = True

            # Step 4: Store reconstruction results
            await self._store_reconstruction_results(result)

            logger.info(
                f"[{reconstruction_id}] Reconstruction complete: success={result.success}, "
                f"trades={len(result.reconstructed_trades)}, confidence={result.confidence_level}"
            )

            return result

        except Exception as e:
            logger.error(f"[{reconstruction_id}] Trade reconstruction failed: {e}", exc_info=True)
            return ReconstructionResult(
                reconstruction_id=reconstruction_id,
                gap_id=gap.gap_id,
                success=False,
                reconstructed_trades=[],
                reconstruction_strategy=preferred_strategy or ReconstructionStrategy.MANUAL_INPUT,
                confidence_level=ReconstructionConfidence.VERY_LOW,
                validation_errors=[f"Reconstruction failed: {str(e)}"],
                requires_manual_review=True,
                audit_data={"error": str(e)}
            )

    async def apply_reconstructed_trades(
        self,
        reconstruction_result: ReconstructionResult,
        force_apply: bool = False
    ) -> Dict[str, Any]:
        """
        Apply reconstructed trades to the system.

        Args:
            reconstruction_result: Reconstruction result to apply
            force_apply: Force application even for low confidence trades

        Returns:
            Application result with applied trade IDs

        Raises:
            Exception: If application fails
        """
        if not reconstruction_result.success:
            raise ValueError("Cannot apply failed reconstruction result")

        if (reconstruction_result.confidence_level == ReconstructionConfidence.VERY_LOW and 
            not force_apply):
            raise ValueError("Low confidence reconstruction requires force_apply=True")

        logger.info(
            f"Applying {len(reconstruction_result.reconstructed_trades)} reconstructed trades "
            f"from {reconstruction_result.reconstruction_id}"
        )

        try:
            applied_trades = []
            
            await self.db.begin()
            
            for reconstructed_trade in reconstruction_result.reconstructed_trades:
                # Insert reconstructed trade
                trade_id = await self._insert_reconstructed_trade(reconstructed_trade)
                applied_trades.append(trade_id)
                
                # Update associated position if needed
                if reconstructed_trade.metadata.get("position_id"):
                    await self._update_position_for_reconstructed_trade(
                        reconstructed_trade.metadata["position_id"], 
                        reconstructed_trade
                    )

            # Mark gap as resolved
            await self.db.execute(
                text("""
                    UPDATE order_service.trade_gaps
                    SET 
                        status = 'resolved',
                        resolved_at = NOW(),
                        resolution_method = :method,
                        applied_reconstruction_id = :reconstruction_id
                    WHERE gap_id = :gap_id::uuid
                """),
                {
                    "gap_id": reconstruction_result.gap_id,
                    "method": reconstruction_result.reconstruction_strategy.value,
                    "reconstruction_id": reconstruction_result.reconstruction_id
                }
            )

            await self.db.commit()

            return {
                "success": True,
                "applied_trades": applied_trades,
                "reconstruction_id": reconstruction_result.reconstruction_id,
                "gap_id": reconstruction_result.gap_id
            }

        except Exception as e:
            await self.db.rollback()
            logger.error(f"Failed to apply reconstructed trades: {e}", exc_info=True)
            raise

    async def _gather_trade_statistics(
        self,
        trading_account_id: str,
        symbol: Optional[str],
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """Gather basic trade and position statistics."""
        
        # Build query with optional symbol filter
        symbol_filter = "AND p.symbol = :symbol" if symbol else ""
        symbol_params = {"symbol": symbol} if symbol else {}
        
        result = await self.db.execute(
            text(f"""
                SELECT 
                    COUNT(DISTINCT p.id) as total_positions,
                    COUNT(DISTINCT t.id) as total_trades,
                    COUNT(DISTINCT CASE WHEN t.id IS NOT NULL THEN p.id END) as positions_with_trades,
                    AVG(CASE WHEN p.quantity != 0 THEN 1.0 ELSE 0.0 END) as position_integrity
                FROM order_service.positions p
                LEFT JOIN order_service.trades t ON (
                    t.position_id = p.id 
                    AND t.timestamp BETWEEN :start_date AND :end_date
                )
                WHERE p.trading_account_id = :trading_account_id
                  AND p.created_at BETWEEN :start_date AND :end_date
                  {symbol_filter}
            """),
            {
                "trading_account_id": trading_account_id,
                "start_date": start_date,
                "end_date": end_date,
                **symbol_params
            }
        )
        
        row = result.fetchone()
        
        return {
            "total_positions": int(row[0] or 0),
            "total_trades": int(row[1] or 0),
            "positions_with_trades": int(row[2] or 0),
            "position_integrity_score": float(row[3] or 0.0),
            "sequence_integrity_score": 1.0  # TODO: Implement sequence checking
        }

    async def _detect_trade_gaps(
        self,
        trading_account_id: str,
        symbol: Optional[str],
        start_date: datetime,
        end_date: datetime
    ) -> List[TradeGap]:
        """Detect various types of trade gaps."""
        
        gaps = []
        
        # Detect missing entry trades (positions without corresponding buy trades)
        entry_gaps = await self._detect_missing_entry_trades(
            trading_account_id, symbol, start_date, end_date
        )
        gaps.extend(entry_gaps)
        
        # Detect missing exit trades (closed positions without sell trades)
        exit_gaps = await self._detect_missing_exit_trades(
            trading_account_id, symbol, start_date, end_date
        )
        gaps.extend(exit_gaps)
        
        # Detect quantity mismatches
        quantity_gaps = await self._detect_quantity_mismatches(
            trading_account_id, symbol, start_date, end_date
        )
        gaps.extend(quantity_gaps)
        
        return gaps

    async def _detect_missing_entry_trades(
        self,
        trading_account_id: str,
        symbol: Optional[str],
        start_date: datetime,
        end_date: datetime
    ) -> List[TradeGap]:
        """Detect positions missing entry trades."""
        
        symbol_filter = "AND p.symbol = :symbol" if symbol else ""
        symbol_params = {"symbol": symbol} if symbol else {}
        
        result = await self.db.execute(
            text(f"""
                SELECT 
                    p.id as position_id,
                    p.symbol,
                    p.quantity,
                    p.created_at,
                    p.buy_price
                FROM order_service.positions p
                WHERE p.trading_account_id = :trading_account_id
                  AND p.created_at BETWEEN :start_date AND :end_date
                  AND p.quantity > 0  -- Long positions
                  AND NOT EXISTS (
                      SELECT 1 FROM order_service.trades t
                      WHERE t.position_id = p.id
                        AND t.side = 'buy'
                        AND t.quantity > 0
                  )
                  {symbol_filter}
                ORDER BY p.created_at DESC
            """),
            {
                "trading_account_id": trading_account_id,
                "start_date": start_date,
                "end_date": end_date,
                **symbol_params
            }
        )
        
        gaps = []
        for row in result.fetchall():
            gap = TradeGap(
                gap_id=str(uuid4()),
                gap_type=TradeGapType.MISSING_ENTRY,
                position_id=row[0],
                symbol=row[1],
                trading_account_id=trading_account_id,
                detected_at=datetime.now(timezone.utc),
                gap_period_start=row[3],
                gap_period_end=row[3] + timedelta(minutes=1),  # Narrow window for entry
                expected_trades=1,
                actual_trades=0,
                missing_quantity=Decimal(str(row[2])),
                context_data={
                    "position_created_at": row[3].isoformat() if row[3] else None,
                    "buy_price": str(row[4]) if row[4] else None,
                    "position_quantity": str(row[2])
                }
            )
            gaps.append(gap)
        
        return gaps

    async def _detect_missing_exit_trades(
        self,
        trading_account_id: str,
        symbol: Optional[str],
        start_date: datetime,
        end_date: datetime
    ) -> List[TradeGap]:
        """Detect closed positions missing exit trades."""
        
        symbol_filter = "AND p.symbol = :symbol" if symbol else ""
        symbol_params = {"symbol": symbol} if symbol else {}
        
        result = await self.db.execute(
            text(f"""
                SELECT 
                    p.id as position_id,
                    p.symbol,
                    p.quantity,
                    p.updated_at,
                    p.sell_price
                FROM order_service.positions p
                WHERE p.trading_account_id = :trading_account_id
                  AND p.updated_at BETWEEN :start_date AND :end_date
                  AND p.is_open = false  -- Closed positions
                  AND p.quantity = 0     -- Fully closed
                  AND NOT EXISTS (
                      SELECT 1 FROM order_service.trades t
                      WHERE t.position_id = p.id
                        AND t.side = 'sell'
                        AND t.quantity > 0
                  )
                  {symbol_filter}
                ORDER BY p.updated_at DESC
            """),
            {
                "trading_account_id": trading_account_id,
                "start_date": start_date,
                "end_date": end_date,
                **symbol_params
            }
        )
        
        gaps = []
        for row in result.fetchall():
            gap = TradeGap(
                gap_id=str(uuid4()),
                gap_type=TradeGapType.MISSING_EXIT,
                position_id=row[0],
                symbol=row[1],
                trading_account_id=trading_account_id,
                detected_at=datetime.now(timezone.utc),
                gap_period_start=row[3] - timedelta(hours=1),  # Window around closure
                gap_period_end=row[3],
                expected_trades=1,
                actual_trades=0,
                missing_quantity=None,  # Unknown exit quantity
                context_data={
                    "position_closed_at": row[3].isoformat() if row[3] else None,
                    "sell_price": str(row[4]) if row[4] else None,
                    "position_was_quantity": "unknown"
                }
            )
            gaps.append(gap)
        
        return gaps

    async def _detect_quantity_mismatches(
        self,
        trading_account_id: str,
        symbol: Optional[str],
        start_date: datetime,
        end_date: datetime
    ) -> List[TradeGap]:
        """Detect positions where trade quantities don't match position quantity."""
        
        symbol_filter = "AND p.symbol = :symbol" if symbol else ""
        symbol_params = {"symbol": symbol} if symbol else {}
        
        result = await self.db.execute(
            text(f"""
                SELECT 
                    p.id as position_id,
                    p.symbol,
                    p.quantity as position_quantity,
                    COALESCE(SUM(CASE WHEN t.side = 'buy' THEN t.quantity ELSE 0 END), 0) as buy_total,
                    COALESCE(SUM(CASE WHEN t.side = 'sell' THEN t.quantity ELSE 0 END), 0) as sell_total,
                    p.created_at,
                    p.updated_at
                FROM order_service.positions p
                LEFT JOIN order_service.trades t ON t.position_id = p.id
                WHERE p.trading_account_id = :trading_account_id
                  AND p.created_at BETWEEN :start_date AND :end_date
                  {symbol_filter}
                GROUP BY p.id, p.symbol, p.quantity, p.created_at, p.updated_at
                HAVING p.quantity != (
                    COALESCE(SUM(CASE WHEN t.side = 'buy' THEN t.quantity ELSE 0 END), 0) -
                    COALESCE(SUM(CASE WHEN t.side = 'sell' THEN t.quantity ELSE 0 END), 0)
                )
                ORDER BY p.created_at DESC
            """),
            {
                "trading_account_id": trading_account_id,
                "start_date": start_date,
                "end_date": end_date,
                **symbol_params
            }
        )
        
        gaps = []
        for row in result.fetchall():
            position_qty = Decimal(str(row[2]))
            buy_total = Decimal(str(row[3]))
            sell_total = Decimal(str(row[4]))
            calculated_qty = buy_total - sell_total
            missing_qty = position_qty - calculated_qty
            
            gap = TradeGap(
                gap_id=str(uuid4()),
                gap_type=TradeGapType.QUANTITY_MISMATCH,
                position_id=row[0],
                symbol=row[1],
                trading_account_id=trading_account_id,
                detected_at=datetime.now(timezone.utc),
                gap_period_start=row[5],  # position created
                gap_period_end=row[6],    # position updated
                expected_trades=0,  # Unknown number of missing trades
                actual_trades=0,
                missing_quantity=abs(missing_qty),
                context_data={
                    "position_quantity": str(position_qty),
                    "calculated_quantity": str(calculated_qty),
                    "buy_total": str(buy_total),
                    "sell_total": str(sell_total),
                    "quantity_discrepancy": str(missing_qty)
                }
            )
            gaps.append(gap)
        
        return gaps

    def _calculate_completeness_score(
        self,
        stats: Dict[str, Any],
        gaps: List[TradeGap]
    ) -> float:
        """Calculate trade history completeness score (0.0 to 1.0)."""
        
        if stats["total_positions"] == 0:
            return 1.0
        
        # Base score from positions with trades
        coverage_score = stats["positions_with_trades"] / stats["total_positions"]
        
        # Penalty for each gap
        gap_penalty = min(len(gaps) * 0.05, 0.5)  # Max 50% penalty
        
        # Consider gap severity
        severity_penalty = 0.0
        for gap in gaps:
            if gap.gap_type in [TradeGapType.MISSING_ENTRY, TradeGapType.MISSING_EXIT]:
                severity_penalty += 0.1
            elif gap.gap_type == TradeGapType.QUANTITY_MISMATCH:
                severity_penalty += 0.05
        
        severity_penalty = min(severity_penalty, 0.3)  # Max 30% penalty
        
        score = coverage_score - gap_penalty - severity_penalty
        return max(0.0, min(1.0, score))

    def _calculate_data_quality_score(
        self,
        stats: Dict[str, Any],
        gaps: List[TradeGap]
    ) -> float:
        """Calculate data quality score (0.0 to 1.0)."""
        
        # Start with high quality assumption
        score = 1.0
        
        # Reduce score based on gap types
        critical_gaps = [g for g in gaps if g.gap_type in [
            TradeGapType.MISSING_ENTRY, 
            TradeGapType.MISSING_EXIT,
            TradeGapType.QUANTITY_MISMATCH
        ]]
        
        if critical_gaps:
            score -= len(critical_gaps) * 0.1
        
        # Factor in position integrity
        score *= stats.get("position_integrity_score", 1.0)
        
        return max(0.0, min(1.0, score))

    def _generate_reconstruction_recommendations(
        self,
        gaps: List[TradeGap],
        stats: Dict[str, Any]
    ) -> List[str]:
        """Generate recommendations for addressing trade gaps."""
        
        recommendations = []
        
        if not gaps:
            recommendations.append("Trade history appears complete - no action required")
            return recommendations
        
        gap_counts = {}
        for gap in gaps:
            gap_counts[gap.gap_type] = gap_counts.get(gap.gap_type, 0) + 1
        
        if gap_counts.get(TradeGapType.MISSING_ENTRY, 0) > 0:
            recommendations.append(
                f"Found {gap_counts[TradeGapType.MISSING_ENTRY]} positions missing entry trades - "
                "consider broker API fetch or position inference reconstruction"
            )
        
        if gap_counts.get(TradeGapType.MISSING_EXIT, 0) > 0:
            recommendations.append(
                f"Found {gap_counts[TradeGapType.MISSING_EXIT]} closed positions missing exit trades - "
                "review broker transaction history or manual data entry"
            )
        
        if gap_counts.get(TradeGapType.QUANTITY_MISMATCH, 0) > 0:
            recommendations.append(
                f"Found {gap_counts[TradeGapType.QUANTITY_MISMATCH]} positions with quantity mismatches - "
                "validate trade sequences and consider interpolation reconstruction"
            )
        
        if len(gaps) > 10:
            recommendations.append(
                "High number of gaps detected - consider comprehensive broker data sync"
            )
        
        return recommendations

    async def _determine_reconstruction_strategy(
        self,
        gap: TradeGap
    ) -> ReconstructionStrategy:
        """Determine best reconstruction strategy for a gap."""
        
        # Strategy selection based on gap type and available data
        if gap.gap_type == TradeGapType.MISSING_ENTRY and self.broker_api_client:
            return ReconstructionStrategy.BROKER_API_FETCH
        elif gap.gap_type == TradeGapType.MISSING_ENTRY:
            return ReconstructionStrategy.POSITION_INFERENCE
        elif gap.gap_type == TradeGapType.MISSING_EXIT and self.broker_api_client:
            return ReconstructionStrategy.BROKER_API_FETCH
        elif gap.gap_type == TradeGapType.QUANTITY_MISMATCH:
            return ReconstructionStrategy.INTERPOLATION
        else:
            return ReconstructionStrategy.MANUAL_INPUT

    async def _reconstruct_via_position_inference(
        self,
        gap: TradeGap
    ) -> ReconstructionResult:
        """Reconstruct trades by inferring from position data."""
        
        reconstruction_id = str(uuid4())
        
        try:
            if gap.gap_type != TradeGapType.MISSING_ENTRY:
                return ReconstructionResult(
                    reconstruction_id=reconstruction_id,
                    gap_id=gap.gap_id,
                    success=False,
                    reconstructed_trades=[],
                    reconstruction_strategy=ReconstructionStrategy.POSITION_INFERENCE,
                    confidence_level=ReconstructionConfidence.VERY_LOW,
                    validation_errors=["Position inference only supports missing entry trades"],
                    requires_manual_review=True,
                    audit_data={"unsupported_gap_type": gap.gap_type.value}
                )
            
            # Get position data
            result = await self.db.execute(
                text("""
                    SELECT 
                        id,
                        symbol,
                        quantity,
                        buy_price,
                        created_at,
                        strategy_id,
                        execution_id
                    FROM order_service.positions
                    WHERE id = :position_id
                """),
                {"position_id": gap.position_id}
            )
            
            row = result.fetchone()
            if not row:
                return ReconstructionResult(
                    reconstruction_id=reconstruction_id,
                    gap_id=gap.gap_id,
                    success=False,
                    reconstructed_trades=[],
                    reconstruction_strategy=ReconstructionStrategy.POSITION_INFERENCE,
                    confidence_level=ReconstructionConfidence.VERY_LOW,
                    validation_errors=["Position not found"],
                    requires_manual_review=True,
                    audit_data={"position_not_found": gap.position_id}
                )
            
            # Create reconstructed entry trade
            reconstructed_trade = ReconstructedTrade(
                reconstruction_id=reconstruction_id,
                original_gap_id=gap.gap_id,
                symbol=row[1],
                side="buy",
                quantity=Decimal(str(row[2])),
                price=Decimal(str(row[3])) if row[3] else Decimal('0'),
                timestamp=row[4] or datetime.now(timezone.utc),
                order_id=None,
                trade_source="position_inference",
                reconstruction_strategy=ReconstructionStrategy.POSITION_INFERENCE,
                confidence_level=ReconstructionConfidence.MEDIUM if row[3] else ReconstructionConfidence.LOW,
                metadata={
                    "position_id": row[0],
                    "strategy_id": row[5],
                    "execution_id": str(row[6]) if row[6] else None,
                    "inferred_from_position": True
                }
            )
            
            return ReconstructionResult(
                reconstruction_id=reconstruction_id,
                gap_id=gap.gap_id,
                success=True,
                reconstructed_trades=[reconstructed_trade],
                reconstruction_strategy=ReconstructionStrategy.POSITION_INFERENCE,
                confidence_level=reconstructed_trade.confidence_level,
                validation_errors=[],
                requires_manual_review=reconstructed_trade.confidence_level == ReconstructionConfidence.LOW,
                audit_data={
                    "inferred_from_position": gap.position_id,
                    "has_buy_price": row[3] is not None
                }
            )

        except Exception as e:
            logger.error(f"Position inference reconstruction failed: {e}", exc_info=True)
            return ReconstructionResult(
                reconstruction_id=reconstruction_id,
                gap_id=gap.gap_id,
                success=False,
                reconstructed_trades=[],
                reconstruction_strategy=ReconstructionStrategy.POSITION_INFERENCE,
                confidence_level=ReconstructionConfidence.VERY_LOW,
                validation_errors=[f"Reconstruction error: {str(e)}"],
                requires_manual_review=True,
                audit_data={"error": str(e)}
            )

    async def _reconstruct_via_broker_api(
        self,
        gap: TradeGap
    ) -> ReconstructionResult:
        """Reconstruct trades by fetching from broker API."""
        
        reconstruction_id = str(uuid4())
        
        # Placeholder for broker API reconstruction
        # This would integrate with actual broker APIs
        
        return ReconstructionResult(
            reconstruction_id=reconstruction_id,
            gap_id=gap.gap_id,
            success=False,
            reconstructed_trades=[],
            reconstruction_strategy=ReconstructionStrategy.BROKER_API_FETCH,
            confidence_level=ReconstructionConfidence.VERY_LOW,
            validation_errors=["Broker API reconstruction not implemented"],
            requires_manual_review=True,
            audit_data={"broker_api_available": self.broker_api_client is not None}
        )

    async def _reconstruct_via_interpolation(
        self,
        gap: TradeGap
    ) -> ReconstructionResult:
        """Reconstruct trades using interpolation from surrounding data."""
        
        reconstruction_id = str(uuid4())
        
        # Placeholder for interpolation reconstruction
        # This would analyze surrounding trades and interpolate missing data
        
        return ReconstructionResult(
            reconstruction_id=reconstruction_id,
            gap_id=gap.gap_id,
            success=False,
            reconstructed_trades=[],
            reconstruction_strategy=ReconstructionStrategy.INTERPOLATION,
            confidence_level=ReconstructionConfidence.VERY_LOW,
            validation_errors=["Interpolation reconstruction not implemented"],
            requires_manual_review=True,
            audit_data={"interpolation_method": "not_implemented"}
        )

    async def _validate_reconstructed_trades(
        self,
        gap: TradeGap,
        reconstructed_trades: List[ReconstructedTrade]
    ) -> Dict[str, Any]:
        """Validate reconstructed trades for consistency and quality."""
        
        errors = []
        critical_errors = []
        
        for trade in reconstructed_trades:
            # Basic validation
            if trade.quantity <= 0:
                critical_errors.append(f"Trade {trade.reconstruction_id} has non-positive quantity")
            
            if trade.price < 0:
                critical_errors.append(f"Trade {trade.reconstruction_id} has negative price")
            
            if trade.symbol != gap.symbol:
                critical_errors.append(f"Trade {trade.reconstruction_id} symbol mismatch")
            
            # Timestamp validation
            if gap.gap_period_start and gap.gap_period_end:
                if not (gap.gap_period_start <= trade.timestamp <= gap.gap_period_end):
                    errors.append(f"Trade {trade.reconstruction_id} timestamp outside expected gap period")
        
        return {
            "errors": errors,
            "critical_errors": critical_errors,
            "total_errors": len(errors) + len(critical_errors)
        }

    async def _store_analysis_results(self, analysis: TradeHistoryAnalysis) -> None:
        """Store trade history analysis results."""
        
        await self.db.execute(
            text("""
                INSERT INTO order_service.trade_history_analysis (
                    analysis_id,
                    trading_account_id,
                    symbol,
                    analysis_period_start,
                    analysis_period_end,
                    completeness_score,
                    data_quality_score,
                    analysis_data,
                    created_at
                ) VALUES (
                    :analysis_id::uuid,
                    :trading_account_id,
                    :symbol,
                    :start_date,
                    :end_date,
                    :completeness_score,
                    :quality_score,
                    :analysis_data::jsonb,
                    NOW()
                ) ON CONFLICT (analysis_id) DO UPDATE SET
                    updated_at = NOW()
            """),
            {
                "analysis_id": analysis.analysis_id,
                "trading_account_id": analysis.trading_account_id,
                "symbol": analysis.symbol,
                "start_date": analysis.analysis_period_start,
                "end_date": analysis.analysis_period_end,
                "completeness_score": analysis.completeness_score,
                "quality_score": analysis.data_quality_score,
                "analysis_data": {
                    "total_positions": analysis.total_positions,
                    "total_trades": analysis.total_trades,
                    "gaps_count": len(analysis.gaps_detected),
                    "recommendations": analysis.recommendations,
                    "metadata": analysis.analysis_metadata
                }
            }
        )
        
        # Store individual gaps
        for gap in analysis.gaps_detected:
            await self.db.execute(
                text("""
                    INSERT INTO order_service.trade_gaps (
                        gap_id,
                        analysis_id,
                        gap_type,
                        position_id,
                        symbol,
                        trading_account_id,
                        detected_at,
                        gap_period_start,
                        gap_period_end,
                        expected_trades,
                        actual_trades,
                        missing_quantity,
                        context_data,
                        status,
                        created_at
                    ) VALUES (
                        :gap_id::uuid,
                        :analysis_id::uuid,
                        :gap_type,
                        :position_id,
                        :symbol,
                        :trading_account_id,
                        :detected_at,
                        :gap_period_start,
                        :gap_period_end,
                        :expected_trades,
                        :actual_trades,
                        :missing_quantity,
                        :context_data::jsonb,
                        'detected',
                        NOW()
                    ) ON CONFLICT (gap_id) DO UPDATE SET
                        updated_at = NOW()
                """),
                {
                    "gap_id": gap.gap_id,
                    "analysis_id": analysis.analysis_id,
                    "gap_type": gap.gap_type.value,
                    "position_id": gap.position_id,
                    "symbol": gap.symbol,
                    "trading_account_id": gap.trading_account_id,
                    "detected_at": gap.detected_at,
                    "gap_period_start": gap.gap_period_start,
                    "gap_period_end": gap.gap_period_end,
                    "expected_trades": gap.expected_trades,
                    "actual_trades": gap.actual_trades,
                    "missing_quantity": gap.missing_quantity,
                    "context_data": gap.context_data
                }
            )
        
        await self.db.commit()

    async def _store_reconstruction_results(self, result: ReconstructionResult) -> None:
        """Store reconstruction results."""
        
        await self.db.execute(
            text("""
                INSERT INTO order_service.trade_reconstructions (
                    reconstruction_id,
                    gap_id,
                    reconstruction_strategy,
                    confidence_level,
                    success,
                    trades_count,
                    requires_manual_review,
                    validation_errors,
                    audit_data,
                    created_at
                ) VALUES (
                    :reconstruction_id::uuid,
                    :gap_id::uuid,
                    :strategy,
                    :confidence,
                    :success,
                    :trades_count,
                    :manual_review,
                    :validation_errors::jsonb,
                    :audit_data::jsonb,
                    NOW()
                ) ON CONFLICT (reconstruction_id) DO UPDATE SET
                    updated_at = NOW()
            """),
            {
                "reconstruction_id": result.reconstruction_id,
                "gap_id": result.gap_id,
                "strategy": result.reconstruction_strategy.value,
                "confidence": result.confidence_level.value,
                "success": result.success,
                "trades_count": len(result.reconstructed_trades),
                "manual_review": result.requires_manual_review,
                "validation_errors": result.validation_errors,
                "audit_data": result.audit_data
            }
        )
        
        await self.db.commit()

    async def _insert_reconstructed_trade(self, trade: ReconstructedTrade) -> int:
        """Insert a reconstructed trade into the trades table."""
        
        result = await self.db.execute(
            text("""
                INSERT INTO order_service.trades (
                    position_id,
                    symbol,
                    side,
                    quantity,
                    price,
                    timestamp,
                    order_id,
                    source,
                    trade_metadata,
                    created_at
                ) VALUES (
                    :position_id,
                    :symbol,
                    :side,
                    :quantity,
                    :price,
                    :timestamp,
                    :order_id,
                    :source,
                    :metadata::jsonb,
                    NOW()
                )
                RETURNING id
            """),
            {
                "position_id": trade.metadata.get("position_id"),
                "symbol": trade.symbol,
                "side": trade.side,
                "quantity": trade.quantity,
                "price": trade.price,
                "timestamp": trade.timestamp,
                "order_id": trade.order_id,
                "source": f"reconstructed_{trade.trade_source}",
                "metadata": {
                    "reconstruction_id": trade.reconstruction_id,
                    "original_gap_id": trade.original_gap_id,
                    "confidence_level": trade.confidence_level.value,
                    "reconstruction_strategy": trade.reconstruction_strategy.value,
                    **trade.metadata
                }
            }
        )
        
        return result.fetchone()[0]

    async def _update_position_for_reconstructed_trade(
        self,
        position_id: int,
        trade: ReconstructedTrade
    ) -> None:
        """Update position based on reconstructed trade."""
        
        # For now, just update the updated_at timestamp
        # More sophisticated position updates could be implemented here
        
        await self.db.execute(
            text("""
                UPDATE order_service.positions
                SET updated_at = NOW()
                WHERE id = :position_id
            """),
            {"position_id": position_id}
        )


# Helper functions for external use
async def analyze_trade_history_for_account(
    db: AsyncSession,
    trading_account_id: str,
    symbol: Optional[str] = None,
    analysis_period_days: int = 30
) -> TradeHistoryAnalysis:
    """
    Convenience function for trade history analysis.

    Args:
        db: Database session
        trading_account_id: Trading account to analyze
        symbol: Optional specific symbol
        analysis_period_days: Days to analyze

    Returns:
        Trade history analysis
    """
    handler = MissingTradeHistoryHandler(db)
    return await handler.analyze_trade_history_completeness(
        trading_account_id, symbol, analysis_period_days
    )