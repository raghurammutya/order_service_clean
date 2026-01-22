"""
Exit Context Matcher Service

Provides robust matching of external exits to internal trades/orders with:
- Multi-fill order handling
- Quantity tolerance for partial fills
- Delayed broker data handling
- Fuzzy timestamp matching
- Order state reconciliation

Key Features:
- Multi-fill order aggregation
- Configurable quantity tolerance
- Timestamp tolerance for delayed data
- Broker order ID fallback matching
- Symbol normalization
- Trading session boundary handling
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from dataclasses import dataclass
from enum import Enum
from uuid import uuid4
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class MatchQuality(str, Enum):
    """Quality levels for exit context matches."""
    EXACT = "exact"                    # Perfect match on all criteria
    HIGH = "high"                     # Minor differences within tolerance
    MEDIUM = "medium"                 # Some differences but likely match
    LOW = "low"                       # Significant differences, questionable
    NO_MATCH = "no_match"             # No suitable match found


class MatchCriteria(str, Enum):
    """Criteria used for matching exit contexts."""
    BROKER_TRADE_ID = "broker_trade_id"       # Exact broker trade ID match
    BROKER_ORDER_ID = "broker_order_id"       # Broker order ID match
    QUANTITY_PRICE_TIME = "quantity_price_time"  # Quantity + price + time match
    SYMBOL_TIME_QUANTITY = "symbol_time_quantity"  # Symbol + time + quantity match
    FUZZY_TIME_QUANTITY = "fuzzy_time_quantity"   # Fuzzy time + quantity match


@dataclass
class ExitContextConfig:
    """Configuration for exit context matching."""
    quantity_tolerance_percent: Decimal = Decimal('0.1')    # 0.1% quantity tolerance
    price_tolerance_percent: Decimal = Decimal('0.5')       # 0.5% price tolerance
    time_tolerance_minutes: int = 15                         # 15 minute time tolerance
    enable_multi_fill_aggregation: bool = True              # Aggregate multi-fill orders
    enable_fuzzy_matching: bool = True                       # Enable fuzzy matching
    max_delayed_data_hours: int = 24                         # Max delay for broker data


@dataclass
class ExitTradeCandidate:
    """Candidate trade for exit context matching."""
    trade_id: int
    broker_trade_id: Optional[str]
    broker_order_id: Optional[str]
    symbol: str
    quantity: Decimal
    price: Decimal
    trade_timestamp: datetime
    order_id: Optional[int]
    transaction_type: str
    source: str


@dataclass
class ExitContextMatch:
    """Result of exit context matching."""
    match_id: str
    external_exit: Dict[str, Any]
    matched_trades: List[ExitTradeCandidate]
    match_quality: MatchQuality
    match_criteria: List[MatchCriteria]
    confidence_score: float  # 0.0 to 1.0
    quantity_difference: Decimal
    price_difference: Optional[Decimal]
    time_difference: Optional[timedelta]
    aggregated_from_multi_fill: bool
    warnings: List[str]
    metadata: Dict[str, Any]


class ExitContextMatcher:
    """
    Service for robust matching of external exits to internal trades.
    
    Handles complex scenarios like multi-fill orders, delayed broker data,
    and tolerance-based matching for reconciliation accuracy.
    """

    def __init__(self, db: AsyncSession, config: Optional[ExitContextConfig] = None):
        """
        Initialize the exit context matcher.

        Args:
            db: Async database session
            config: Optional configuration for matching behavior
        """
        self.db = db
        self.config = config or ExitContextConfig()

    async def match_exit_context(
        self,
        external_exit: Dict[str, Any],
        trading_account_id: str,
        symbol: str,
        search_window_hours: int = 48
    ) -> ExitContextMatch:
        """
        Match an external exit to internal trades with reconciliation-enhanced execution context determination.

        Args:
            external_exit: External exit data from broker
            trading_account_id: Trading account ID
            symbol: Trading symbol
            search_window_hours: Time window to search for matches

        Returns:
            Exit context match result

        Raises:
            Exception: If matching process fails
        """
        match_id = str(uuid4())
        logger.info(
            f"[{match_id}] Matching exit context for {symbol} "
            f"qty={external_exit.get('quantity')} account={trading_account_id}"
        )

        try:
            # Step 1: Get candidate trades within search window
            candidates = await self._get_candidate_trades(
                trading_account_id, symbol, external_exit, search_window_hours
            )

            # Step 2: Get position-based reconciliation context for better execution ID determination
            position_context = await self._get_position_reconciliation_context(
                trading_account_id, symbol, external_exit
            )

            if not candidates:
                # Try reconciliation-based context determination even without trade matches
                if position_context:
                    return await self._create_reconciliation_based_match(
                        match_id, external_exit, position_context
                    )
                return self._create_no_match_result(match_id, external_exit, "No candidate trades found")

            # Step 3: Try exact matching first
            exact_match = await self._try_exact_matching(match_id, external_exit, candidates)
            if exact_match.match_quality == MatchQuality.EXACT:
                # Enhance exact match with reconciliation context
                await self._enhance_match_with_reconciliation_context(exact_match, position_context)
                return exact_match

            # Step 4: Try high-quality matching with tolerance
            high_match = await self._try_tolerance_matching(match_id, external_exit, candidates)
            if high_match.match_quality in [MatchQuality.HIGH, MatchQuality.EXACT]:
                await self._enhance_match_with_reconciliation_context(high_match, position_context)
                return high_match

            # Step 5: Try multi-fill aggregation if enabled
            if self.config.enable_multi_fill_aggregation:
                multi_fill_match = await self._try_multi_fill_matching(match_id, external_exit, candidates)
                if multi_fill_match.match_quality in [MatchQuality.HIGH, MatchQuality.EXACT]:
                    await self._enhance_match_with_reconciliation_context(multi_fill_match, position_context)
                    return multi_fill_match

            # Step 6: Try reconciliation-driven matching if trades don't match well
            if position_context:
                reconciliation_match = await self._try_reconciliation_driven_matching(
                    match_id, external_exit, candidates, position_context
                )
                if reconciliation_match.match_quality != MatchQuality.NO_MATCH:
                    return reconciliation_match

            # Step 7: Try fuzzy matching if enabled
            if self.config.enable_fuzzy_matching:
                fuzzy_match = await self._try_fuzzy_matching(match_id, external_exit, candidates)
                if fuzzy_match.match_quality != MatchQuality.NO_MATCH:
                    await self._enhance_match_with_reconciliation_context(fuzzy_match, position_context)
                    return fuzzy_match

            # Step 8: Return best available match or no match
            return self._create_no_match_result(match_id, external_exit, "No suitable matches found within tolerance")

        except Exception as e:
            logger.error(f"[{match_id}] Exit context matching failed: {e}", exc_info=True)
            return self._create_error_result(match_id, external_exit, str(e))

    async def _get_candidate_trades(
        self,
        trading_account_id: str,
        symbol: str,
        external_exit: Dict[str, Any],
        search_window_hours: int
    ) -> List[ExitTradeCandidate]:
        """Get candidate trades for matching."""
        exit_time = external_exit.get('timestamp')
        if isinstance(exit_time, str):
            exit_time = datetime.fromisoformat(exit_time.replace('Z', '+00:00'))
        elif not exit_time:
            exit_time = datetime.now(timezone.utc)

        # Search window around exit time
        start_time = exit_time - timedelta(hours=search_window_hours)
        end_time = exit_time + timedelta(hours=self.config.max_delayed_data_hours)

        result = await self.db.execute(
            text("""
                SELECT 
                    t.id as trade_id,
                    t.broker_trade_id,
                    t.broker_order_id,
                    t.symbol,
                    t.quantity,
                    t.price,
                    t.trade_time,
                    t.order_id,
                    t.transaction_type,
                    t.source
                FROM order_service.trades t
                WHERE t.trading_account_id = :trading_account_id
                  AND t.symbol = :symbol
                  AND t.trade_time BETWEEN :start_time AND :end_time
                  AND t.transaction_type = 'SELL'  -- Assuming this is a sell exit
                ORDER BY t.trade_time DESC, t.id DESC
                LIMIT 100  -- Prevent excessive candidates
            """),
            {
                "trading_account_id": trading_account_id,
                "symbol": symbol,
                "start_time": start_time,
                "end_time": end_time
            }
        )

        candidates = []
        for row in result.fetchall():
            candidates.append(ExitTradeCandidate(
                trade_id=row[0],
                broker_trade_id=row[1],
                broker_order_id=row[2],
                symbol=row[3],
                quantity=Decimal(str(row[4])),
                price=Decimal(str(row[5])),
                trade_timestamp=row[6],
                order_id=row[7],
                transaction_type=row[8],
                source=row[9]
            ))

        logger.debug(f"Found {len(candidates)} candidate trades for matching")
        return candidates

    async def _try_exact_matching(
        self,
        match_id: str,
        external_exit: Dict[str, Any],
        candidates: List[ExitTradeCandidate]
    ) -> ExitContextMatch:
        """Try exact matching on broker trade ID."""
        external_trade_id = external_exit.get('broker_trade_id') or external_exit.get('trade_id')
        
        if not external_trade_id:
            return self._create_no_match_result(match_id, external_exit, "No broker trade ID for exact matching")

        for candidate in candidates:
            if candidate.broker_trade_id == external_trade_id:
                return ExitContextMatch(
                    match_id=match_id,
                    external_exit=external_exit,
                    matched_trades=[candidate],
                    match_quality=MatchQuality.EXACT,
                    match_criteria=[MatchCriteria.BROKER_TRADE_ID],
                    confidence_score=1.0,
                    quantity_difference=Decimal('0'),
                    price_difference=None,
                    time_difference=None,
                    aggregated_from_multi_fill=False,
                    warnings=[],
                    metadata={"exact_match_on": "broker_trade_id"}
                )

        return self._create_no_match_result(match_id, external_exit, "No exact broker trade ID match")

    async def _try_tolerance_matching(
        self,
        match_id: str,
        external_exit: Dict[str, Any],
        candidates: List[ExitTradeCandidate]
    ) -> ExitContextMatch:
        """Try matching with quantity and price tolerance."""
        external_qty = Decimal(str(external_exit.get('quantity', 0)))
        external_price = external_exit.get('price')
        external_time = external_exit.get('timestamp')

        if isinstance(external_time, str):
            external_time = datetime.fromisoformat(external_time.replace('Z', '+00:00'))

        best_match = None
        best_score = 0.0

        for candidate in candidates:
            # Check quantity tolerance
            qty_diff = abs(candidate.quantity - external_qty)
            qty_tolerance = external_qty * (self.config.quantity_tolerance_percent / 100)
            
            if qty_diff > qty_tolerance:
                continue  # Quantity out of tolerance

            # Check price tolerance if available
            price_score = 1.0
            if external_price and candidate.price:
                price_diff = abs(candidate.price - Decimal(str(external_price)))
                price_tolerance = Decimal(str(external_price)) * (self.config.price_tolerance_percent / 100)
                if price_diff <= price_tolerance:
                    price_score = 1.0 - float(price_diff / price_tolerance) * 0.2
                else:
                    price_score = 0.6  # Reduce score but don't eliminate

            # Check time tolerance if available
            time_score = 1.0
            if external_time and candidate.trade_timestamp:
                time_diff = abs(candidate.trade_timestamp - external_time)
                time_tolerance = timedelta(minutes=self.config.time_tolerance_minutes)
                if time_diff <= time_tolerance:
                    time_score = 1.0 - (time_diff.total_seconds() / time_tolerance.total_seconds()) * 0.2
                else:
                    time_score = 0.7  # Reduce score for time differences

            # Calculate overall score
            qty_score = 1.0 - float(qty_diff / max(qty_tolerance, Decimal('0.01')))
            overall_score = (qty_score * 0.5) + (price_score * 0.3) + (time_score * 0.2)

            if overall_score > best_score:
                best_score = overall_score
                best_match = candidate

        if best_match and best_score >= 0.8:
            quality = MatchQuality.HIGH if best_score >= 0.9 else MatchQuality.MEDIUM
            
            return ExitContextMatch(
                match_id=match_id,
                external_exit=external_exit,
                matched_trades=[best_match],
                match_quality=quality,
                match_criteria=[MatchCriteria.QUANTITY_PRICE_TIME],
                confidence_score=best_score,
                quantity_difference=abs(best_match.quantity - external_qty),
                price_difference=abs(best_match.price - Decimal(str(external_price))) if external_price else None,
                time_difference=abs(best_match.trade_timestamp - external_time) if external_time else None,
                aggregated_from_multi_fill=False,
                warnings=[] if best_score >= 0.9 else ["Match quality below excellent threshold"],
                metadata={"tolerance_matching_score": best_score}
            )

        return self._create_no_match_result(match_id, external_exit, "No matches within tolerance")

    async def _try_multi_fill_matching(
        self,
        match_id: str,
        external_exit: Dict[str, Any],
        candidates: List[ExitTradeCandidate]
    ) -> ExitContextMatch:
        """Try matching by aggregating multi-fill orders."""
        external_qty = Decimal(str(external_exit.get('quantity', 0)))
        external_order_id = external_exit.get('broker_order_id') or external_exit.get('order_id')

        if not external_order_id:
            return self._create_no_match_result(match_id, external_exit, "No order ID for multi-fill matching")

        # Group candidates by broker order ID
        order_groups = {}
        for candidate in candidates:
            if candidate.broker_order_id:
                if candidate.broker_order_id not in order_groups:
                    order_groups[candidate.broker_order_id] = []
                order_groups[candidate.broker_order_id].append(candidate)

        # Try to match against aggregated fills
        for order_id, fills in order_groups.items():
            total_qty = sum(fill.quantity for fill in fills)
            avg_price = sum(fill.price * fill.quantity for fill in fills) / total_qty
            
            # Check if aggregated quantity matches
            qty_diff = abs(total_qty - external_qty)
            qty_tolerance = external_qty * (self.config.quantity_tolerance_percent / 100)
            
            if qty_diff <= qty_tolerance:
                confidence = 0.9 if qty_diff == 0 else 0.8
                
                return ExitContextMatch(
                    match_id=match_id,
                    external_exit=external_exit,
                    matched_trades=fills,
                    match_quality=MatchQuality.HIGH,
                    match_criteria=[MatchCriteria.BROKER_ORDER_ID],
                    confidence_score=confidence,
                    quantity_difference=qty_diff,
                    price_difference=None,
                    time_difference=None,
                    aggregated_from_multi_fill=True,
                    warnings=[f"Aggregated {len(fills)} fills from order {order_id}"],
                    metadata={
                        "multi_fill_count": len(fills),
                        "order_id": order_id,
                        "aggregated_quantity": str(total_qty),
                        "average_price": str(avg_price)
                    }
                )

        return self._create_no_match_result(match_id, external_exit, "No multi-fill matches found")

    async def _try_fuzzy_matching(
        self,
        match_id: str,
        external_exit: Dict[str, Any],
        candidates: List[ExitTradeCandidate]
    ) -> ExitContextMatch:
        """Try fuzzy matching with relaxed criteria."""
        external_qty = Decimal(str(external_exit.get('quantity', 0)))
        
        # Relax tolerances for fuzzy matching
        fuzzy_qty_tolerance = external_qty * Decimal('0.05')  # 5% tolerance
        
        for candidate in candidates:
            qty_diff = abs(candidate.quantity - external_qty)
            
            if qty_diff <= fuzzy_qty_tolerance:
                confidence = 0.6 - float(qty_diff / fuzzy_qty_tolerance) * 0.2
                
                return ExitContextMatch(
                    match_id=match_id,
                    external_exit=external_exit,
                    matched_trades=[candidate],
                    match_quality=MatchQuality.LOW,
                    match_criteria=[MatchCriteria.FUZZY_TIME_QUANTITY],
                    confidence_score=confidence,
                    quantity_difference=qty_diff,
                    price_difference=None,
                    time_difference=None,
                    aggregated_from_multi_fill=False,
                    warnings=["Fuzzy matching used - manual verification recommended"],
                    metadata={"fuzzy_match": True, "relaxed_tolerance": True}
                )

        return self._create_no_match_result(match_id, external_exit, "No fuzzy matches found")

    async def _get_position_reconciliation_context(
        self,
        trading_account_id: str,
        symbol: str,
        external_exit: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Get position-based reconciliation context for better execution context determination.
        
        This method analyzes current positions and recent holdings reconciliation data
        to determine the most likely execution contexts for the exit.
        """
        try:
            # Get all open positions for this symbol
            result = await self.db.execute(
                text("""
                    SELECT 
                        p.id as position_id,
                        p.strategy_id,
                        p.execution_id,
                        p.portfolio_id,
                        p.quantity,
                        p.buy_price as entry_price,
                        p.created_at as entry_timestamp,
                        p.source,
                        p.metadata,
                        -- Get total quantity per execution_id for reconciliation analysis
                        SUM(p2.quantity) OVER (PARTITION BY p2.execution_id) as execution_total_quantity,
                        -- Get total quantity per strategy for attribution analysis  
                        SUM(p3.quantity) OVER (PARTITION BY p3.strategy_id) as strategy_total_quantity,
                        -- Get recent reconciliation variance data
                        rv.variance_quantity,
                        rv.variance_type,
                        rv.detected_at as last_variance_detected
                    FROM order_service.positions p
                    LEFT JOIN order_service.positions p2 ON p2.execution_id = p.execution_id AND p2.symbol = p.symbol AND p2.is_open = true
                    LEFT JOIN order_service.positions p3 ON p3.strategy_id = p.strategy_id AND p3.symbol = p.symbol AND p3.is_open = true
                    LEFT JOIN (
                        SELECT DISTINCT ON (execution_id, symbol) 
                            execution_id, symbol, variance_quantity, variance_type, detected_at
                        FROM order_service.reconciliation_variances 
                        WHERE detected_at > NOW() - INTERVAL '7 days'
                        ORDER BY execution_id, symbol, detected_at DESC
                    ) rv ON rv.execution_id = p.execution_id AND rv.symbol = p.symbol
                    WHERE p.trading_account_id = :trading_account_id
                      AND p.symbol = :symbol
                      AND p.is_open = true
                      AND p.quantity > 0
                      AND p.execution_id IS NOT NULL
                    ORDER BY p.created_at ASC  -- FIFO ordering for attribution
                """),
                {
                    "trading_account_id": trading_account_id,
                    "symbol": symbol
                }
            )

            positions = []
            execution_contexts = {}
            total_available_quantity = Decimal('0')
            
            for row in result.fetchall():
                position_data = {
                    "position_id": row[0],
                    "strategy_id": row[1],
                    "execution_id": row[2],
                    "portfolio_id": row[3],
                    "quantity": Decimal(str(row[4])),
                    "entry_price": Decimal(str(row[5])),
                    "entry_timestamp": row[6],
                    "source": row[7],
                    "metadata": row[8] or {},
                    "execution_total_quantity": Decimal(str(row[9])) if row[9] else Decimal('0'),
                    "strategy_total_quantity": Decimal(str(row[10])) if row[10] else Decimal('0'),
                    "variance_quantity": Decimal(str(row[11])) if row[11] else None,
                    "variance_type": row[12],
                    "last_variance_detected": row[13]
                }
                
                positions.append(position_data)
                total_available_quantity += position_data["quantity"]
                
                # Track execution contexts for analysis
                execution_id = position_data["execution_id"]
                if execution_id not in execution_contexts:
                    execution_contexts[execution_id] = {
                        "execution_id": execution_id,
                        "total_quantity": position_data["execution_total_quantity"],
                        "positions": [],
                        "strategies": set(),
                        "has_recent_variance": bool(position_data["variance_quantity"]),
                        "variance_info": {
                            "quantity": position_data["variance_quantity"],
                            "type": position_data["variance_type"],
                            "detected_at": position_data["last_variance_detected"]
                        } if position_data["variance_quantity"] else None
                    }
                
                execution_contexts[execution_id]["positions"].append(position_data)
                execution_contexts[execution_id]["strategies"].add(position_data["strategy_id"])

            if not positions:
                return None

            return {
                "positions": positions,
                "execution_contexts": execution_contexts,
                "total_available_quantity": total_available_quantity,
                "exit_quantity": Decimal(str(external_exit.get('quantity', 0))),
                "reconciliation_timestamp": datetime.now(timezone.utc)
            }

        except Exception as e:
            logger.error(f"Failed to get position reconciliation context: {e}")
            return None

    async def _create_reconciliation_based_match(
        self,
        match_id: str,
        external_exit: Dict[str, Any],
        position_context: Dict[str, Any]
    ) -> ExitContextMatch:
        """
        Create exit context match based on reconciliation data when no trades match.
        
        Uses position state and reconciliation history to determine the most likely
        execution context for the exit.
        """
        exit_quantity = abs(Decimal(str(external_exit.get('quantity', 0))))
        execution_contexts = position_context["execution_contexts"]
        
        # Find the best execution context based on reconciliation data
        best_execution_id = None
        best_score = 0.0
        match_reason = "No suitable execution context found"
        
        for execution_id, context in execution_contexts.items():
            score = 0.0
            
            # Score based on quantity match
            if context["total_quantity"] >= exit_quantity:
                quantity_ratio = float(exit_quantity / context["total_quantity"])
                if quantity_ratio <= 1.0:
                    score += 0.4 * (1.0 - abs(1.0 - quantity_ratio))
            
            # Score based on recent variance (suggests this execution has mismatches)
            if context["has_recent_variance"]:
                variance_info = context["variance_info"]
                if variance_info and variance_info["type"] == "unknown_exit":
                    # This execution has unexplained exits - likely candidate
                    score += 0.3
                    
                    # Check if variance quantity aligns with exit quantity
                    if variance_info["quantity"]:
                        variance_diff = abs(abs(variance_info["quantity"]) - exit_quantity)
                        if variance_diff <= exit_quantity * Decimal('0.1'):  # Within 10%
                            score += 0.2
            
            # Score based on number of strategies (fewer is simpler attribution)
            strategy_count = len(context["strategies"])
            if strategy_count == 1:
                score += 0.2  # Single strategy is easier attribution
            elif strategy_count <= 3:
                score += 0.1  # Few strategies still manageable
                
            # Score based on position age (older positions more likely to be exited)
            oldest_position = min(context["positions"], key=lambda p: p["entry_timestamp"])
            position_age_days = (datetime.now(timezone.utc) - oldest_position["entry_timestamp"]).days
            if position_age_days > 1:
                score += min(0.1, position_age_days / 100)  # Up to 0.1 for age
            
            if score > best_score:
                best_score = score
                best_execution_id = execution_id
                
                if context["has_recent_variance"]:
                    match_reason = f"Reconciliation variance detected in execution {execution_id}"
                else:
                    match_reason = f"Best position quantity match in execution {execution_id}"

        if best_execution_id and best_score >= 0.3:  # Minimum confidence threshold
            # Create synthetic trade candidates from positions in best execution
            best_context = execution_contexts[best_execution_id]
            synthetic_trades = []
            
            for position in best_context["positions"]:
                synthetic_trades.append(ExitTradeCandidate(
                    trade_id=-1,  # Synthetic trade ID
                    broker_trade_id=None,
                    broker_order_id=None,
                    symbol=position["symbol"] if "symbol" in position else external_exit.get("symbol", ""),
                    quantity=position["quantity"],
                    price=position["entry_price"],
                    trade_timestamp=position["entry_timestamp"],
                    order_id=None,
                    transaction_type="SELL",
                    source=f"reconciliation_inference:{position['source']}"
                ))
            
            return ExitContextMatch(
                match_id=match_id,
                external_exit=external_exit,
                matched_trades=synthetic_trades,
                match_quality=MatchQuality.MEDIUM if best_score >= 0.6 else MatchQuality.LOW,
                match_criteria=[MatchCriteria.SYMBOL_TIME_QUANTITY],
                confidence_score=best_score,
                quantity_difference=Decimal('0'),  # Will be refined during allocation
                price_difference=None,
                time_difference=None,
                aggregated_from_multi_fill=False,
                warnings=[f"Reconciliation-based match: {match_reason}"],
                metadata={
                    "reconciliation_based": True,
                    "execution_id": best_execution_id,
                    "execution_total_quantity": str(best_context["total_quantity"]),
                    "strategies_involved": list(best_context["strategies"]),
                    "has_variance": best_context["has_recent_variance"],
                    "variance_info": best_context["variance_info"],
                    "match_score": best_score,
                    "match_reason": match_reason
                }
            )
        
        return self._create_no_match_result(match_id, external_exit, "No suitable reconciliation-based match found")

    async def _enhance_match_with_reconciliation_context(
        self,
        match: ExitContextMatch,
        position_context: Optional[Dict[str, Any]]
    ) -> None:
        """
        Enhance an existing match with reconciliation context data.
        
        Adds execution_id and reconciliation metadata to improve attribution accuracy.
        """
        if not position_context or not match.matched_trades:
            return
            
        try:
            # Try to determine execution_id from matched trades and position context
            execution_contexts = position_context["execution_contexts"]
            
            for trade in match.matched_trades:
                # Look for positions that could correspond to this trade
                for execution_id, context in execution_contexts.items():
                    for position in context["positions"]:
                        # Check if this position could be related to the matched trade
                        quantity_match = abs(position["quantity"] - trade.quantity) <= position["quantity"] * Decimal('0.1')
                        price_similarity = abs(position["entry_price"] - trade.price) <= trade.price * Decimal('0.05')
                        time_proximity = abs((position["entry_timestamp"] - trade.trade_timestamp).total_seconds()) <= 86400  # 1 day
                        
                        if quantity_match or (price_similarity and time_proximity):
                            # Enhance match metadata with execution context
                            match.metadata.update({
                                "inferred_execution_id": execution_id,
                                "execution_total_quantity": str(context["total_quantity"]),
                                "strategies_in_execution": list(context["strategies"]),
                                "reconciliation_enhanced": True,
                                "has_recent_variance": context["has_recent_variance"],
                                "variance_info": context["variance_info"]
                            })
                            
                            if context["has_recent_variance"]:
                                match.warnings.append(
                                    f"Recent reconciliation variance detected in execution {execution_id}"
                                )
                            
                            logger.info(
                                f"Enhanced match {match.match_id} with execution context {execution_id}"
                            )
                            return
                            
        except Exception as e:
            logger.error(f"Failed to enhance match with reconciliation context: {e}")

    async def _try_reconciliation_driven_matching(
        self,
        match_id: str,
        external_exit: Dict[str, Any],
        candidates: List[ExitTradeCandidate],
        position_context: Dict[str, Any]
    ) -> ExitContextMatch:
        """
        Try matching using reconciliation data to prioritize candidates.
        
        Uses recent reconciliation variances and position state to score trade candidates
        more intelligently than pure quantity/price/time matching.
        """
        exit_quantity = abs(Decimal(str(external_exit.get('quantity', 0))))
        execution_contexts = position_context["execution_contexts"]
        
        # Score each candidate based on reconciliation context
        scored_candidates = []
        
        for candidate in candidates:
            score = 0.0
            reconciliation_factors = []
            
            # Find which execution context this candidate might belong to
            for execution_id, context in execution_contexts.items():
                for position in context["positions"]:
                    # Check if candidate could match this position
                    quantity_similarity = 1.0 - float(abs(position["quantity"] - candidate.quantity) / max(position["quantity"], candidate.quantity))
                    price_similarity = 1.0 - float(abs(position["entry_price"] - candidate.price) / max(position["entry_price"], candidate.price))
                    
                    if quantity_similarity > 0.7 or price_similarity > 0.8:
                        # This candidate likely relates to this execution context
                        context_score = 0.0
                        
                        # Score based on reconciliation variance
                        if context["has_recent_variance"]:
                            variance_info = context["variance_info"]
                            if variance_info and variance_info["type"] == "unknown_exit":
                                context_score += 0.4
                                reconciliation_factors.append(f"matches_variance_execution:{execution_id}")
                                
                                # Check if variance quantity aligns
                                if variance_info["quantity"]:
                                    variance_alignment = 1.0 - float(abs(abs(variance_info["quantity"]) - candidate.quantity) / candidate.quantity)
                                    if variance_alignment > 0.8:
                                        context_score += 0.3
                                        reconciliation_factors.append("variance_quantity_aligned")
                        
                        # Score based on execution completeness
                        if context["total_quantity"] >= exit_quantity:
                            completion_ratio = float(exit_quantity / context["total_quantity"])
                            if 0.8 <= completion_ratio <= 1.0:
                                context_score += 0.2
                                reconciliation_factors.append("execution_completion_ratio_good")
                        
                        # Score based on strategy complexity
                        if len(context["strategies"]) == 1:
                            context_score += 0.1
                            reconciliation_factors.append("single_strategy_simple")
                        
                        score = max(score, context_score)
            
            if score > 0.2:  # Minimum reconciliation relevance
                scored_candidates.append((candidate, score, reconciliation_factors))
        
        # Sort by reconciliation score
        scored_candidates.sort(key=lambda x: x[1], reverse=True)
        
        if scored_candidates:
            best_candidate, best_score, factors = scored_candidates[0]
            
            if best_score >= 0.4:  # Good reconciliation match
                return ExitContextMatch(
                    match_id=match_id,
                    external_exit=external_exit,
                    matched_trades=[best_candidate],
                    match_quality=MatchQuality.HIGH if best_score >= 0.7 else MatchQuality.MEDIUM,
                    match_criteria=[MatchCriteria.QUANTITY_PRICE_TIME],
                    confidence_score=best_score,
                    quantity_difference=abs(best_candidate.quantity - exit_quantity),
                    price_difference=None,
                    time_difference=None,
                    aggregated_from_multi_fill=False,
                    warnings=[f"Reconciliation-driven match: {', '.join(factors)}"],
                    metadata={
                        "reconciliation_driven": True,
                        "reconciliation_score": best_score,
                        "reconciliation_factors": factors,
                        "candidate_source": best_candidate.source
                    }
                )
        
        return self._create_no_match_result(match_id, external_exit, "No reconciliation-driven matches found")

    def _create_no_match_result(
        self,
        match_id: str,
        external_exit: Dict[str, Any],
        reason: str
    ) -> ExitContextMatch:
        """Create a no-match result."""
        return ExitContextMatch(
            match_id=match_id,
            external_exit=external_exit,
            matched_trades=[],
            match_quality=MatchQuality.NO_MATCH,
            match_criteria=[],
            confidence_score=0.0,
            quantity_difference=Decimal('0'),
            price_difference=None,
            time_difference=None,
            aggregated_from_multi_fill=False,
            warnings=[f"No match found: {reason}"],
            metadata={"no_match_reason": reason}
        )

    def _create_error_result(
        self,
        match_id: str,
        external_exit: Dict[str, Any],
        error: str
    ) -> ExitContextMatch:
        """Create an error result."""
        return ExitContextMatch(
            match_id=match_id,
            external_exit=external_exit,
            matched_trades=[],
            match_quality=MatchQuality.NO_MATCH,
            match_criteria=[],
            confidence_score=0.0,
            quantity_difference=Decimal('0'),
            price_difference=None,
            time_difference=None,
            aggregated_from_multi_fill=False,
            warnings=[f"Matching failed: {error}"],
            metadata={"error": error, "matching_failed": True}
        )


# Helper function for external use
async def match_exit_context(
    db: AsyncSession,
    external_exit: Dict[str, Any],
    trading_account_id: str,
    symbol: str,
    config: Optional[ExitContextConfig] = None
) -> ExitContextMatch:
    """
    Convenience function for exit context matching.

    Args:
        db: Database session
        external_exit: External exit data
        trading_account_id: Trading account ID
        symbol: Trading symbol
        config: Optional matching configuration

    Returns:
        Exit context match result
    """
    matcher = ExitContextMatcher(db, config)
    return await matcher.match_exit_context(external_exit, trading_account_id, symbol)