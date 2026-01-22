"""
External Order Tagging Validation Service

Validates integrity of external order tagging to ensure all external orders
are properly tagged to strategies and portfolios without orphans or conflicts.

Key Features:
- Validation of external order tagging integrity
- Detection of orphaned external orders (missing strategy/portfolio tags)
- Detection of tagging conflicts (inconsistent strategy-portfolio mappings)
- Auto-fixing capabilities for common tagging issues
- Comprehensive reporting for audit and compliance
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from dataclasses import dataclass
from enum import Enum
from uuid import uuid4
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .default_strategy_service import DefaultStrategyService
from .default_portfolio_service import DefaultPortfolioService

logger = logging.getLogger(__name__)


class TaggingIssueType(str, Enum):
    """Types of tagging issues."""
    ORPHAN_ORDER = "orphan_order"                    # Order missing strategy or portfolio
    ORPHAN_POSITION = "orphan_position"              # Position missing strategy or portfolio
    ORPHAN_TRADE = "orphan_trade"                    # Trade missing strategy or portfolio
    STRATEGY_PORTFOLIO_MISMATCH = "strategy_portfolio_mismatch"  # Strategy and portfolio don't match
    DUPLICATE_TAGGING = "duplicate_tagging"          # Multiple strategies for same external item
    INVALID_STRATEGY_LINK = "invalid_strategy_link"  # Strategy ID doesn't exist or invalid
    INVALID_PORTFOLIO_LINK = "invalid_portfolio_link" # Portfolio ID doesn't exist or invalid


class ValidationSeverity(str, Enum):
    """Severity levels for validation issues."""
    CRITICAL = "critical"     # Breaks attribution/reconciliation
    HIGH = "high"            # Should be fixed soon
    MEDIUM = "medium"        # Should be fixed eventually
    LOW = "low"              # Minor issue, can be ignored


@dataclass
class TaggingIssue:
    """Represents a tagging validation issue."""
    issue_id: str
    issue_type: TaggingIssueType
    severity: ValidationSeverity
    trading_account_id: str
    entity_type: str  # 'order', 'position', 'trade'
    entity_id: int
    symbol: str
    strategy_id: Optional[int]
    portfolio_id: Optional[int]
    execution_id: Optional[str]
    description: str
    metadata: Dict[str, Any]
    detected_at: datetime
    auto_fixable: bool


@dataclass
class ValidationReport:
    """External order tagging validation report."""
    validation_id: str
    trading_account_id: Optional[str]
    total_external_items: int
    total_issues: int
    issues_by_type: Dict[str, int]
    issues_by_severity: Dict[str, int]
    auto_fixable_count: int
    critical_issues: List[TaggingIssue]
    validation_timestamp: datetime
    coverage_percentage: float
    recommendations: List[str]


@dataclass
class FixResult:
    """Result of auto-fixing tagging issues."""
    fix_session_id: str
    issues_fixed: int
    issues_failed: int
    entities_updated: int
    errors: List[str]
    warnings: List[str]
    audit_trail: List[Dict[str, Any]]


class ExternalOrderTaggingValidationService:
    """
    Service for validating external order tagging integrity.

    Ensures all external orders, positions, and trades are properly tagged
    with consistent strategy and portfolio mappings.
    """

    def __init__(self, db: AsyncSession):
        """
        Initialize the tagging validation service.

        Args:
            db: Async database session
        """
        self.db = db
        self.default_strategy_service = DefaultStrategyService(db)
        self.default_portfolio_service = DefaultPortfolioService(db)

    async def validate_tagging_integrity(
        self,
        trading_account_id: Optional[str] = None,
        symbol: Optional[str] = None,
        include_auto_fix_suggestions: bool = True
    ) -> ValidationReport:
        """
        Validate external order tagging integrity.

        Args:
            trading_account_id: Optional - validate specific account only
            symbol: Optional - validate specific symbol only
            include_auto_fix_suggestions: Whether to suggest auto-fixes

        Returns:
            Comprehensive validation report

        Raises:
            Exception: If validation fails
        """
        validation_id = str(uuid4())
        validation_start = datetime.now(timezone.utc)
        
        logger.info(
            f"[{validation_id}] Starting external order tagging validation "
            f"(account={trading_account_id}, symbol={symbol})"
        )

        try:
            # Step 1: Collect all external items
            external_orders = await self._get_external_orders(trading_account_id, symbol)
            external_positions = await self._get_external_positions(trading_account_id, symbol)
            external_trades = await self._get_external_trades(trading_account_id, symbol)

            total_external_items = len(external_orders) + len(external_positions) + len(external_trades)
            
            # Step 2: Validate each type
            all_issues = []
            
            # Validate orders
            order_issues = await self._validate_orders_tagging(external_orders)
            all_issues.extend(order_issues)
            
            # Validate positions
            position_issues = await self._validate_positions_tagging(external_positions)
            all_issues.extend(position_issues)
            
            # Validate trades
            trade_issues = await self._validate_trades_tagging(external_trades)
            all_issues.extend(trade_issues)

            # Step 3: Analyze cross-entity consistency
            consistency_issues = await self._validate_cross_entity_consistency(
                external_orders, external_positions, external_trades
            )
            all_issues.extend(consistency_issues)

            # Step 4: Generate statistics
            issues_by_type = {}
            issues_by_severity = {}
            auto_fixable_count = 0
            critical_issues = []

            for issue in all_issues:
                # Count by type
                if issue.issue_type.value not in issues_by_type:
                    issues_by_type[issue.issue_type.value] = 0
                issues_by_type[issue.issue_type.value] += 1

                # Count by severity
                if issue.severity.value not in issues_by_severity:
                    issues_by_severity[issue.severity.value] = 0
                issues_by_severity[issue.severity.value] += 1

                # Count auto-fixable
                if issue.auto_fixable:
                    auto_fixable_count += 1

                # Collect critical issues
                if issue.severity == ValidationSeverity.CRITICAL:
                    critical_issues.append(issue)

            # Step 5: Calculate coverage
            coverage_percentage = (
                (total_external_items - len(all_issues)) / max(total_external_items, 1)
            ) * 100

            # Step 6: Generate recommendations
            recommendations = self._generate_recommendations(
                all_issues, coverage_percentage, include_auto_fix_suggestions
            )

            # Step 7: Store validation results
            await self._store_validation_results(validation_id, all_issues)

            report = ValidationReport(
                validation_id=validation_id,
                trading_account_id=trading_account_id,
                total_external_items=total_external_items,
                total_issues=len(all_issues),
                issues_by_type=issues_by_type,
                issues_by_severity=issues_by_severity,
                auto_fixable_count=auto_fixable_count,
                critical_issues=critical_issues,
                validation_timestamp=validation_start,
                coverage_percentage=coverage_percentage,
                recommendations=recommendations
            )

            logger.info(
                f"[{validation_id}] Validation complete: "
                f"{len(all_issues)} issues found in {total_external_items} items "
                f"({coverage_percentage:.1f}% coverage)"
            )

            return report

        except Exception as e:
            logger.error(f"[{validation_id}] Validation failed: {e}", exc_info=True)
            raise

    async def auto_fix_tagging_issues(
        self,
        validation_report: ValidationReport,
        fix_orphans: bool = True,
        fix_mismatches: bool = False,
        dry_run: bool = False
    ) -> FixResult:
        """
        Automatically fix tagging issues where possible.

        Args:
            validation_report: Validation report with issues to fix
            fix_orphans: Whether to fix orphan issues
            fix_mismatches: Whether to fix strategy-portfolio mismatches
            dry_run: If True, don't make actual changes

        Returns:
            Fix result summary

        Raises:
            Exception: If fixing fails
        """
        fix_session_id = str(uuid4())
        
        logger.info(
            f"[{fix_session_id}] Starting auto-fix for {validation_report.auto_fixable_count} issues "
            f"(dry_run={dry_run})"
        )

        try:
            issues_fixed = 0
            issues_failed = 0
            entities_updated = 0
            errors = []
            warnings = []
            audit_trail = []

            # Get fixable issues from the critical issues list and other auto-fixable issues
            all_fixable_issues = [
                issue for issue in validation_report.critical_issues 
                if issue.auto_fixable
            ]

            for issue in all_fixable_issues:
                try:
                    if issue.issue_type in [TaggingIssueType.ORPHAN_ORDER, TaggingIssueType.ORPHAN_POSITION, TaggingIssueType.ORPHAN_TRADE] and fix_orphans:
                        success = await self._fix_orphan_issue(issue, dry_run, fix_session_id)
                    elif issue.issue_type == TaggingIssueType.STRATEGY_PORTFOLIO_MISMATCH and fix_mismatches:
                        success = await self._fix_mismatch_issue(issue, dry_run, fix_session_id)
                    else:
                        continue  # Skip this issue type

                    if success:
                        issues_fixed += 1
                        entities_updated += 1
                        audit_trail.append({
                            "issue_id": issue.issue_id,
                            "action": "fixed",
                            "entity_type": issue.entity_type,
                            "entity_id": issue.entity_id
                        })
                    else:
                        issues_failed += 1

                except Exception as e:
                    issues_failed += 1
                    error_msg = f"Failed to fix {issue.issue_type} for {issue.entity_type}:{issue.entity_id}: {str(e)}"
                    errors.append(error_msg)
                    logger.error(f"[{fix_session_id}] {error_msg}")

            if not dry_run and issues_fixed > 0:
                await self.db.commit()

            result = FixResult(
                fix_session_id=fix_session_id,
                issues_fixed=issues_fixed,
                issues_failed=issues_failed,
                entities_updated=entities_updated,
                errors=errors,
                warnings=warnings,
                audit_trail=audit_trail
            )

            logger.info(
                f"[{fix_session_id}] Auto-fix complete: "
                f"{issues_fixed} fixed, {issues_failed} failed"
            )

            return result

        except Exception as e:
            logger.error(f"[{fix_session_id}] Auto-fix failed: {e}", exc_info=True)
            if not dry_run:
                await self.db.rollback()
            raise

    async def _get_external_orders(
        self,
        trading_account_id: Optional[str],
        symbol: Optional[str]
    ) -> List[Dict[str, Any]]:
        """Get external orders for validation."""
        where_clauses = ["source = 'external' OR source IS NULL"]
        params = {}

        if trading_account_id:
            where_clauses.append("trading_account_id = :trading_account_id")
            params["trading_account_id"] = trading_account_id

        if symbol:
            where_clauses.append("symbol = :symbol")
            params["symbol"] = symbol

        where_clause = " AND ".join(where_clauses)

        result = await self.db.execute(
            text(f"""
                SELECT 
                    id,
                    trading_account_id,
                    symbol,
                    strategy_id,
                    portfolio_id,
                    execution_id,
                    source,
                    created_at
                FROM order_service.orders
                WHERE {where_clause}
                ORDER BY created_at DESC
                LIMIT 1000
            """),
            params
        )

        return [
            {
                "id": row[0],
                "trading_account_id": row[1],
                "symbol": row[2],
                "strategy_id": row[3],
                "portfolio_id": row[4],
                "execution_id": row[5],
                "source": row[6],
                "created_at": row[7]
            }
            for row in result.fetchall()
        ]

    async def _get_external_positions(
        self,
        trading_account_id: Optional[str],
        symbol: Optional[str]
    ) -> List[Dict[str, Any]]:
        """Get external positions for validation."""
        where_clauses = ["source = 'external' OR source IS NULL"]
        params = {}

        if trading_account_id:
            where_clauses.append("trading_account_id = :trading_account_id")
            params["trading_account_id"] = trading_account_id

        if symbol:
            where_clauses.append("symbol = :symbol")
            params["symbol"] = symbol

        where_clause = " AND ".join(where_clauses)

        result = await self.db.execute(
            text(f"""
                SELECT 
                    id,
                    trading_account_id,
                    symbol,
                    strategy_id,
                    portfolio_id,
                    execution_id,
                    source,
                    created_at
                FROM order_service.positions
                WHERE {where_clause}
                ORDER BY created_at DESC
                LIMIT 1000
            """),
            params
        )

        return [
            {
                "id": row[0],
                "trading_account_id": row[1],
                "symbol": row[2],
                "strategy_id": row[3],
                "portfolio_id": row[4],
                "execution_id": row[5],
                "source": row[6],
                "created_at": row[7]
            }
            for row in result.fetchall()
        ]

    async def _get_external_trades(
        self,
        trading_account_id: Optional[str],
        symbol: Optional[str]
    ) -> List[Dict[str, Any]]:
        """Get external trades for validation."""
        where_clauses = ["source = 'external' OR source IS NULL"]
        params = {}

        if trading_account_id:
            where_clauses.append("trading_account_id = :trading_account_id")
            params["trading_account_id"] = trading_account_id

        if symbol:
            where_clauses.append("symbol = :symbol")
            params["symbol"] = symbol

        where_clause = " AND ".join(where_clauses)

        result = await self.db.execute(
            text(f"""
                SELECT 
                    id,
                    trading_account_id,
                    symbol,
                    strategy_id,
                    portfolio_id,
                    execution_id,
                    source,
                    created_at
                FROM order_service.trades
                WHERE {where_clause}
                ORDER BY created_at DESC
                LIMIT 1000
            """),
            params
        )

        return [
            {
                "id": row[0],
                "trading_account_id": row[1],
                "symbol": row[2],
                "strategy_id": row[3],
                "portfolio_id": row[4],
                "execution_id": row[5],
                "source": row[6],
                "created_at": row[7]
            }
            for row in result.fetchall()
        ]

    async def _validate_orders_tagging(
        self,
        orders: List[Dict[str, Any]]
    ) -> List[TaggingIssue]:
        """Validate order tagging."""
        issues = []

        for order in orders:
            # Check for orphan orders (missing strategy or portfolio)
            if not order["strategy_id"]:
                issue = TaggingIssue(
                    issue_id=str(uuid4()),
                    issue_type=TaggingIssueType.ORPHAN_ORDER,
                    severity=ValidationSeverity.CRITICAL,
                    trading_account_id=order["trading_account_id"],
                    entity_type="order",
                    entity_id=order["id"],
                    symbol=order["symbol"],
                    strategy_id=order["strategy_id"],
                    portfolio_id=order["portfolio_id"],
                    execution_id=order["execution_id"],
                    description=f"External order {order['id']} missing strategy_id",
                    metadata={"order_data": order},
                    detected_at=datetime.now(timezone.utc),
                    auto_fixable=True
                )
                issues.append(issue)

            elif not order["portfolio_id"]:
                issue = TaggingIssue(
                    issue_id=str(uuid4()),
                    issue_type=TaggingIssueType.ORPHAN_ORDER,
                    severity=ValidationSeverity.HIGH,
                    trading_account_id=order["trading_account_id"],
                    entity_type="order",
                    entity_id=order["id"],
                    symbol=order["symbol"],
                    strategy_id=order["strategy_id"],
                    portfolio_id=order["portfolio_id"],
                    execution_id=order["execution_id"],
                    description=f"External order {order['id']} missing portfolio_id",
                    metadata={"order_data": order},
                    detected_at=datetime.now(timezone.utc),
                    auto_fixable=True
                )
                issues.append(issue)

        return issues

    async def _validate_positions_tagging(
        self,
        positions: List[Dict[str, Any]]
    ) -> List[TaggingIssue]:
        """Validate position tagging."""
        issues = []

        for position in positions:
            # Check for orphan positions (missing strategy or portfolio)
            if not position["strategy_id"]:
                issue = TaggingIssue(
                    issue_id=str(uuid4()),
                    issue_type=TaggingIssueType.ORPHAN_POSITION,
                    severity=ValidationSeverity.CRITICAL,
                    trading_account_id=position["trading_account_id"],
                    entity_type="position",
                    entity_id=position["id"],
                    symbol=position["symbol"],
                    strategy_id=position["strategy_id"],
                    portfolio_id=position["portfolio_id"],
                    execution_id=position["execution_id"],
                    description=f"External position {position['id']} missing strategy_id",
                    metadata={"position_data": position},
                    detected_at=datetime.now(timezone.utc),
                    auto_fixable=True
                )
                issues.append(issue)

            elif not position["portfolio_id"]:
                issue = TaggingIssue(
                    issue_id=str(uuid4()),
                    issue_type=TaggingIssueType.ORPHAN_POSITION,
                    severity=ValidationSeverity.HIGH,
                    trading_account_id=position["trading_account_id"],
                    entity_type="position",
                    entity_id=position["id"],
                    symbol=position["symbol"],
                    strategy_id=position["strategy_id"],
                    portfolio_id=position["portfolio_id"],
                    execution_id=position["execution_id"],
                    description=f"External position {position['id']} missing portfolio_id",
                    metadata={"position_data": position},
                    detected_at=datetime.now(timezone.utc),
                    auto_fixable=True
                )
                issues.append(issue)

        return issues

    async def _validate_trades_tagging(
        self,
        trades: List[Dict[str, Any]]
    ) -> List[TaggingIssue]:
        """Validate trade tagging."""
        issues = []

        for trade in trades:
            # Check for orphan trades (missing strategy or portfolio)
            if not trade["strategy_id"]:
                issue = TaggingIssue(
                    issue_id=str(uuid4()),
                    issue_type=TaggingIssueType.ORPHAN_TRADE,
                    severity=ValidationSeverity.HIGH,
                    trading_account_id=trade["trading_account_id"],
                    entity_type="trade",
                    entity_id=trade["id"],
                    symbol=trade["symbol"],
                    strategy_id=trade["strategy_id"],
                    portfolio_id=trade["portfolio_id"],
                    execution_id=trade["execution_id"],
                    description=f"External trade {trade['id']} missing strategy_id",
                    metadata={"trade_data": trade},
                    detected_at=datetime.now(timezone.utc),
                    auto_fixable=True
                )
                issues.append(issue)

            elif not trade["portfolio_id"]:
                issue = TaggingIssue(
                    issue_id=str(uuid4()),
                    issue_type=TaggingIssueType.ORPHAN_TRADE,
                    severity=ValidationSeverity.MEDIUM,
                    trading_account_id=trade["trading_account_id"],
                    entity_type="trade",
                    entity_id=trade["id"],
                    symbol=trade["symbol"],
                    strategy_id=trade["strategy_id"],
                    portfolio_id=trade["portfolio_id"],
                    execution_id=trade["execution_id"],
                    description=f"External trade {trade['id']} missing portfolio_id",
                    metadata={"trade_data": trade},
                    detected_at=datetime.now(timezone.utc),
                    auto_fixable=True
                )
                issues.append(issue)

        return issues

    async def _validate_cross_entity_consistency(
        self,
        orders: List[Dict[str, Any]],
        positions: List[Dict[str, Any]],
        trades: List[Dict[str, Any]]
    ) -> List[TaggingIssue]:
        """Validate consistency across related entities."""
        # For now, return empty list
        # In a full implementation, this would check:
        # - Related orders/trades have same strategy/portfolio
        # - Positions and their constituent trades are consistent
        # - Strategy-portfolio mappings are valid
        return []

    async def _fix_orphan_issue(
        self,
        issue: TaggingIssue,
        dry_run: bool,
        fix_session_id: str
    ) -> bool:
        """Fix orphan tagging issue."""
        try:
            if issue.entity_type == "order":
                return await self._fix_orphan_order(issue, dry_run, fix_session_id)
            elif issue.entity_type == "position":
                return await self._fix_orphan_position(issue, dry_run, fix_session_id)
            elif issue.entity_type == "trade":
                return await self._fix_orphan_trade(issue, dry_run, fix_session_id)
            else:
                logger.warning(f"Unknown entity type for fix: {issue.entity_type}")
                return False

        except Exception as e:
            logger.error(f"Failed to fix orphan issue {issue.issue_id}: {e}")
            return False

    async def _fix_orphan_order(
        self,
        issue: TaggingIssue,
        dry_run: bool,
        fix_session_id: str
    ) -> bool:
        """Fix orphan order by tagging to default strategy/portfolio."""
        if dry_run:
            logger.info(f"[DRY-RUN] Would fix orphan order {issue.entity_id}")
            return True

        # Tag order using default strategy service
        try:
            strategy_id, execution_id = await self.default_strategy_service.tag_orphan_order(
                issue.entity_id,
                issue.trading_account_id
            )

            # Tag order using default portfolio service
            portfolio_id, _, _ = await self.default_portfolio_service.tag_orphan_order_with_portfolio(
                issue.entity_id,
                issue.trading_account_id
            )

            logger.info(
                f"[{fix_session_id}] Fixed orphan order {issue.entity_id}: "
                f"strategy={strategy_id}, portfolio={portfolio_id}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to fix orphan order {issue.entity_id}: {e}")
            return False

    async def _fix_orphan_position(
        self,
        issue: TaggingIssue,
        dry_run: bool,
        fix_session_id: str
    ) -> bool:
        """Fix orphan position by tagging to default strategy/portfolio."""
        if dry_run:
            logger.info(f"[DRY-RUN] Would fix orphan position {issue.entity_id}")
            return True

        # Tag position using default strategy service
        try:
            strategy_id, execution_id = await self.default_strategy_service.tag_orphan_position(
                issue.entity_id,
                issue.trading_account_id
            )

            # Tag position using default portfolio service
            portfolio_id, _, _ = await self.default_portfolio_service.tag_orphan_position_with_portfolio(
                issue.entity_id,
                issue.trading_account_id
            )

            logger.info(
                f"[{fix_session_id}] Fixed orphan position {issue.entity_id}: "
                f"strategy={strategy_id}, portfolio={portfolio_id}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to fix orphan position {issue.entity_id}: {e}")
            return False

    async def _fix_orphan_trade(
        self,
        issue: TaggingIssue,
        dry_run: bool,
        fix_session_id: str
    ) -> bool:
        """Fix orphan trade by tagging to default strategy/portfolio."""
        if dry_run:
            logger.info(f"[DRY-RUN] Would fix orphan trade {issue.entity_id}")
            return True

        # For trades, we need to implement similar logic as orders/positions
        # but for the trades table
        try:
            # Get default strategy and portfolio
            strategy_id, execution_id = await self.default_strategy_service.get_or_create_default_strategy(
                issue.trading_account_id
            )
            portfolio_id, _ = await self.default_portfolio_service.get_or_create_default_portfolio(
                issue.trading_account_id
            )

            # Update the trade
            await self.db.execute(
                text("""
                    UPDATE order_service.trades
                    SET strategy_id = :strategy_id,
                        portfolio_id = :portfolio_id,
                        execution_id = :execution_id::uuid,
                        source = 'external',
                        updated_at = NOW()
                    WHERE id = :trade_id
                      AND (strategy_id IS NULL OR portfolio_id IS NULL)
                """),
                {
                    "strategy_id": strategy_id,
                    "portfolio_id": portfolio_id,
                    "execution_id": execution_id,
                    "trade_id": issue.entity_id
                }
            )

            logger.info(
                f"[{fix_session_id}] Fixed orphan trade {issue.entity_id}: "
                f"strategy={strategy_id}, portfolio={portfolio_id}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to fix orphan trade {issue.entity_id}: {e}")
            return False

    async def _fix_mismatch_issue(
        self,
        issue: TaggingIssue,
        dry_run: bool,
        fix_session_id: str
    ) -> bool:
        """Fix strategy-portfolio mismatch issue."""
        # For now, return False (not implemented)
        # In a full implementation, this would resolve strategy-portfolio inconsistencies
        return False

    def _generate_recommendations(
        self,
        issues: List[TaggingIssue],
        coverage_percentage: float,
        include_auto_fix: bool
    ) -> List[str]:
        """Generate recommendations based on validation results."""
        recommendations = []

        # Coverage recommendations
        if coverage_percentage < 95:
            recommendations.append(
                f"Low tagging coverage ({coverage_percentage:.1f}%). "
                f"Consider running auto-fix to improve coverage."
            )

        # Critical issue recommendations
        critical_count = len([i for i in issues if i.severity == ValidationSeverity.CRITICAL])
        if critical_count > 0:
            recommendations.append(
                f"{critical_count} critical tagging issues found. "
                f"These should be fixed immediately to prevent attribution errors."
            )

        # Auto-fix recommendations
        if include_auto_fix:
            auto_fixable_count = len([i for i in issues if i.auto_fixable])
            if auto_fixable_count > 0:
                recommendations.append(
                    f"{auto_fixable_count} issues can be auto-fixed. "
                    f"Run auto_fix_tagging_issues() to resolve them."
                )

        # Pattern-based recommendations
        orphan_count = len([
            i for i in issues 
            if i.issue_type in [TaggingIssueType.ORPHAN_ORDER, TaggingIssueType.ORPHAN_POSITION, TaggingIssueType.ORPHAN_TRADE]
        ])
        if orphan_count > 10:
            recommendations.append(
                f"High number of orphan items ({orphan_count}). "
                f"Consider reviewing external order import process."
            )

        return recommendations

    async def _store_validation_results(
        self,
        validation_id: str,
        issues: List[TaggingIssue]
    ) -> None:
        """Store validation results for audit."""
        # Store validation summary
        await self.db.execute(
            text("""
                INSERT INTO order_service.tagging_validation_runs (
                    validation_id,
                    total_issues,
                    critical_issues,
                    auto_fixable_issues,
                    validation_timestamp,
                    metadata
                ) VALUES (
                    :validation_id,
                    :total_issues,
                    :critical_issues,
                    :auto_fixable_issues,
                    :validation_timestamp,
                    :metadata::jsonb
                )
            """),
            {
                "validation_id": validation_id,
                "total_issues": len(issues),
                "critical_issues": len([i for i in issues if i.severity == ValidationSeverity.CRITICAL]),
                "auto_fixable_issues": len([i for i in issues if i.auto_fixable]),
                "validation_timestamp": datetime.now(timezone.utc),
                "metadata": {"issues_summary": [
                    {
                        "issue_id": issue.issue_id,
                        "issue_type": issue.issue_type.value,
                        "severity": issue.severity.value,
                        "entity_type": issue.entity_type,
                        "entity_id": issue.entity_id,
                        "auto_fixable": issue.auto_fixable
                    }
                    for issue in issues
                ]}
            }
        )

        await self.db.commit()


# Helper function for use outside of class context
async def validate_external_order_tagging(
    db: AsyncSession,
    trading_account_id: Optional[str] = None,
    symbol: Optional[str] = None
) -> ValidationReport:
    """
    Validate external order tagging integrity.

    Args:
        db: Database session
        trading_account_id: Optional account to validate
        symbol: Optional symbol to validate

    Returns:
        Validation report
    """
    service = ExternalOrderTaggingValidationService(db)
    return await service.validate_tagging_integrity(trading_account_id, symbol)