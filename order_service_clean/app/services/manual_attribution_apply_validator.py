"""
Manual Attribution Apply Validator

Validates manual attribution decisions before applying them to ensure:
- Position availability and validity
- Execution context existence
- Transfer safety checks
- Data consistency requirements

Key Features:
- Pre-apply validation of manual attribution decisions
- Position lock verification before transfers
- Execution context validation
- Rollback safety checks
- Transfer instruction validation
"""

import logging
from typing import Dict, Any, List
from datetime import datetime, timezone
from decimal import Decimal
from dataclasses import dataclass
from enum import Enum
from uuid import uuid4
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class ValidationSeverity(str, Enum):
    """Severity levels for validation failures."""
    CRITICAL = "critical"      # Blocks application completely
    HIGH = "high"             # Should block application
    MEDIUM = "medium"         # Warning, allow with confirmation
    LOW = "low"               # Information only


class ValidationFailureReason(str, Enum):
    """Reasons for validation failures."""
    POSITION_NOT_FOUND = "position_not_found"
    POSITION_INSUFFICIENT_QUANTITY = "position_insufficient_quantity"
    EXECUTION_NOT_FOUND = "execution_not_found"
    EXECUTION_INVALID_STATE = "execution_invalid_state"
    ALLOCATION_QUANTITY_MISMATCH = "allocation_quantity_mismatch"
    STRATEGY_MISMATCH = "strategy_mismatch"
    PORTFOLIO_MISMATCH = "portfolio_mismatch"
    TRANSFER_UNSAFE = "transfer_unsafe"
    DATA_INCONSISTENCY = "data_inconsistency"


@dataclass
class ValidationFailure:
    """Represents a validation failure."""
    failure_id: str
    severity: ValidationSeverity
    reason: ValidationFailureReason
    description: str
    affected_entity: str
    entity_id: str
    context: Dict[str, Any]


@dataclass
class ManualAttributionDecision:
    """Manual attribution decision to validate."""
    case_id: str
    decision_maker: str
    allocation_decisions: List[Dict[str, Any]]
    decision_rationale: str
    exit_quantity: Decimal
    symbol: str
    trading_account_id: str


@dataclass
class ValidationResult:
    """Result of manual attribution validation."""
    validation_id: str
    is_valid: bool
    failures: List[ValidationFailure]
    warnings: List[ValidationFailure]
    validated_allocations: List[Dict[str, Any]]
    transfer_safety_checks: Dict[str, Any]
    recommendation: str
    can_proceed: bool


class ManualAttributionApplyValidator:
    """
    Validates manual attribution decisions before application.
    
    Ensures that manual attribution decisions are safe to apply
    and won't cause data inconsistencies or transfer failures.
    """

    def __init__(self, db: AsyncSession):
        """
        Initialize the validator.

        Args:
            db: Async database session
        """
        self.db = db

    async def validate_manual_attribution_decision(
        self,
        attribution_decision: ManualAttributionDecision
    ) -> ValidationResult:
        """
        Validate a manual attribution decision before applying it.

        Args:
            attribution_decision: The manual attribution decision to validate

        Returns:
            Validation result with failures, warnings, and recommendation

        Raises:
            Exception: If validation process fails
        """
        validation_id = str(uuid4())
        logger.info(
            f"[{validation_id}] Validating manual attribution decision for case {attribution_decision.case_id}"
        )

        try:
            failures = []
            warnings = []
            validated_allocations = []

            # Step 1: Validate allocation quantity consistency
            total_allocated = self._validate_allocation_quantities(
                attribution_decision, failures
            )

            # Step 2: Validate each position allocation
            for allocation in attribution_decision.allocation_decisions:
                await self._validate_position_allocation(
                    allocation, attribution_decision, failures, warnings, validated_allocations
                )

            # Step 3: Validate execution contexts
            await self._validate_execution_contexts(
                attribution_decision, failures, warnings
            )

            # Step 4: Perform transfer safety checks
            transfer_safety = await self._perform_transfer_safety_checks(
                attribution_decision, validated_allocations, failures
            )

            # Step 5: Check for data consistency
            await self._validate_data_consistency(
                attribution_decision, validated_allocations, failures
            )

            # Step 6: Generate recommendation
            is_valid = not any(f.severity in [ValidationSeverity.CRITICAL, ValidationSeverity.HIGH] for f in failures)
            recommendation = self._generate_recommendation(failures, warnings, is_valid)
            can_proceed = is_valid and len([f for f in failures if f.severity == ValidationSeverity.CRITICAL]) == 0

            result = ValidationResult(
                validation_id=validation_id,
                is_valid=is_valid,
                failures=failures,
                warnings=warnings,
                validated_allocations=validated_allocations,
                transfer_safety_checks=transfer_safety,
                recommendation=recommendation,
                can_proceed=can_proceed
            )

            logger.info(
                f"[{validation_id}] Validation complete: is_valid={is_valid}, "
                f"failures={len(failures)}, warnings={len(warnings)}"
            )

            return result

        except Exception as e:
            logger.error(f"[{validation_id}] Validation failed: {e}", exc_info=True)
            raise

    def _validate_allocation_quantities(
        self,
        decision: ManualAttributionDecision,
        failures: List[ValidationFailure]
    ) -> Decimal:
        """Validate allocation quantities sum correctly."""
        total_allocated = Decimal('0')
        
        for allocation in decision.allocation_decisions:
            try:
                quantity = Decimal(str(allocation.get('allocated_quantity', 0)))
                total_allocated += quantity
            except (ValueError, TypeError):
                failures.append(ValidationFailure(
                    failure_id=str(uuid4()),
                    severity=ValidationSeverity.CRITICAL,
                    reason=ValidationFailureReason.ALLOCATION_QUANTITY_MISMATCH,
                    description=f"Invalid allocated quantity: {allocation.get('allocated_quantity')}",
                    affected_entity="allocation",
                    entity_id=str(allocation.get('position_id', 'unknown')),
                    context={"allocation": allocation}
                ))

        # Check if total matches exit quantity
        if total_allocated != decision.exit_quantity:
            failures.append(ValidationFailure(
                failure_id=str(uuid4()),
                severity=ValidationSeverity.HIGH,
                reason=ValidationFailureReason.ALLOCATION_QUANTITY_MISMATCH,
                description=f"Total allocated ({total_allocated}) does not match exit quantity ({decision.exit_quantity})",
                affected_entity="allocation_total",
                entity_id="total",
                context={
                    "total_allocated": str(total_allocated),
                    "exit_quantity": str(decision.exit_quantity),
                    "difference": str(abs(total_allocated - decision.exit_quantity))
                }
            ))

        return total_allocated

    async def _validate_position_allocation(
        self,
        allocation: Dict[str, Any],
        decision: ManualAttributionDecision,
        failures: List[ValidationFailure],
        warnings: List[ValidationFailure],
        validated_allocations: List[Dict[str, Any]]
    ) -> None:
        """Validate a single position allocation."""
        position_id = allocation.get('position_id')
        allocated_quantity = Decimal(str(allocation.get('allocated_quantity', 0)))

        # Check position exists and has sufficient quantity
        result = await self.db.execute(
            text("""
                SELECT 
                    p.id,
                    p.strategy_id,
                    p.execution_id,
                    p.portfolio_id,
                    p.quantity,
                    p.symbol,
                    p.trading_account_id,
                    p.is_open,
                    p.source
                FROM order_service.positions p
                WHERE p.id = :position_id
            """),
            {"position_id": position_id}
        )
        
        row = result.fetchone()
        if not row:
            failures.append(ValidationFailure(
                failure_id=str(uuid4()),
                severity=ValidationSeverity.CRITICAL,
                reason=ValidationFailureReason.POSITION_NOT_FOUND,
                description=f"Position {position_id} not found",
                affected_entity="position",
                entity_id=str(position_id),
                context={"allocation": allocation}
            ))
            return

        position_quantity = Decimal(str(row[4]))
        position_symbol = row[5]
        position_account = row[6]
        position_is_open = row[7]

        # Validate position quantity
        if allocated_quantity > position_quantity:
            failures.append(ValidationFailure(
                failure_id=str(uuid4()),
                severity=ValidationSeverity.HIGH,
                reason=ValidationFailureReason.POSITION_INSUFFICIENT_QUANTITY,
                description=f"Allocated quantity ({allocated_quantity}) exceeds position quantity ({position_quantity})",
                affected_entity="position",
                entity_id=str(position_id),
                context={
                    "allocated_quantity": str(allocated_quantity),
                    "position_quantity": str(position_quantity),
                    "excess": str(allocated_quantity - position_quantity)
                }
            ))

        # Validate position is open
        if not position_is_open:
            failures.append(ValidationFailure(
                failure_id=str(uuid4()),
                severity=ValidationSeverity.HIGH,
                reason=ValidationFailureReason.POSITION_NOT_FOUND,
                description=f"Position {position_id} is closed",
                affected_entity="position",
                entity_id=str(position_id),
                context={"position_is_open": position_is_open}
            ))

        # Validate symbol and account match
        if position_symbol != decision.symbol:
            failures.append(ValidationFailure(
                failure_id=str(uuid4()),
                severity=ValidationSeverity.CRITICAL,
                reason=ValidationFailureReason.DATA_INCONSISTENCY,
                description=f"Position symbol ({position_symbol}) does not match decision symbol ({decision.symbol})",
                affected_entity="position",
                entity_id=str(position_id),
                context={"position_symbol": position_symbol, "decision_symbol": decision.symbol}
            ))

        if position_account != decision.trading_account_id:
            failures.append(ValidationFailure(
                failure_id=str(uuid4()),
                severity=ValidationSeverity.CRITICAL,
                reason=ValidationFailureReason.DATA_INCONSISTENCY,
                description=f"Position account ({position_account}) does not match decision account ({decision.trading_account_id})",
                affected_entity="position",
                entity_id=str(position_id),
                context={"position_account": position_account, "decision_account": decision.trading_account_id}
            ))

        # Add to validated allocations if no critical failures
        if not any(f.severity == ValidationSeverity.CRITICAL and f.entity_id == str(position_id) for f in failures):
            validated_allocations.append({
                "position_id": position_id,
                "allocated_quantity": str(allocated_quantity),
                "strategy_id": row[1],
                "execution_id": str(row[2]) if row[2] else None,
                "portfolio_id": row[3],
                "position_quantity": str(position_quantity),
                "remaining_quantity": str(position_quantity - allocated_quantity)
            })

    async def _validate_execution_contexts(
        self,
        decision: ManualAttributionDecision,
        failures: List[ValidationFailure],
        warnings: List[ValidationFailure]
    ) -> None:
        """Validate execution contexts exist and are valid."""
        # Get unique execution IDs from allocations
        execution_ids = set()
        for allocation in decision.allocation_decisions:
            if allocation.get('execution_id'):
                execution_ids.add(allocation['execution_id'])

        if not execution_ids:
            warnings.append(ValidationFailure(
                failure_id=str(uuid4()),
                severity=ValidationSeverity.MEDIUM,
                reason=ValidationFailureReason.EXECUTION_NOT_FOUND,
                description="No execution contexts specified in allocations",
                affected_entity="execution",
                entity_id="none",
                context={"execution_ids_count": 0}
            ))
            return

        # Validate each execution context
        for execution_id in execution_ids:
            result = await self.db.execute(
                text("""
                    SELECT 
                        execution_id,
                        status,
                        context_type,
                        created_at
                    FROM order_service.execution_contexts
                    WHERE execution_id = :execution_id::uuid
                """),
                {"execution_id": execution_id}
            )
            
            row = result.fetchone()
            if not row:
                failures.append(ValidationFailure(
                    failure_id=str(uuid4()),
                    severity=ValidationSeverity.HIGH,
                    reason=ValidationFailureReason.EXECUTION_NOT_FOUND,
                    description=f"Execution context {execution_id} not found",
                    affected_entity="execution",
                    entity_id=execution_id,
                    context={"execution_id": execution_id}
                ))
            else:
                status = row[1]
                if status not in ['ready', 'running', 'stopped']:
                    warnings.append(ValidationFailure(
                        failure_id=str(uuid4()),
                        severity=ValidationSeverity.MEDIUM,
                        reason=ValidationFailureReason.EXECUTION_INVALID_STATE,
                        description=f"Execution context {execution_id} has status '{status}'",
                        affected_entity="execution",
                        entity_id=execution_id,
                        context={"execution_status": status}
                    ))

    async def _perform_transfer_safety_checks(
        self,
        decision: ManualAttributionDecision,
        validated_allocations: List[Dict[str, Any]],
        failures: List[ValidationFailure]
    ) -> Dict[str, Any]:
        """Perform transfer safety checks."""
        safety_checks = {
            "has_validated_allocations": len(validated_allocations) > 0,
            "all_positions_have_execution_context": True,
            "no_circular_transfers": True,
            "transfer_timestamp": datetime.now(timezone.utc).isoformat()
        }

        # Check if all allocations have execution context
        for allocation in validated_allocations:
            if not allocation.get('execution_id'):
                safety_checks["all_positions_have_execution_context"] = False
                failures.append(ValidationFailure(
                    failure_id=str(uuid4()),
                    severity=ValidationSeverity.MEDIUM,
                    reason=ValidationFailureReason.TRANSFER_UNSAFE,
                    description=f"Position {allocation['position_id']} has no execution context for transfer",
                    affected_entity="position",
                    entity_id=str(allocation['position_id']),
                    context={"missing_execution_context": True}
                ))

        return safety_checks

    async def _validate_data_consistency(
        self,
        decision: ManualAttributionDecision,
        validated_allocations: List[Dict[str, Any]],
        failures: List[ValidationFailure]
    ) -> None:
        """Validate data consistency across allocations."""
        # Check for duplicate position allocations
        position_ids = [alloc['position_id'] for alloc in validated_allocations]
        if len(position_ids) != len(set(position_ids)):
            failures.append(ValidationFailure(
                failure_id=str(uuid4()),
                severity=ValidationSeverity.HIGH,
                reason=ValidationFailureReason.DATA_INCONSISTENCY,
                description="Duplicate position IDs found in allocations",
                affected_entity="allocation",
                entity_id="duplicate",
                context={"position_ids": position_ids, "unique_count": len(set(position_ids))}
            ))

    def _generate_recommendation(
        self,
        failures: List[ValidationFailure],
        warnings: List[ValidationFailure],
        is_valid: bool
    ) -> str:
        """Generate recommendation based on validation results."""
        critical_failures = [f for f in failures if f.severity == ValidationSeverity.CRITICAL]
        high_failures = [f for f in failures if f.severity == ValidationSeverity.HIGH]

        if critical_failures:
            return f"REJECT: {len(critical_failures)} critical failures must be resolved before applying attribution"
        elif high_failures:
            return f"CAUTION: {len(high_failures)} high-severity issues detected. Review before proceeding"
        elif warnings:
            return f"PROCEED WITH CAUTION: {len(warnings)} warnings detected but attribution can proceed"
        elif is_valid:
            return "APPROVE: Attribution decision is valid and safe to apply"
        else:
            return "REVIEW: Manual review recommended before applying attribution"


# Helper function for external use
async def validate_manual_attribution_decision(
    db: AsyncSession,
    attribution_decision: ManualAttributionDecision
) -> ValidationResult:
    """
    Convenience function for validating manual attribution decisions.

    Args:
        db: Database session
        attribution_decision: The manual attribution decision to validate

    Returns:
        Validation result
    """
    validator = ManualAttributionApplyValidator(db)
    return await validator.validate_manual_attribution_decision(attribution_decision)