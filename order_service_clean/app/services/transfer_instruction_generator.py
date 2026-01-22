"""
Transfer Instruction Generator

Generates comprehensive transfer instructions with explicit types and priority rules.
Provides type-safe transfer instruction generation for reconciliation-driven transfers.

Key Features:
- Transfer instruction types (POSITION, EXECUTION, PORTFOLIO)
- Priority-based transfer ordering  
- Validation of transfer parameters
- Rollback instruction generation
- Batch transfer instruction generation
- Safety checks for transfer execution
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from decimal import Decimal
from dataclasses import dataclass
from enum import Enum
from uuid import uuid4
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class TransferInstructionType(str, Enum):
    """Types of transfer instructions."""
    POSITION_TRANSFER = "position_transfer"           # Transfer position between executions
    EXECUTION_HANDOFF = "execution_handoff"          # Handoff execution control
    PORTFOLIO_REALLOCATION = "portfolio_reallocation"  # Reallocate between portfolios
    ATTRIBUTION_CORRECTION = "attribution_correction"  # Correct attribution after reconciliation
    EMERGENCY_EXTRACTION = "emergency_extraction"     # Emergency position extraction


class TransferPriority(int, Enum):
    """Transfer priority levels (lower number = higher priority)."""
    CRITICAL = 10      # Emergency transfers, immediate execution
    HIGH = 20          # Important transfers, execute soon
    NORMAL = 50        # Standard transfers, normal queue
    LOW = 100          # Bulk transfers, execute when convenient
    BACKGROUND = 200   # Background cleanup, lowest priority


class TransferDirection(str, Enum):
    """Direction of transfer."""
    OUTBOUND = "outbound"    # Moving away from source
    INBOUND = "inbound"      # Moving into target
    BIDIRECTIONAL = "bidirectional"  # Both directions


@dataclass
class TransferInstruction:
    """Comprehensive transfer instruction."""
    instruction_id: str
    instruction_type: TransferInstructionType
    priority: TransferPriority
    direction: TransferDirection
    
    # Source and target
    source_execution_id: Optional[str]
    target_execution_id: Optional[str]
    source_portfolio_id: Optional[str]
    target_portfolio_id: Optional[str]
    source_strategy_id: Optional[int]
    target_strategy_id: Optional[int]
    
    # Transfer details
    symbol: str
    quantity: Decimal
    price_hint: Optional[Decimal]
    transfer_reason: str
    
    # Execution parameters
    execute_after: datetime
    expire_after: Optional[datetime]
    requires_confirmation: bool
    rollback_enabled: bool
    
    # Dependencies and constraints
    depends_on_instructions: List[str]
    blocks_instructions: List[str]
    execution_constraints: Dict[str, Any]
    
    # Metadata
    created_by: str
    created_at: datetime
    source_allocation_id: Optional[str]
    reconciliation_case_id: Optional[str]
    metadata: Dict[str, Any]


@dataclass
class TransferBatch:
    """Batch of transfer instructions."""
    batch_id: str
    instructions: List[TransferInstruction]
    batch_priority: TransferPriority
    batch_type: str
    atomic_execution: bool  # All or none
    parallel_execution: bool  # Can execute in parallel
    created_at: datetime
    metadata: Dict[str, Any]


@dataclass
class TransferValidationResult:
    """Result of transfer instruction validation."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    validated_instructions: List[TransferInstruction]
    estimated_execution_time: Optional[datetime]


class TransferInstructionGenerator:
    """
    Generates comprehensive transfer instructions for reconciliation-driven transfers.
    
    Creates type-safe, prioritized transfer instructions with proper validation
    and dependency management.
    """

    def __init__(self, db: AsyncSession):
        """
        Initialize the transfer instruction generator.

        Args:
            db: Async database session
        """
        self.db = db

    async def generate_attribution_transfer_instructions(
        self,
        allocation_result: Dict[str, Any],
        reconciliation_case_id: str,
        created_by: str = "system"
    ) -> TransferBatch:
        """
        Generate transfer instructions from attribution allocation results.

        Args:
            allocation_result: Allocation result from attribution service
            reconciliation_case_id: ID of reconciliation case
            created_by: Who created the instructions

        Returns:
            Transfer instruction batch

        Raises:
            Exception: If instruction generation fails
        """
        batch_id = str(uuid4())
        logger.info(f"[{batch_id}] Generating attribution transfer instructions for case {reconciliation_case_id}")

        try:
            instructions = []
            
            # Extract allocation information
            allocations = allocation_result.get('allocations', [])
            allocation_id = allocation_result.get('allocation_id')
            
            for i, allocation in enumerate(allocations):
                instruction = await self._create_attribution_transfer_instruction(
                    allocation, allocation_id, reconciliation_case_id, created_by, i
                )
                instructions.append(instruction)

            # Determine batch priority based on allocation
            batch_priority = self._determine_batch_priority(allocation_result)
            
            batch = TransferBatch(
                batch_id=batch_id,
                instructions=instructions,
                batch_priority=batch_priority,
                batch_type="attribution_correction",
                atomic_execution=True,  # Attribution transfers must be atomic
                parallel_execution=False,  # Execute sequentially for attribution
                created_at=datetime.now(timezone.utc),
                metadata={
                    "allocation_id": allocation_id,
                    "reconciliation_case_id": reconciliation_case_id,
                    "total_allocations": len(allocations),
                    "source_type": "attribution_resolution"
                }
            )

            logger.info(f"[{batch_id}] Generated {len(instructions)} transfer instructions")
            return batch

        except Exception as e:
            logger.error(f"[{batch_id}] Failed to generate attribution transfer instructions: {e}", exc_info=True)
            raise

    async def generate_handoff_transfer_instructions(
        self,
        source_execution_id: str,
        target_execution_id: str,
        symbol_positions: List[Dict[str, Any]],
        handoff_reason: str,
        created_by: str
    ) -> TransferBatch:
        """
        Generate transfer instructions for execution handoff.

        Args:
            source_execution_id: Source execution context
            target_execution_id: Target execution context
            symbol_positions: Positions to transfer
            handoff_reason: Reason for handoff
            created_by: Who initiated the handoff

        Returns:
            Transfer instruction batch
        """
        batch_id = str(uuid4())
        logger.info(f"[{batch_id}] Generating handoff transfer instructions: {source_execution_id} -> {target_execution_id}")

        try:
            instructions = []
            
            for i, position in enumerate(symbol_positions):
                instruction = TransferInstruction(
                    instruction_id=str(uuid4()),
                    instruction_type=TransferInstructionType.EXECUTION_HANDOFF,
                    priority=TransferPriority.HIGH,
                    direction=TransferDirection.OUTBOUND,
                    
                    source_execution_id=source_execution_id,
                    target_execution_id=target_execution_id,
                    source_portfolio_id=position.get('source_portfolio_id'),
                    target_portfolio_id=position.get('target_portfolio_id'),
                    source_strategy_id=position.get('source_strategy_id'),
                    target_strategy_id=position.get('target_strategy_id'),
                    
                    symbol=position['symbol'],
                    quantity=Decimal(str(position['quantity'])),
                    price_hint=Decimal(str(position['current_price'])) if position.get('current_price') else None,
                    transfer_reason=f"Execution handoff: {handoff_reason}",
                    
                    execute_after=datetime.now(timezone.utc),
                    expire_after=datetime.now(timezone.utc) + timedelta(hours=24),
                    requires_confirmation=False,  # Handoffs are automatic
                    rollback_enabled=True,
                    
                    depends_on_instructions=[],
                    blocks_instructions=[],
                    execution_constraints={
                        "market_hours_only": True,
                        "price_protection": True
                    },
                    
                    created_by=created_by,
                    created_at=datetime.now(timezone.utc),
                    source_allocation_id=None,
                    reconciliation_case_id=None,
                    metadata={
                        "handoff_type": "execution_control",
                        "position_id": position.get('position_id'),
                        "order_index": i
                    }
                )
                instructions.append(instruction)

            batch = TransferBatch(
                batch_id=batch_id,
                instructions=instructions,
                batch_priority=TransferPriority.HIGH,
                batch_type="execution_handoff",
                atomic_execution=True,
                parallel_execution=True,  # Handoff transfers can be parallel
                created_at=datetime.now(timezone.utc),
                metadata={
                    "source_execution_id": source_execution_id,
                    "target_execution_id": target_execution_id,
                    "handoff_reason": handoff_reason,
                    "positions_count": len(symbol_positions)
                }
            )

            return batch

        except Exception as e:
            logger.error(f"[{batch_id}] Failed to generate handoff transfer instructions: {e}", exc_info=True)
            raise

    async def generate_emergency_extraction_instructions(
        self,
        execution_id: str,
        emergency_reason: str,
        created_by: str
    ) -> TransferBatch:
        """
        Generate emergency extraction instructions for an execution.

        Args:
            execution_id: Execution to extract positions from
            emergency_reason: Reason for emergency extraction
            created_by: Who initiated the emergency

        Returns:
            Transfer instruction batch with highest priority
        """
        batch_id = str(uuid4())
        logger.warning(f"[{batch_id}] Generating EMERGENCY extraction instructions for {execution_id}")

        try:
            # Get all positions for this execution
            positions = await self._get_execution_positions(execution_id)
            instructions = []
            
            for i, position in enumerate(positions):
                instruction = TransferInstruction(
                    instruction_id=str(uuid4()),
                    instruction_type=TransferInstructionType.EMERGENCY_EXTRACTION,
                    priority=TransferPriority.CRITICAL,
                    direction=TransferDirection.OUTBOUND,
                    
                    source_execution_id=execution_id,
                    target_execution_id=None,  # Emergency extraction to manual control
                    source_portfolio_id=position.get('portfolio_id'),
                    target_portfolio_id=position.get('default_portfolio_id'),  # Move to default
                    source_strategy_id=position.get('strategy_id'),
                    target_strategy_id=position.get('default_strategy_id'),
                    
                    symbol=position['symbol'],
                    quantity=Decimal(str(position['quantity'])),
                    price_hint=None,  # Emergency - no price protection
                    transfer_reason=f"EMERGENCY EXTRACTION: {emergency_reason}",
                    
                    execute_after=datetime.now(timezone.utc),  # Execute immediately
                    expire_after=None,  # No expiry for emergency
                    requires_confirmation=False,
                    rollback_enabled=False,  # No rollback for emergency
                    
                    depends_on_instructions=[],
                    blocks_instructions=[],
                    execution_constraints={
                        "market_hours_only": False,  # Emergency can execute anytime
                        "price_protection": False,   # No price protection
                        "force_execution": True      # Force execution
                    },
                    
                    created_by=created_by,
                    created_at=datetime.now(timezone.utc),
                    source_allocation_id=None,
                    reconciliation_case_id=None,
                    metadata={
                        "emergency": True,
                        "emergency_reason": emergency_reason,
                        "position_id": position.get('position_id'),
                        "extraction_order": i
                    }
                )
                instructions.append(instruction)

            batch = TransferBatch(
                batch_id=batch_id,
                instructions=instructions,
                batch_priority=TransferPriority.CRITICAL,
                batch_type="emergency_extraction",
                atomic_execution=False,  # Process individually for emergency
                parallel_execution=True,   # Execute all in parallel
                created_at=datetime.now(timezone.utc),
                metadata={
                    "emergency": True,
                    "execution_id": execution_id,
                    "emergency_reason": emergency_reason,
                    "extracted_positions": len(positions)
                }
            )

            logger.warning(f"[{batch_id}] Generated {len(instructions)} EMERGENCY extraction instructions")
            return batch

        except Exception as e:
            logger.error(f"[{batch_id}] Failed to generate emergency extraction instructions: {e}", exc_info=True)
            raise

    async def validate_transfer_instructions(
        self,
        instructions: List[TransferInstruction]
    ) -> TransferValidationResult:
        """
        Validate transfer instructions for safety and consistency.

        Args:
            instructions: List of transfer instructions to validate

        Returns:
            Validation result with errors and warnings
        """
        logger.info(f"Validating {len(instructions)} transfer instructions")

        errors = []
        warnings = []
        validated_instructions = []

        try:
            for instruction in instructions:
                # Validate basic required fields
                if not instruction.symbol:
                    errors.append(f"Instruction {instruction.instruction_id}: Symbol is required")
                    continue

                if instruction.quantity <= 0:
                    errors.append(f"Instruction {instruction.instruction_id}: Quantity must be positive")
                    continue

                # Validate execution contexts exist
                if instruction.source_execution_id:
                    if not await self._execution_context_exists(instruction.source_execution_id):
                        errors.append(f"Instruction {instruction.instruction_id}: Source execution context not found")
                        continue

                if instruction.target_execution_id:
                    if not await self._execution_context_exists(instruction.target_execution_id):
                        warnings.append(f"Instruction {instruction.instruction_id}: Target execution context not found")

                # Validate transfer type specific requirements
                if instruction.instruction_type == TransferInstructionType.POSITION_TRANSFER:
                    if not instruction.source_execution_id or not instruction.target_execution_id:
                        errors.append(f"Instruction {instruction.instruction_id}: Position transfer requires both source and target execution IDs")
                        continue

                # Add to validated list if no errors for this instruction
                validated_instructions.append(instruction)

            is_valid = len(errors) == 0
            estimated_execution_time = datetime.now(timezone.utc) if is_valid else None

            return TransferValidationResult(
                is_valid=is_valid,
                errors=errors,
                warnings=warnings,
                validated_instructions=validated_instructions,
                estimated_execution_time=estimated_execution_time
            )

        except Exception as e:
            logger.error(f"Transfer instruction validation failed: {e}", exc_info=True)
            return TransferValidationResult(
                is_valid=False,
                errors=[f"Validation failed: {str(e)}"],
                warnings=[],
                validated_instructions=[],
                estimated_execution_time=None
            )

    async def _create_attribution_transfer_instruction(
        self,
        allocation: Dict[str, Any],
        allocation_id: str,
        reconciliation_case_id: str,
        created_by: str,
        order_index: int
    ) -> TransferInstruction:
        """Create a transfer instruction for an attribution allocation."""
        from datetime import timedelta
        
        return TransferInstruction(
            instruction_id=str(uuid4()),
            instruction_type=TransferInstructionType.ATTRIBUTION_CORRECTION,
            priority=TransferPriority.NORMAL,
            direction=TransferDirection.INBOUND,
            
            source_execution_id=None,  # Attribution from external
            target_execution_id=allocation.get('execution_id'),
            source_portfolio_id=None,
            target_portfolio_id=allocation.get('portfolio_id'),
            source_strategy_id=None,
            target_strategy_id=allocation.get('strategy_id'),
            
            symbol=allocation['symbol'],
            quantity=Decimal(str(allocation['allocated_quantity'])),
            price_hint=Decimal(str(allocation.get('entry_price', 0))) if allocation.get('entry_price') else None,
            transfer_reason=f"Attribution correction: {allocation.get('allocation_reason', 'external_exit')}",
            
            execute_after=datetime.now(timezone.utc),
            expire_after=datetime.now(timezone.utc) + timedelta(hours=48),
            requires_confirmation=False,
            rollback_enabled=True,
            
            depends_on_instructions=[],
            blocks_instructions=[],
            execution_constraints={
                "market_hours_only": False,
                "price_protection": False  # Attribution corrections don't need price protection
            },
            
            created_by=created_by,
            created_at=datetime.now(timezone.utc),
            source_allocation_id=allocation_id,
            reconciliation_case_id=reconciliation_case_id,
            metadata={
                "allocation_reason": allocation.get('allocation_reason'),
                "position_id": allocation.get('position_id'),
                "remaining_quantity": allocation.get('remaining_quantity'),
                "order_index": order_index
            }
        )

    def _determine_batch_priority(self, allocation_result: Dict[str, Any]) -> TransferPriority:
        """Determine batch priority based on allocation result."""
        requires_manual = allocation_result.get('requires_manual_intervention', False)
        unallocated_qty = allocation_result.get('unallocated_quantity', 0)
        total_qty = allocation_result.get('total_exit_quantity', 1)
        
        if requires_manual:
            return TransferPriority.LOW  # Manual cases are lower priority
        elif unallocated_qty > 0:
            return TransferPriority.NORMAL  # Partial allocation
        else:
            return TransferPriority.HIGH  # Full allocation

    async def _get_execution_positions(self, execution_id: str) -> List[Dict[str, Any]]:
        """Get all positions for an execution context."""
        from sqlalchemy import text
        
        result = await self.db.execute(
            text("""
                SELECT 
                    p.id as position_id,
                    p.symbol,
                    p.quantity,
                    p.strategy_id,
                    p.portfolio_id
                FROM order_service.positions p
                WHERE p.execution_id = :execution_id::uuid
                  AND p.is_open = true
                  AND p.quantity > 0
            """),
            {"execution_id": execution_id}
        )

        positions = []
        for row in result.fetchall():
            strategy_id = row[3]
            # Generate strategy name without cross-schema access
            strategy_name = f"Strategy_{strategy_id}" if strategy_id else "Manual"
            
            positions.append({
                "position_id": row[0],
                "symbol": row[1],
                "quantity": row[2],
                "strategy_id": strategy_id,
                "portfolio_id": row[4],
                "strategy_name": strategy_name
            })

        return positions

    async def _execution_context_exists(self, execution_id: str) -> bool:
        """Check if execution context exists."""
        from sqlalchemy import text
        
        result = await self.db.execute(
            text("""
                SELECT 1 FROM order_service.execution_contexts 
                WHERE execution_id = :execution_id::uuid
            """),
            {"execution_id": execution_id}
        )
        
        return result.fetchone() is not None


# Helper functions for external use
async def generate_attribution_transfer_instructions(
    db: AsyncSession,
    allocation_result: Dict[str, Any],
    reconciliation_case_id: str,
    created_by: str = "system"
) -> TransferBatch:
    """
    Convenience function for generating attribution transfer instructions.

    Args:
        db: Database session
        allocation_result: Attribution allocation result
        reconciliation_case_id: Reconciliation case ID
        created_by: Creator identifier

    Returns:
        Transfer instruction batch
    """
    generator = TransferInstructionGenerator(db)
    return await generator.generate_attribution_transfer_instructions(
        allocation_result, reconciliation_case_id, created_by
    )