"""
Reconciliation-Driven Execution Transfers Service

Integrates reconciliation outcomes with execution transfers to automatically
move positions/trades/orders based on allocation results from attribution.

Key Features:
- Integration with PartialExitAttributionService for allocation results  
- Integration with ExecutionTransferService for position movements
- Automatic execution of transfers based on reconciliation decisions
- Rollback capabilities for failed transfers
- Comprehensive audit trail linking reconciliation to transfers
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

from .partial_exit_attribution_service import (
    AllocationResult
)

logger = logging.getLogger(__name__)


class TransferTrigger(str, Enum):
    """Triggers for reconciliation-driven transfers."""
    ATTRIBUTION_RESOLVED = "attribution_resolved"          # After manual attribution decision
    PARTIAL_EXIT_ALLOCATED = "partial_exit_allocated"      # After partial exit allocation
    HOLDINGS_RECONCILIATION = "holdings_reconciliation"    # Holdings variance resolution
    MANUAL_INSTRUCTION = "manual_instruction"              # Manual transfer request


class TransferStatus(str, Enum):
    """Status of reconciliation-driven transfers."""
    PENDING = "pending"               # Transfer queued but not started
    IN_PROGRESS = "in_progress"       # Transfer actively happening
    COMPLETED = "completed"           # Transfer completed successfully
    FAILED = "failed"                 # Transfer failed
    ROLLED_BACK = "rolled_back"       # Failed transfer was rolled back
    CANCELLED = "cancelled"           # Transfer was cancelled


@dataclass
class TransferInstruction:
    """Instruction for executing a transfer based on reconciliation."""
    source_execution_id: str
    target_execution_id: str
    symbol: str
    quantity: Decimal
    allocation_reason: str
    priority: int = 100  # Lower number = higher priority
    metadata: Dict[str, Any] = None


@dataclass
class ReconciliationTransferResult:
    """Result of reconciliation-driven transfer execution."""
    transfer_id: str
    trigger: TransferTrigger
    source_allocation_id: Optional[str]
    instructions_count: int
    executed_count: int
    failed_count: int
    total_quantity_transferred: Decimal
    errors: List[str]
    warnings: List[str]
    audit_trail: List[Dict[str, Any]]


class ReconciliationDrivenTransferService:
    """
    Service for executing transfers driven by reconciliation outcomes.

    Listens to attribution results and automatically executes position transfers
    to align positions with the determined allocation.
    """

    def __init__(self, db: AsyncSession):
        """
        Initialize the reconciliation-driven transfer service.

        Args:
            db: Async database session
        """
        self.db = db

    async def execute_attribution_transfers(
        self,
        allocation_result: AllocationResult,
        trigger: TransferTrigger = TransferTrigger.PARTIAL_EXIT_ALLOCATED
    ) -> ReconciliationTransferResult:
        """
        Execute transfers based on partial exit attribution results.

        Args:
            allocation_result: Result from partial exit attribution
            trigger: What triggered this transfer

        Returns:
            Transfer execution result

        Raises:
            Exception: If transfers fail
        """
        transfer_id = str(uuid4())
        start_time = datetime.now(timezone.utc)
        
        logger.info(
            f"[{transfer_id}] Executing attribution transfers for allocation {allocation_result.allocation_id}"
        )

        try:
            # Step 1: Generate transfer instructions from allocation using dedicated generator
            # GAP-REC-11: Use TransferInstructionGenerator instead of internal method
            from .transfer_instruction_generator import TransferInstructionGenerator
            
            instruction_generator = TransferInstructionGenerator(self.db)
            instructions = await instruction_generator.generate_transfer_instructions(
                allocation_result=allocation_result,
                trigger_context={
                    "trigger": trigger,
                    "transfer_id": transfer_id,
                    "source": "reconciliation_driven_transfers",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            )
            
            if not instructions:
                logger.info(f"[{transfer_id}] No transfers needed for allocation {allocation_result.allocation_id}")
                return ReconciliationTransferResult(
                    transfer_id=transfer_id,
                    trigger=trigger,
                    source_allocation_id=allocation_result.allocation_id,
                    instructions_count=0,
                    executed_count=0,
                    failed_count=0,
                    total_quantity_transferred=Decimal('0'),
                    errors=[],
                    warnings=["No transfers needed"],
                    audit_trail=[]
                )

            # Step 2: Record transfer batch
            await self._record_transfer_batch(
                transfer_id, allocation_result.allocation_id, instructions, trigger
            )

            # Step 3: Execute transfers
            executed_count = 0
            failed_count = 0
            errors = []
            warnings = []
            total_transferred = Decimal('0')

            for instruction in instructions:
                try:
                    success, quantity_transferred = await self._execute_single_transfer(
                        transfer_id, instruction
                    )
                    
                    if success:
                        executed_count += 1
                        total_transferred += quantity_transferred
                        logger.debug(f"[{transfer_id}] Successfully transferred {quantity_transferred} {instruction.symbol}")
                    else:
                        failed_count += 1
                        errors.append(f"Failed to transfer {instruction.symbol}: {instruction.allocation_reason}")
                        
                except Exception as e:
                    failed_count += 1
                    error_msg = f"Transfer failed for {instruction.symbol}: {str(e)}"
                    errors.append(error_msg)
                    logger.error(f"[{transfer_id}] {error_msg}", exc_info=True)

            # Step 4: Update final status
            final_status = TransferStatus.COMPLETED if failed_count == 0 else (
                TransferStatus.FAILED if executed_count == 0 else TransferStatus.COMPLETED
            )

            await self._update_transfer_batch_status(
                transfer_id, final_status, {
                    "executed_count": executed_count,
                    "failed_count": failed_count,
                    "total_transferred": str(total_transferred),
                    "completion_time": datetime.now(timezone.utc).isoformat()
                }
            )

            if failed_count > 0 and executed_count > 0:
                warnings.append(f"Partial success: {executed_count}/{len(instructions)} transfers completed")

            result = ReconciliationTransferResult(
                transfer_id=transfer_id,
                trigger=trigger,
                source_allocation_id=allocation_result.allocation_id,
                instructions_count=len(instructions),
                executed_count=executed_count,
                failed_count=failed_count,
                total_quantity_transferred=total_transferred,
                errors=errors,
                warnings=warnings,
                audit_trail=[]
            )

            logger.info(
                f"[{transfer_id}] Transfer batch completed: "
                f"{executed_count}/{len(instructions)} succeeded, {failed_count} failed"
            )

            return result

        except Exception as e:
            logger.error(f"[{transfer_id}] Transfer batch failed: {e}", exc_info=True)
            
            await self._update_transfer_batch_status(
                transfer_id, TransferStatus.FAILED, {"error": str(e)}
            )

            raise

    async def execute_manual_transfer_instructions(
        self,
        case_id: str,
        transfer_instructions: List[Dict[str, Any]],
        executed_by: str
    ) -> ReconciliationTransferResult:
        """
        Execute transfers based on manual attribution decisions.

        Args:
            case_id: Manual attribution case ID
            transfer_instructions: List of transfer instruction dictionaries
            executed_by: User executing the transfers

        Returns:
            Transfer execution result
        """
        transfer_id = str(uuid4())
        
        logger.info(
            f"[{transfer_id}] Executing manual transfer instructions for case {case_id}"
        )

        try:
            # GAP-REC-11: Use Transfer Instruction Generator for comprehensive instructions
            from .transfer_instruction_generator import (
                TransferInstructionGenerator
            )
            
            generator = TransferInstructionGenerator(self.db)
            
            # Convert manual instructions to symbol positions format
            symbol_positions = []
            for instr_data in transfer_instructions:
                symbol_positions.append({
                    "symbol": instr_data["symbol"],
                    "quantity": instr_data["quantity"],
                    "source_execution_id": instr_data["source_execution_id"],
                    "target_execution_id": instr_data["target_execution_id"],
                    "metadata": instr_data.get("metadata", {})
                })
            
            # Generate comprehensive transfer instructions
            transfer_batch = await generator.generate_attribution_transfer_instructions(
                allocation_result={"allocations": symbol_positions, "allocation_id": case_id},
                reconciliation_case_id=case_id,
                created_by=executed_by
            )
            
            # Convert TransferInstruction objects to our internal format
            instructions = []
            for generated_instr in transfer_batch.instructions:
                instruction = TransferInstruction(
                    source_execution_id=generated_instr.source_execution_id,
                    target_execution_id=generated_instr.target_execution_id,
                    symbol=generated_instr.symbol,
                    quantity=generated_instr.quantity,
                    allocation_reason=generated_instr.transfer_reason,
                    priority=int(generated_instr.priority),
                    metadata=generated_instr.metadata
                )
                instructions.append(instruction)

            # Record and execute using same logic as attribution transfers
            await self._record_transfer_batch(
                transfer_id, case_id, instructions, TransferTrigger.MANUAL_INSTRUCTION
            )

            # Execute transfers (same logic as above)
            executed_count = 0
            failed_count = 0
            errors = []
            warnings = []
            total_transferred = Decimal('0')

            for instruction in instructions:
                try:
                    success, quantity_transferred = await self._execute_single_transfer(
                        transfer_id, instruction
                    )
                    
                    if success:
                        executed_count += 1
                        total_transferred += quantity_transferred
                    else:
                        failed_count += 1
                        errors.append(f"Failed to transfer {instruction.symbol}")
                        
                except Exception as e:
                    failed_count += 1
                    errors.append(f"Transfer failed for {instruction.symbol}: {str(e)}")

            # Update status
            final_status = TransferStatus.COMPLETED if failed_count == 0 else (
                TransferStatus.FAILED if executed_count == 0 else TransferStatus.COMPLETED
            )

            await self._update_transfer_batch_status(
                transfer_id, final_status, {
                    "executed_by": executed_by,
                    "executed_count": executed_count,
                    "failed_count": failed_count
                }
            )

            return ReconciliationTransferResult(
                transfer_id=transfer_id,
                trigger=TransferTrigger.MANUAL_INSTRUCTION,
                source_allocation_id=case_id,
                instructions_count=len(instructions),
                executed_count=executed_count,
                failed_count=failed_count,
                total_quantity_transferred=total_transferred,
                errors=errors,
                warnings=warnings,
                audit_trail=[]
            )

        except Exception as e:
            logger.error(f"[{transfer_id}] Manual transfer execution failed: {e}", exc_info=True)
            await self._update_transfer_batch_status(
                transfer_id, TransferStatus.FAILED, {"error": str(e), "executed_by": executed_by}
            )
            raise

    async def _generate_transfer_instructions(
        self,
        allocation_result: AllocationResult
    ) -> List[TransferInstruction]:
        """
        Generate transfer instructions from allocation result.

        Args:
            allocation_result: Partial exit attribution result

        Returns:
            List of transfer instructions
        """
        instructions = []

        # Get the exit context to determine transfer logic
        exit_is_external = allocation_result.total_allocated_quantity < allocation_result.exit_quantity
        exit_execution_context = await self._determine_exit_execution_context(allocation_result)

        # Process each allocation to determine required transfers
        for allocation in allocation_result.allocations:
            
            # Determine if this allocation requires a position transfer based on:
            # 1. Whether this is a partial allocation from a larger position
            # 2. Whether the exit source differs from the position's current execution
            # 3. Whether manual intervention changed the attribution
            
            requires_transfer = False
            target_execution_id = None
            transfer_reason = allocation.allocation_reason
            
            # Case 1: Partial allocation from larger position - need to split/transfer the allocated portion
            if allocation.allocated_quantity < (allocation.allocated_quantity + allocation.remaining_quantity):
                requires_transfer = True
                
                # Determine target based on exit context
                if exit_execution_context["is_external_exit"]:
                    # External exit - transfer allocated portion to manual control for reconciliation
                    target_execution_id = "manual"
                    transfer_reason = f"External exit attribution: {allocation.allocation_reason}"
                elif exit_execution_context["exit_execution_id"] != allocation.execution_id:
                    # Exit from different execution - transfer to that execution
                    target_execution_id = exit_execution_context["exit_execution_id"]
                    transfer_reason = f"Cross-execution attribution: {allocation.allocation_reason}"
                else:
                    # Same execution partial exit - reduce position in place (no transfer needed)
                    requires_transfer = False
            
            # Case 2: Full allocation but from wrong execution context
            elif exit_execution_context["exit_execution_id"] and exit_execution_context["exit_execution_id"] != allocation.execution_id:
                requires_transfer = True
                target_execution_id = exit_execution_context["exit_execution_id"]
                transfer_reason = f"Execution consolidation: {allocation.allocation_reason}"
                
            # Case 3: Manual intervention override - transfer to manual control
            elif allocation_result.requires_manual_intervention and not exit_execution_context["is_external_exit"]:
                requires_transfer = True
                target_execution_id = "manual"
                transfer_reason = f"Manual intervention required: {allocation.allocation_reason}"
            
            # Generate transfer instruction if needed
            if requires_transfer and target_execution_id:
                instruction = TransferInstruction(
                    source_execution_id=allocation.execution_id,
                    target_execution_id=target_execution_id,
                    symbol=allocation.symbol,
                    quantity=allocation.allocated_quantity,
                    allocation_reason=transfer_reason,
                    priority=self._calculate_transfer_priority(allocation, exit_execution_context),
                    metadata={
                        "position_id": allocation.position_id,
                        "strategy_id": allocation.strategy_id,
                        "allocation_id": allocation_result.allocation_id,
                        "exit_execution_context": exit_execution_context,
                        "transfer_type": self._classify_transfer_type(allocation, exit_execution_context)
                    }
                )
                instructions.append(instruction)
                
            # Case 4: Position reduction without transfer (same execution, full/partial)
            elif allocation.allocated_quantity > 0:
                # Create in-place position reduction instruction
                instruction = TransferInstruction(
                    source_execution_id=allocation.execution_id,
                    target_execution_id=allocation.execution_id,  # Same execution
                    symbol=allocation.symbol,
                    quantity=allocation.allocated_quantity,
                    allocation_reason=f"Position reduction: {allocation.allocation_reason}",
                    priority=100,  # Standard priority for in-place reductions
                    metadata={
                        "position_id": allocation.position_id,
                        "strategy_id": allocation.strategy_id,
                        "allocation_id": allocation_result.allocation_id,
                        "transfer_type": "position_reduction",
                        "in_place": True
                    }
                )
                instructions.append(instruction)

        return instructions

    async def _determine_exit_execution_context(
        self,
        allocation_result: AllocationResult
    ) -> Dict[str, Any]:
        """
        Determine the execution context for the exit to properly route transfers.
        Uses real reconciliation data to identify the true exit source.

        Args:
            allocation_result: Allocation result to analyze

        Returns:
            Exit execution context information
        """
        exit_context = {
            "is_external_exit": False,
            "exit_execution_id": None,
            "exit_source": "unknown",
            "requires_reconciliation": False,
            "exit_order_id": None,
            "exit_trade_id": None
        }

        # Step 1: Look up the actual exit trade/order from reconciliation data
        exit_trade_info = await self._get_exit_trade_details(allocation_result)
        
        if exit_trade_info:
            # Real exit found in reconciliation data
            exit_context["exit_source"] = exit_trade_info.get("source", "unknown")
            exit_context["exit_execution_id"] = exit_trade_info.get("execution_id")
            exit_context["exit_order_id"] = exit_trade_info.get("order_id")
            exit_context["exit_trade_id"] = exit_trade_info.get("trade_id")
            
            # Determine if this is external based on source
            if exit_trade_info.get("source") in ["external", "manual", "broker_direct"]:
                exit_context["is_external_exit"] = True
                exit_context["requires_reconciliation"] = True
            
            # Cross-execution scenario detection
            position_execution_ids = set(alloc.execution_id for alloc in allocation_result.allocations if alloc.execution_id)
            if exit_context["exit_execution_id"] and exit_context["exit_execution_id"] not in position_execution_ids:
                exit_context["requires_reconciliation"] = True
                
        else:
            # Fallback: Analyze allocation patterns when no direct exit trade found
            logger.warning(f"No exit trade found for allocation {allocation_result.allocation_id}, using pattern analysis")
            
            # Look for external exit indicators in allocation metadata
            if hasattr(allocation_result, 'audit_trail') and allocation_result.audit_trail:
                for audit_entry in allocation_result.audit_trail:
                    if audit_entry.get("event_type") == "exit_detected":
                        exit_context["exit_source"] = audit_entry.get("source", "unknown")
                        exit_context["is_external_exit"] = exit_context["exit_source"] in ["external", "manual"]

            # Analyze allocation patterns to infer exit context
            if allocation_result.requires_manual_intervention:
                exit_context["requires_reconciliation"] = True
                
            # If unallocated quantity exists, likely external exit
            if allocation_result.unallocated_quantity > 0:
                exit_context["is_external_exit"] = True
                exit_context["requires_reconciliation"] = True

            # Try to determine target execution from allocation pattern
            execution_ids = [alloc.execution_id for alloc in allocation_result.allocations if alloc.execution_id]
            if len(set(execution_ids)) == 1:
                # Single execution affected - likely the exit source
                exit_context["exit_execution_id"] = execution_ids[0]
            elif len(set(execution_ids)) > 1:
                # Multiple executions - cross-execution exit, use manual for reconciliation
                exit_context["exit_execution_id"] = "manual"
                exit_context["requires_reconciliation"] = True

        # Step 2: Cross-reference with holdings reconciliation variance data
        if hasattr(allocation_result, 'variance_id') and allocation_result.variance_id:
            variance_context = await self._get_variance_context(allocation_result.variance_id)
            if variance_context:
                # Update context based on variance resolution
                exit_context.update(variance_context)

        logger.debug(f"Exit context determined: {exit_context}")
        return exit_context

    async def _get_exit_trade_details(
        self,
        allocation_result: AllocationResult
    ) -> Optional[Dict[str, Any]]:
        """
        Get the actual exit trade/order details from reconciliation data.

        Args:
            allocation_result: Allocation result to look up

        Returns:
            Trade/order details if found, None otherwise
        """
        try:
            # Step 1: Look up exit trade from allocation metadata
            if hasattr(allocation_result, 'exit_timestamp') and allocation_result.exit_timestamp:
                exit_timestamp = allocation_result.exit_timestamp
                exit_symbol = getattr(allocation_result, 'symbol', None)
                exit_quantity = getattr(allocation_result, 'exit_quantity', None)
                
                if exit_symbol and exit_quantity:
                    # Search for matching trades around the exit timestamp (±5 minutes)
                    time_window_start = exit_timestamp - timedelta(minutes=5)
                    time_window_end = exit_timestamp + timedelta(minutes=5)
                    
                    result = await self.db.execute(
                        text("""
                            SELECT 
                                t.trade_id,
                                t.order_id,
                                t.symbol,
                                t.side,
                                t.quantity,
                                t.price,
                                t.timestamp,
                                t.source,
                                t.execution_id,
                                o.order_type,
                                o.status as order_status
                            FROM order_service.trades t
                            LEFT JOIN order_service.orders o ON t.order_id = o.order_id
                            WHERE t.symbol = :symbol
                              AND t.side = 'SELL'
                              AND t.timestamp BETWEEN :start_time AND :end_time
                              AND ABS(t.quantity) = :exit_quantity
                            ORDER BY ABS(EXTRACT(EPOCH FROM t.timestamp - :exit_timestamp)) ASC
                            LIMIT 1
                        """),
                        {
                            "symbol": exit_symbol,
                            "start_time": time_window_start,
                            "end_time": time_window_end,
                            "exit_quantity": abs(float(exit_quantity)),
                            "exit_timestamp": exit_timestamp
                        }
                    )
                    
                    row = result.fetchone()
                    if row:
                        return {
                            "trade_id": row[0],
                            "order_id": row[1],
                            "symbol": row[2],
                            "side": row[3],
                            "quantity": row[4],
                            "price": row[5],
                            "timestamp": row[6],
                            "source": row[7],
                            "execution_id": str(row[8]) if row[8] else None,
                            "order_type": row[9],
                            "order_status": row[10]
                        }

            # Step 2: Look up from holdings reconciliation variance if available
            if hasattr(allocation_result, 'variance_id') and allocation_result.variance_id:
                variance_result = await self.db.execute(
                    text("""
                        SELECT 
                            v.external_trade_id,
                            v.external_order_id,
                            v.variance_source,
                            v.resolution_data
                        FROM order_service.holdings_variances v
                        WHERE v.variance_id = :variance_id
                    """),
                    {"variance_id": allocation_result.variance_id}
                )
                
                variance_row = variance_result.fetchone()
                if variance_row and variance_row[0]:  # external_trade_id exists
                    # Look up the external trade details
                    external_result = await self.db.execute(
                        text("""
                            SELECT 
                                trade_id,
                                order_id,
                                symbol,
                                side,
                                quantity,
                                price,
                                timestamp,
                                'external' as source,
                                NULL as execution_id
                            FROM order_service.external_trades
                            WHERE trade_id = :external_trade_id
                        """),
                        {"external_trade_id": variance_row[0]}
                    )
                    
                    external_row = external_result.fetchone()
                    if external_row:
                        return {
                            "trade_id": external_row[0],
                            "order_id": external_row[1],
                            "symbol": external_row[2],
                            "side": external_row[3],
                            "quantity": external_row[4],
                            "price": external_row[5],
                            "timestamp": external_row[6],
                            "source": external_row[7],
                            "execution_id": external_row[8]
                        }
            
            # Step 3: Check allocation audit trail for trade references
            if hasattr(allocation_result, 'audit_trail') and allocation_result.audit_trail:
                for audit_entry in allocation_result.audit_trail:
                    if audit_entry.get("event_type") == "exit_trade_identified":
                        trade_id = audit_entry.get("trade_id")
                        if trade_id:
                            # Look up trade details
                            trade_result = await self.db.execute(
                                text("""
                                    SELECT 
                                        trade_id,
                                        order_id,
                                        symbol,
                                        side,
                                        quantity,
                                        price,
                                        timestamp,
                                        source,
                                        execution_id
                                    FROM order_service.trades
                                    WHERE trade_id = :trade_id
                                """),
                                {"trade_id": trade_id}
                            )
                            
                            trade_row = trade_result.fetchone()
                            if trade_row:
                                return {
                                    "trade_id": trade_row[0],
                                    "order_id": trade_row[1],
                                    "symbol": trade_row[2],
                                    "side": trade_row[3],
                                    "quantity": trade_row[4],
                                    "price": trade_row[5],
                                    "timestamp": trade_row[6],
                                    "source": trade_row[7],
                                    "execution_id": str(trade_row[8]) if trade_row[8] else None
                                }

            return None
            
        except Exception as e:
            logger.error(f"Failed to get exit trade details: {e}")
            return None

    async def _get_variance_context(
        self,
        variance_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get additional context from holdings reconciliation variance.

        Args:
            variance_id: Holdings variance ID

        Returns:
            Variance context data if found
        """
        try:
            result = await self.db.execute(
                text("""
                    SELECT 
                        variance_source,
                        resolution_type,
                        resolution_data,
                        external_order_id,
                        external_trade_id,
                        broker_account_id
                    FROM order_service.holdings_variances
                    WHERE variance_id = :variance_id
                """),
                {"variance_id": variance_id}
            )
            
            row = result.fetchone()
            if row:
                variance_source = row[0]
                resolution_data = row[2] or {}
                
                context = {
                    "variance_source": variance_source,
                    "resolution_type": row[1]
                }
                
                # External variance indicates external exit
                if variance_source in ["external", "broker_direct", "manual"]:
                    context["is_external_exit"] = True
                    context["requires_reconciliation"] = True
                    
                # Add broker context if available
                if row[5]:  # broker_account_id
                    context["broker_account_id"] = row[5]
                    
                # Extract execution context from resolution data
                if "execution_id" in resolution_data:
                    context["exit_execution_id"] = resolution_data["execution_id"]
                
                return context
                
        except Exception as e:
            logger.error(f"Failed to get variance context: {e}")
            
        return None

    def _calculate_transfer_priority(
        self,
        allocation: Any,
        exit_execution_context: Dict[str, Any]
    ) -> int:
        """
        Calculate transfer priority based on allocation and exit context.

        Args:
            allocation: Position allocation
            exit_execution_context: Exit context information

        Returns:
            Priority value (lower = higher priority)
        """
        priority = 100  # Default priority

        # Higher priority for external exits requiring reconciliation
        if exit_execution_context["is_external_exit"]:
            priority -= 20

        # Higher priority for manual intervention cases
        if exit_execution_context["requires_reconciliation"]:
            priority -= 10

        # Higher priority for larger quantities
        if hasattr(allocation, 'allocated_quantity') and allocation.allocated_quantity:
            quantity = float(allocation.allocated_quantity)
            if quantity > 1000:
                priority -= 15
            elif quantity > 100:
                priority -= 5

        # Ensure priority is within valid range
        return max(1, min(priority, 1000))

    def _classify_transfer_type(
        self,
        allocation: Any,
        exit_execution_context: Dict[str, Any]
    ) -> str:
        """
        Classify the type of transfer needed.

        Args:
            allocation: Position allocation
            exit_execution_context: Exit context information

        Returns:
            Transfer type classification
        """
        if exit_execution_context["is_external_exit"]:
            return "external_exit_reconciliation"
        elif exit_execution_context["requires_reconciliation"]:
            return "manual_reconciliation"
        elif exit_execution_context["exit_execution_id"] != allocation.execution_id:
            return "cross_execution_transfer"
        else:
            return "same_execution_reduction"

    async def _execute_single_transfer(
        self,
        transfer_id: str,
        instruction: TransferInstruction
    ) -> Tuple[bool, Decimal]:
        """
        Execute a single transfer instruction.

        Args:
            transfer_id: Transfer batch ID
            instruction: Transfer instruction to execute

        Returns:
            Tuple of (success: bool, quantity_transferred: Decimal)
        """
        try:
            logger.info(
                f"[{transfer_id}] Executing transfer: {instruction.quantity} {instruction.symbol} "
                f"from {instruction.source_execution_id} to {instruction.target_execution_id}"
            )

            # Step 1: Find the position(s) to transfer
            source_positions = await self._get_positions_by_execution_and_symbol(
                instruction.source_execution_id, instruction.symbol
            )

            if not source_positions:
                raise Exception(f"No positions found for execution {instruction.source_execution_id} symbol {instruction.symbol}")

            total_available = sum(Decimal(str(pos.get('quantity', 0))) for pos in source_positions)
            
            if abs(instruction.quantity) > abs(total_available):
                raise Exception(f"Insufficient quantity: requested {instruction.quantity}, available {total_available}")

            # Step 2: Execute the transfer based on instruction type
            quantity_transferred = Decimal('0')

            if instruction.target_execution_id == "manual":
                # Transfer to manual control
                quantity_transferred = await self._transfer_to_manual_control(
                    source_positions, instruction.quantity, instruction.metadata or {}
                )
            else:
                # Transfer between executions
                quantity_transferred = await self._transfer_between_executions(
                    instruction.source_execution_id,
                    instruction.target_execution_id,
                    instruction.symbol,
                    instruction.quantity,
                    instruction.metadata or {}
                )

            await self._record_transfer_execution(
                transfer_id, instruction, True, quantity_transferred, None
            )

            logger.info(
                f"[{transfer_id}] Transfer successful: {quantity_transferred} {instruction.symbol} "
                f"transferred from {instruction.source_execution_id} to {instruction.target_execution_id}"
            )

            return True, quantity_transferred

        except Exception as e:
            logger.error(
                f"[{transfer_id}] Transfer execution failed: {e}", 
                exc_info=True
            )

            await self._record_transfer_execution(
                transfer_id, instruction, False, Decimal('0'), str(e)
            )

            return False, Decimal('0')

    async def _get_positions_by_execution_and_symbol(
        self,
        execution_id: str,
        symbol: str
    ) -> List[Dict[str, Any]]:
        """Get positions for a specific execution and symbol."""
        result = await self.db.execute(
            text("""
                SELECT 
                    id,
                    symbol,
                    quantity,
                    strategy_id,
                    execution_id,
                    portfolio_id,
                    trading_account_id,
                    source
                FROM order_service.positions
                WHERE execution_id = :execution_id::uuid
                  AND symbol = :symbol
                  AND is_open = true
            """),
            {
                "execution_id": execution_id,
                "symbol": symbol
            }
        )

        positions = []
        for row in result.fetchall():
            positions.append({
                "id": row[0],
                "symbol": row[1],
                "quantity": row[2],
                "strategy_id": row[3],
                "execution_id": str(row[4]) if row[4] else None,
                "portfolio_id": row[5],
                "trading_account_id": row[6],
                "source": row[7]
            })

        return positions

    async def _transfer_to_manual_control(
        self,
        source_positions: List[Dict[str, Any]],
        transfer_quantity: Decimal,
        metadata: Dict[str, Any]
    ) -> Decimal:
        """Transfer positions to manual control."""
        quantity_to_transfer = abs(transfer_quantity)
        quantity_transferred = Decimal('0')

        for position in source_positions:
            if quantity_transferred >= quantity_to_transfer:
                break

            position_quantity = abs(Decimal(str(position.get('quantity', 0))))
            transfer_from_position = min(quantity_to_transfer - quantity_transferred, position_quantity)

            # Update position to manual control
            await self.db.execute(
                text("""
                    UPDATE order_service.positions
                    SET source = 'manual',
                        execution_id = NULL,
                        updated_at = NOW(),
                        metadata = COALESCE(metadata, '{}'::jsonb) || :transfer_metadata::jsonb
                    WHERE id = :position_id
                """),
                {
                    "position_id": position["id"],
                    "transfer_metadata": {
                        **metadata,
                        "transfer_source": "reconciliation_driven",
                        "transferred_quantity": str(transfer_from_position),
                        "original_execution_id": position["execution_id"]
                    }
                }
            )

            quantity_transferred += transfer_from_position

        await self.db.commit()
        return quantity_transferred

    async def _transfer_between_executions(
        self,
        source_execution_id: str,
        target_execution_id: str,
        symbol: str,
        transfer_quantity: Decimal,
        metadata: Dict[str, Any]
    ) -> Decimal:
        """Transfer positions between executions."""
        quantity_to_transfer = abs(transfer_quantity)
        quantity_transferred = Decimal('0')

        # Get source positions
        source_positions = await self._get_positions_by_execution_and_symbol(
            source_execution_id, symbol
        )

        for position in source_positions:
            if quantity_transferred >= quantity_to_transfer:
                break

            position_quantity = abs(Decimal(str(position.get('quantity', 0))))
            transfer_from_position = min(quantity_to_transfer - quantity_transferred, position_quantity)

            if transfer_from_position == position_quantity:
                # Transfer entire position
                await self.db.execute(
                    text("""
                        UPDATE order_service.positions
                        SET execution_id = :target_execution_id::uuid,
                            source = 'script',
                            updated_at = NOW(),
                            metadata = COALESCE(metadata, '{}'::jsonb) || :transfer_metadata::jsonb
                        WHERE id = :position_id
                    """),
                    {
                        "position_id": position["id"],
                        "target_execution_id": target_execution_id,
                        "transfer_metadata": {
                            **metadata,
                            "transfer_source": "reconciliation_driven",
                            "transferred_quantity": str(transfer_from_position),
                            "original_execution_id": source_execution_id
                        }
                    }
                )
            else:
                # Split position - reduce original and create new
                new_quantity = position_quantity - transfer_from_position
                await self.db.execute(
                    text("""
                        UPDATE order_service.positions
                        SET quantity = :new_quantity,
                            updated_at = NOW()
                        WHERE id = :position_id
                    """),
                    {
                        "position_id": position["id"],
                        "new_quantity": str(new_quantity)
                    }
                )

                # Create new position for transferred portion
                await self.db.execute(
                    text("""
                        INSERT INTO order_service.positions (
                            trading_account_id, symbol, exchange, product_type,
                            quantity, strategy_id, execution_id, portfolio_id,
                            source, buy_price, sell_price,
                            created_at, updated_at, metadata
                        )
                        SELECT 
                            trading_account_id, symbol, exchange, product_type,
                            :transfer_quantity, strategy_id, :target_execution_id::uuid, portfolio_id,
                            'script', buy_price, sell_price,
                            NOW(), NOW(), 
                            COALESCE(metadata, '{}'::jsonb) || :transfer_metadata::jsonb
                        FROM order_service.positions
                        WHERE id = :source_position_id
                    """),
                    {
                        "transfer_quantity": str(transfer_from_position),
                        "target_execution_id": target_execution_id,
                        "source_position_id": position["id"],
                        "transfer_metadata": {
                            **metadata,
                            "transfer_source": "reconciliation_driven",
                            "split_from_position_id": position["id"],
                            "original_execution_id": source_execution_id
                        }
                    }
                )

            quantity_transferred += transfer_from_position

        await self.db.commit()
        return quantity_transferred

    async def _record_transfer_batch(
        self,
        transfer_id: str,
        source_id: str,
        instructions: List[TransferInstruction],
        trigger: TransferTrigger
    ) -> None:
        """
        Record transfer batch in database.

        Args:
            transfer_id: Transfer batch ID
            source_id: Source allocation/case ID
            instructions: List of transfer instructions
            trigger: What triggered this transfer
        """
        await self.db.execute(
            text("""
                INSERT INTO order_service.reconciliation_transfer_batches (
                    transfer_id,
                    source_id,
                    trigger_type,
                    instructions_count,
                    status,
                    created_at,
                    metadata
                ) VALUES (
                    :transfer_id,
                    :source_id,
                    :trigger_type,
                    :instructions_count,
                    :status,
                    :created_at,
                    :metadata::jsonb
                )
            """),
            {
                "transfer_id": transfer_id,
                "source_id": source_id,
                "trigger_type": trigger.value,
                "instructions_count": len(instructions),
                "status": TransferStatus.IN_PROGRESS.value,
                "created_at": datetime.now(timezone.utc),
                "metadata": {
                    "instructions": [
                        {
                            "source_execution_id": instr.source_execution_id,
                            "target_execution_id": instr.target_execution_id,
                            "symbol": instr.symbol,
                            "quantity": str(instr.quantity),
                            "reason": instr.allocation_reason,
                            "priority": instr.priority
                        }
                        for instr in instructions
                    ]
                }
            }
        )
        await self.db.commit()

    async def _update_transfer_batch_status(
        self,
        transfer_id: str,
        status: TransferStatus,
        result_metadata: Dict[str, Any]
    ) -> None:
        """
        Update transfer batch status and results.

        Args:
            transfer_id: Transfer batch ID
            status: Final status
            result_metadata: Result information
        """
        await self.db.execute(
            text("""
                UPDATE order_service.reconciliation_transfer_batches
                SET status = :status,
                    result_metadata = :result_metadata::jsonb,
                    completed_at = :completed_at,
                    updated_at = :updated_at
                WHERE transfer_id = :transfer_id
            """),
            {
                "transfer_id": transfer_id,
                "status": status.value,
                "result_metadata": result_metadata,
                "completed_at": datetime.now(timezone.utc) if status in [TransferStatus.COMPLETED, TransferStatus.FAILED] else None,
                "updated_at": datetime.now(timezone.utc)
            }
        )
        await self.db.commit()

    async def _record_transfer_execution(
        self,
        transfer_id: str,
        instruction: TransferInstruction,
        success: bool,
        quantity_transferred: Decimal,
        error_message: Optional[str]
    ) -> None:
        """
        Record individual transfer execution result.

        Args:
            transfer_id: Transfer batch ID
            instruction: Transfer instruction
            success: Whether transfer succeeded
            quantity_transferred: Actual quantity transferred
            error_message: Error message if failed
        """
        await self.db.execute(
            text("""
                INSERT INTO order_service.reconciliation_transfer_executions (
                    transfer_id,
                    source_execution_id,
                    target_execution_id,
                    symbol,
                    requested_quantity,
                    transferred_quantity,
                    success,
                    error_message,
                    executed_at,
                    metadata
                ) VALUES (
                    :transfer_id,
                    :source_execution_id,
                    :target_execution_id,
                    :symbol,
                    :requested_quantity,
                    :transferred_quantity,
                    :success,
                    :error_message,
                    :executed_at,
                    :metadata::jsonb
                )
            """),
            {
                "transfer_id": transfer_id,
                "source_execution_id": instruction.source_execution_id,
                "target_execution_id": instruction.target_execution_id,
                "symbol": instruction.symbol,
                "requested_quantity": str(instruction.quantity),
                "transferred_quantity": str(quantity_transferred),
                "success": success,
                "error_message": error_message,
                "executed_at": datetime.now(timezone.utc),
                "metadata": instruction.metadata or {}
            }
        )

    async def get_transfer_status(
        self,
        transfer_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get transfer batch status and results.

        Args:
            transfer_id: Transfer batch ID

        Returns:
            Transfer status information or None if not found
        """
        result = await self.db.execute(
            text("""
                SELECT 
                    transfer_id,
                    source_id,
                    trigger_type,
                    instructions_count,
                    status,
                    created_at,
                    completed_at,
                    metadata,
                    result_metadata
                FROM order_service.reconciliation_transfer_batches
                WHERE transfer_id = :transfer_id
            """),
            {"transfer_id": transfer_id}
        )

        row = result.fetchone()
        if not row:
            return None

        # Get execution details
        executions_result = await self.db.execute(
            text("""
                SELECT 
                    source_execution_id,
                    target_execution_id,
                    symbol,
                    requested_quantity,
                    transferred_quantity,
                    success,
                    error_message,
                    executed_at
                FROM order_service.reconciliation_transfer_executions
                WHERE transfer_id = :transfer_id
                ORDER BY executed_at
            """),
            {"transfer_id": transfer_id}
        )

        executions = [
            {
                "source_execution_id": exec_row[0],
                "target_execution_id": exec_row[1],
                "symbol": exec_row[2],
                "requested_quantity": exec_row[3],
                "transferred_quantity": exec_row[4],
                "success": exec_row[5],
                "error_message": exec_row[6],
                "executed_at": exec_row[7].isoformat() if exec_row[7] else None
            }
            for exec_row in executions_result.fetchall()
        ]

        return {
            "transfer_id": row[0],
            "source_id": row[1],
            "trigger_type": row[2],
            "instructions_count": row[3],
            "status": row[4],
            "created_at": row[5].isoformat() if row[5] else None,
            "completed_at": row[6].isoformat() if row[6] else None,
            "metadata": row[7],
            "result_metadata": row[8],
            "executions": executions
        }

    async def list_recent_transfers(
        self,
        limit: int = 50,
        status_filter: Optional[TransferStatus] = None
    ) -> List[Dict[str, Any]]:
        """
        List recent transfer batches.

        Args:
            limit: Maximum transfers to return
            status_filter: Optional status filter

        Returns:
            List of transfer batch summaries
        """
        where_clause = ""
        params = {"limit": limit}

        if status_filter:
            where_clause = "WHERE status = :status_filter"
            params["status_filter"] = status_filter.value

        result = await self.db.execute(
            text(f"""
                SELECT 
                    transfer_id,
                    source_id,
                    trigger_type,
                    instructions_count,
                    status,
                    created_at,
                    completed_at,
                    result_metadata
                FROM order_service.reconciliation_transfer_batches
                {where_clause}
                ORDER BY created_at DESC
                LIMIT :limit
            """),
            params
        )

        return [
            {
                "transfer_id": row[0],
                "source_id": row[1],
                "trigger_type": row[2],
                "instructions_count": row[3],
                "status": row[4],
                "created_at": row[5].isoformat() if row[5] else None,
                "completed_at": row[6].isoformat() if row[6] else None,
                "result_summary": row[7]
            }
            for row in result.fetchall()
        ]


# Helper function for use outside of class context
async def execute_attribution_transfers(
    db: AsyncSession,
    allocation_result: AllocationResult,
    trigger: TransferTrigger = TransferTrigger.PARTIAL_EXIT_ALLOCATED
) -> ReconciliationTransferResult:
    """
    Execute transfers based on attribution results.

    Args:
        db: Database session
        allocation_result: Result from partial exit attribution
        trigger: What triggered this transfer

    Returns:
        Transfer execution result
    """
    service = ReconciliationDrivenTransferService(db)
    return await service.execute_attribution_transfers(allocation_result, trigger)