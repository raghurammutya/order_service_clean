"""
External Order Tagging Idempotency Service

Ensures idempotent handling of external order tagging operations by implementing:
- Duplicate detection mechanisms
- Idempotency key management
- Safe retry operations
- State consistency checking
- Conflict resolution strategies

Key Features:
- Content-based duplicate detection
- Temporal deduplication windows
- Idempotency key validation
- Automatic conflict resolution
- Comprehensive audit trails
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from dataclasses import dataclass
from enum import Enum
from uuid import uuid4
import hashlib
import json
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class IdempotencyResult(str, Enum):
    """Results of idempotency checking."""
    NEW_OPERATION = "new_operation"              # First time seeing this operation
    DUPLICATE_DETECTED = "duplicate_detected"    # Exact duplicate found
    CONFLICT_DETECTED = "conflict_detected"      # Similar but conflicting operation
    STALE_OPERATION = "stale_operation"          # Operation too old to process
    IDEMPOTENCY_VIOLATION = "idempotency_violation"  # Idempotency key reused inappropriately


class ConflictResolutionAction(str, Enum):
    """Actions for resolving conflicts."""
    USE_EXISTING = "use_existing"        # Use the existing operation result
    USE_NEW = "use_new"                  # Process new operation, update existing
    MANUAL_REVIEW = "manual_review"      # Escalate to manual review
    REJECT_NEW = "reject_new"            # Reject new operation
    MERGE_OPERATIONS = "merge_operations"  # Merge new with existing


class TaggingOperation(str, Enum):
    """Types of external order tagging operations."""
    ORDER_CREATE = "order_create"        # Create new order tag
    ORDER_UPDATE = "order_update"        # Update existing order tag
    ORDER_CANCEL = "order_cancel"        # Cancel order tag
    ORDER_FILL = "order_fill"            # Mark order as filled
    ORDER_PARTIAL_FILL = "order_partial_fill"  # Partial fill update


@dataclass
class ExternalOrderTag:
    """External order tag data structure."""
    external_order_id: str
    broker_order_id: Optional[str]
    trading_account_id: str
    symbol: str
    side: str
    quantity: Decimal
    price: Optional[Decimal]
    order_type: str
    status: str
    timestamp: datetime
    execution_id: Optional[str]
    strategy_id: Optional[int]
    metadata: Dict[str, Any]


@dataclass
class IdempotencyCheck:
    """Idempotency check request."""
    operation_type: TaggingOperation
    idempotency_key: Optional[str]
    request_data: Dict[str, Any]
    client_id: str
    deduplication_window_minutes: int = 30


@dataclass
class IdempotencyCheckResult:
    """Result of idempotency checking."""
    check_id: str
    result: IdempotencyResult
    existing_operation_id: Optional[str]
    conflict_resolution: Optional[ConflictResolutionAction]
    can_proceed: bool
    reason: str
    existing_data: Optional[Dict[str, Any]]
    fingerprint: str
    metadata: Dict[str, Any]


@dataclass
class TaggingOperationResult:
    """Result of external order tagging operation."""
    operation_id: str
    success: bool
    order_tag: Optional[ExternalOrderTag]
    idempotency_result: IdempotencyCheckResult
    processing_time_ms: float
    conflicts_resolved: List[str]
    audit_trail: Dict[str, Any]


class ExternalOrderTaggingIdempotency:
    """
    Service for ensuring idempotent external order tagging operations.
    
    Prevents duplicate processing and ensures consistent behavior
    when the same external order operations are submitted multiple times.
    """

    def __init__(self, db: AsyncSession):
        """
        Initialize the idempotency service.

        Args:
            db: Async database session
        """
        self.db = db

    async def process_external_order_operation(
        self,
        operation_type: TaggingOperation,
        order_data: Dict[str, Any],
        client_id: str,
        idempotency_key: Optional[str] = None,
        deduplication_window_minutes: int = 30
    ) -> TaggingOperationResult:
        """
        Process external order operation with idempotency guarantees.

        Args:
            operation_type: Type of tagging operation
            order_data: External order data
            client_id: Client/source identifier
            idempotency_key: Optional explicit idempotency key
            deduplication_window_minutes: Window for duplicate detection

        Returns:
            Operation result with idempotency information

        Raises:
            Exception: If operation fails
        """
        operation_id = str(uuid4())
        start_time = datetime.now(timezone.utc)
        
        logger.info(
            f"[{operation_id}] Processing external order operation: "
            f"{operation_type} for {order_data.get('external_order_id')} client={client_id}"
        )

        try:
            # Step 1: Perform idempotency check
            idempotency_check = IdempotencyCheck(
                operation_type=operation_type,
                idempotency_key=idempotency_key,
                request_data=order_data,
                client_id=client_id,
                deduplication_window_minutes=deduplication_window_minutes
            )
            
            idempotency_result = await self._check_idempotency(idempotency_check)

            # Step 2: Handle different idempotency results
            if idempotency_result.result == IdempotencyResult.DUPLICATE_DETECTED:
                # Return existing result
                existing_data = idempotency_result.existing_data or {}
                processing_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
                
                return TaggingOperationResult(
                    operation_id=operation_id,
                    success=True,
                    order_tag=self._dict_to_order_tag(existing_data) if existing_data else None,
                    idempotency_result=idempotency_result,
                    processing_time_ms=processing_time,
                    conflicts_resolved=[],
                    audit_trail={
                        "duplicate_of": idempotency_result.existing_operation_id,
                        "returned_existing_result": True,
                        "original_fingerprint": idempotency_result.fingerprint
                    }
                )

            elif idempotency_result.result == IdempotencyResult.CONFLICT_DETECTED:
                # Handle conflict based on resolution strategy
                conflict_result = await self._resolve_conflict(
                    idempotency_result, operation_type, order_data, client_id
                )
                
                if not conflict_result["can_proceed"]:
                    processing_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
                    
                    return TaggingOperationResult(
                        operation_id=operation_id,
                        success=False,
                        order_tag=None,
                        idempotency_result=idempotency_result,
                        processing_time_ms=processing_time,
                        conflicts_resolved=[],
                        audit_trail={
                            "conflict_detected": True,
                            "resolution_action": conflict_result["resolution_action"],
                            "conflict_reason": conflict_result["reason"]
                        }
                    )

            elif idempotency_result.result in [
                IdempotencyResult.STALE_OPERATION,
                IdempotencyResult.IDEMPOTENCY_VIOLATION
            ]:
                # Reject operation
                processing_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
                
                return TaggingOperationResult(
                    operation_id=operation_id,
                    success=False,
                    order_tag=None,
                    idempotency_result=idempotency_result,
                    processing_time_ms=processing_time,
                    conflicts_resolved=[],
                    audit_trail={
                        "rejected": True,
                        "rejection_reason": idempotency_result.reason
                    }
                )

            # Step 3: Process new operation
            operation_result = await self._execute_tagging_operation(
                operation_id, operation_type, order_data, client_id
            )

            # Step 4: Store operation for future idempotency checks
            await self._store_operation_for_idempotency(
                operation_id, idempotency_check, operation_result, idempotency_result.fingerprint
            )

            processing_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

            return TaggingOperationResult(
                operation_id=operation_id,
                success=operation_result["success"],
                order_tag=operation_result.get("order_tag"),
                idempotency_result=idempotency_result,
                processing_time_ms=processing_time,
                conflicts_resolved=operation_result.get("conflicts_resolved", []),
                audit_trail={
                    "new_operation": True,
                    "fingerprint": idempotency_result.fingerprint,
                    "execution_details": operation_result.get("execution_details", {})
                }
            )

        except Exception as e:
            logger.error(f"[{operation_id}] External order operation failed: {e}", exc_info=True)
            processing_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            
            # Create error result
            error_idempotency = IdempotencyCheckResult(
                check_id=str(uuid4()),
                result=IdempotencyResult.NEW_OPERATION,
                existing_operation_id=None,
                conflict_resolution=None,
                can_proceed=False,
                reason=f"Operation failed: {str(e)}",
                existing_data=None,
                fingerprint="",
                metadata={"error": str(e)}
            )
            
            return TaggingOperationResult(
                operation_id=operation_id,
                success=False,
                order_tag=None,
                idempotency_result=error_idempotency,
                processing_time_ms=processing_time,
                conflicts_resolved=[],
                audit_trail={"error": str(e), "operation_failed": True}
            )

    async def cleanup_stale_operations(
        self,
        cleanup_older_than_hours: int = 24
    ) -> Dict[str, Any]:
        """
        Clean up stale idempotency records.

        Args:
            cleanup_older_than_hours: Clean up records older than this

        Returns:
            Cleanup summary
        """
        logger.info(f"Cleaning up idempotency records older than {cleanup_older_than_hours} hours")
        
        try:
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=cleanup_older_than_hours)
            
            result = await self.db.execute(
                text("""
                    DELETE FROM order_service.external_order_idempotency
                    WHERE created_at < :cutoff_time
                """),
                {"cutoff_time": cutoff_time}
            )
            
            deleted_count = result.rowcount
            await self.db.commit()
            
            logger.info(f"Cleaned up {deleted_count} stale idempotency records")
            
            return {
                "deleted_records": deleted_count,
                "cutoff_time": cutoff_time.isoformat(),
                "cleanup_successful": True
            }

        except Exception as e:
            logger.error(f"Idempotency cleanup failed: {e}", exc_info=True)
            await self.db.rollback()
            return {
                "deleted_records": 0,
                "cleanup_successful": False,
                "error": str(e)
            }

    async def get_operation_history(
        self,
        external_order_id: str,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get operation history for an external order.

        Args:
            external_order_id: External order ID
            limit: Maximum records to return

        Returns:
            List of operation history records
        """
        result = await self.db.execute(
            text("""
                SELECT 
                    operation_id,
                    operation_type,
                    client_id,
                    idempotency_key,
                    fingerprint,
                    result_data,
                    created_at
                FROM order_service.external_order_idempotency
                WHERE (request_data ->> 'external_order_id') = :external_order_id
                ORDER BY created_at DESC
                LIMIT :limit
            """),
            {
                "external_order_id": external_order_id,
                "limit": limit
            }
        )
        
        return [
            {
                "operation_id": str(row[0]),
                "operation_type": row[1],
                "client_id": row[2],
                "idempotency_key": row[3],
                "fingerprint": row[4],
                "result_data": row[5],
                "created_at": row[6].isoformat() if row[6] else None
            }
            for row in result.fetchall()
        ]

    async def _check_idempotency(self, check: IdempotencyCheck) -> IdempotencyCheckResult:
        """Perform comprehensive idempotency checking."""
        
        check_id = str(uuid4())
        
        # Generate operation fingerprint
        fingerprint = self._generate_operation_fingerprint(check)
        
        # Check for exact duplicates by fingerprint
        fingerprint_result = await self._check_fingerprint_duplicate(fingerprint, check)
        if fingerprint_result:
            return fingerprint_result
        
        # Check for idempotency key duplicates
        if check.idempotency_key:
            key_result = await self._check_idempotency_key_duplicate(check)
            if key_result:
                return key_result
        
        # Check for content-based duplicates in deduplication window
        content_result = await self._check_content_duplicate(check, fingerprint)
        if content_result:
            return content_result
        
        # No duplicates found - this is a new operation
        return IdempotencyCheckResult(
            check_id=check_id,
            result=IdempotencyResult.NEW_OPERATION,
            existing_operation_id=None,
            conflict_resolution=None,
            can_proceed=True,
            reason="New operation - no duplicates detected",
            existing_data=None,
            fingerprint=fingerprint,
            metadata={
                "fingerprint_method": "sha256",
                "deduplication_window_minutes": check.deduplication_window_minutes
            }
        )

    def _generate_operation_fingerprint(self, check: IdempotencyCheck) -> str:
        """Generate unique fingerprint for operation."""
        
        # Create normalized representation for fingerprinting
        normalized_data = {
            "operation_type": check.operation_type.value,
            "external_order_id": check.request_data.get("external_order_id"),
            "broker_order_id": check.request_data.get("broker_order_id"),
            "symbol": check.request_data.get("symbol"),
            "side": check.request_data.get("side"),
            "quantity": str(check.request_data.get("quantity", "")),
            "price": str(check.request_data.get("price", "")),
            "order_type": check.request_data.get("order_type"),
            "trading_account_id": check.request_data.get("trading_account_id"),
            "client_id": check.client_id
        }
        
        # Remove None values and sort keys for consistent fingerprinting
        cleaned_data = {k: v for k, v in normalized_data.items() if v is not None}
        normalized_json = json.dumps(cleaned_data, sort_keys=True, separators=(',', ':'))
        
        # Generate SHA-256 hash
        return hashlib.sha256(normalized_json.encode('utf-8')).hexdigest()

    async def _check_fingerprint_duplicate(
        self,
        fingerprint: str,
        check: IdempotencyCheck
    ) -> Optional[IdempotencyCheckResult]:
        """Check for exact fingerprint match."""
        
        result = await self.db.execute(
            text("""
                SELECT 
                    operation_id,
                    created_at,
                    result_data
                FROM order_service.external_order_idempotency
                WHERE fingerprint = :fingerprint
                ORDER BY created_at DESC
                LIMIT 1
            """),
            {"fingerprint": fingerprint}
        )
        
        row = result.fetchone()
        if row:
            return IdempotencyCheckResult(
                check_id=str(uuid4()),
                result=IdempotencyResult.DUPLICATE_DETECTED,
                existing_operation_id=str(row[0]),
                conflict_resolution=ConflictResolutionAction.USE_EXISTING,
                can_proceed=False,
                reason=f"Exact duplicate found (fingerprint match)",
                existing_data=row[2],
                fingerprint=fingerprint,
                metadata={
                    "match_type": "fingerprint",
                    "original_timestamp": row[1].isoformat() if row[1] else None
                }
            )
        
        return None

    async def _check_idempotency_key_duplicate(
        self,
        check: IdempotencyCheck
    ) -> Optional[IdempotencyCheckResult]:
        """Check for idempotency key reuse."""
        
        result = await self.db.execute(
            text("""
                SELECT 
                    operation_id,
                    operation_type,
                    fingerprint,
                    created_at,
                    result_data
                FROM order_service.external_order_idempotency
                WHERE idempotency_key = :idempotency_key
                  AND client_id = :client_id
                ORDER BY created_at DESC
                LIMIT 1
            """),
            {
                "idempotency_key": check.idempotency_key,
                "client_id": check.client_id
            }
        )
        
        row = result.fetchone()
        if row:
            existing_fingerprint = row[2]
            current_fingerprint = self._generate_operation_fingerprint(check)
            
            if existing_fingerprint == current_fingerprint:
                # Same idempotency key, same operation - duplicate
                return IdempotencyCheckResult(
                    check_id=str(uuid4()),
                    result=IdempotencyResult.DUPLICATE_DETECTED,
                    existing_operation_id=str(row[0]),
                    conflict_resolution=ConflictResolutionAction.USE_EXISTING,
                    can_proceed=False,
                    reason="Duplicate idempotency key with identical operation",
                    existing_data=row[4],
                    fingerprint=current_fingerprint,
                    metadata={
                        "match_type": "idempotency_key",
                        "existing_fingerprint": existing_fingerprint
                    }
                )
            else:
                # Same idempotency key, different operation - violation
                return IdempotencyCheckResult(
                    check_id=str(uuid4()),
                    result=IdempotencyResult.IDEMPOTENCY_VIOLATION,
                    existing_operation_id=str(row[0]),
                    conflict_resolution=ConflictResolutionAction.REJECT_NEW,
                    can_proceed=False,
                    reason="Idempotency key reused with different operation",
                    existing_data=row[4],
                    fingerprint=current_fingerprint,
                    metadata={
                        "violation_type": "key_reuse",
                        "existing_fingerprint": existing_fingerprint,
                        "new_fingerprint": current_fingerprint
                    }
                )
        
        return None

    async def _check_content_duplicate(
        self,
        check: IdempotencyCheck,
        fingerprint: str
    ) -> Optional[IdempotencyCheckResult]:
        """Check for content-based duplicates in deduplication window."""
        
        window_start = datetime.now(timezone.utc) - timedelta(minutes=check.deduplication_window_minutes)
        
        # Look for similar operations in the deduplication window
        result = await self.db.execute(
            text("""
                SELECT 
                    operation_id,
                    fingerprint,
                    created_at,
                    request_data,
                    result_data
                FROM order_service.external_order_idempotency
                WHERE (request_data ->> 'external_order_id') = :external_order_id
                  AND client_id = :client_id
                  AND operation_type = :operation_type
                  AND created_at >= :window_start
                ORDER BY created_at DESC
                LIMIT 5
            """),
            {
                "external_order_id": check.request_data.get("external_order_id"),
                "client_id": check.client_id,
                "operation_type": check.operation_type.value,
                "window_start": window_start
            }
        )
        
        for row in result.fetchall():
            existing_fingerprint = row[1]
            
            if existing_fingerprint == fingerprint:
                # Exact content match in window
                return IdempotencyCheckResult(
                    check_id=str(uuid4()),
                    result=IdempotencyResult.DUPLICATE_DETECTED,
                    existing_operation_id=str(row[0]),
                    conflict_resolution=ConflictResolutionAction.USE_EXISTING,
                    can_proceed=False,
                    reason="Duplicate operation in deduplication window",
                    existing_data=row[4],
                    fingerprint=fingerprint,
                    metadata={
                        "match_type": "content_window",
                        "window_minutes": check.deduplication_window_minutes,
                        "existing_timestamp": row[2].isoformat() if row[2] else None
                    }
                )
            else:
                # Similar but different operation - potential conflict
                conflict_analysis = self._analyze_operation_conflict(
                    check.request_data, row[3], check.operation_type
                )
                
                if conflict_analysis["is_conflict"]:
                    return IdempotencyCheckResult(
                        check_id=str(uuid4()),
                        result=IdempotencyResult.CONFLICT_DETECTED,
                        existing_operation_id=str(row[0]),
                        conflict_resolution=conflict_analysis["suggested_resolution"],
                        can_proceed=conflict_analysis["can_proceed"],
                        reason=conflict_analysis["reason"],
                        existing_data=row[4],
                        fingerprint=fingerprint,
                        metadata={
                            "conflict_type": conflict_analysis["conflict_type"],
                            "conflict_severity": conflict_analysis["severity"]
                        }
                    )
        
        return None

    def _analyze_operation_conflict(
        self,
        new_data: Dict[str, Any],
        existing_data: Dict[str, Any],
        operation_type: TaggingOperation
    ) -> Dict[str, Any]:
        """Analyze potential conflict between operations."""
        
        # Compare key fields for conflicts
        conflicts = []
        
        if new_data.get("quantity") != existing_data.get("quantity"):
            conflicts.append("quantity_mismatch")
        
        if new_data.get("price") != existing_data.get("price"):
            conflicts.append("price_mismatch")
        
        if new_data.get("side") != existing_data.get("side"):
            conflicts.append("side_mismatch")
        
        if new_data.get("order_type") != existing_data.get("order_type"):
            conflicts.append("order_type_mismatch")
        
        # Determine conflict severity and resolution
        if not conflicts:
            return {
                "is_conflict": False,
                "can_proceed": True,
                "reason": "No significant conflicts detected",
                "conflict_type": None,
                "severity": "none",
                "suggested_resolution": ConflictResolutionAction.USE_NEW
            }
        
        # High severity conflicts
        high_severity_conflicts = ["side_mismatch", "order_type_mismatch"]
        if any(c in high_severity_conflicts for c in conflicts):
            return {
                "is_conflict": True,
                "can_proceed": False,
                "reason": f"High severity conflict: {', '.join(conflicts)}",
                "conflict_type": "data_mismatch",
                "severity": "high",
                "suggested_resolution": ConflictResolutionAction.MANUAL_REVIEW
            }
        
        # Medium severity conflicts
        return {
            "is_conflict": True,
            "can_proceed": True,
            "reason": f"Medium severity conflict: {', '.join(conflicts)}",
            "conflict_type": "data_mismatch",
            "severity": "medium",
            "suggested_resolution": ConflictResolutionAction.USE_NEW
        }

    async def _resolve_conflict(
        self,
        idempotency_result: IdempotencyCheckResult,
        operation_type: TaggingOperation,
        order_data: Dict[str, Any],
        client_id: str
    ) -> Dict[str, Any]:
        """Resolve detected conflict based on resolution strategy."""
        
        if idempotency_result.conflict_resolution == ConflictResolutionAction.USE_EXISTING:
            return {
                "can_proceed": False,
                "resolution_action": "use_existing",
                "reason": "Using existing operation result"
            }
        
        elif idempotency_result.conflict_resolution == ConflictResolutionAction.USE_NEW:
            return {
                "can_proceed": True,
                "resolution_action": "use_new",
                "reason": "Proceeding with new operation, will update existing"
            }
        
        elif idempotency_result.conflict_resolution == ConflictResolutionAction.MANUAL_REVIEW:
            return {
                "can_proceed": False,
                "resolution_action": "manual_review",
                "reason": "Conflict requires manual review"
            }
        
        else:
            return {
                "can_proceed": False,
                "resolution_action": "reject_new",
                "reason": "Default conflict resolution - reject new operation"
            }

    async def _execute_tagging_operation(
        self,
        operation_id: str,
        operation_type: TaggingOperation,
        order_data: Dict[str, Any],
        client_id: str
    ) -> Dict[str, Any]:
        """Execute the actual tagging operation."""
        
        try:
            # Create external order tag
            order_tag = ExternalOrderTag(
                external_order_id=order_data["external_order_id"],
                broker_order_id=order_data.get("broker_order_id"),
                trading_account_id=order_data["trading_account_id"],
                symbol=order_data["symbol"],
                side=order_data["side"],
                quantity=Decimal(str(order_data["quantity"])),
                price=Decimal(str(order_data["price"])) if order_data.get("price") else None,
                order_type=order_data.get("order_type", "market"),
                status=order_data.get("status", "pending"),
                timestamp=datetime.fromisoformat(order_data.get("timestamp", datetime.now(timezone.utc).isoformat())),
                execution_id=order_data.get("execution_id"),
                strategy_id=order_data.get("strategy_id"),
                metadata=order_data.get("metadata", {})
            )
            
            # Store or update the external order tag
            if operation_type == TaggingOperation.ORDER_CREATE:
                result = await self._create_order_tag(order_tag, operation_id)
            elif operation_type == TaggingOperation.ORDER_UPDATE:
                result = await self._update_order_tag(order_tag, operation_id)
            elif operation_type == TaggingOperation.ORDER_CANCEL:
                result = await self._cancel_order_tag(order_tag, operation_id)
            else:
                result = {
                    "success": False,
                    "error": f"Unsupported operation type: {operation_type}"
                }
            
            if result["success"]:
                result["order_tag"] = order_tag
            
            return result

        except Exception as e:
            logger.error(f"Tagging operation execution failed: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "order_tag": None
            }

    async def _create_order_tag(self, order_tag: ExternalOrderTag, operation_id: str) -> Dict[str, Any]:
        """Create new external order tag."""
        
        try:
            await self.db.execute(
                text("""
                    INSERT INTO order_service.external_order_tags (
                        external_order_id,
                        broker_order_id,
                        trading_account_id,
                        symbol,
                        side,
                        quantity,
                        price,
                        order_type,
                        status,
                        order_timestamp,
                        execution_id,
                        strategy_id,
                        tag_metadata,
                        created_by_operation_id,
                        created_at
                    ) VALUES (
                        :external_order_id,
                        :broker_order_id,
                        :trading_account_id,
                        :symbol,
                        :side,
                        :quantity,
                        :price,
                        :order_type,
                        :status,
                        :order_timestamp,
                        :execution_id::uuid,
                        :strategy_id,
                        :metadata::jsonb,
                        :operation_id::uuid,
                        NOW()
                    )
                    ON CONFLICT (external_order_id, trading_account_id) DO UPDATE SET
                        updated_at = NOW(),
                        last_updated_by_operation_id = :operation_id::uuid
                """),
                {
                    "external_order_id": order_tag.external_order_id,
                    "broker_order_id": order_tag.broker_order_id,
                    "trading_account_id": order_tag.trading_account_id,
                    "symbol": order_tag.symbol,
                    "side": order_tag.side,
                    "quantity": order_tag.quantity,
                    "price": order_tag.price,
                    "order_type": order_tag.order_type,
                    "status": order_tag.status,
                    "order_timestamp": order_tag.timestamp,
                    "execution_id": order_tag.execution_id,
                    "strategy_id": order_tag.strategy_id,
                    "metadata": order_tag.metadata,
                    "operation_id": operation_id
                }
            )
            
            await self.db.commit()
            
            return {
                "success": True,
                "action": "created",
                "external_order_id": order_tag.external_order_id
            }

        except Exception as e:
            await self.db.rollback()
            logger.error(f"Failed to create order tag: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }

    async def _update_order_tag(self, order_tag: ExternalOrderTag, operation_id: str) -> Dict[str, Any]:
        """Update existing external order tag."""
        
        try:
            result = await self.db.execute(
                text("""
                    UPDATE order_service.external_order_tags
                    SET 
                        status = :status,
                        price = COALESCE(:price, price),
                        tag_metadata = :metadata::jsonb,
                        last_updated_by_operation_id = :operation_id::uuid,
                        updated_at = NOW()
                    WHERE external_order_id = :external_order_id
                      AND trading_account_id = :trading_account_id
                """),
                {
                    "external_order_id": order_tag.external_order_id,
                    "trading_account_id": order_tag.trading_account_id,
                    "status": order_tag.status,
                    "price": order_tag.price,
                    "metadata": order_tag.metadata,
                    "operation_id": operation_id
                }
            )
            
            if result.rowcount > 0:
                await self.db.commit()
                return {
                    "success": True,
                    "action": "updated",
                    "external_order_id": order_tag.external_order_id
                }
            else:
                await self.db.rollback()
                return {
                    "success": False,
                    "error": "Order tag not found for update"
                }

        except Exception as e:
            await self.db.rollback()
            logger.error(f"Failed to update order tag: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }

    async def _cancel_order_tag(self, order_tag: ExternalOrderTag, operation_id: str) -> Dict[str, Any]:
        """Cancel external order tag."""
        
        try:
            result = await self.db.execute(
                text("""
                    UPDATE order_service.external_order_tags
                    SET 
                        status = 'cancelled',
                        last_updated_by_operation_id = :operation_id::uuid,
                        updated_at = NOW()
                    WHERE external_order_id = :external_order_id
                      AND trading_account_id = :trading_account_id
                      AND status NOT IN ('cancelled', 'filled')
                """),
                {
                    "external_order_id": order_tag.external_order_id,
                    "trading_account_id": order_tag.trading_account_id,
                    "operation_id": operation_id
                }
            )
            
            if result.rowcount > 0:
                await self.db.commit()
                return {
                    "success": True,
                    "action": "cancelled",
                    "external_order_id": order_tag.external_order_id
                }
            else:
                await self.db.rollback()
                return {
                    "success": False,
                    "error": "Order tag not found or already in terminal state"
                }

        except Exception as e:
            await self.db.rollback()
            logger.error(f"Failed to cancel order tag: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }

    async def _store_operation_for_idempotency(
        self,
        operation_id: str,
        check: IdempotencyCheck,
        operation_result: Dict[str, Any],
        fingerprint: str
    ) -> None:
        """Store operation for future idempotency checking."""
        
        await self.db.execute(
            text("""
                INSERT INTO order_service.external_order_idempotency (
                    operation_id,
                    operation_type,
                    client_id,
                    idempotency_key,
                    fingerprint,
                    request_data,
                    result_data,
                    success,
                    created_at
                ) VALUES (
                    :operation_id::uuid,
                    :operation_type,
                    :client_id,
                    :idempotency_key,
                    :fingerprint,
                    :request_data::jsonb,
                    :result_data::jsonb,
                    :success,
                    NOW()
                )
            """),
            {
                "operation_id": operation_id,
                "operation_type": check.operation_type.value,
                "client_id": check.client_id,
                "idempotency_key": check.idempotency_key,
                "fingerprint": fingerprint,
                "request_data": check.request_data,
                "result_data": operation_result,
                "success": operation_result.get("success", False)
            }
        )
        
        await self.db.commit()

    def _dict_to_order_tag(self, data: Dict[str, Any]) -> Optional[ExternalOrderTag]:
        """Convert dictionary to ExternalOrderTag object."""
        
        if not data or not data.get("order_tag"):
            return None
        
        tag_data = data["order_tag"]
        
        return ExternalOrderTag(
            external_order_id=tag_data["external_order_id"],
            broker_order_id=tag_data.get("broker_order_id"),
            trading_account_id=tag_data["trading_account_id"],
            symbol=tag_data["symbol"],
            side=tag_data["side"],
            quantity=Decimal(str(tag_data["quantity"])),
            price=Decimal(str(tag_data["price"])) if tag_data.get("price") else None,
            order_type=tag_data["order_type"],
            status=tag_data["status"],
            timestamp=datetime.fromisoformat(tag_data["timestamp"]),
            execution_id=tag_data.get("execution_id"),
            strategy_id=tag_data.get("strategy_id"),
            metadata=tag_data.get("metadata", {})
        )


# Helper function for external use
async def process_external_order_with_idempotency(
    db: AsyncSession,
    operation_type: TaggingOperation,
    order_data: Dict[str, Any],
    client_id: str,
    idempotency_key: Optional[str] = None
) -> TaggingOperationResult:
    """
    Convenience function for processing external orders with idempotency.

    Args:
        db: Database session
        operation_type: Type of operation
        order_data: Order data
        client_id: Client identifier
        idempotency_key: Optional idempotency key

    Returns:
        Operation result
    """
    service = ExternalOrderTaggingIdempotency(db)
    return await service.process_external_order_operation(
        operation_type, order_data, client_id, idempotency_key
    )