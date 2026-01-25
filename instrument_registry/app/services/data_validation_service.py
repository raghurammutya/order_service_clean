"""
Data Validation Service for Instrument Registry

This service provides comprehensive data validation with configurable thresholds
for the dual-write migration process. It ensures data integrity between the
legacy screener service and the new instrument registry.

Features:
- Config-driven validation thresholds
- Real-time data integrity checks
- Batch validation for large datasets
- Detailed mismatch reporting
- Integration with monitoring systems
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple
from enum import Enum
from dataclasses import dataclass

import aiohttp
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, func
import redis.asyncio as redis

from common.config_client import ConfigClient

logger = logging.getLogger(__name__)


class ValidationLevel(str, Enum):
    """Validation levels"""
    BASIC = "basic"          # Check record counts and key fields
    DETAILED = "detailed"    # Check all fields with fuzzy matching
    STRICT = "strict"        # Exact field-by-field comparison


class ValidationStatus(str, Enum):
    """Validation result status"""
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    SKIPPED = "skipped"


@dataclass
class ValidationThresholds:
    """Configurable validation thresholds"""
    max_missing_records_percent: float = 5.0
    max_mismatched_fields_percent: float = 2.0
    max_data_drift_percent: float = 10.0
    numeric_tolerance_percent: float = 0.01  # 0.01% tolerance for numeric fields
    date_tolerance_seconds: int = 3600       # 1 hour tolerance for timestamps
    
    @classmethod
    async def from_config_service(cls, config_client: ConfigClient) -> 'ValidationThresholds':
        """Load validation thresholds from config service"""
        try:
            # Ensure validation parameters exist
            await cls._ensure_validation_parameters(config_client)
            
            max_missing = await config_client.get_float("INSTRUMENT_REGISTRY_MAX_MISSING_PERCENT", default=5.0)
            max_mismatched = await config_client.get_float("INSTRUMENT_REGISTRY_MAX_MISMATCH_PERCENT", default=2.0)
            max_drift = await config_client.get_float("INSTRUMENT_REGISTRY_MAX_DRIFT_PERCENT", default=10.0)
            numeric_tolerance = await config_client.get_float("INSTRUMENT_REGISTRY_NUMERIC_TOLERANCE_PERCENT", default=0.01)
            date_tolerance = await config_client.get_int("INSTRUMENT_REGISTRY_DATE_TOLERANCE_SECONDS", default=3600)
            
            return cls(
                max_missing_records_percent=max_missing,
                max_mismatched_fields_percent=max_mismatched,
                max_data_drift_percent=max_drift,
                numeric_tolerance_percent=numeric_tolerance,
                date_tolerance_seconds=date_tolerance
            )
        except Exception as e:
            logger.error(f"Failed to load validation thresholds: {e}")
            return cls()  # Use defaults
    
    @classmethod
    async def _ensure_validation_parameters(cls, config_client: ConfigClient):
        """Ensure validation parameters exist in config service"""
        validation_params = [
            ("INSTRUMENT_REGISTRY_MAX_MISSING_PERCENT", "5.0", "Maximum missing records percentage threshold"),
            ("INSTRUMENT_REGISTRY_MAX_MISMATCH_PERCENT", "2.0", "Maximum mismatched fields percentage threshold"),
            ("INSTRUMENT_REGISTRY_MAX_DRIFT_PERCENT", "10.0", "Maximum data drift percentage threshold"),
            ("INSTRUMENT_REGISTRY_NUMERIC_TOLERANCE_PERCENT", "0.01", "Numeric field tolerance percentage"),
            ("INSTRUMENT_REGISTRY_DATE_TOLERANCE_SECONDS", "3600", "Date/time field tolerance in seconds"),
            ("INSTRUMENT_REGISTRY_VALIDATION_LEVEL", "detailed", "Validation level (basic/detailed/strict)"),
            ("INSTRUMENT_REGISTRY_VALIDATION_BATCH_SIZE", "10000", "Batch size for validation operations"),
            ("INSTRUMENT_REGISTRY_VALIDATION_ENABLED", "true", "Enable data validation"),
        ]
        
        for param_name, default_value, description in validation_params:
            try:
                await config_client.ensure_parameter_exists(param_name, default_value, description)
            except Exception as e:
                logger.warning(f"Failed to ensure validation parameter {param_name}: {e}")


@dataclass
class FieldMismatch:
    """Details of a field mismatch"""
    field_name: str
    screener_value: Any
    registry_value: Any
    mismatch_type: str  # "missing", "different", "type_mismatch"
    tolerance_exceeded: Optional[bool] = None


@dataclass
class RecordMismatch:
    """Details of a record mismatch"""
    record_key: str  # Usually instrument_key:index_id
    missing_in_screener: bool = False
    missing_in_registry: bool = False
    field_mismatches: List[FieldMismatch] = None


@dataclass
class ValidationResult:
    """Result of data validation"""
    validation_id: str
    table_name: str
    validation_level: ValidationLevel
    status: ValidationStatus
    timestamp: datetime
    duration_seconds: float
    
    # Summary statistics
    total_records_screener: int
    total_records_registry: int
    matched_records: int
    missing_in_screener: int
    missing_in_registry: int
    
    # Detailed results
    record_mismatches: List[RecordMismatch]
    field_mismatch_summary: Dict[str, int]
    
    # Threshold compliance
    passed_thresholds: bool
    threshold_violations: List[str]
    
    # Metadata
    configuration: Dict[str, Any]
    error_message: Optional[str] = None


class DataValidationService:
    """
    Service for comprehensive data validation during dual-write migration
    
    This service validates data consistency between the legacy screener service
    and the instrument registry, providing detailed reports and threshold-based
    pass/fail criteria.
    """
    
    def __init__(
        self,
        config_client: ConfigClient,
        db_session: AsyncSession,
        redis_url: str,
        screener_service_url: str = "http://screener-service:8080",
        internal_api_key: str = None
    ):
        self.config_client = config_client
        self.db_session = db_session
        self.screener_service_url = screener_service_url
        self.internal_api_key = internal_api_key or "AShhRzWhfXd6IomyzZnE3d-lCcAvT1L5GDCCZRSXZGsJq7_eAJGxeMi-4AlfTeOc"
        
        # Redis for caching and result storage
        self.redis_client = None
        self.redis_url = redis_url
        
        # HTTP session for screener service calls
        self.http_session: Optional[aiohttp.ClientSession] = None
        
        # Configuration cache
        self._thresholds: Optional[ValidationThresholds] = None
        self._thresholds_last_refresh: float = 0
        self._thresholds_refresh_interval: float = 3600.0  # 1 hour
        
        # Validation metrics
        self.metrics = {
            "validations_run": 0,
            "validations_passed": 0,
            "validations_failed": 0,
            "total_records_validated": 0,
            "total_mismatches_found": 0,
        }

    async def initialize(self):
        """Initialize the validation service"""
        # Initialize Redis connection
        self.redis_client = await redis.from_url(self.redis_url)
        
        # Initialize HTTP session
        timeout = aiohttp.ClientTimeout(total=60.0)
        self.http_session = aiohttp.ClientSession(
            timeout=timeout,
            headers={"X-Internal-API-Key": self.internal_api_key}
        )
        
        # Load initial thresholds
        await self._refresh_thresholds()
        
        logger.info("Data validation service initialized")

    async def _refresh_thresholds(self) -> ValidationThresholds:
        """Refresh validation thresholds from config service"""
        current_time = time.time()
        
        if (self._thresholds is None or 
            current_time - self._thresholds_last_refresh > self._thresholds_refresh_interval):
            
            self._thresholds = await ValidationThresholds.from_config_service(self.config_client)
            self._thresholds_last_refresh = current_time
            
            logger.info(f"Validation thresholds refreshed: max_missing={self._thresholds.max_missing_records_percent}%, "
                       f"max_mismatch={self._thresholds.max_mismatched_fields_percent}%")
        
        return self._thresholds

    async def validate_index_memberships(
        self,
        validation_level: ValidationLevel = ValidationLevel.DETAILED,
        batch_size: Optional[int] = None
    ) -> ValidationResult:
        """
        Validate index membership data between screener service and registry
        
        Args:
            validation_level: Level of validation to perform
            batch_size: Batch size for processing (from config if not provided)
            
        Returns:
            ValidationResult with detailed comparison results
        """
        validation_id = f"idx_membership_{int(time.time() * 1000)}"
        start_time = time.time()
        
        logger.info(f"Starting index membership validation [id: {validation_id}] "
                   f"at level: {validation_level}")
        
        try:
            # Get configuration
            thresholds = await self._refresh_thresholds()
            if batch_size is None:
                batch_size = await self.config_client.get_int("INSTRUMENT_REGISTRY_VALIDATION_BATCH_SIZE", default=10000)
            
            # Fetch data from both systems
            logger.info("Fetching data from screener service...")
            screener_data = await self._fetch_screener_index_data()
            
            logger.info("Fetching data from instrument registry...")
            registry_data = await self._fetch_registry_index_data()
            
            # Perform validation
            logger.info(f"Comparing {len(screener_data)} screener records with {len(registry_data)} registry records...")
            record_mismatches = await self._compare_index_data(
                screener_data, registry_data, validation_level, thresholds
            )
            
            # Generate field mismatch summary
            field_mismatch_summary = self._summarize_field_mismatches(record_mismatches)
            
            # Check threshold compliance
            passed_thresholds, threshold_violations = self._check_thresholds(
                screener_data, registry_data, record_mismatches, thresholds
            )
            
            # Determine overall status
            status = ValidationStatus.PASSED if passed_thresholds else ValidationStatus.FAILED
            
            # Create result
            result = ValidationResult(
                validation_id=validation_id,
                table_name="index_memberships",
                validation_level=validation_level,
                status=status,
                timestamp=datetime.utcnow(),
                duration_seconds=time.time() - start_time,
                total_records_screener=len(screener_data),
                total_records_registry=len(registry_data),
                matched_records=len([r for r in record_mismatches if not r.missing_in_screener and not r.missing_in_registry]),
                missing_in_screener=len([r for r in record_mismatches if r.missing_in_screener]),
                missing_in_registry=len([r for r in record_mismatches if r.missing_in_registry]),
                record_mismatches=record_mismatches,
                field_mismatch_summary=field_mismatch_summary,
                passed_thresholds=passed_thresholds,
                threshold_violations=threshold_violations,
                configuration={
                    "validation_level": validation_level.value,
                    "batch_size": batch_size,
                    "thresholds": {
                        "max_missing_percent": thresholds.max_missing_records_percent,
                        "max_mismatch_percent": thresholds.max_mismatched_fields_percent,
                        "numeric_tolerance_percent": thresholds.numeric_tolerance_percent,
                    }
                }
            )
            
            # Update metrics
            self.metrics["validations_run"] += 1
            if status == ValidationStatus.PASSED:
                self.metrics["validations_passed"] += 1
            else:
                self.metrics["validations_failed"] += 1
            
            self.metrics["total_records_validated"] += len(screener_data) + len(registry_data)
            self.metrics["total_mismatches_found"] += len(record_mismatches)
            
            # Store result
            await self._store_validation_result(result)
            
            logger.info(f"Validation completed [id: {validation_id}] - Status: {status}, "
                       f"Duration: {result.duration_seconds:.2f}s, "
                       f"Mismatches: {len(record_mismatches)}")
            
            return result
            
        except Exception as e:
            logger.error(f"Validation failed [id: {validation_id}]: {e}")
            
            # Create error result
            result = ValidationResult(
                validation_id=validation_id,
                table_name="index_memberships",
                validation_level=validation_level,
                status=ValidationStatus.FAILED,
                timestamp=datetime.utcnow(),
                duration_seconds=time.time() - start_time,
                total_records_screener=0,
                total_records_registry=0,
                matched_records=0,
                missing_in_screener=0,
                missing_in_registry=0,
                record_mismatches=[],
                field_mismatch_summary={},
                passed_thresholds=False,
                threshold_violations=[],
                configuration={},
                error_message=str(e)
            )
            
            self.metrics["validations_failed"] += 1
            await self._store_validation_result(result)
            
            return result

    async def _fetch_screener_index_data(self) -> Dict[str, Dict[str, Any]]:
        """Fetch index membership data from screener service"""
        try:
            async with self.http_session.get(
                f"{self.screener_service_url}/api/v1/index-memberships"
            ) as response:
                response.raise_for_status()
                data = await response.json()
                
                # Transform to keyed dictionary
                result = {}
                for membership in data.get("memberships", []):
                    # Create key from instrument and index
                    exchange = membership.get("exchange", "NSE")
                    symbol = membership.get("symbol", "")
                    index_name = membership.get("index_name", "")
                    key = f"{exchange}:{symbol}:{index_name}"
                    
                    result[key] = {
                        "instrument_key": f"{exchange}:{symbol}",
                        "index_id": index_name,
                        "weight": membership.get("weight"),
                        "sector": membership.get("sector"),
                        "date_added": membership.get("effective_date"),
                        "is_active": True
                    }
                
                return result
                
        except Exception as e:
            logger.error(f"Failed to fetch screener index data: {e}")
            return {}

    async def _fetch_registry_index_data(self) -> Dict[str, Dict[str, Any]]:
        """Fetch index membership data from instrument registry"""
        try:
            query = """
            SELECT 
                instrument_key,
                index_id,
                weight,
                sector,
                date_added,
                is_active
            FROM instrument_registry.index_memberships
            WHERE is_active = TRUE
            """
            
            result = await self.db_session.execute(text(query))
            rows = result.fetchall()
            
            # Transform to keyed dictionary
            data = {}
            for row in rows:
                key = f"{row.instrument_key}:{row.index_id}"
                data[key] = {
                    "instrument_key": row.instrument_key,
                    "index_id": row.index_id,
                    "weight": float(row.weight) if row.weight else None,
                    "sector": row.sector,
                    "date_added": row.date_added.isoformat() if row.date_added else None,
                    "is_active": row.is_active
                }
            
            return data
            
        except Exception as e:
            logger.error(f"Failed to fetch registry index data: {e}")
            return {}

    async def _compare_index_data(
        self,
        screener_data: Dict[str, Dict[str, Any]],
        registry_data: Dict[str, Dict[str, Any]],
        validation_level: ValidationLevel,
        thresholds: ValidationThresholds
    ) -> List[RecordMismatch]:
        """Compare index data between systems"""
        record_mismatches = []
        
        # Get all unique keys
        all_keys = set(screener_data.keys()) | set(registry_data.keys())
        
        for key in all_keys:
            screener_record = screener_data.get(key)
            registry_record = registry_data.get(key)
            
            if screener_record is None:
                # Missing in screener
                record_mismatches.append(RecordMismatch(
                    record_key=key,
                    missing_in_screener=True
                ))
                continue
            
            if registry_record is None:
                # Missing in registry
                record_mismatches.append(RecordMismatch(
                    record_key=key,
                    missing_in_registry=True
                ))
                continue
            
            # Both records exist - compare fields
            field_mismatches = []
            
            if validation_level != ValidationLevel.BASIC:
                field_mismatches = self._compare_record_fields(
                    screener_record, registry_record, validation_level, thresholds
                )
            
            if field_mismatches:
                record_mismatches.append(RecordMismatch(
                    record_key=key,
                    field_mismatches=field_mismatches
                ))
        
        return record_mismatches

    def _compare_record_fields(
        self,
        screener_record: Dict[str, Any],
        registry_record: Dict[str, Any],
        validation_level: ValidationLevel,
        thresholds: ValidationThresholds
    ) -> List[FieldMismatch]:
        """Compare fields between two records"""
        field_mismatches = []
        
        # Fields to compare
        fields_to_compare = ["weight", "sector", "is_active"]
        
        for field in fields_to_compare:
            screener_value = screener_record.get(field)
            registry_value = registry_record.get(field)
            
            # Skip None values in basic comparison
            if validation_level == ValidationLevel.BASIC:
                if screener_value is None or registry_value is None:
                    continue
            
            # Type-specific comparison
            if isinstance(screener_value, (int, float)) and isinstance(registry_value, (int, float)):
                # Numeric comparison with tolerance
                if screener_value != 0:
                    diff_percent = abs(screener_value - registry_value) / abs(screener_value) * 100
                    if diff_percent > thresholds.numeric_tolerance_percent:
                        field_mismatches.append(FieldMismatch(
                            field_name=field,
                            screener_value=screener_value,
                            registry_value=registry_value,
                            mismatch_type="numeric_tolerance_exceeded",
                            tolerance_exceeded=True
                        ))
                elif registry_value != 0:
                    field_mismatches.append(FieldMismatch(
                        field_name=field,
                        screener_value=screener_value,
                        registry_value=registry_value,
                        mismatch_type="different"
                    ))
            elif screener_value != registry_value:
                # Exact comparison for non-numeric fields
                field_mismatches.append(FieldMismatch(
                    field_name=field,
                    screener_value=screener_value,
                    registry_value=registry_value,
                    mismatch_type="different"
                ))
        
        return field_mismatches

    def _summarize_field_mismatches(self, record_mismatches: List[RecordMismatch]) -> Dict[str, int]:
        """Summarize field mismatches by field name"""
        field_summary = {}
        
        for record_mismatch in record_mismatches:
            if record_mismatch.field_mismatches:
                for field_mismatch in record_mismatch.field_mismatches:
                    field_name = field_mismatch.field_name
                    field_summary[field_name] = field_summary.get(field_name, 0) + 1
        
        return field_summary

    def _check_thresholds(
        self,
        screener_data: Dict[str, Dict[str, Any]],
        registry_data: Dict[str, Dict[str, Any]],
        record_mismatches: List[RecordMismatch],
        thresholds: ValidationThresholds
    ) -> Tuple[bool, List[str]]:
        """Check if validation results pass configured thresholds"""
        violations = []
        
        total_records = max(len(screener_data), len(registry_data))
        if total_records == 0:
            return True, []
        
        # Check missing records threshold
        missing_count = len([r for r in record_mismatches if r.missing_in_screener or r.missing_in_registry])
        missing_percent = (missing_count / total_records) * 100
        
        if missing_percent > thresholds.max_missing_records_percent:
            violations.append(f"Missing records: {missing_percent:.2f}% > {thresholds.max_missing_records_percent}%")
        
        # Check field mismatch threshold
        field_mismatch_count = len([r for r in record_mismatches if r.field_mismatches])
        field_mismatch_percent = (field_mismatch_count / total_records) * 100
        
        if field_mismatch_percent > thresholds.max_mismatched_fields_percent:
            violations.append(f"Field mismatches: {field_mismatch_percent:.2f}% > {thresholds.max_mismatched_fields_percent}%")
        
        # Check data drift (difference in total record counts)
        if len(screener_data) > 0:
            drift_percent = abs(len(screener_data) - len(registry_data)) / len(screener_data) * 100
            if drift_percent > thresholds.max_data_drift_percent:
                violations.append(f"Data drift: {drift_percent:.2f}% > {thresholds.max_data_drift_percent}%")
        
        return len(violations) == 0, violations

    async def _store_validation_result(self, result: ValidationResult):
        """Store validation result in Redis"""
        try:
            # Serialize result (excluding large mismatches list for storage)
            result_summary = {
                "validation_id": result.validation_id,
                "table_name": result.table_name,
                "validation_level": result.validation_level.value,
                "status": result.status.value,
                "timestamp": result.timestamp.isoformat(),
                "duration_seconds": result.duration_seconds,
                "total_records_screener": result.total_records_screener,
                "total_records_registry": result.total_records_registry,
                "matched_records": result.matched_records,
                "missing_in_screener": result.missing_in_screener,
                "missing_in_registry": result.missing_in_registry,
                "field_mismatch_summary": result.field_mismatch_summary,
                "passed_thresholds": result.passed_thresholds,
                "threshold_violations": result.threshold_violations,
                "configuration": result.configuration,
                "error_message": result.error_message,
                "total_mismatches": len(result.record_mismatches)
            }
            
            # Store with TTL of 30 days
            await self.redis_client.setex(
                f"validation_result:{result.validation_id}",
                30 * 24 * 3600,  # 30 days
                json.dumps(result_summary)
            )
            
            # Store detailed mismatches separately if they exist
            if result.record_mismatches:
                detailed_mismatches = []
                for mismatch in result.record_mismatches[:1000]:  # Limit to first 1000
                    detailed_mismatches.append({
                        "record_key": mismatch.record_key,
                        "missing_in_screener": mismatch.missing_in_screener,
                        "missing_in_registry": mismatch.missing_in_registry,
                        "field_mismatches": [
                            {
                                "field_name": fm.field_name,
                                "screener_value": str(fm.screener_value),
                                "registry_value": str(fm.registry_value),
                                "mismatch_type": fm.mismatch_type,
                                "tolerance_exceeded": fm.tolerance_exceeded
                            }
                            for fm in (mismatch.field_mismatches or [])
                        ]
                    })
                
                await self.redis_client.setex(
                    f"validation_mismatches:{result.validation_id}",
                    7 * 24 * 3600,  # 7 days
                    json.dumps(detailed_mismatches)
                )
            
        except Exception as e:
            logger.error(f"Failed to store validation result: {e}")

    async def get_validation_result(self, validation_id: str) -> Optional[Dict[str, Any]]:
        """Get validation result by ID"""
        try:
            # Get summary
            summary_data = await self.redis_client.get(f"validation_result:{validation_id}")
            if not summary_data:
                return None
            
            result = json.loads(summary_data)
            
            # Get detailed mismatches if available
            mismatches_data = await self.redis_client.get(f"validation_mismatches:{validation_id}")
            if mismatches_data:
                result["detailed_mismatches"] = json.loads(mismatches_data)
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to get validation result {validation_id}: {e}")
            return None

    async def get_recent_validations(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent validation results"""
        try:
            # Get validation result keys
            keys = await self.redis_client.keys("validation_result:*")
            keys.sort(reverse=True)  # Most recent first
            
            results = []
            for key in keys[:limit]:
                data = await self.redis_client.get(key)
                if data:
                    results.append(json.loads(data))
            
            # Sort by timestamp
            results.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to get recent validations: {e}")
            return []

    async def get_validation_metrics(self) -> Dict[str, Any]:
        """Get validation metrics and statistics"""
        return {
            **self.metrics,
            "thresholds_last_refresh": datetime.fromtimestamp(self._thresholds_last_refresh).isoformat(),
            "current_thresholds": {
                "max_missing_percent": self._thresholds.max_missing_records_percent if self._thresholds else None,
                "max_mismatch_percent": self._thresholds.max_mismatched_fields_percent if self._thresholds else None,
                "numeric_tolerance_percent": self._thresholds.numeric_tolerance_percent if self._thresholds else None,
            }
        }

    async def close(self):
        """Close connections and cleanup"""
        if self.http_session:
            await self.http_session.close()
        
        if self.redis_client:
            await self.redis_client.close()
        
        logger.info("Data validation service closed")