# Sprint 7A & 7B Test Suite

Comprehensive test suite for Sprint 7A: Manual Trading Reconciliation & Handoff functionality and Sprint 7B: Manual Trading Reconciliation Edge Cases.

## Test Structure

```
tests/
├── conftest.py                          # Shared fixtures and configuration
├── README.md                           # This file
├── services/                           # Unit tests for service layer
│   ├── test_default_portfolio_service.py
│   ├── test_partial_exit_attribution_service.py
│   ├── test_manual_attribution_service.py
│   ├── test_handoff_state_machine.py
│   ├── test_reconciliation_driven_transfers.py
│   ├── test_holdings_reconciliation_integration.py
│   # Sprint 7B Edge Cases Tests
│   ├── test_exit_attribution_policy.py
│   ├── test_manual_attribution_apply_validator.py
│   ├── test_exit_context_matcher.py
│   ├── test_transfer_instruction_generator.py
│   ├── test_redis_unavailable_handoff_manager.py
│   ├── test_handoff_concurrency_manager.py
│   ├── test_missing_trade_history_handler.py
│   └── test_external_order_tagging_idempotency.py
├── api/                               # API endpoint tests
│   └── test_manual_attribution_endpoints.py
└── integration/                       # End-to-end integration tests
    ├── test_sprint_7a_end_to_end.py
    └── test_sprint_7b_edge_cases_integration.py
```

## Test Categories

### Unit Tests (`tests/services/`)
Tests individual service components in isolation with mocked dependencies.

- **Default Portfolio Service**: Tests orphan position detection and portfolio mapping
- **Partial Exit Attribution**: Tests FIFO/LIFO/proportional allocation algorithms  
- **Manual Attribution**: Tests case management, assignment, and resolution workflow
- **Handoff State Machine**: Tests state transitions between manual and script control
- **Reconciliation-Driven Transfers**: Tests position transfer execution
- **Holdings Reconciliation**: Tests variance detection and resolution

#### Sprint 7B Edge Cases Tests

- **Exit Attribution Policy**: Tests explicit policy enforcement for single vs multi-strategy scenarios
- **Manual Attribution Apply Validator**: Tests pre-apply validation with position and transfer safety checks
- **Exit Context Matcher**: Tests robust matching with multi-fill handling and tolerance algorithms
- **Transfer Instruction Generator**: Tests comprehensive transfer instruction types with priority rules
- **Redis Unavailable Handoff Manager**: Tests safe pending states and retry logic when Redis is down
- **Handoff Concurrency Manager**: Tests concurrency safety with distributed locking and conflict resolution
- **Missing Trade History Handler**: Tests gap detection and trade reconstruction strategies
- **External Order Tagging Idempotency**: Tests duplicate detection and idempotent operation handling

### API Tests (`tests/api/`)
Tests REST API endpoints with proper authentication and validation.

- **Manual Attribution Endpoints**: Tests case CRUD operations, assignment, resolution

### Integration Tests (`tests/integration/`)
Tests complete workflows with multiple services working together.

- **End-to-End Workflows**: Tests complete reconciliation and handoff scenarios

## Running Tests

### Prerequisites
```bash
# Install test dependencies
pip install pytest pytest-asyncio pytest-mock

# Install Sprint 7A dependencies  
pip install -r requirements.txt
```

### Run All Tests
```bash
# Run complete test suite
pytest order_service/tests/

# Run with coverage report
pytest order_service/tests/ --cov=order_service --cov-report=html
```

### Run Specific Test Categories
```bash
# Unit tests only
pytest order_service/tests/services/

# API tests only  
pytest order_service/tests/api/

# Integration tests only
pytest order_service/tests/integration/

# Specific service tests
pytest order_service/tests/services/test_partial_exit_attribution_service.py
```

### Run with Markers
```bash
# Run only fast tests (exclude slow/performance tests)
pytest -m "not slow"

# Run integration tests
pytest -m "integration"

# Run performance tests (if enabled)
RUN_SLOW_TESTS=1 pytest -m "performance"
```

### Debug Mode
```bash
# Run with verbose output and no capture
pytest -v -s order_service/tests/

# Run specific test with debugging
pytest -v -s order_service/tests/services/test_manual_attribution_service.py::TestManualAttributionService::test_create_attribution_case_success
```

## Test Fixtures

### Common Fixtures (from `conftest.py`)
- `mock_db_session`: Mocked async database session
- `sample_trading_account`: Sample trading account data
- `sample_strategies`: Sample strategy configurations
- `sample_positions`: Sample position data with relationships
- `sample_orders`: Sample order data
- `mock_auth_context`: Mocked authentication context
- `test_data_factory`: Factory for creating test data
- `sprint7a_assertions`: Custom assertions for Sprint 7A

### Using Fixtures
```python
async def test_my_function(mock_db_session, sample_positions, sprint7a_assertions):
    service = MyService(mock_db_session)
    result = await service.process_positions(sample_positions)
    sprint7a_assertions.assert_allocation_result_valid(result)
```

## Test Data

### Sample Data Structure
Test fixtures provide realistic data with proper relationships:
- Trading accounts linked to users
- Strategies with execution IDs
- Positions with strategy/execution/portfolio relationships
- Orders in various states (pending, complete, etc.)
- Entry trades for attribution testing

### Creating Custom Test Data
```python
def test_with_custom_data(test_data_factory):
    # Create position data
    position = test_data_factory.create_position_data(
        symbol="AAPL",
        quantity=100,
        strategy_id=1,
        execution_id="custom_exec"
    )
    
    # Create attribution case data
    case_data = test_data_factory.create_attribution_case_data(
        trading_account_id="acc_001",
        symbol="AAPL", 
        exit_quantity=Decimal("75"),
        affected_positions=[position]
    )
```

## Mocking Strategies

### Database Mocking
```python
async def test_database_operation(mock_db_session, db_mock_helper):
    # Mock SELECT result
    mock_result = db_mock_helper.mock_select_result([
        ("pos_1", "AAPL", 100, 1, "exec_1")
    ])
    mock_db_session.execute.return_value = mock_result
    
    # Test service operation
    service = MyService(mock_db_session)
    result = await service.get_positions()
    
    assert len(result) == 1
    mock_db_session.execute.assert_called_once()
```

### Service Mocking
```python
async def test_service_integration():
    with patch('module.ServiceA') as mock_service_a:
        mock_service_a.return_value.process.return_value = expected_result
        
        service_b = ServiceB()
        result = await service_b.use_service_a()
        
        mock_service_a.assert_called_once()
        assert result == expected_result
```

## Testing Patterns

### Async Testing
```python
@pytest.mark.asyncio
async def test_async_function(mock_db_session):
    service = AsyncService(mock_db_session)
    result = await service.async_operation()
    assert result is not None
```

### Parametrized Testing
```python
@pytest.mark.parametrize("quantity,expected_allocations", [
    (100, 1),
    (250, 2), 
    (500, 3)
])
async def test_allocation_scenarios(quantity, expected_allocations, mock_db_session):
    service = AttributionService(mock_db_session)
    result = await service.allocate(quantity)
    assert len(result.allocations) == expected_allocations
```

### Exception Testing
```python
async def test_error_handling(mock_db_session):
    mock_db_session.execute.side_effect = Exception("Database error")
    
    service = MyService(mock_db_session)
    with pytest.raises(Exception) as exc_info:
        await service.failing_operation()
    
    assert "Database error" in str(exc_info.value)
    mock_db_session.rollback.assert_called()
```

## Performance Testing

### Timing Tests
```python
async def test_performance(performance_timer, mock_db_session):
    performance_timer.start()
    
    service = MyService(mock_db_session)
    result = await service.large_operation()
    
    performance_timer.stop()
    assert performance_timer.elapsed_seconds < 5.0  # Under 5 seconds
```

### Memory Testing
```python
@pytest.mark.performance
async def test_memory_usage():
    # Test with large datasets
    large_positions = [create_position(i) for i in range(10000)]
    
    # Monitor memory usage during operation
    import tracemalloc
    tracemalloc.start()
    
    result = await process_large_dataset(large_positions)
    
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    assert peak < 100 * 1024 * 1024  # Less than 100MB
```

## Integration Testing

### Database Integration
```python
@pytest.mark.integration
async def test_real_database_integration(integration_test_config):
    # Skip if no database available
    if not database_available():
        pytest.skip("Database not available")
    
    # Use real database connection
    async with create_test_database() as db:
        service = MyService(db)
        result = await service.real_operation()
        assert result is not None
```

### Service Integration
```python
@pytest.mark.integration  
async def test_service_chain_integration():
    # Test multiple services working together
    db = create_test_db()
    
    # Chain of operations
    portfolio_service = DefaultPortfolioService(db)
    attribution_service = PartialExitAttributionService(db)
    transfer_service = ReconciliationDrivenTransferService(db)
    
    # Execute workflow
    orphans = await portfolio_service.get_orphan_positions("acc_001")
    allocation = await attribution_service.attribute_exit(orphans[0])
    transfer = await transfer_service.execute_transfers(allocation)
    
    assert transfer.success
```

## Debugging Tests

### Logging in Tests
```python
import logging

def test_with_logging(caplog):
    with caplog.at_level(logging.INFO):
        result = my_function_that_logs()
    
    assert "Expected log message" in caplog.text
    assert result is not None
```

### Debugging Async Tests
```python
import asyncio

async def test_async_debugging():
    # Enable asyncio debug mode
    asyncio.get_event_loop().set_debug(True)
    
    # Test async operation
    result = await async_operation()
    assert result is not None
```

## Test Maintenance

### Updating Test Data
When schema changes, update fixtures in `conftest.py`:
1. Update `sample_positions` structure
2. Update database mock helpers
3. Update custom assertions
4. Run tests to verify compatibility

### Adding New Tests
1. Follow naming convention: `test_<functionality>_<scenario>`
2. Use appropriate fixtures from `conftest.py`
3. Mock external dependencies
4. Add integration tests for new workflows
5. Update this README with new test patterns

### Test Quality Guidelines
- Each test should test one specific behavior
- Use descriptive test names that explain the scenario
- Mock external dependencies consistently  
- Verify both success and failure cases
- Test edge cases and boundary conditions
- Use custom assertions for domain-specific validations

## CI/CD Integration

### GitHub Actions Example
```yaml
name: Sprint 7A Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Setup Python
        uses: actions/setup-python@v2
        with:
          python-version: 3.9
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run tests
        run: |
          pytest order_service/tests/ \
            --cov=order_service \
            --cov-report=xml \
            --junitxml=test-results.xml
      - name: Upload coverage
        uses: codecov/codecov-action@v1
```

### Test Reports
```bash
# Generate HTML coverage report
pytest --cov=order_service --cov-report=html order_service/tests/

# Generate JUnit XML for CI
pytest --junitxml=test-results.xml order_service/tests/

# Generate performance report  
pytest --benchmark-json=benchmark.json order_service/tests/
```

## Sprint 7B Edge Cases Test Coverage

Sprint 7B implements edge case handling for manual trading reconciliation. Tests are mapped to specific requirements (GAP-REC-8 through GAP-REC-16) with 80 target test cases.

### Test Coverage Mapping

#### GAP-REC-8: Explicit Exit Attribution Policy
**File**: `test_exit_attribution_policy.py`
**Target**: 10 test cases

| Test Case | Description | Exit Criteria |
|-----------|-------------|---------------|
| `test_single_strategy_auto_approval` | Single strategy position → auto-approve | Policy = AUTO_SINGLE_STRATEGY, decision = AUTO_APPROVED |
| `test_multi_strategy_full_exit_auto` | Multi-strategy full exit → auto-approve FIFO | Policy = AUTO_MULTI_FULL, FIFO allocation |
| `test_multi_strategy_partial_manual` | Multi-strategy partial exit → manual required | Policy = MANUAL_MULTI_PARTIAL, requires manual |
| `test_insufficient_data_blocked` | Missing position data → blocked | Policy = BLOCKED_INSUFFICIENT_DATA |
| `test_ambiguous_context_manual` | Unclear attribution context → manual | Policy = MANUAL_AMBIGUOUS |
| `test_policy_override_enforcement` | Override policy applied correctly | Policy override respected |
| `test_audit_trail_generation` | Policy decisions create audit trails | Audit data captured |
| `test_quantity_mismatch_handling` | Exit > available quantity handling | Error handling + manual escalation |
| `test_concurrent_policy_evaluation` | Thread-safe policy evaluation | No race conditions |
| `test_policy_performance_metrics` | Policy evaluation performance | < 100ms per evaluation |

#### GAP-REC-9: Manual Attribution Apply Validation
**File**: `test_manual_attribution_apply_validator.py`  
**Target**: 8 test cases

| Test Case | Description | Exit Criteria |
|-----------|-------------|---------------|
| `test_position_availability_validation` | Validate positions exist and sufficient quantity | Critical errors for missing/insufficient positions |
| `test_execution_context_validation` | Validate execution contexts exist and valid state | Errors for missing/invalid execution contexts |
| `test_transfer_safety_checks` | Pre-transfer safety validation | Safety check failures prevent application |
| `test_data_consistency_validation` | Cross-validate attribution decision consistency | Inconsistency errors detected |
| `test_allocation_quantity_matching` | Total allocation matches exit quantity | Quantity mismatch validation |
| `test_validation_recommendation_generation` | Generate actionable recommendations | APPROVE/CAUTION/REJECT recommendations |
| `test_validation_error_categorization` | Categorize errors by severity | CRITICAL/HIGH/MEDIUM/LOW severity levels |
| `test_concurrent_validation_safety` | Thread-safe validation operations | No concurrent validation conflicts |

#### GAP-REC-10: Exit Context Matching Robustness
**File**: `test_exit_context_matcher.py`
**Target**: 12 test cases

| Test Case | Description | Exit Criteria |
|-----------|-------------|---------------|
| `test_exact_broker_trade_id_match` | Perfect match via broker trade ID | MatchQuality.EXACT |
| `test_multi_fill_order_aggregation` | Aggregate multiple fills into single match | Multi-fill detection + aggregation |
| `test_quantity_tolerance_matching` | Match within configurable quantity tolerance | 0.1% default tolerance |
| `test_price_tolerance_matching` | Match within configurable price tolerance | 0.5% default tolerance |
| `test_timestamp_tolerance_matching` | Match within configurable time window | 15-minute default window |
| `test_delayed_broker_data_handling` | Handle broker data delayed up to 24 hours | Extended time window matching |
| `test_fuzzy_matching_fallback` | Fuzzy matching when exact fails | Confidence scoring |
| `test_no_match_scenarios` | Proper handling when no match found | NO_MATCH result with reasons |
| `test_conflicting_matches_resolution` | Handle multiple potential matches | Best match selection algorithm |
| `test_match_confidence_scoring` | Score match quality (0.0-1.0) | Confidence thresholds enforced |
| `test_broker_order_id_fallback` | Use order ID when trade ID unavailable | Fallback matching strategy |
| `test_symbol_normalization` | Handle symbol variations | Consistent symbol matching |

#### GAP-REC-11: Transfer Instruction Completeness
**File**: `test_transfer_instruction_generator.py`
**Target**: 10 test cases

| Test Case | Description | Exit Criteria |
|-----------|-------------|---------------|
| `test_position_transfer_instructions` | Generate position transfer instructions | POSITION_TRANSFER type created |
| `test_execution_handoff_instructions` | Generate execution handoff instructions | EXECUTION_HANDOFF type created |
| `test_portfolio_reallocation_instructions` | Generate portfolio reallocation | PORTFOLIO_REALLOCATION type created |
| `test_attribution_correction_instructions` | Generate attribution correction | ATTRIBUTION_CORRECTION type created |
| `test_emergency_extraction_instructions` | Generate emergency extraction | EMERGENCY_EXTRACTION type created |
| `test_priority_level_assignment` | Assign correct priority levels | CRITICAL=10, HIGH=20, etc. |
| `test_transfer_instruction_validation` | Validate generated instructions | Validation errors caught |
| `test_batch_transfer_generation` | Generate instruction batches | Atomic/parallel batch options |
| `test_dependency_chain_creation` | Create instruction dependencies | depends_on/blocks_instructions |
| `test_rollback_instruction_generation` | Generate rollback instructions | Safe rollback capability |

#### GAP-REC-12: Redis Unavailable Handoff Management
**File**: `test_redis_unavailable_handoff_manager.py`
**Target**: 10 test cases

| Test Case | Description | Exit Criteria |
|-----------|-------------|---------------|
| `test_redis_availability_detection` | Detect Redis unavailability | Timeout/connection error detection |
| `test_database_coordination_fallback` | Fall back to database coordination | Database locking mechanism |
| `test_safe_pending_state_creation` | Create persistent pending state | Pending handoff stored in DB |
| `test_exponential_backoff_retry` | Implement exponential backoff | 30s, 60s, 120s... retry intervals |
| `test_retry_queue_processing` | Process pending retry queue | Batch retry processing |
| `test_handoff_state_persistence` | Persist handoff coordination state | State survives process restarts |
| `test_coordination_timeout_handling` | Handle coordination timeouts | Timeout escalation to manual |
| `test_retry_limit_enforcement` | Enforce maximum retry attempts | Max 10 attempts before manual |
| `test_jitter_in_retry_timing` | Add jitter to prevent thundering herd | Random retry timing variation |
| `test_handoff_recovery_on_redis_return` | Resume coordination when Redis returns | Automatic recovery mechanism |

#### GAP-REC-13: Concurrency Safety for Handoffs
**File**: `test_handoff_concurrency_manager.py`
**Target**: 10 test cases

| Test Case | Description | Exit Criteria |
|-----------|-------------|---------------|
| `test_distributed_lock_acquisition` | Acquire locks in deterministic order | Deadlock prevention |
| `test_concurrent_handoff_prevention` | Prevent conflicting handoffs | Lock conflicts detected |
| `test_transaction_isolation` | Isolate handoff transactions | ACID properties maintained |
| `test_rollback_on_failure` | Roll back failed handoff attempts | Safe state restoration |
| `test_checkpoint_creation` | Create transaction checkpoints | Partial rollback capability |
| `test_conflict_detection` | Detect handoff conflicts | Conflict analysis and reporting |
| `test_priority_based_resolution` | Resolve conflicts via priority | Higher priority wins |
| `test_timeout_handling` | Handle transaction timeouts | Timeout rollback and cleanup |
| `test_lock_expiration` | Expire stale locks automatically | 10-minute lock timeout |
| `test_concurrent_stress_testing` | Stress test concurrent handoffs | No data corruption under load |

#### GAP-REC-14: Missing Trade History Handling
**File**: `test_missing_trade_history_handler.py`
**Target**: 10 test cases

| Test Case | Description | Exit Criteria |
|-----------|-------------|---------------|
| `test_trade_gap_detection` | Detect missing entry/exit trades | Gap types identified |
| `test_position_inference_reconstruction` | Reconstruct from position data | HIGH confidence trades created |
| `test_broker_api_fetch_reconstruction` | Fetch missing trades from broker API | External data integration |
| `test_interpolation_reconstruction` | Interpolate from surrounding data | MEDIUM confidence trades |
| `test_quantity_mismatch_detection` | Detect position-trade quantity mismatches | Data inconsistency flagging |
| `test_reconstruction_confidence_scoring` | Score reconstruction confidence | HIGH/MEDIUM/LOW/VERY_LOW levels |
| `test_manual_review_escalation` | Escalate low confidence cases | Manual intervention workflow |
| `test_reconstruction_validation` | Validate reconstructed trades | Data quality checks |
| `test_trade_history_completeness_analysis` | Analyze overall completeness | Completeness score (0.0-1.0) |
| `test_audit_trail_for_reconstruction` | Audit all reconstruction activities | Complete audit logging |

#### GAP-REC-15: External Order Tagging Idempotency
**File**: `test_external_order_tagging_idempotency.py`
**Target**: 10 test cases

| Test Case | Description | Exit Criteria |
|-----------|-------------|---------------|
| `test_duplicate_detection_by_content` | Detect exact content duplicates | Fingerprint-based detection |
| `test_idempotency_key_enforcement` | Enforce idempotency key uniqueness | Key reuse prevention |
| `test_temporal_deduplication` | Deduplicate within time windows | 30-minute deduplication window |
| `test_conflict_resolution_strategies` | Resolve operation conflicts | USE_EXISTING/USE_NEW/MANUAL_REVIEW |
| `test_stale_operation_rejection` | Reject operations too old to process | Configurable staleness threshold |
| `test_operation_fingerprinting` | Generate unique operation fingerprints | SHA-256 content fingerprints |
| `test_safe_retry_operations` | Support safe operation retries | Idempotent retry behavior |
| `test_audit_trail_preservation` | Preserve complete operation history | Operation audit logging |
| `test_cleanup_of_stale_records` | Clean up old idempotency records | Automated cleanup process |
| `test_high_throughput_handling` | Handle high-volume idempotency checks | Performance under load |

### Running Sprint 7B Tests

#### Run All Sprint 7B Tests
```bash
# Run all Sprint 7B edge case tests
pytest order_service/tests/services/test_exit_attribution_policy.py
pytest order_service/tests/services/test_manual_attribution_apply_validator.py
pytest order_service/tests/services/test_exit_context_matcher.py
pytest order_service/tests/services/test_transfer_instruction_generator.py
pytest order_service/tests/services/test_redis_unavailable_handoff_manager.py
pytest order_service/tests/services/test_handoff_concurrency_manager.py
pytest order_service/tests/services/test_missing_trade_history_handler.py
pytest order_service/tests/services/test_external_order_tagging_idempotency.py

# Or run Sprint 7B integration test
pytest order_service/tests/services/test_sprint_7b_integration_quick.py
```

#### Run by GAP Requirement
```bash
# GAP-REC-8: Exit Attribution Policy
pytest -k "test_exit_attribution_policy"

# GAP-REC-9: Manual Attribution Apply Validation
pytest -k "test_manual_attribution_apply_validator"

# GAP-REC-12: Redis Unavailable Handoff
pytest -k "test_redis_unavailable_handoff_manager"
```

#### Sprint 7B Performance Requirements
```bash
# Run performance-critical tests
pytest -m "performance" order_service/tests/services/

# Policy evaluation: < 100ms per operation
# Context matching: < 500ms for complex cases
# Concurrency operations: < 1s with 10 concurrent handoffs
```

### Sprint 7B Exit Criteria Validation

Each GAP requirement maps to specific test outcomes:

1. **GAP-REC-8**: Policy enforcement with audit trails
2. **GAP-REC-9**: Pre-apply validation preventing errors  
3. **GAP-REC-10**: Robust matching with 95%+ accuracy
4. **GAP-REC-11**: Complete transfer instruction coverage
5. **GAP-REC-12**: Safe handoff continuation without Redis
6. **GAP-REC-13**: Zero data corruption under concurrency
7. **GAP-REC-14**: Trade gap detection and reconstruction
8. **GAP-REC-15**: 100% idempotency for external operations

**Target**: 80 test cases across 8 edge case categories
**Success Criteria**: All tests pass + documented exit criteria met