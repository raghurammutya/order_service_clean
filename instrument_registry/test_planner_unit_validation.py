#!/usr/bin/env python3
"""
Unit Validation Tests for Subscription Planner Service

Tests that validate the planner service logic independently of external dependencies.
Tests config integration, optimization levels, filtering, and caching without database.
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, Mock

from app.services.subscription_planner_service import (
    SubscriptionPlannerService, 
    OptimizationLevel, 
    FilteringStrictness, 
    SubscriptionType
)
from common.config_client import ConfigClient
from app.services.monitoring_service import MonitoringService


async def test_config_service_integration():
    """Test 1: Config service integration without external dependencies"""
    print("\n🧪 Test 1: Config Service Integration (Unit Test)")
    
    # Mock config client
    config_client = Mock(spec=ConfigClient)
    
    # Mock successful config responses
    config_client.get_secret = AsyncMock(side_effect=lambda key, environment: {
        'INSTRUMENT_REGISTRY_PLANNER_OPTIMIZATION_LEVEL': 'moderate',
        'INSTRUMENT_REGISTRY_PLANNER_TIMEOUT': '30', 
        'INSTRUMENT_REGISTRY_MAX_INSTRUMENTS_PER_PLAN': '1000',
        'INSTRUMENT_REGISTRY_FILTERING_STRICTNESS': 'moderate',
        'INSTRUMENT_REGISTRY_PLAN_CACHE_TTL': '300'
    }.get(key, 'default_value'))
    
    # Mock monitoring and profile service
    monitoring = Mock(spec=MonitoringService)
    monitoring.record_operation_duration = Mock()
    
    profile_service = Mock()
    
    # Create service instance
    service = SubscriptionPlannerService(
        database_url="mock://localhost",
        config_client=config_client,
        monitoring=monitoring,
        profile_service=profile_service
    )
    
    # Test config loading
    config = await service._get_planner_config()
    
    assert config['INSTRUMENT_REGISTRY_PLANNER_OPTIMIZATION_LEVEL'] == 'moderate'
    assert config['INSTRUMENT_REGISTRY_MAX_INSTRUMENTS_PER_PLAN'] == '1000'
    assert config['INSTRUMENT_REGISTRY_FILTERING_STRICTNESS'] == 'moderate'
    
    print("✅ Config service integration working (mocked)")
    return config


async def test_optimization_levels_unit():
    """Test 2: Optimization levels logic without database"""
    print("\n🧪 Test 2: Optimization Levels (Unit Test)")
    
    # Mock service setup
    config_client = Mock(spec=ConfigClient)
    monitoring = Mock(spec=MonitoringService)
    monitoring.record_operation_duration = Mock()
    profile_service = Mock()
    
    service = SubscriptionPlannerService(
        database_url="mock://localhost",
        config_client=config_client,
        monitoring=monitoring,
        profile_service=profile_service
    )
    
    test_instruments = [f"NSE:STOCK{i}" for i in range(100)]
    max_instruments = 50
    
    # Test LOW optimization
    result_low = await service._optimize_instrument_list(
        test_instruments, OptimizationLevel.LOW, max_instruments
    )
    
    # Test MODERATE optimization
    result_moderate = await service._optimize_instrument_list(
        test_instruments, OptimizationLevel.MODERATE, max_instruments
    )
    
    # Test AGGRESSIVE optimization
    result_aggressive = await service._optimize_instrument_list(
        test_instruments, OptimizationLevel.AGGRESSIVE, max_instruments
    )
    
    # Validate optimization strategies
    assert result_low["optimization_strategy"] == "simple_truncation"
    assert result_low["performance_impact"] == "minimal"
    
    assert result_moderate["optimization_strategy"] == "dedup_and_sort"
    assert result_moderate["performance_impact"] == "moderate"
    
    assert result_aggressive["optimization_strategy"] == "advanced_exchange_prioritization"
    assert result_aggressive["performance_impact"] == "significant"
    
    # Validate instrument limits are respected
    assert len(result_low["optimized_instruments"]) <= max_instruments
    assert len(result_moderate["optimized_instruments"]) <= max_instruments
    assert len(result_aggressive["optimized_instruments"]) <= max_instruments
    
    print("✅ Optimization levels working correctly:")
    print(f"   Low: {result_low['optimization_strategy']} ({result_low['performance_impact']})")
    print(f"   Moderate: {result_moderate['optimization_strategy']} ({result_moderate['performance_impact']})")
    print(f"   Aggressive: {result_aggressive['optimization_strategy']} ({result_aggressive['performance_impact']})")


async def test_filtering_strictness_unit():
    """Test 3: Filtering strictness without database connection"""
    print("\n🧪 Test 3: Filtering Strictness (Unit Test)")
    
    # Mock service setup
    config_client = Mock(spec=ConfigClient)
    monitoring = Mock(spec=MonitoringService)
    monitoring.record_operation_duration = Mock()
    profile_service = Mock()
    
    service = SubscriptionPlannerService(
        database_url="mock://localhost",
        config_client=config_client,
        monitoring=monitoring,
        profile_service=profile_service
    )
    
    test_instruments = ["NSE:VALID1", "BSE:VALID2", "INVALID:BAD", "NSE:VALID3"]
    
    # Mock database connection
    mock_conn = AsyncMock()
    mock_conn.fetch.return_value = [
        {"instrument_key": "NSE:VALID1", "is_active": True},
        {"instrument_key": "BSE:VALID2", "is_active": True},
        {"instrument_key": "NSE:VALID3", "is_active": False}  # Inactive
    ]
    
    # Test LENIENT filtering (should return all)
    result_lenient = await service._filter_instruments_by_strictness(
        mock_conn, test_instruments, FilteringStrictness.LENIENT
    )
    
    # Test MODERATE filtering (should include all in registry)
    result_moderate = await service._filter_instruments_by_strictness(
        mock_conn, test_instruments, FilteringStrictness.MODERATE
    )
    
    # Test STRICT filtering (should include only active)
    result_strict = await service._filter_instruments_by_strictness(
        mock_conn, test_instruments, FilteringStrictness.STRICT
    )
    
    # Validate filtering behavior
    assert len(result_lenient) == 4  # All instruments kept
    assert len(result_moderate) == 3  # Only registry instruments
    assert len(result_strict) == 2  # Only active instruments
    
    assert "NSE:VALID1" in result_strict
    assert "BSE:VALID2" in result_strict  
    assert "NSE:VALID3" not in result_strict  # Inactive
    assert "INVALID:BAD" not in result_strict  # Not in registry
    
    print("✅ Filtering strictness working correctly:")
    print(f"   Lenient: {len(result_lenient)} instruments kept")
    print(f"   Moderate: {len(result_moderate)} instruments kept") 
    print(f"   Strict: {len(result_strict)} instruments kept")


async def test_cache_key_generation():
    """Test 4: Cache key generation logic"""
    print("\n🧪 Test 4: Cache Key Generation")
    
    # Mock service setup
    config_client = Mock(spec=ConfigClient)
    monitoring = Mock(spec=MonitoringService)
    profile_service = Mock()
    
    service = SubscriptionPlannerService(
        database_url="mock://localhost",
        config_client=config_client,
        monitoring=monitoring,
        profile_service=profile_service
    )
    
    user_id = "test_user"
    instruments = ["NSE:STOCK1", "NSE:STOCK2"]
    options = {"optimization_level": "moderate", "filtering_strictness": "strict"}
    
    # Generate cache key
    cache_key1 = service._generate_cache_key(user_id, instruments, options)
    
    # Same inputs should generate same key
    cache_key2 = service._generate_cache_key(user_id, instruments, options)
    
    # Different order of instruments should generate same key (sorted)
    cache_key3 = service._generate_cache_key(user_id, ["NSE:STOCK2", "NSE:STOCK1"], options)
    
    # Different options should generate different key
    different_options = {"optimization_level": "aggressive", "filtering_strictness": "strict"}
    cache_key4 = service._generate_cache_key(user_id, instruments, different_options)
    
    assert cache_key1 == cache_key2, "Same inputs should generate same cache key"
    assert cache_key1 == cache_key3, "Instrument order should not affect cache key"
    assert cache_key1 != cache_key4, "Different options should generate different cache key"
    assert len(cache_key1) == 64, "Cache key should be SHA256 hash (64 chars)"
    
    print("✅ Cache key generation working correctly")
    print(f"   Sample cache key: {cache_key1[:16]}...")


async def test_cache_ttl_validation():
    """Test 5: Cache TTL validation logic"""
    print("\n🧪 Test 5: Cache TTL Validation")
    
    # Mock service setup
    config_client = Mock(spec=ConfigClient)
    monitoring = Mock(spec=MonitoringService)
    profile_service = Mock()
    
    service = SubscriptionPlannerService(
        database_url="mock://localhost",
        config_client=config_client,
        monitoring=monitoring,
        profile_service=profile_service
    )
    
    # Test fresh cache entry (should be valid)
    fresh_entry = {
        "cached_at": datetime.now(timezone.utc),
        "plan": {"plan_id": "test123"}
    }
    
    is_valid_fresh = service._is_cache_valid(fresh_entry, 300)  # 5 minute TTL
    
    # Test stale cache entry (should be invalid)
    stale_entry = {
        "cached_at": datetime.now(timezone.utc) - timedelta(seconds=400),  # 6+ minutes old
        "plan": {"plan_id": "test456"}
    }
    
    is_valid_stale = service._is_cache_valid(stale_entry, 300)  # 5 minute TTL
    
    # Test missing cache time (should be invalid)
    invalid_entry = {
        "plan": {"plan_id": "test789"}
    }
    
    is_valid_missing = service._is_cache_valid(invalid_entry, 300)
    
    assert is_valid_fresh == True, "Fresh cache entry should be valid"
    assert is_valid_stale == False, "Stale cache entry should be invalid"
    assert is_valid_missing == False, "Cache entry without timestamp should be invalid"
    
    print("✅ Cache TTL validation working correctly")
    print("   Fresh entries: valid, stale entries: invalid")


async def test_performance_metrics_calculation():
    """Test 6: Performance metrics calculation logic"""
    print("\n🧪 Test 6: Performance Metrics Calculation")
    
    # Mock service setup
    config_client = Mock(spec=ConfigClient)
    monitoring = Mock(spec=MonitoringService)
    monitoring.record_operation_duration = Mock()
    profile_service = Mock()
    
    service = SubscriptionPlannerService(
        database_url="mock://localhost",
        config_client=config_client,
        monitoring=monitoring,
        profile_service=profile_service
    )
    
    # Test optimization with metrics
    test_instruments = [f"NSE:STOCK{i}" for i in range(50)]
    optimization_result = await service._optimize_instrument_list(
        test_instruments, OptimizationLevel.AGGRESSIVE, 1000
    )
    
    # Validate optimization metrics
    assert "optimized_instruments" in optimization_result
    assert "removed_count" in optimization_result
    assert "optimization_strategy" in optimization_result
    assert "performance_impact" in optimization_result
    
    # For aggressive optimization with exchanges
    if "optimization_time_ms" in optimization_result:
        assert optimization_result["optimization_time_ms"] >= 0
    
    if "exchange_distribution" in optimization_result:
        assert isinstance(optimization_result["exchange_distribution"], dict)
    
    print("✅ Performance metrics calculation working correctly")
    print(f"   Strategy: {optimization_result['optimization_strategy']}")
    print(f"   Impact: {optimization_result['performance_impact']}")


async def main():
    """Run all unit validation tests"""
    print("🚀 Starting Subscription Planner Unit Validation Tests")
    print("=" * 60)
    
    try:
        # Test 1: Config service integration
        config = await test_config_service_integration()
        
        # Test 2: Optimization levels
        await test_optimization_levels_unit()
        
        # Test 3: Filtering strictness
        await test_filtering_strictness_unit()
        
        # Test 4: Cache key generation
        await test_cache_key_generation()
        
        # Test 5: Cache TTL validation
        await test_cache_ttl_validation()
        
        # Test 6: Performance metrics calculation
        await test_performance_metrics_calculation()
        
        print("\n" + "=" * 60)
        print("🎉 ALL UNIT TESTS PASSED! Planner core logic is working correctly!")
        print("✅ Config service integration: WORKING")
        print("✅ Optimization levels: WORKING")
        print("✅ Filtering strictness: WORKING")
        print("✅ Cache key generation: WORKING")
        print("✅ Cache TTL validation: WORKING")
        print("✅ Performance metrics: WORKING")
        print("\n💡 Note: These tests validate core planner logic without external dependencies.")
        print("   Full integration tests require running services and database connectivity.")
        
        return True
        
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        return False
    except Exception as e:
        print(f"\n💥 UNEXPECTED ERROR: {e}")
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)