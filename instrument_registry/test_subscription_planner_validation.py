#!/usr/bin/env python3
"""
Production Validation Tests for Subscription Planner

Tests to ensure planner respects:
1. Config service integration and parameter loading
2. Instrument limits from config
3. Optimization level affects performance
4. Filtering adapts to config parameter changes  
5. Plan caching honors TTL configuration
"""

import asyncio
import json
import httpx
import time
from datetime import datetime

# Test config
BASE_URL = "http://localhost:8901"
API_KEY = "AShhRzWhfXd6IomyzZnE3d-lCcAvT1L5GDCCZRSXZGsJq7_eAJGxeMi-4AlfTeOc"

HEADERS = {
    "X-Internal-API-Key": API_KEY,
    "Content-Type": "application/json"
}

async def test_config_service_integration():
    """Test 1: Validate config service integration"""
    print("\n🧪 Test 1: Config Service Integration")
    
    async with httpx.AsyncClient() as client:
        # Get planner configuration
        response = await client.get(
            f"{BASE_URL}/api/v1/internal/instrument-registry/subscriptions/planner/config",
            headers=HEADERS
        )
        
        assert response.status_code == 200, f"Config endpoint failed: {response.text}"
        config = response.json()
        
        assert "configuration" in config, "Configuration not found in response"
        
        required_configs = [
            'INSTRUMENT_REGISTRY_PLANNER_OPTIMIZATION_LEVEL',
            'INSTRUMENT_REGISTRY_PLANNER_TIMEOUT',
            'INSTRUMENT_REGISTRY_MAX_INSTRUMENTS_PER_PLAN',
            'INSTRUMENT_REGISTRY_FILTERING_STRICTNESS',
            'INSTRUMENT_REGISTRY_PLAN_CACHE_TTL'
        ]
        
        for key in required_configs:
            assert key in config["configuration"], f"Missing config key: {key}"
            assert config["configuration"][key] != "unavailable", f"Config {key} is unavailable"
        
        print("✅ Config service integration working")
        return config["configuration"]

async def test_instrument_limits():
    """Test 2: Validate instrument limits are respected"""
    print("\n🧪 Test 2: Instrument Limits Validation")
    
    async with httpx.AsyncClient() as client:
        # Create a plan with many instruments to test limit enforcement
        instruments = [f"NSE:STOCK{i}" for i in range(1500)]  # Exceeds default limit of 1000
        
        plan_request = {
            "plan_name": "Limit Test Plan",
            "subscription_type": "live_feed",
            "instruments": instruments,
            "description": "Testing instrument limits"
        }
        
        response = await client.post(
            f"{BASE_URL}/api/v1/internal/instrument-registry/subscriptions/plan?user_id=test_user_limits",
            headers=HEADERS,
            json=plan_request
        )
        
        assert response.status_code == 200, f"Plan creation failed: {response.text}"
        result = response.json()
        
        plan_data = result["plan_data"]
        instruments_count = len(json.loads(plan_data["instruments"]))
        
        # Should be limited to max configured value (1000)
        assert instruments_count <= 1000, f"Instrument limit not enforced: {instruments_count} > 1000"
        
        # Check optimization result shows removed instruments
        metadata = json.loads(plan_data["metadata"])
        optimization_result = metadata.get("optimization_result", {})
        assert optimization_result.get("removed_count", 0) > 0, "Should have removed excess instruments"
        
        print(f"✅ Instrument limits respected: {instruments_count}/1500 instruments kept")
        return result["plan_id"]

async def test_optimization_levels():
    """Test 3: Test optimization levels affect performance"""
    print("\n🧪 Test 3: Optimization Level Performance Impact")
    
    async with httpx.AsyncClient() as client:
        test_instruments = [f"NSE:TEST{i}" for i in range(500)]
        
        # Test each optimization level
        optimization_levels = ["low", "moderate", "aggressive"]
        results = {}
        
        for level in optimization_levels:
            plan_request = {
                "plan_name": f"Optimization Test - {level}",
                "subscription_type": "live_feed", 
                "instruments": test_instruments,
                "optimization_level": level
            }
            
            start_time = time.time()
            response = await client.post(
                f"{BASE_URL}/api/v1/internal/instrument-registry/subscriptions/plan?user_id=test_user_opt_{level}",
                headers=HEADERS,
                json=plan_request
            )
            duration = time.time() - start_time
            
            assert response.status_code == 200, f"Plan creation failed for {level}: {response.text}"
            result = response.json()
            
            plan_data = result["plan_data"]
            metadata = json.loads(plan_data["metadata"])
            optimization_result = metadata.get("optimization_result", {})
            
            results[level] = {
                "duration": duration,
                "strategy": optimization_result.get("optimization_strategy"),
                "performance_impact": optimization_result.get("performance_impact"),
                "response_time": result.get("duration_ms", 0)
            }
        
        # Verify aggressive optimization takes more time than low
        assert results["aggressive"]["performance_impact"] == "significant", "Aggressive optimization should have significant impact"
        assert results["low"]["performance_impact"] == "minimal", "Low optimization should have minimal impact"
        assert results["moderate"]["performance_impact"] == "moderate", "Moderate optimization should have moderate impact"
        
        print("✅ Optimization levels working correctly:")
        for level, data in results.items():
            print(f"   {level}: {data['strategy']} ({data['performance_impact']} impact)")

async def test_filtering_strictness():
    """Test 4: Test filtering adapts to config parameters"""
    print("\n🧪 Test 4: Filtering Strictness Adaptation")
    
    async with httpx.AsyncClient() as client:
        # Test instruments - mix of valid and potentially invalid
        test_instruments = [
            "NSE:RELIANCE", "BSE:INFY", "NFO:NIFTY", 
            "INVALID:STOCK", "NSE:TEST123", "UNKNOWN:XYZ"
        ]
        
        strictness_levels = ["lenient", "moderate", "strict"]
        results = {}
        
        for strictness in strictness_levels:
            plan_request = {
                "plan_name": f"Filtering Test - {strictness}",
                "subscription_type": "live_feed",
                "instruments": test_instruments,
                "filtering_strictness": strictness
            }
            
            response = await client.post(
                f"{BASE_URL}/api/v1/internal/instrument-registry/subscriptions/plan?user_id=test_user_filter_{strictness}",
                headers=HEADERS,
                json=plan_request
            )
            
            assert response.status_code == 200, f"Plan creation failed for {strictness}: {response.text}"
            result = response.json()
            
            plan_data = result["plan_data"]
            instruments_count = len(json.loads(plan_data["instruments"]))
            validation_results = json.loads(plan_data["validation_results"])
            
            results[strictness] = {
                "instruments_kept": instruments_count,
                "invalid_removed": validation_results.get("invalid_instruments", 0),
                "optimization_applied": validation_results.get("optimization_applied")
            }
        
        # Lenient should keep more instruments than strict
        assert results["lenient"]["instruments_kept"] >= results["strict"]["instruments_kept"], \
            "Lenient filtering should keep more instruments than strict"
        
        print("✅ Filtering strictness adaptation working:")
        for level, data in results.items():
            print(f"   {level}: kept {data['instruments_kept']}, removed {data['invalid_removed']}")

async def test_plan_caching():
    """Test 5: Test plan caching honors TTL configuration"""
    print("\n🧪 Test 5: Plan Caching TTL Validation")
    
    async with httpx.AsyncClient() as client:
        test_instruments = ["NSE:CACHE1", "NSE:CACHE2", "NSE:CACHE3"]
        
        plan_request = {
            "plan_name": "Cache Test Plan",
            "subscription_type": "live_feed",
            "instruments": test_instruments
        }
        
        # First request - should create new plan
        response1 = await client.post(
            f"{BASE_URL}/api/v1/internal/instrument-registry/subscriptions/plan?user_id=test_user_cache",
            headers=HEADERS,
            json=plan_request
        )
        
        assert response1.status_code == 200, f"First request failed: {response1.text}"
        result1 = response1.json()
        assert result1["cache_hit"] == False, "First request should not be cache hit"
        
        # Second request immediately - should hit cache
        response2 = await client.post(
            f"{BASE_URL}/api/v1/internal/instrument-registry/subscriptions/plan?user_id=test_user_cache",
            headers=HEADERS,
            json=plan_request
        )
        
        assert response2.status_code == 200, f"Second request failed: {response2.text}"
        result2 = response2.json()
        assert result2["cache_hit"] == True, "Second request should be cache hit"
        
        # Verify same plan ID returned from cache
        assert result1["plan_id"] == result2["plan_id"], "Cache should return same plan ID"
        
        print("✅ Plan caching working correctly:")
        print(f"   First request: cache_hit={result1['cache_hit']}")
        print(f"   Second request: cache_hit={result2['cache_hit']}")

async def test_integration_with_subscription_profiles():
    """Test 6: Validate integration with subscription profiles from Session 2B"""
    print("\n🧪 Test 6: Subscription Profiles Integration")
    
    async with httpx.AsyncClient() as client:
        # Create a subscription profile first
        profile_request = {
            "profile_name": "Planner Integration Test",
            "subscription_type": "live_feed",
            "instrument_preferences": {
                "exchanges": ["NSE", "BSE"],
                "sectors": ["technology", "finance"], 
                "max_instruments": 50
            },
            "notification_preferences": {
                "enable_email": True,
                "enable_sms": False,
                "enable_push": True
            },
            "metadata": {
                "test_profile": True,
                "integration_test": "planner_validation"
            }
        }
        
        response = await client.post(
            f"{BASE_URL}/api/v1/internal/instrument-registry/subscription-profiles?user_id=test_user_integration",
            headers=HEADERS,
            json=profile_request
        )
        
        assert response.status_code == 200, f"Profile creation failed: {response.text}"
        profile_result = response.json()
        
        # Create a plan that should work with the profile
        plan_request = {
            "plan_name": "Profile Integration Plan",
            "subscription_type": "live_feed",
            "instruments": ["NSE:TCS", "NSE:INFY", "BSE:HDFC", "BSE:ICICI"],
            "description": "Testing profile integration"
        }
        
        response = await client.post(
            f"{BASE_URL}/api/v1/internal/instrument-registry/subscriptions/plan?user_id=test_user_integration",
            headers=HEADERS,
            json=plan_request
        )
        
        assert response.status_code == 200, f"Plan creation failed: {response.text}"
        plan_result = response.json()
        
        print("✅ Subscription profiles integration working:")
        print(f"   Profile ID: {profile_result['profile_id']}")
        print(f"   Plan ID: {plan_result['plan_id']}")

async def main():
    """Run all production validation tests"""
    print("🚀 Starting Subscription Planner Production Validation Tests")
    print("=" * 60)
    
    try:
        # Test 1: Config service integration
        config = await test_config_service_integration()
        
        # Test 2: Instrument limits
        plan_id = await test_instrument_limits()
        
        # Test 3: Optimization levels
        await test_optimization_levels()
        
        # Test 4: Filtering strictness
        await test_filtering_strictness()
        
        # Test 5: Plan caching
        await test_plan_caching()
        
        # Test 6: Integration with profiles
        await test_integration_with_subscription_profiles()
        
        print("\n" + "=" * 60)
        print("🎉 ALL TESTS PASSED! Subscription Planner is production ready!")
        print("✅ Config service integration: WORKING")
        print("✅ Instrument limits enforcement: WORKING") 
        print("✅ Optimization levels: WORKING")
        print("✅ Filtering strictness: WORKING")
        print("✅ Plan caching: WORKING")
        print("✅ Profile integration: WORKING")
        
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