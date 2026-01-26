#!/usr/bin/env python3
"""
Event Streaming Configuration Verification

Tests all event streaming parameters before implementing infrastructure
"""

import asyncio
import httpx
import json
from datetime import datetime
from typing import Dict, Any, List

CONFIG_SERVICE_URL = "http://localhost:8100"
API_KEY = "AShhRzWhfXd6IomyzZnE3d-lCcAvT1L5GDCCZRSXZGsJq7_eAJGxeMi-4AlfTeOc"

# Event streaming configuration parameters
EVENT_CONFIG_PARAMS = [
    'INSTRUMENT_REGISTRY_EVENT_BROKER_URL',
    'INSTRUMENT_REGISTRY_EVENT_RETRY_ATTEMPTS',
    'INSTRUMENT_REGISTRY_EVENT_BATCH_SIZE',
    'INSTRUMENT_REGISTRY_EVENT_ORDERING_GUARANTEE',
    'INSTRUMENT_REGISTRY_DLQ_RETENTION_HOURS'
]

async def verify_event_parameter(key: str) -> Dict[str, Any]:
    """Verify an event configuration parameter can be retrieved"""
    headers = {"X-Internal-API-Key": API_KEY}
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{CONFIG_SERVICE_URL}/api/v1/secrets/{key}/value?environment=prod",
                headers=headers
            )
            
            if response.status_code == 200:
                result = response.json()
                return {
                    "key": key,
                    "value": result.get("value"),
                    "status": "SUCCESS",
                    "accessible": True
                }
            else:
                return {
                    "key": key,
                    "status": "FAILED",
                    "error": f"HTTP {response.status_code}: {response.text}",
                    "accessible": False
                }
                
    except Exception as e:
        return {
            "key": key,
            "status": "ERROR",
            "error": str(e),
            "accessible": False
        }

async def verify_all_event_parameters() -> Dict[str, Any]:
    """Verify all event streaming configuration parameters"""
    print("🔧 Testing Event Streaming Configuration Parameters...")
    print("=" * 60)
    
    results = {}
    accessible_count = 0
    
    # Test each parameter
    for param in EVENT_CONFIG_PARAMS:
        print(f"Testing: {param}")
        result = await verify_event_parameter(param)
        results[param] = result
        
        if result["accessible"]:
            accessible_count += 1
            print(f"✅ {param} = {result['value']}")
        else:
            print(f"❌ {param} - {result['error']}")
        print()
    
    # Calculate success rate
    total_params = len(EVENT_CONFIG_PARAMS)
    success_rate = (accessible_count / total_params) * 100
    
    print("=" * 60)
    print(f"📊 Event Configuration Test Results:")
    print(f"   Total Parameters: {total_params}")
    print(f"   Accessible: {accessible_count}")
    print(f"   Failed: {total_params - accessible_count}")
    print(f"   Success Rate: {success_rate:.1f}%")
    
    if success_rate == 100:
        print("✅ ALL EVENT PARAMETERS ACCESSIBLE - Ready for streaming implementation")
    elif success_rate >= 80:
        print("⚠️  MOST PARAMETERS ACCESSIBLE - Some missing parameters need registration")
    else:
        print("❌ CRITICAL MISSING PARAMETERS - Registration required before implementation")
    
    return {
        "timestamp": datetime.now().isoformat(),
        "total_parameters": total_params,
        "accessible_count": accessible_count,
        "success_rate": success_rate,
        "parameters": results,
        "ready_for_implementation": success_rate == 100
    }

async def test_event_broker_connectivity() -> Dict[str, Any]:
    """Test connectivity to event broker using config-driven URL"""
    try:
        # First get the broker URL from config
        broker_result = await verify_event_parameter('INSTRUMENT_REGISTRY_EVENT_BROKER_URL')
        
        if not broker_result["accessible"]:
            return {
                "status": "FAILED",
                "error": "Cannot retrieve broker URL from config service"
            }
        
        broker_url = broker_result["value"]
        print(f"🔗 Testing broker connectivity: {broker_url}")
        
        # Test Redis connectivity (assuming Redis broker)
        import redis.asyncio as redis
        
        # Parse Redis URL
        redis_client = redis.from_url(broker_url)
        
        # Test connection
        await redis_client.ping()
        await redis_client.close()
        
        return {
            "status": "SUCCESS",
            "broker_url": broker_url,
            "connectivity": "VERIFIED"
        }
        
    except Exception as e:
        return {
            "status": "FAILED",
            "error": f"Broker connectivity test failed: {str(e)}"
        }

async def main():
    """Main verification process"""
    print("🚀 Event Streaming Configuration Verification")
    print("=" * 60)
    
    # Step 1: Verify all event parameters
    config_results = await verify_all_event_parameters()
    
    # Step 2: Test broker connectivity if parameters accessible
    if config_results["ready_for_implementation"]:
        print("\n🔗 Testing Event Broker Connectivity...")
        broker_test = await test_event_broker_connectivity()
        
        if broker_test["status"] == "SUCCESS":
            print("✅ Event broker connectivity verified")
        else:
            print(f"❌ Event broker connectivity failed: {broker_test['error']}")
        
        config_results["broker_connectivity"] = broker_test
    
    # Step 3: Generate evidence file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    evidence_file = f"event_streaming_config_verification_{timestamp}.json"
    
    with open(evidence_file, "w") as f:
        json.dump(config_results, f, indent=2)
    
    print(f"\n📄 Evidence file generated: {evidence_file}")
    
    # Step 4: Production readiness assessment
    print("\n🎯 Production Readiness Assessment:")
    if config_results["ready_for_implementation"]:
        if config_results.get("broker_connectivity", {}).get("status") == "SUCCESS":
            print("✅ READY - All parameters accessible and broker connectivity verified")
            print("🚀 Proceeding to event streaming implementation...")
        else:
            print("⚠️  CONFIG READY - Parameters accessible but broker needs attention")
    else:
        print("❌ NOT READY - Missing configuration parameters need to be registered")
        print("📝 Next step: Register missing parameters in config service")

if __name__ == "__main__":
    asyncio.run(main())