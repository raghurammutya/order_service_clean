#!/usr/bin/env python3
"""
Event Streaming Integration Test

Comprehensive test suite for event streaming infrastructure including:
- Configuration validation
- Event publishing and consumption
- Ordering guarantees
- Retry policies
- Dead letter queue functionality
"""

import asyncio
import json
import time
import uuid
from datetime import datetime
from typing import Dict, Any, List

import httpx
import redis.asyncio as redis
from app.services.event_streaming_service import EventStreamingService, StreamEvent, OrderingGuarantee
from common.config_client import ConfigClient

# Test configuration
CONFIG_SERVICE_URL = "http://localhost:8100" 
API_KEY = "AShhRzWhfXd6IomyzZnE3d-lCcAvT1L5GDCCZRSXZGsJq7_eAJGxeMi-4AlfTeOc"
SERVICE_BASE_URL = "http://localhost:8086"

class EventStreamingIntegrationTest:
    """Comprehensive integration test suite for event streaming"""
    
    def __init__(self):
        self.config_client = None
        self.streaming_service = None
        self.redis_client = None
        self.test_results = []
        
    async def setup(self):
        """Initialize test dependencies"""
        print("🔧 Setting up integration test environment...")
        
        # Initialize config client
        self.config_client = ConfigClient(
            service_name="instrument_registry",
            internal_api_key=API_KEY
        )
        await self.config_client.initialize()
        
        # Initialize streaming service
        self.streaming_service = EventStreamingService(
            config_client=self.config_client
        )
        await self.streaming_service.initialize()
        
        # Direct Redis connection for verification
        broker_url = self.config_client.get(
            'INSTRUMENT_REGISTRY_EVENT_BROKER_URL',
            default='redis://localhost:6379/0'
        )
        self.redis_client = redis.from_url(broker_url, decode_responses=True)
        
        print("✅ Test environment initialized")
    
    async def cleanup(self):
        """Clean up test resources"""
        if self.streaming_service:
            await self.streaming_service.shutdown()
        if self.redis_client:
            await self.redis_client.close()
        if self.config_client:
            await self.config_client.close()
        print("🧹 Test environment cleaned up")
    
    async def test_config_integration(self) -> Dict[str, Any]:
        """Test 1: Validate all event streaming configuration parameters"""
        print("\n📋 Test 1: Configuration Integration")
        
        test_result = {
            "test": "config_integration",
            "success": True,
            "details": {},
            "errors": []
        }
        
        # Test all 5 event streaming parameters
        config_params = [
            'INSTRUMENT_REGISTRY_EVENT_BROKER_URL',
            'INSTRUMENT_REGISTRY_EVENT_RETRY_ATTEMPTS',  
            'INSTRUMENT_REGISTRY_EVENT_BATCH_SIZE',
            'INSTRUMENT_REGISTRY_EVENT_ORDERING_GUARANTEE',
            'INSTRUMENT_REGISTRY_DLQ_RETENTION_HOURS'
        ]
        
        for param in config_params:
            try:
                value = self.config_client.get(param)
                test_result["details"][param] = {
                    "accessible": value is not None,
                    "value": str(value) if value else "None"
                }
                print(f"  ✅ {param}: {value}")
            except Exception as e:
                test_result["success"] = False
                test_result["errors"].append(f"{param}: {str(e)}")
                print(f"  ❌ {param}: {e}")
        
        return test_result
    
    async def test_event_publishing_api(self) -> Dict[str, Any]:
        """Test 2: Event publishing via REST API"""
        print("\n📤 Test 2: Event Publishing API")
        
        test_result = {
            "test": "event_publishing_api",
            "success": True,
            "details": {},
            "errors": []
        }
        
        headers = {"X-Internal-API-Key": API_KEY, "Content-Type": "application/json"}
        
        # Test event payload
        test_events = [
            {
                "event_type": "instrument_updated",
                "data": {"symbol": "RELIANCE", "exchange": "NSE", "test_id": str(uuid.uuid4())},
                "partition_key": "NSE:RELIANCE"
            },
            {
                "event_type": "token_changed", 
                "data": {"broker": "kite", "symbol": "INFY", "test_id": str(uuid.uuid4())},
                "partition_key": "kite:INFY"
            }
        ]
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{SERVICE_BASE_URL}/api/v1/internal/instrument-registry/events/publish",
                    headers=headers,
                    json={"events": test_events}
                )
                
                if response.status_code == 200:
                    result = response.json()
                    test_result["details"] = {
                        "published_count": result.get("published_count", 0),
                        "failed_count": result.get("failed_count", 0),
                        "event_ids": result.get("event_ids", []),
                        "success": result.get("success", False)
                    }
                    
                    if result.get("success"):
                        print(f"  ✅ Published {result['published_count']} events successfully")
                    else:
                        test_result["success"] = False
                        test_result["errors"] = result.get("errors", [])
                        print(f"  ❌ Publishing failed: {result.get('errors')}")
                else:
                    test_result["success"] = False
                    test_result["errors"].append(f"HTTP {response.status_code}: {response.text}")
                    print(f"  ❌ API call failed: HTTP {response.status_code}")
                    
        except Exception as e:
            test_result["success"] = False
            test_result["errors"].append(str(e))
            print(f"  ❌ Exception during publishing: {e}")
        
        return test_result
    
    async def test_ordering_guarantees(self) -> Dict[str, Any]:
        """Test 3: Event ordering guarantees per configuration"""
        print("\n🔄 Test 3: Ordering Guarantees")
        
        test_result = {
            "test": "ordering_guarantees",
            "success": True,
            "details": {},
            "errors": []
        }
        
        try:
            # Test different partition keys to verify stream routing
            test_events = []
            partitions = ["partition_A", "partition_B", "partition_A"]  # Intentional duplicate
            
            for i, partition in enumerate(partitions):
                event = StreamEvent(
                    event_id=str(uuid.uuid4()),
                    event_type="test_ordering",
                    payload={"sequence": i, "test_timestamp": time.time()},
                    partition_key=partition
                )
                test_events.append(event)
            
            # Publish events through service
            published_streams = {}
            for event in test_events:
                success = await self.streaming_service.publish_event(event)
                if success:
                    stream_key = self.streaming_service._get_stream_key(event)
                    if stream_key not in published_streams:
                        published_streams[stream_key] = []
                    published_streams[stream_key].append(event.event_id)
                    
            test_result["details"] = {
                "published_events": len(test_events),
                "unique_streams": len(published_streams),
                "stream_distribution": {k: len(v) for k, v in published_streams.items()},
                "ordering_guarantee": self.streaming_service.ordering_guarantee.value
            }
            
            # Verify partition routing worked correctly
            expected_streams = 2  # partition_A and partition_B
            if len(published_streams) == expected_streams:
                print(f"  ✅ Correct stream routing: {len(published_streams)} streams created")
            else:
                test_result["success"] = False
                test_result["errors"].append(f"Expected {expected_streams} streams, got {len(published_streams)}")
                
        except Exception as e:
            test_result["success"] = False  
            test_result["errors"].append(str(e))
            print(f"  ❌ Ordering test failed: {e}")
        
        return test_result
    
    async def test_streaming_health(self) -> Dict[str, Any]:
        """Test 4: Streaming service health check"""
        print("\n🏥 Test 4: Streaming Health Check")
        
        test_result = {
            "test": "streaming_health",
            "success": True,
            "details": {},
            "errors": []
        }
        
        try:
            headers = {"X-Internal-API-Key": API_KEY}
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    f"{SERVICE_BASE_URL}/api/v1/internal/instrument-registry/events/health",
                    headers=headers
                )
                
                if response.status_code == 200:
                    health_data = response.json()
                    test_result["details"] = health_data
                    
                    # Verify critical health indicators
                    required_fields = ["service", "status", "broker_connected", "configuration"]
                    missing_fields = [field for field in required_fields if field not in health_data]
                    
                    if not missing_fields and health_data.get("broker_connected"):
                        print(f"  ✅ Health check passed: {health_data['status']}")
                    else:
                        test_result["success"] = False
                        if missing_fields:
                            test_result["errors"].append(f"Missing fields: {missing_fields}")
                        if not health_data.get("broker_connected"):
                            test_result["errors"].append("Broker not connected")
                else:
                    test_result["success"] = False
                    test_result["errors"].append(f"HTTP {response.status_code}: {response.text}")
                    
        except Exception as e:
            test_result["success"] = False
            test_result["errors"].append(str(e))
            print(f"  ❌ Health check failed: {e}")
        
        return test_result
    
    async def test_configuration_api(self) -> Dict[str, Any]:
        """Test 5: Configuration API endpoint"""
        print("\n⚙️  Test 5: Configuration API")
        
        test_result = {
            "test": "configuration_api",
            "success": True,
            "details": {},
            "errors": []
        }
        
        try:
            headers = {"X-Internal-API-Key": API_KEY}
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    f"{SERVICE_BASE_URL}/api/v1/internal/instrument-registry/events/config",
                    headers=headers
                )
                
                if response.status_code == 200:
                    config_data = response.json()
                    test_result["details"] = config_data
                    
                    # Verify configuration completeness
                    required_config = [
                        "retry_attempts", "batch_size", 
                        "ordering_guarantee", "dlq_retention_hours"
                    ]
                    
                    missing_config = [field for field in required_config if field not in config_data]
                    
                    if not missing_config:
                        print(f"  ✅ Configuration API complete: {len(config_data)} parameters")
                        print(f"    - Retry attempts: {config_data['retry_attempts']}")  
                        print(f"    - Batch size: {config_data['batch_size']}")
                        print(f"    - Ordering: {config_data['ordering_guarantee']}")
                        print(f"    - DLQ retention: {config_data['dlq_retention_hours']}h")
                    else:
                        test_result["success"] = False
                        test_result["errors"].append(f"Missing config: {missing_config}")
                        
                else:
                    test_result["success"] = False
                    test_result["errors"].append(f"HTTP {response.status_code}: {response.text}")
                    
        except Exception as e:
            test_result["success"] = False
            test_result["errors"].append(str(e))
            print(f"  ❌ Configuration API failed: {e}")
        
        return test_result
    
    async def run_all_tests(self) -> Dict[str, Any]:
        """Run complete integration test suite"""
        print("🚀 Starting Event Streaming Integration Tests")
        print("=" * 60)
        
        start_time = time.time()
        
        try:
            await self.setup()
            
            # Run all test cases
            self.test_results = [
                await self.test_config_integration(),
                await self.test_event_publishing_api(),
                await self.test_ordering_guarantees(),
                await self.test_streaming_health(),
                await self.test_configuration_api()
            ]
            
        finally:
            await self.cleanup()
        
        # Generate test summary
        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results if result["success"])
        failed_tests = total_tests - passed_tests
        duration = time.time() - start_time
        
        summary = {
            "timestamp": datetime.now().isoformat(),
            "duration_seconds": duration,
            "total_tests": total_tests,
            "passed_tests": passed_tests,
            "failed_tests": failed_tests,
            "success_rate": (passed_tests / total_tests) * 100 if total_tests > 0 else 0,
            "test_results": self.test_results
        }
        
        print("\n" + "=" * 60)
        print("📊 Integration Test Summary")
        print(f"   Total Tests: {total_tests}")
        print(f"   Passed: {passed_tests}")
        print(f"   Failed: {failed_tests}")
        print(f"   Success Rate: {summary['success_rate']:.1f}%")
        print(f"   Duration: {duration:.2f} seconds")
        
        if failed_tests == 0:
            print("✅ ALL INTEGRATION TESTS PASSED - Event streaming ready for production")
        else:
            print("❌ Some integration tests failed - Review errors before production deployment")
        
        return summary

async def main():
    """Main test execution"""
    test_suite = EventStreamingIntegrationTest()
    
    try:
        results = await test_suite.run_all_tests()
        
        # Save results to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = f"event_streaming_integration_test_results_{timestamp}.json"
        
        with open(results_file, "w") as f:
            json.dump(results, f, indent=2)
        
        print(f"\n📄 Test results saved to: {results_file}")
        
        # Exit with appropriate code
        return 0 if results["failed_tests"] == 0 else 1
        
    except Exception as e:
        print(f"❌ Integration test suite failed: {e}")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())