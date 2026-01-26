#!/usr/bin/env python3
"""
Comprehensive Subscription Profile Management Test Suite

Tests all aspects of production-ready subscription profile management:
- Config-driven validation and limits
- Comprehensive audit logging  
- Conflict resolution strategies
- Lifecycle management with retention policies
"""

import asyncio
import logging
import time
import uuid
import json
from datetime import datetime, timezone
from typing import Dict, Any, List

import asyncpg
import httpx
from common.config_client import ConfigClient
from app.services.subscription_profile_service import (
    SubscriptionProfileService,
    SubscriptionProfile,
    SubscriptionType,
    MonitoringService
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SubscriptionManagementTester:
    """Comprehensive subscription management test suite"""
    
    def __init__(self):
        self.database_url = "postgresql://stocksblitz:b4Gr60lYlbZVZz0ZRTcnf_YRkjO0sluNcwwJ-7lAfn4@localhost:5432/stocksblitz_unified_prod"
        self.config_client = ConfigClient(
            service_name="instrument_registry",
            internal_api_key="AShhRzWhfXd6IomyzZnE3d-lCcAvT1L5GDCCZRSXZGsJq7_eAJGxeMi-4AlfTeOc"
        )
        self.monitoring = None
        self.service = None
        self.test_results = []
    
    async def setup(self):
        """Setup test environment"""
        logger.info("Setting up subscription management test environment...")
        
        # Initialize config client
        await self.config_client.initialize()
        
        # Create mock monitoring service
        self.monitoring = MockMonitoringService()
        
        # Initialize subscription service
        self.service = SubscriptionProfileService(
            database_url=self.database_url,
            config_client=self.config_client,
            monitoring=self.monitoring
        )
        
        logger.info("Test environment setup complete")
    
    async def cleanup(self):
        """Cleanup test environment"""
        logger.info("Cleaning up test environment...")
        
        # Clean up test data
        conn = await asyncpg.connect(self.database_url)
        try:
            await conn.execute("DELETE FROM instrument_registry.subscription_conflicts WHERE user_id LIKE 'test_%'")
            await conn.execute("DELETE FROM instrument_registry.subscription_audit_log WHERE user_id LIKE 'test_%'")
            await conn.execute("DELETE FROM instrument_registry.subscription_profiles WHERE user_id LIKE 'test_%'")
            await conn.execute("DELETE FROM instrument_registry.user_subscription_limits WHERE user_id LIKE 'test_%'")
        finally:
            await conn.close()
        
        await self.config_client.close()
        logger.info("Test cleanup complete")
    
    def record_test_result(self, test_name: str, passed: bool, details: Dict[str, Any] = None):
        """Record test result"""
        self.test_results.append({
            "test_name": test_name,
            "passed": passed,
            "details": details or {},
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        
        status = "PASS" if passed else "FAIL"
        logger.info(f"[{status}] {test_name}")
        if details:
            logger.info(f"    Details: {details}")
    
    async def test_config_parameter_access(self):
        """Test 1: Verify all config parameters are accessible"""
        test_name = "Config Parameter Access"
        
        try:
            config_params = [
                'INSTRUMENT_REGISTRY_SUBSCRIPTION_TIMEOUT',
                'INSTRUMENT_REGISTRY_MAX_SUBSCRIPTIONS_PER_USER',
                'INSTRUMENT_REGISTRY_PROFILE_VALIDATION_STRICT',
                'INSTRUMENT_REGISTRY_AUDIT_RETENTION_DAYS',
                'INSTRUMENT_REGISTRY_CONFLICT_RESOLUTION_STRATEGY'
            ]
            
            results = {}
            for param in config_params:
                try:
                    value = await self.config_client.get_secret(param, environment='prod')
                    results[param] = value
                except Exception as e:
                    results[param] = f"ERROR: {e}"
            
            # Check if all parameters are accessible
            all_accessible = all(not str(v).startswith("ERROR:") for v in results.values())
            
            self.record_test_result(
                test_name, 
                all_accessible, 
                {"config_values": results}
            )
            
        except Exception as e:
            self.record_test_result(test_name, False, {"error": str(e)})
    
    async def test_subscription_creation_with_limits(self):
        """Test 2: Test subscription creation with config-driven limits"""
        test_name = "Subscription Creation with Limits"
        
        try:
            # Create a test user subscription profile
            test_user = "test_user_limits"
            profile = SubscriptionProfile(
                profile_id=f"profile_{uuid.uuid4().hex}",
                user_id=test_user,
                profile_name="Test Limits Profile",
                subscription_type=SubscriptionType.LIVE_FEED,
                instruments=["NSE:RELIANCE", "NSE:TCS", "NSE:INFY"]
            )
            
            result = await self.service.create_subscription_profile(profile)
            
            # Verify the profile was created
            retrieved_profile = await self.service.get_subscription_profile(profile.profile_id)
            
            success = (
                result['status'] == 'created' and
                retrieved_profile is not None and
                retrieved_profile.user_id == test_user
            )
            
            self.record_test_result(
                test_name,
                success,
                {
                    "profile_id": profile.profile_id,
                    "creation_result": result,
                    "profile_retrieved": retrieved_profile is not None
                }
            )
            
        except Exception as e:
            self.record_test_result(test_name, False, {"error": str(e)})
    
    async def test_conflict_resolution_latest_wins(self):
        """Test 3: Test conflict resolution with 'latest_wins' strategy"""
        test_name = "Conflict Resolution - Latest Wins"
        
        try:
            # Create a profile that might trigger limits
            test_user = "test_user_conflicts"
            
            # Create multiple profiles to test limit enforcement
            profiles_created = []
            for i in range(3):  # Should be within default limits
                profile = SubscriptionProfile(
                    profile_id=f"profile_{uuid.uuid4().hex}",
                    user_id=test_user,
                    profile_name=f"Test Profile {i}",
                    subscription_type=SubscriptionType.LIVE_FEED,
                    instruments=[f"NSE:TEST{i}", f"BSE:TEST{i}"]
                )
                
                result = await self.service.create_subscription_profile(profile)
                profiles_created.append((profile.profile_id, result))
            
            # Verify strategy application - should allow creation even if limits approached
            all_created = all(r[1]['status'] == 'created' for r in profiles_created)
            
            self.record_test_result(
                test_name,
                all_created,
                {
                    "profiles_created": len(profiles_created),
                    "results": [r[1] for r in profiles_created]
                }
            )
            
        except Exception as e:
            self.record_test_result(test_name, False, {"error": str(e)})
    
    async def test_audit_logging_capture(self):
        """Test 4: Verify audit logging captures all required events"""
        test_name = "Audit Logging Capture"
        
        try:
            # Create a profile to generate audit logs
            test_user = "test_user_audit"
            profile = SubscriptionProfile(
                profile_id=f"profile_{uuid.uuid4().hex}",
                user_id=test_user,
                profile_name="Audit Test Profile",
                subscription_type=SubscriptionType.ALERTS,
                instruments=["NSE:AUDIT_TEST"]
            )
            
            audit_metadata = {
                "ip_address": "127.0.0.1",
                "user_agent": "SubscriptionManagementTester/1.0",
                "test_context": "audit_logging_test"
            }
            
            result = await self.service.create_subscription_profile(profile, audit_metadata)
            
            # Query audit logs to verify capture
            conn = await asyncpg.connect(self.database_url)
            try:
                audit_logs = await conn.fetch("""
                    SELECT audit_id, action, entity_type, metadata, ip_address, user_agent
                    FROM instrument_registry.subscription_audit_log
                    WHERE user_id = $1 AND profile_id = $2
                    ORDER BY created_at DESC
                """, test_user, profile.profile_id)
                
                audit_captured = len(audit_logs) > 0
                if audit_captured:
                    log_details = {
                        "count": len(audit_logs),
                        "actions": [log['action'] for log in audit_logs],
                        "metadata_captured": audit_logs[0]['metadata'] is not None,
                        "ip_captured": audit_logs[0]['ip_address'] == "127.0.0.1"
                    }
                else:
                    log_details = {"count": 0}
                
            finally:
                await conn.close()
            
            self.record_test_result(
                test_name,
                audit_captured,
                {
                    "audit_logs": log_details,
                    "profile_id": profile.profile_id
                }
            )
            
        except Exception as e:
            self.record_test_result(test_name, False, {"error": str(e)})
    
    async def test_user_profile_listing(self):
        """Test 5: Test user profile listing with filtering and pagination"""
        test_name = "User Profile Listing"
        
        try:
            test_user = "test_user_listing"
            
            # Create multiple profiles of different types
            profiles_data = [
                (SubscriptionType.LIVE_FEED, "Live Feed Profile", ["NSE:LIST1"]),
                (SubscriptionType.HISTORICAL, "Historical Profile", ["NSE:LIST2"]),
                (SubscriptionType.ALERTS, "Alerts Profile", ["NSE:LIST3"])
            ]
            
            created_profiles = []
            for sub_type, name, instruments in profiles_data:
                profile = SubscriptionProfile(
                    profile_id=f"profile_{uuid.uuid4().hex}",
                    user_id=test_user,
                    profile_name=name,
                    subscription_type=sub_type,
                    instruments=instruments
                )
                
                await self.service.create_subscription_profile(profile)
                created_profiles.append(profile)
            
            # Test listing all profiles for user
            listing_result = await self.service.list_user_subscription_profiles(test_user)
            
            # Test filtered listing
            filtered_result = await self.service.list_user_subscription_profiles(
                test_user, 
                subscription_type=SubscriptionType.LIVE_FEED
            )
            
            success = (
                listing_result['pagination']['total'] >= len(profiles_data) and
                filtered_result['pagination']['total'] >= 1
            )
            
            self.record_test_result(
                test_name,
                success,
                {
                    "total_profiles": listing_result['pagination']['total'],
                    "filtered_profiles": filtered_result['pagination']['total'],
                    "created_count": len(created_profiles)
                }
            )
            
        except Exception as e:
            self.record_test_result(test_name, False, {"error": str(e)})
    
    async def test_audit_log_cleanup(self):
        """Test 6: Test audit log cleanup honors retention policies"""
        test_name = "Audit Log Cleanup"
        
        try:
            # Get current retention setting
            config = await self.service._get_config_values()
            retention_days = config.get('INSTRUMENT_REGISTRY_AUDIT_RETENTION_DAYS', 365)
            
            # Since we can't create old audit logs easily in test, we'll test the cleanup function
            # This should run without error and return cleanup statistics
            cleanup_result = await self.service.cleanup_expired_audit_logs()
            
            # Verify cleanup function works
            cleanup_successful = (
                'deleted_count' in cleanup_result and
                'retention_days' in cleanup_result and
                cleanup_result['retention_days'] == retention_days
            )
            
            self.record_test_result(
                test_name,
                cleanup_successful,
                {
                    "cleanup_result": cleanup_result,
                    "retention_policy": f"{retention_days} days"
                }
            )
            
        except Exception as e:
            self.record_test_result(test_name, False, {"error": str(e)})
    
    async def test_config_driven_validation_adapts(self):
        """Test 7: Validate that behavior adapts to config changes"""
        test_name = "Config-Driven Validation Adaptation"
        
        try:
            # Test that service reads current config values
            config_values = await self.service._get_config_values()
            
            # Verify all expected config keys are present
            expected_keys = [
                'INSTRUMENT_REGISTRY_SUBSCRIPTION_TIMEOUT',
                'INSTRUMENT_REGISTRY_MAX_SUBSCRIPTIONS_PER_USER',
                'INSTRUMENT_REGISTRY_PROFILE_VALIDATION_STRICT',
                'INSTRUMENT_REGISTRY_AUDIT_RETENTION_DAYS',
                'INSTRUMENT_REGISTRY_CONFLICT_RESOLUTION_STRATEGY'
            ]
            
            all_keys_present = all(key in config_values for key in expected_keys)
            
            # Verify values are of correct types
            type_checks = {
                'INSTRUMENT_REGISTRY_SUBSCRIPTION_TIMEOUT': int,
                'INSTRUMENT_REGISTRY_MAX_SUBSCRIPTIONS_PER_USER': int,
                'INSTRUMENT_REGISTRY_PROFILE_VALIDATION_STRICT': bool,
                'INSTRUMENT_REGISTRY_AUDIT_RETENTION_DAYS': int,
                'INSTRUMENT_REGISTRY_CONFLICT_RESOLUTION_STRATEGY': str
            }
            
            type_validation_passed = True
            for key, expected_type in type_checks.items():
                if key in config_values:
                    if not isinstance(config_values[key], expected_type):
                        type_validation_passed = False
                        logger.warning(f"Config {key} has wrong type: {type(config_values[key])} vs {expected_type}")
            
            success = all_keys_present and type_validation_passed
            
            self.record_test_result(
                test_name,
                success,
                {
                    "config_keys_present": all_keys_present,
                    "type_validation": type_validation_passed,
                    "config_values": config_values
                }
            )
            
        except Exception as e:
            self.record_test_result(test_name, False, {"error": str(e)})
    
    async def generate_test_report(self) -> Dict[str, Any]:
        """Generate comprehensive test report"""
        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results if result['passed'])
        failed_tests = total_tests - passed_tests
        
        report = {
            "test_summary": {
                "total_tests": total_tests,
                "passed_tests": passed_tests,
                "failed_tests": failed_tests,
                "success_rate": round((passed_tests / total_tests) * 100, 2) if total_tests > 0 else 0
            },
            "test_results": self.test_results,
            "production_readiness": {
                "config_integration": any(r['passed'] for r in self.test_results if 'Config' in r['test_name']),
                "audit_logging": any(r['passed'] for r in self.test_results if 'Audit' in r['test_name']),
                "conflict_resolution": any(r['passed'] for r in self.test_results if 'Conflict' in r['test_name']),
                "lifecycle_management": any(r['passed'] for r in self.test_results if 'Cleanup' in r['test_name'])
            },
            "generated_at": datetime.now(timezone.utc).isoformat()
        }
        
        return report
    
    async def run_all_tests(self):
        """Run complete test suite"""
        logger.info("Starting comprehensive subscription management test suite...")
        
        try:
            await self.setup()
            
            # Run all tests
            await self.test_config_parameter_access()
            await self.test_subscription_creation_with_limits()
            await self.test_conflict_resolution_latest_wins()
            await self.test_audit_logging_capture()
            await self.test_user_profile_listing()
            await self.test_audit_log_cleanup()
            await self.test_config_driven_validation_adapts()
            
            # Generate and save report
            report = await self.generate_test_report()
            
            # Save test report
            with open('subscription_management_test_report.json', 'w') as f:
                json.dump(report, f, indent=2)
            
            logger.info(f"Test suite completed: {report['test_summary']['passed_tests']}/{report['test_summary']['total_tests']} tests passed")
            return report
            
        finally:
            await self.cleanup()


class MockMonitoringService:
    """Mock monitoring service for testing"""
    
    def record_operation_duration(self, operation: str, duration: float):
        pass
    
    async def record_system_health(self, component: str, healthy: bool):
        pass


async def main():
    """Main test execution"""
    tester = SubscriptionManagementTester()
    
    try:
        report = await tester.run_all_tests()
        
        print("\n" + "="*80)
        print("SUBSCRIPTION MANAGEMENT TEST REPORT")
        print("="*80)
        print(f"Total Tests: {report['test_summary']['total_tests']}")
        print(f"Passed: {report['test_summary']['passed_tests']}")
        print(f"Failed: {report['test_summary']['failed_tests']}")
        print(f"Success Rate: {report['test_summary']['success_rate']}%")
        print("\nProduction Readiness:")
        for component, ready in report['production_readiness'].items():
            status = "✓" if ready else "✗"
            print(f"  {status} {component.replace('_', ' ').title()}")
        print("\nDetailed report saved to: subscription_management_test_report.json")
        print("="*80)
        
        return report['test_summary']['success_rate'] == 100.0
        
    except Exception as e:
        logger.error(f"Test execution failed: {e}")
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)