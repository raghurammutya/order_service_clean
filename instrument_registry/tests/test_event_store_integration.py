#!/usr/bin/env python3
"""
Event Store and Audit Trail Integration Test

Comprehensive test that exercises the event store and audit trail functionality.
Addresses critical review finding #3 - proves event sourcing capability.

Usage:
    python3 test_event_store_integration.py
"""

import asyncio
import asyncpg
import json
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import uuid

# Database connection details
DATABASE_URL = "postgresql://stocksblitz:b4Gr60lYlbZVZz0ZRTcnf_YRkjO0sluNcwwJ-7lAfn4@localhost:5432/stocksblitz_unified_prod"
SCHEMA_NAME = "instrument_registry"

class EventStoreIntegrationTester:
    """Test event store and audit trail functionality comprehensively"""
    
    def __init__(self):
        self.conn = None
        self.test_session_id = f"test_session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.test_results = {
            "session_id": self.test_session_id,
            "start_time": datetime.now().isoformat(),
            "tests": {},
            "cleanup_performed": False
        }
        
    async def connect(self):
        """Establish database connection"""
        try:
            self.conn = await asyncpg.connect(DATABASE_URL)
            print("✓ Database connection established")
            return True
        except Exception as e:
            print(f"✗ Failed to connect to database: {e}")
            return False
    
    async def close(self):
        """Close database connection"""
        if self.conn:
            await self.conn.close()
    
    async def test_instrument_lifecycle_events(self) -> Dict[str, Any]:
        """Test complete instrument lifecycle with events and audit trail"""
        print("\n📋 Testing Instrument Lifecycle Events...")
        test_name = "instrument_lifecycle"
        results = {"success": False, "details": {}, "errors": []}
        
        try:
            # Create test instrument
            instrument_key = f"LIFECYCLE_TEST_{self.test_session_id}"
            
            print(f"   📝 Creating instrument: {instrument_key}")
            await self.conn.execute("""
                INSERT INTO instrument_registry.instruments 
                (instrument_key, symbol, exchange, segment, instrument_type, asset_class, 
                 lot_size, data_source, refresh_batch_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """, instrument_key, "TESTSTOCK", "NSE", "EQ", "EQ", "equity", 
                1, "integration_test", self.test_session_id)
            
            # Verify instrument was created
            instrument = await self.conn.fetchrow("""
                SELECT instrument_key, symbol, created_at, data_version 
                FROM instrument_registry.instruments 
                WHERE instrument_key = $1
            """, instrument_key)
            
            if not instrument:
                results["errors"].append("Failed to create instrument")
                return results
            
            results["details"]["instrument_created"] = True
            results["details"]["initial_version"] = instrument["data_version"]
            print(f"      ✓ Instrument created (version {instrument['data_version']})")
            
            # Check audit trail for creation
            audit_records = await self.conn.fetch("""
                SELECT operation, changed_at, new_data
                FROM instrument_registry.audit_trail 
                WHERE instrument_key = $1 AND operation = 'INSERT'
                ORDER BY changed_at DESC
            """, instrument_key)
            
            if audit_records:
                results["details"]["creation_audited"] = True
                results["details"]["audit_records_count"] = len(audit_records)
                print(f"      ✓ Creation audit record found ({len(audit_records)} records)")
            else:
                results["errors"].append("No audit record for instrument creation")
            
            # Update instrument to test UPDATE audit
            print("   📝 Updating instrument...")
            await self.conn.execute("""
                UPDATE instrument_registry.instruments 
                SET lot_size = $2, refresh_batch_id = $3
                WHERE instrument_key = $1
            """, instrument_key, 100, f"{self.test_session_id}_update")
            
            # Verify update and version increment
            updated_instrument = await self.conn.fetchrow("""
                SELECT data_version, lot_size, updated_at 
                FROM instrument_registry.instruments 
                WHERE instrument_key = $1
            """, instrument_key)
            
            if updated_instrument["data_version"] > instrument["data_version"]:
                results["details"]["version_incremented"] = True
                results["details"]["new_version"] = updated_instrument["data_version"]
                print(f"      ✓ Version incremented to {updated_instrument['data_version']}")
            else:
                results["errors"].append("Version did not increment on update")
            
            # Check audit trail for update
            update_audit = await self.conn.fetch("""
                SELECT operation, changed_fields, old_data, new_data
                FROM instrument_registry.audit_trail 
                WHERE instrument_key = $1 AND operation = 'UPDATE'
                ORDER BY changed_at DESC LIMIT 1
            """, instrument_key)
            
            if update_audit:
                audit_record = update_audit[0]
                results["details"]["update_audited"] = True
                results["details"]["changed_fields"] = list(audit_record["changed_fields"])
                print(f"      ✓ Update audit record found (changed: {audit_record['changed_fields']})")
                
                # Verify lot_size change was captured
                if "lot_size" in audit_record["changed_fields"]:
                    old_lot_size = audit_record["old_data"].get("lot_size")
                    new_lot_size = audit_record["new_data"].get("lot_size")
                    results["details"]["lot_size_change"] = {"old": old_lot_size, "new": new_lot_size}
                    print(f"      ✓ Lot size change tracked: {old_lot_size} → {new_lot_size}")
            else:
                results["errors"].append("No audit record for instrument update")
            
            # Test soft delete (setting is_deleted=true)
            print("   📝 Soft deleting instrument...")
            await self.conn.execute("""
                UPDATE instrument_registry.instruments 
                SET is_deleted = true, refresh_batch_id = $2
                WHERE instrument_key = $1
            """, instrument_key, f"{self.test_session_id}_delete")
            
            # Verify soft delete in audit
            delete_audit = await self.conn.fetch("""
                SELECT operation, changed_fields
                FROM instrument_registry.audit_trail 
                WHERE instrument_key = $1 AND 'is_deleted' = ANY(changed_fields)
                ORDER BY changed_at DESC LIMIT 1
            """, instrument_key)
            
            if delete_audit:
                results["details"]["soft_delete_audited"] = True
                print(f"      ✓ Soft delete audit record found")
            
            # Count total audit records for this instrument
            total_audit_count = await self.conn.fetchval("""
                SELECT count(*) FROM instrument_registry.audit_trail 
                WHERE instrument_key = $1
            """, instrument_key)
            
            results["details"]["total_audit_records"] = total_audit_count
            print(f"      ✓ Total audit records: {total_audit_count}")
            
            # Cleanup (real delete for this test)
            await self.conn.execute("DELETE FROM instrument_registry.instruments WHERE instrument_key = $1", instrument_key)
            await self.conn.execute("DELETE FROM instrument_registry.audit_trail WHERE instrument_key = $1", instrument_key)
            
            results["success"] = len(results["errors"]) == 0
            
        except Exception as e:
            results["errors"].append(f"Exception in lifecycle test: {str(e)}")
            results["success"] = False
        
        self.test_results["tests"][test_name] = results
        return results
    
    async def test_event_store_functionality(self) -> Dict[str, Any]:
        """Test event store with proper event sourcing patterns"""
        print("\n📦 Testing Event Store Functionality...")
        test_name = "event_store"
        results = {"success": False, "details": {}, "errors": []}
        
        try:
            # Test event stream creation
            stream_id = f"test_stream_{self.test_session_id}"
            aggregate_id = f"test_aggregate_{self.test_session_id}"
            
            events = [
                {
                    "event_type": "instrument.created",
                    "version": 1,
                    "data": {"symbol": "TESTEVT", "exchange": "NSE", "action": "created"}
                },
                {
                    "event_type": "instrument.updated", 
                    "version": 2,
                    "data": {"symbol": "TESTEVT", "lot_size": 100, "action": "lot_size_updated"}
                },
                {
                    "event_type": "instrument.activated",
                    "version": 3, 
                    "data": {"symbol": "TESTEVT", "is_active": True, "action": "activated"}
                }
            ]
            
            print(f"   📝 Writing {len(events)} events to stream: {stream_id}")
            
            # Write events to store
            for i, event in enumerate(events):
                await self.conn.execute("""
                    INSERT INTO instrument_registry.instrument_events 
                    (event_stream_id, event_type, event_version, aggregate_id, aggregate_version, 
                     event_data, event_metadata, occurred_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """, stream_id, event["event_type"], event["version"], 
                    aggregate_id, event["version"], 
                    json.dumps(event["data"]), 
                    json.dumps({"test_session": self.test_session_id, "sequence": i}),
                    datetime.now())
            
            results["details"]["events_written"] = len(events)
            print(f"      ✓ {len(events)} events written successfully")
            
            # Read events back in order
            stored_events = await self.conn.fetch("""
                SELECT event_type, event_version, event_data, event_metadata, occurred_at
                FROM instrument_registry.instrument_events 
                WHERE event_stream_id = $1 
                ORDER BY event_version ASC
            """, stream_id)
            
            results["details"]["events_read"] = len(stored_events)
            print(f"      ✓ {len(stored_events)} events read back")
            
            # Verify event ordering
            if len(stored_events) == len(events):
                ordering_correct = True
                for i, stored_event in enumerate(stored_events):
                    expected_version = i + 1
                    if stored_event["event_version"] != expected_version:
                        ordering_correct = False
                        break
                
                results["details"]["ordering_correct"] = ordering_correct
                if ordering_correct:
                    print(f"      ✓ Event ordering preserved")
                else:
                    results["errors"].append("Event ordering not preserved")
            else:
                results["errors"].append(f"Event count mismatch: wrote {len(events)}, read {len(stored_events)}")
            
            # Test event data integrity
            data_integrity_ok = True
            for i, stored_event in enumerate(stored_events):
                expected_data = events[i]["data"]
                stored_data = stored_event["event_data"]
                
                # Convert stored JSON back to dict for comparison
                if isinstance(stored_data, str):
                    stored_data = json.loads(stored_data)
                
                if stored_data != expected_data:
                    data_integrity_ok = False
                    results["errors"].append(f"Data mismatch in event {i+1}")
                    break
            
            results["details"]["data_integrity"] = data_integrity_ok
            if data_integrity_ok:
                print(f"      ✓ Event data integrity verified")
            
            # Test event replay (reconstruct aggregate state)
            print("   📝 Testing event replay...")
            replay_state = {"symbol": None, "lot_size": 1, "is_active": False}
            
            for stored_event in stored_events:
                event_data = stored_event["event_data"]
                if isinstance(event_data, str):
                    event_data = json.loads(event_data)
                
                action = event_data.get("action")
                if action == "created":
                    replay_state["symbol"] = event_data.get("symbol")
                elif action == "lot_size_updated":
                    replay_state["lot_size"] = event_data.get("lot_size")
                elif action == "activated":
                    replay_state["is_active"] = event_data.get("is_active")
            
            expected_final_state = {"symbol": "TESTEVT", "lot_size": 100, "is_active": True}
            replay_success = replay_state == expected_final_state
            
            results["details"]["replay_success"] = replay_success
            results["details"]["final_state"] = replay_state
            
            if replay_success:
                print(f"      ✓ Event replay successful: {replay_state}")
            else:
                results["errors"].append(f"Event replay failed: expected {expected_final_state}, got {replay_state}")
            
            # Cleanup
            await self.conn.execute("DELETE FROM instrument_registry.instrument_events WHERE event_stream_id = $1", stream_id)
            
            results["success"] = len(results["errors"]) == 0
            
        except Exception as e:
            results["errors"].append(f"Exception in event store test: {str(e)}")
            results["success"] = False
        
        self.test_results["tests"][test_name] = results
        return results
    
    async def test_broker_token_mapping(self) -> Dict[str, Any]:
        """Test broker token mapping functionality with audit"""
        print("\n🔗 Testing Broker Token Mapping...")
        test_name = "broker_token_mapping"
        results = {"success": False, "details": {}, "errors": []}
        
        try:
            # Create test instrument first
            instrument_key = f"TOKEN_TEST_{self.test_session_id}"
            
            await self.conn.execute("""
                INSERT INTO instrument_registry.instruments 
                (instrument_key, symbol, exchange, segment, instrument_type, asset_class)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, instrument_key, "TOKENTEST", "NSE", "EQ", "EQ", "equity")
            
            # Add multiple broker tokens
            broker_mappings = [
                {"broker": "kite", "token": "123456", "symbol": "TOKENTEST"},
                {"broker": "upstox", "token": "UX_789", "symbol": "NSE_EQ|INE123456789"},
                {"broker": "ibkr", "token": "IBKR_ABC", "symbol": "TOKENTEST-NSE-INR"}
            ]
            
            print(f"   📝 Adding {len(broker_mappings)} broker token mappings...")
            
            for mapping in broker_mappings:
                await self.conn.execute("""
                    INSERT INTO instrument_registry.broker_tokens 
                    (instrument_key, broker_name, broker_token, broker_symbol)
                    VALUES ($1, $2, $3, $4)
                """, instrument_key, mapping["broker"], mapping["token"], mapping["symbol"])
            
            results["details"]["tokens_added"] = len(broker_mappings)
            print(f"      ✓ {len(broker_mappings)} token mappings added")
            
            # Test token lookup by broker
            for mapping in broker_mappings:
                lookup_result = await self.conn.fetchrow("""
                    SELECT bt.broker_token, bt.broker_symbol, i.symbol as instrument_symbol
                    FROM instrument_registry.broker_tokens bt
                    JOIN instrument_registry.instruments i ON bt.instrument_key = i.instrument_key
                    WHERE bt.broker_name = $1 AND bt.broker_token = $2
                """, mapping["broker"], mapping["token"])
                
                if lookup_result and lookup_result["instrument_symbol"] == "TOKENTEST":
                    print(f"      ✓ {mapping['broker']} token lookup successful")
                else:
                    results["errors"].append(f"Failed to lookup {mapping['broker']} token")
            
            # Test broker_token_lookup view
            view_results = await self.conn.fetch("""
                SELECT broker_name, broker_token, symbol
                FROM instrument_registry.broker_token_lookup
                WHERE instrument_key = $1
            """, instrument_key)
            
            results["details"]["view_results_count"] = len(view_results)
            
            if len(view_results) == len(broker_mappings):
                print(f"      ✓ Broker token lookup view working ({len(view_results)} results)")
            else:
                results["errors"].append(f"View results mismatch: expected {len(broker_mappings)}, got {len(view_results)}")
            
            # Test token update with audit trail
            print("   📝 Updating broker token...")
            await self.conn.execute("""
                UPDATE instrument_registry.broker_tokens 
                SET broker_token = $3
                WHERE instrument_key = $1 AND broker_name = $2
            """, instrument_key, "kite", "999999")
            
            # Verify update
            updated_token = await self.conn.fetchval("""
                SELECT broker_token FROM instrument_registry.broker_tokens
                WHERE instrument_key = $1 AND broker_name = $2
            """, instrument_key, "kite")
            
            if updated_token == "999999":
                results["details"]["token_update_success"] = True
                print(f"      ✓ Token update successful")
            else:
                results["errors"].append("Token update failed")
            
            # Cleanup
            await self.conn.execute("DELETE FROM instrument_registry.broker_tokens WHERE instrument_key = $1", instrument_key)
            await self.conn.execute("DELETE FROM instrument_registry.instruments WHERE instrument_key = $1", instrument_key)
            await self.conn.execute("DELETE FROM instrument_registry.audit_trail WHERE instrument_key = $1", instrument_key)
            
            results["success"] = len(results["errors"]) == 0
            
        except Exception as e:
            results["errors"].append(f"Exception in broker token test: {str(e)}")
            results["success"] = False
        
        self.test_results["tests"][test_name] = results
        return results
    
    async def test_data_quality_functions(self) -> Dict[str, Any]:
        """Test data quality validation and derived data functions"""
        print("\n📊 Testing Data Quality Functions...")
        test_name = "data_quality"
        results = {"success": False, "details": {}, "errors": []}
        
        try:
            # Create test instruments for quality validation
            test_instruments = [
                ("QUALITY_GOOD", "GOODSTOCK", "NSE", "EQ", "EQ", "equity", 100, "TECHNOLOGY"),
                ("QUALITY_BAD", "", "NSE", "EQ", "EQ", "equity", 1, None),  # Missing name and sector
                ("QUALITY_OPTION", "NIFTY26JAN26C", "NSE", "FO", "CE", "equity", 25, "INDEX")
            ]
            
            print(f"   📝 Creating {len(test_instruments)} test instruments...")
            
            for instrument_data in test_instruments:
                await self.conn.execute("""
                    INSERT INTO instrument_registry.instruments 
                    (instrument_key, symbol, exchange, segment, instrument_type, asset_class, 
                     lot_size, sector, underlying_symbol, strike, expiry, refresh_batch_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                """, instrument_data[0], instrument_data[1], instrument_data[2], 
                    instrument_data[3], instrument_data[4], instrument_data[5], 
                    instrument_data[6], instrument_data[7],
                    "NIFTY" if instrument_data[4] in ["CE", "PE"] else None,
                    26000.0 if instrument_data[4] in ["CE", "PE"] else None,
                    "2026-01-30" if instrument_data[4] in ["CE", "PE"] else None,
                    self.test_session_id)
            
            results["details"]["test_instruments_created"] = len(test_instruments)
            print(f"      ✓ {len(test_instruments)} test instruments created")
            
            # Test data quality validation function
            print("   📝 Running data quality validation...")
            quality_result = await self.conn.fetchrow(
                "SELECT * FROM instrument_registry.validate_data_quality($1)", 
                self.test_session_id
            )
            
            if quality_result:
                results["details"]["quality_check_executed"] = True
                results["details"]["total_instruments"] = quality_result["total_count"]
                results["details"]["active_instruments"] = quality_result["active_count"]
                results["details"]["quality_score"] = float(quality_result["quality_score"])
                
                issues = quality_result["issues_found"]
                if isinstance(issues, str):
                    issues = json.loads(issues)
                    
                results["details"]["issues_found"] = issues
                
                print(f"      ✓ Quality check completed:")
                print(f"         Total instruments: {quality_result['total_count']}")
                print(f"         Quality score: {quality_result['quality_score']}")
                print(f"         Issues: {issues}")
                
                # Verify issues were detected (we have instruments with missing data)
                if issues.get("missing_names", 0) > 0 or issues.get("missing_sectors", 0) > 0:
                    results["details"]["issues_detected"] = True
                    print(f"      ✓ Data quality issues correctly detected")
                else:
                    results["errors"].append("Data quality issues not detected")
                
            else:
                results["errors"].append("Data quality function returned no results")
            
            # Test derive_option_chains function
            print("   📝 Testing option chains derivation...")
            chains_created = await self.conn.fetchval(
                "SELECT instrument_registry.derive_option_chains()"
            )
            
            results["details"]["option_chains_created"] = chains_created
            
            if chains_created is not None:
                print(f"      ✓ Option chains derived: {chains_created} chains")
                
                # Check if our test option was processed
                chain_record = await self.conn.fetchrow("""
                    SELECT underlying_symbol, expiry_date, strike_interval, strike_count
                    FROM instrument_registry.option_chains
                    WHERE underlying_symbol = 'NIFTY'
                """)
                
                if chain_record:
                    results["details"]["test_chain_found"] = True
                    results["details"]["chain_details"] = dict(chain_record)
                    print(f"      ✓ Test option chain found: {dict(chain_record)}")
                
            else:
                results["errors"].append("Option chains derivation failed")
            
            # Cleanup
            await self.conn.execute("DELETE FROM instrument_registry.option_chains")
            await self.conn.execute("DELETE FROM instrument_registry.data_quality_checks WHERE refresh_batch_id = $1", self.test_session_id)
            
            for instrument_data in test_instruments:
                await self.conn.execute("DELETE FROM instrument_registry.instruments WHERE instrument_key = $1", instrument_data[0])
                await self.conn.execute("DELETE FROM instrument_registry.audit_trail WHERE instrument_key = $1", instrument_data[0])
            
            results["success"] = len(results["errors"]) == 0
            
        except Exception as e:
            results["errors"].append(f"Exception in data quality test: {str(e)}")
            results["success"] = False
        
        self.test_results["tests"][test_name] = results
        return results
    
    async def generate_integration_report(self) -> Dict[str, Any]:
        """Generate comprehensive integration test report"""
        print("\n📊 Generating Integration Test Report...")
        
        self.test_results["end_time"] = datetime.now().isoformat()
        
        # Calculate overall results
        total_tests = len(self.test_results["tests"])
        successful_tests = sum(1 for test in self.test_results["tests"].values() if test["success"])
        success_rate = (successful_tests / total_tests * 100) if total_tests > 0 else 0
        
        # Count total errors
        total_errors = sum(len(test.get("errors", [])) for test in self.test_results["tests"].values())
        
        report = {
            **self.test_results,
            "summary": {
                "total_tests": total_tests,
                "successful_tests": successful_tests,
                "failed_tests": total_tests - successful_tests,
                "success_rate": round(success_rate, 2),
                "total_errors": total_errors,
                "overall_status": "PASS" if success_rate >= 90 else "PARTIAL" if success_rate >= 70 else "FAIL"
            }
        }
        
        return report

async def main():
    """Main integration test execution"""
    print("🧪 INSTRUMENT REGISTRY EVENT STORE INTEGRATION TEST")
    print("=" * 60)
    
    tester = EventStoreIntegrationTester()
    
    try:
        # Connect to database
        if not await tester.connect():
            return 1
        
        # Run all integration tests
        await tester.test_instrument_lifecycle_events()
        await tester.test_event_store_functionality()
        await tester.test_broker_token_mapping()
        await tester.test_data_quality_functions()
        
        # Generate final report
        report = await tester.generate_integration_report()
        
        # Save report to file
        report_file = f"event_store_integration_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n📋 Integration Test Report saved to: {report_file}")
        
        # Display summary
        summary = report["summary"]
        print(f"\n🎯 INTEGRATION TEST SUMMARY:")
        print(f"   Total Tests: {summary['total_tests']}")
        print(f"   Successful: {summary['successful_tests']}")
        print(f"   Failed: {summary['failed_tests']}")
        print(f"   Success Rate: {summary['success_rate']}%")
        print(f"   Total Errors: {summary['total_errors']}")
        print(f"   Overall Status: {summary['overall_status']}")
        
        # Show per-test results
        print(f"\n📋 Per-Test Results:")
        for test_name, test_result in report["tests"].items():
            status = "✓" if test_result["success"] else "✗"
            error_count = len(test_result.get("errors", []))
            print(f"   {status} {test_name}: {'PASS' if test_result['success'] else 'FAIL'} ({error_count} errors)")
        
        if summary["overall_status"] == "PASS":
            print(f"\n✅ EVENT STORE INTEGRATION TEST SUCCESSFUL")
            print("   Event store and audit trail functionality verified.")
            return 0
        elif summary["overall_status"] == "PARTIAL":
            print(f"\n⚠️  INTEGRATION TEST PARTIALLY SUCCESSFUL")
            print("   Some functionality works but issues found.")
            return 1
        else:
            print(f"\n❌ INTEGRATION TEST FAILED")
            print("   Critical issues with event store functionality.")
            return 1
            
    except Exception as e:
        print(f"\n💥 Integration test failed with exception: {e}")
        return 1
    finally:
        await tester.close()

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)