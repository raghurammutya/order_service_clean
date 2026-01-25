#!/usr/bin/env python3
"""
Schema Deployment Verification Script

Automated verification that the instrument_registry schema deployment succeeded.
Addresses critical review finding #2 - provides proofs that schema is operational.

Usage:
    python3 verify_schema_deployment.py
"""

import asyncio
import asyncpg
import json
import sys
from datetime import datetime
from typing import Dict, List, Any

# Database connection details
DATABASE_URL = "postgresql://stocksblitz:b4Gr60lYlbZVZz0ZRTcnf_YRkjO0sluNcwwJ-7lAfn4@localhost:5432/stocksblitz_unified_prod"
SCHEMA_NAME = "instrument_registry"

class SchemaVerifier:
    """Verify instrument registry schema deployment"""
    
    def __init__(self):
        self.conn = None
        self.verification_results = {
            "schema_exists": False,
            "tables": {},
            "indexes": {},
            "functions": {},
            "views": {},
            "triggers": {},
            "permissions": {},
            "smoke_tests": {}
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
    
    async def verify_schema_exists(self) -> bool:
        """Verify the instrument_registry schema exists"""
        try:
            result = await self.conn.fetchval("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.schemata 
                    WHERE schema_name = $1
                )
            """, SCHEMA_NAME)
            
            self.verification_results["schema_exists"] = result
            if result:
                print(f"✓ Schema '{SCHEMA_NAME}' exists")
            else:
                print(f"✗ Schema '{SCHEMA_NAME}' does not exist")
            
            return result
        except Exception as e:
            print(f"✗ Error verifying schema: {e}")
            return False
    
    async def verify_tables(self) -> Dict[str, bool]:
        """Verify all expected tables exist with correct structure"""
        expected_tables = [
            "instruments", "broker_tokens", "instrument_events", "audit_trail",
            "option_chains", "lot_sizes", "data_quality_checks"
        ]
        
        print(f"\n📋 Verifying {len(expected_tables)} tables...")
        table_results = {}
        
        for table_name in expected_tables:
            try:
                # Check table exists
                exists = await self.conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = $1 AND table_name = $2
                    )
                """, SCHEMA_NAME, table_name)
                
                if exists:
                    # Get column count
                    col_count = await self.conn.fetchval("""
                        SELECT count(*) FROM information_schema.columns 
                        WHERE table_schema = $1 AND table_name = $2
                    """, SCHEMA_NAME, table_name)
                    
                    print(f"   ✓ Table '{table_name}' exists with {col_count} columns")
                    table_results[table_name] = True
                else:
                    print(f"   ✗ Table '{table_name}' missing")
                    table_results[table_name] = False
                    
            except Exception as e:
                print(f"   ✗ Error checking table '{table_name}': {e}")
                table_results[table_name] = False
        
        self.verification_results["tables"] = table_results
        return table_results
    
    async def verify_indexes(self) -> Dict[str, int]:
        """Verify performance indexes exist"""
        try:
            # Count indexes per table
            index_query = """
                SELECT 
                    schemaname, tablename, indexname
                FROM pg_indexes 
                WHERE schemaname = $1
                ORDER BY tablename, indexname
            """
            
            indexes = await self.conn.fetch(index_query, SCHEMA_NAME)
            
            # Group by table
            index_by_table = {}
            for idx in indexes:
                table = idx['tablename']
                if table not in index_by_table:
                    index_by_table[table] = []
                index_by_table[table].append(idx['indexname'])
            
            print(f"\n🔍 Found {len(indexes)} indexes across {len(index_by_table)} tables:")
            for table, table_indexes in index_by_table.items():
                print(f"   ✓ {table}: {len(table_indexes)} indexes")
                for idx_name in table_indexes[:3]:  # Show first 3
                    print(f"     - {idx_name}")
                if len(table_indexes) > 3:
                    print(f"     - ... and {len(table_indexes) - 3} more")
            
            self.verification_results["indexes"] = {
                "total_count": len(indexes),
                "by_table": {table: len(idxs) for table, idxs in index_by_table.items()}
            }
            
            return index_by_table
            
        except Exception as e:
            print(f"✗ Error verifying indexes: {e}")
            return {}
    
    async def verify_functions(self) -> Dict[str, bool]:
        """Verify stored functions exist"""
        expected_functions = [
            "update_timestamp_and_version",
            "audit_instrument_changes", 
            "derive_option_chains",
            "validate_data_quality"
        ]
        
        print(f"\n⚙️ Verifying {len(expected_functions)} functions...")
        function_results = {}
        
        for func_name in expected_functions:
            try:
                exists = await self.conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM information_schema.routines 
                        WHERE routine_schema = $1 AND routine_name = $2
                    )
                """, SCHEMA_NAME, func_name)
                
                if exists:
                    print(f"   ✓ Function '{func_name}' exists")
                    function_results[func_name] = True
                else:
                    print(f"   ✗ Function '{func_name}' missing")
                    function_results[func_name] = False
                    
            except Exception as e:
                print(f"   ✗ Error checking function '{func_name}': {e}")
                function_results[func_name] = False
        
        self.verification_results["functions"] = function_results
        return function_results
    
    async def verify_views(self) -> Dict[str, bool]:
        """Verify materialized views exist"""
        expected_views = [
            "active_instruments",
            "option_chains_summary", 
            "broker_token_lookup"
        ]
        
        print(f"\n👁️ Verifying {len(expected_views)} views...")
        view_results = {}
        
        for view_name in expected_views:
            try:
                exists = await self.conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM information_schema.views 
                        WHERE table_schema = $1 AND table_name = $2
                    )
                """, SCHEMA_NAME, view_name)
                
                if exists:
                    # Test view accessibility
                    count = await self.conn.fetchval(
                        f"SELECT count(*) FROM {SCHEMA_NAME}.{view_name}"
                    )
                    print(f"   ✓ View '{view_name}' exists and accessible (0 rows expected)")
                    view_results[view_name] = True
                else:
                    print(f"   ✗ View '{view_name}' missing")
                    view_results[view_name] = False
                    
            except Exception as e:
                print(f"   ✗ Error checking view '{view_name}': {e}")
                view_results[view_name] = False
        
        self.verification_results["views"] = view_results
        return view_results
    
    async def verify_triggers(self) -> Dict[str, bool]:
        """Verify triggers are active"""
        try:
            triggers = await self.conn.fetch("""
                SELECT trigger_name, event_object_table, action_timing, event_manipulation
                FROM information_schema.triggers 
                WHERE trigger_schema = $1
                ORDER BY event_object_table, trigger_name
            """, SCHEMA_NAME)
            
            print(f"\n🎯 Found {len(triggers)} active triggers:")
            trigger_results = {}
            
            for trigger in triggers:
                trigger_name = trigger['trigger_name']
                table_name = trigger['event_object_table']
                timing = trigger['action_timing']
                event = trigger['event_manipulation']
                
                print(f"   ✓ {trigger_name} on {table_name} ({timing} {event})")
                trigger_results[trigger_name] = True
            
            self.verification_results["triggers"] = trigger_results
            return trigger_results
            
        except Exception as e:
            print(f"✗ Error verifying triggers: {e}")
            return {}
    
    async def run_smoke_tests(self) -> Dict[str, bool]:
        """Run smoke tests to prove schema is operational"""
        print("\n🧪 Running Smoke Tests...")
        smoke_results = {}
        
        try:
            # Test 1: Insert and query instruments table
            print("   Test 1: Insert/Query instruments table...")
            test_instrument_key = f"TEST_INSTRUMENT_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Insert test record
            await self.conn.execute("""
                INSERT INTO instrument_registry.instruments 
                (instrument_key, symbol, exchange, segment, instrument_type, asset_class)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, test_instrument_key, "TEST", "NSE", "EQ", "EQ", "equity")
            
            # Query test record
            result = await self.conn.fetchrow("""
                SELECT instrument_key, symbol, created_at 
                FROM instrument_registry.instruments 
                WHERE instrument_key = $1
            """, test_instrument_key)
            
            if result:
                print(f"      ✓ Successfully inserted and retrieved test instrument")
                smoke_results["instruments_crud"] = True
            else:
                print(f"      ✗ Failed to retrieve inserted test instrument")
                smoke_results["instruments_crud"] = False
            
            # Cleanup
            await self.conn.execute("""
                DELETE FROM instrument_registry.instruments WHERE instrument_key = $1
            """, test_instrument_key)
            
        except Exception as e:
            print(f"      ✗ Instruments CRUD test failed: {e}")
            smoke_results["instruments_crud"] = False
        
        try:
            # Test 2: Verify audit trail trigger works
            print("   Test 2: Audit trail trigger functionality...")
            test_key = f"AUDIT_TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Insert to trigger audit
            await self.conn.execute("""
                INSERT INTO instrument_registry.instruments 
                (instrument_key, symbol, exchange, segment, instrument_type, asset_class)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, test_key, "AUDIT_TEST", "NSE", "EQ", "EQ", "equity")
            
            # Check if audit record was created
            audit_count = await self.conn.fetchval("""
                SELECT count(*) FROM instrument_registry.audit_trail 
                WHERE instrument_key = $1 AND operation = 'INSERT'
            """, test_key)
            
            if audit_count > 0:
                print(f"      ✓ Audit trail trigger working (found {audit_count} audit records)")
                smoke_results["audit_trigger"] = True
            else:
                print(f"      ✗ Audit trail trigger not working")
                smoke_results["audit_trigger"] = False
            
            # Cleanup
            await self.conn.execute("DELETE FROM instrument_registry.instruments WHERE instrument_key = $1", test_key)
            await self.conn.execute("DELETE FROM instrument_registry.audit_trail WHERE instrument_key = $1", test_key)
            
        except Exception as e:
            print(f"      ✗ Audit trail test failed: {e}")
            smoke_results["audit_trigger"] = False
        
        try:
            # Test 3: Event store functionality
            print("   Test 3: Event store functionality...")
            test_stream = f"test_stream_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Insert test event
            await self.conn.execute("""
                INSERT INTO instrument_registry.instrument_events 
                (event_stream_id, event_type, event_version, aggregate_id, aggregate_version, event_data)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, test_stream, "test.created", 1, "test_aggregate", 1, '{"test": true}')
            
            # Query event
            event = await self.conn.fetchrow("""
                SELECT event_type, event_data FROM instrument_registry.instrument_events 
                WHERE event_stream_id = $1
            """, test_stream)
            
            if event:
                event_data = event['event_data']
                if isinstance(event_data, str):
                    event_data = json.loads(event_data)
                if event_data.get('test') is True:
                    print(f"      ✓ Event store working correctly")
                smoke_results["event_store"] = True
            else:
                print(f"      ✗ Event store not working correctly")
                smoke_results["event_store"] = False
            
            # Cleanup
            await self.conn.execute("DELETE FROM instrument_registry.instrument_events WHERE event_stream_id = $1", test_stream)
            
        except Exception as e:
            print(f"      ✗ Event store test failed: {e}")
            smoke_results["event_store"] = False
        
        try:
            # Test 4: Data quality function
            print("   Test 4: Data quality validation function...")
            # Create a minimal quality check record first
            await self.conn.execute("""
                INSERT INTO instrument_registry.data_quality_checks 
                (check_date, check_type, refresh_batch_id, total_count, active_count) 
                VALUES (CURRENT_DATE, 'smoke_test', 'test_batch', 0, 0)
                ON CONFLICT DO NOTHING
            """)
            result = await self.conn.fetchrow("SELECT * FROM instrument_registry.validate_data_quality('test_batch')")
            
            if result and 'total_count' in result:
                print(f"      ✓ Data quality function working (total: {result['total_count']})")
                smoke_results["quality_function"] = True
            else:
                print(f"      ✗ Data quality function not working")
                smoke_results["quality_function"] = False
                
        except Exception as e:
            print(f"      ✗ Data quality function test failed: {e}")
            smoke_results["quality_function"] = False
        
        self.verification_results["smoke_tests"] = smoke_results
        return smoke_results
    
    async def generate_deployment_report(self) -> Dict[str, Any]:
        """Generate comprehensive deployment verification report"""
        print("\n📊 Generating Deployment Report...")
        
        # Calculate overall health
        total_tables = len(self.verification_results["tables"])
        successful_tables = sum(1 for success in self.verification_results["tables"].values() if success)
        
        total_functions = len(self.verification_results["functions"])
        successful_functions = sum(1 for success in self.verification_results["functions"].values() if success)
        
        total_views = len(self.verification_results["views"])
        successful_views = sum(1 for success in self.verification_results["views"].values() if success)
        
        total_smoke_tests = len(self.verification_results["smoke_tests"])
        successful_smoke_tests = sum(1 for success in self.verification_results["smoke_tests"].values() if success)
        
        # Overall score
        total_checks = total_tables + total_functions + total_views + total_smoke_tests
        successful_checks = successful_tables + successful_functions + successful_views + successful_smoke_tests
        success_rate = (successful_checks / total_checks * 100) if total_checks > 0 else 0
        
        report = {
            "deployment_timestamp": datetime.now().isoformat(),
            "schema_name": SCHEMA_NAME,
            "overall_success_rate": round(success_rate, 2),
            "schema_exists": self.verification_results["schema_exists"],
            "component_summary": {
                "tables": f"{successful_tables}/{total_tables} successful",
                "functions": f"{successful_functions}/{total_functions} successful",
                "views": f"{successful_views}/{total_views} successful",
                "indexes": f"{self.verification_results['indexes'].get('total_count', 0)} created",
                "triggers": f"{len(self.verification_results.get('triggers', {}))} active",
                "smoke_tests": f"{successful_smoke_tests}/{total_smoke_tests} passed"
            },
            "detailed_results": self.verification_results,
            "deployment_status": "SUCCESS" if success_rate >= 95 else "PARTIAL" if success_rate >= 80 else "FAILED"
        }
        
        return report

async def main():
    """Main verification execution"""
    print("🔍 INSTRUMENT REGISTRY SCHEMA DEPLOYMENT VERIFICATION")
    print("=" * 60)
    
    verifier = SchemaVerifier()
    
    try:
        # Connect to database
        if not await verifier.connect():
            return 1
        
        # Run all verifications
        schema_ok = await verifier.verify_schema_exists()
        if not schema_ok:
            print("\n❌ Schema does not exist - deployment failed")
            return 1
        
        await verifier.verify_tables()
        await verifier.verify_indexes()
        await verifier.verify_functions()
        await verifier.verify_views()
        await verifier.verify_triggers()
        await verifier.run_smoke_tests()
        
        # Generate final report
        report = await verifier.generate_deployment_report()
        
        # Save report to file
        report_file = f"schema_deployment_verification_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n📋 Verification Report saved to: {report_file}")
        
        # Display summary
        print(f"\n🎯 VERIFICATION SUMMARY:")
        print(f"   Overall Success Rate: {report['overall_success_rate']}%")
        print(f"   Deployment Status: {report['deployment_status']}")
        print(f"   Schema Exists: {'✓' if report['schema_exists'] else '✗'}")
        
        for component, status in report['component_summary'].items():
            print(f"   {component.title()}: {status}")
        
        if report['deployment_status'] == 'SUCCESS':
            print(f"\n✅ SCHEMA DEPLOYMENT VERIFICATION SUCCESSFUL")
            print("   All critical components are operational and tested.")
            return 0
        elif report['deployment_status'] == 'PARTIAL':
            print(f"\n⚠️  SCHEMA DEPLOYMENT PARTIALLY SUCCESSFUL")
            print("   Some components have issues but core functionality works.")
            return 1
        else:
            print(f"\n❌ SCHEMA DEPLOYMENT VERIFICATION FAILED")
            print("   Critical issues found - deployment not ready.")
            return 1
            
    except Exception as e:
        print(f"\n💥 Verification failed with exception: {e}")
        return 1
    finally:
        await verifier.close()

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)