#!/usr/bin/env python3
"""
Event Store Capability Test

Concrete proof that event store and audit trail work as designed.
No aspirational claims - only verified operations.
"""

import asyncio
import asyncpg
import json
import sys
from datetime import datetime

DATABASE_URL = "postgresql://stocksblitz:b4Gr60lYlbZVZz0ZRTcnf_YRkjO0sluNcwwJ-7lAfn4@localhost:5432/stocksblitz_unified_prod"

async def test_event_store_capability():
    """Concrete test of event store functionality"""
    
    print("🧪 EVENT STORE & AUDIT TRAIL CAPABILITY TEST")
    print("=" * 50)
    print("Testing ACTUAL functionality with database operations")
    print()
    
    try:
        # Connect to database
        conn = await asyncpg.connect(DATABASE_URL)
        print("✅ Database connection established")
        
        # Verify tables exist
        table_check = await conn.fetchval("""
            SELECT count(*) FROM information_schema.tables 
            WHERE table_schema = 'instrument_registry' 
            AND table_name IN ('instruments', 'instrument_events', 'audit_trail')
        """)
        
        if table_check < 3:
            print("❌ Required tables missing")
            return 1
        
        print(f"✅ Required tables exist (found {table_check})")
        
        # Test 1: Event Store Write/Read
        print("\n📝 TEST 1: Event Store Write/Read")
        test_session = datetime.now().strftime('%Y%m%d_%H%M%S')
        stream_id = f"capability_test_{test_session}"
        
        # Write test event
        event_data = {
            "action": "capability_test",
            "timestamp": datetime.now().isoformat(),
            "test_id": test_session
        }
        
        await conn.execute("""
            INSERT INTO instrument_registry.instrument_events 
            (event_stream_id, event_type, event_version, aggregate_id, aggregate_version, event_data)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, stream_id, "test.capability", 1, "test_aggregate", 1, json.dumps(event_data))
        
        print("   ✅ Event written to store")
        
        # Read event back
        stored_event = await conn.fetchrow("""
            SELECT event_type, event_data, event_version FROM instrument_registry.instrument_events 
            WHERE event_stream_id = $1
        """, stream_id)
        
        if stored_event:
            stored_data = json.loads(stored_event['event_data'])
            if stored_data.get('test_id') == test_session:
                print(f"   ✅ Event read correctly (version {stored_event['event_version']})")
                print(f"      Event type: {stored_event['event_type']}")
                print(f"      Data integrity: VERIFIED")
            else:
                print("   ❌ Event data corrupted")
                return 1
        else:
            print("   ❌ Event not found after write")
            return 1
        
        # Test 2: Audit Trail with Trigger
        print("\n📝 TEST 2: Audit Trail Trigger")
        instrument_key = f"CAPABILITY_TEST_{test_session}"
        
        # Create instrument (should trigger audit)
        await conn.execute("""
            INSERT INTO instrument_registry.instruments 
            (instrument_key, symbol, exchange, segment, instrument_type, asset_class)
            VALUES ($1, $2, 'NSE', 'EQ', 'EQ', 'equity')
        """, instrument_key, 'CAPTEST')
        
        print("   ✅ Instrument created")
        
        # Check audit record was created
        audit_record = await conn.fetchrow("""
            SELECT operation, new_data, changed_at FROM instrument_registry.audit_trail 
            WHERE instrument_key = $1 AND operation = 'INSERT'
        """, instrument_key)
        
        if audit_record:
            new_data = audit_record['new_data']
            if isinstance(new_data, str):
                new_data = json.loads(new_data)
            if new_data.get('symbol') == 'CAPTEST':
                print(f"   ✅ Audit record created automatically")
                print(f"      Operation: {audit_record['operation']}")
                print(f"      Timestamp: {audit_record['changed_at']}")
                print(f"      Data captured: VERIFIED")
            else:
                print("   ❌ Audit data incomplete")
                return 1
        else:
            print("   ❌ Audit record not created")
            return 1
        
        # Test 3: Update Audit with Field Tracking
        print("\n📝 TEST 3: Update Audit & Field Tracking")
        
        # Update instrument (should trigger update audit)
        await conn.execute("""
            UPDATE instrument_registry.instruments 
            SET lot_size = 100, name = 'Capability Test Stock'
            WHERE instrument_key = $1
        """, instrument_key)
        
        print("   ✅ Instrument updated")
        
        # Check update audit
        update_audit = await conn.fetchrow("""
            SELECT operation, changed_fields, old_data, new_data FROM instrument_registry.audit_trail 
            WHERE instrument_key = $1 AND operation = 'UPDATE'
        """, instrument_key)
        
        if update_audit:
            changed_fields = update_audit['changed_fields']
            if 'lot_size' in changed_fields:
                old_data = update_audit['old_data']
                new_data = update_audit['new_data']
                if isinstance(old_data, str):
                    old_data = json.loads(old_data)
                if isinstance(new_data, str):
                    new_data = json.loads(new_data)
                old_lot_size = old_data.get('lot_size', 1)
                new_lot_size = new_data.get('lot_size', 1)
                print(f"   ✅ Update audit captured field changes")
                print(f"      Changed fields: {changed_fields}")
                print(f"      lot_size: {old_lot_size} → {new_lot_size}")
                print(f"      Field tracking: VERIFIED")
            else:
                print("   ❌ Field changes not tracked")
                return 1
        else:
            print("   ❌ Update audit not created")
            return 1
        
        # Test 4: Event Ordering & Versioning
        print("\n📝 TEST 4: Event Ordering & Versioning")
        
        # Write multiple events in sequence
        for i in range(3):
            await conn.execute("""
                INSERT INTO instrument_registry.instrument_events 
                (event_stream_id, event_type, event_version, aggregate_id, aggregate_version, event_data)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, stream_id, f"test.sequence", i+2, "test_aggregate", i+2, 
                json.dumps({"sequence": i+1, "action": f"step_{i+1}"}))
        
        # Verify ordering
        events = await conn.fetch("""
            SELECT event_version, event_data FROM instrument_registry.instrument_events 
            WHERE event_stream_id = $1 
            ORDER BY event_version
        """, stream_id)
        
        if len(events) == 4:  # 1 original + 3 new
            versions = [e['event_version'] for e in events]
            if versions == [1, 2, 3, 4]:
                print("   ✅ Event ordering preserved")
                print(f"      Event sequence: {versions}")
                print(f"      Version integrity: VERIFIED")
            else:
                print(f"   ❌ Event ordering broken: {versions}")
                return 1
        else:
            print(f"   ❌ Wrong event count: expected 4, got {len(events)}")
            return 1
        
        # Cleanup
        await conn.execute("DELETE FROM instrument_registry.instrument_events WHERE event_stream_id = $1", stream_id)
        await conn.execute("DELETE FROM instrument_registry.instruments WHERE instrument_key = $1", instrument_key)
        await conn.execute("DELETE FROM instrument_registry.audit_trail WHERE instrument_key = $1", instrument_key)
        print("\n🧹 Test data cleaned up")
        
        await conn.close()
        
        # Final assessment
        print("\n📊 CAPABILITY TEST RESULTS")
        print("=" * 50)
        print("✅ Event Store: Write/Read operations working")
        print("✅ Audit Trail: Automatic trigger-based audit working")
        print("✅ Field Tracking: Changed field detection working")
        print("✅ Event Ordering: Version sequence preservation working")
        print("\n✅ CONCRETE EVIDENCE: Event sourcing & audit capabilities VERIFIED")
        print("   Not just table definitions - actual working functionality")
        
        return 0
        
    except Exception as e:
        print(f"\n💥 Test failed with exception: {e}")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(test_event_store_capability())
    sys.exit(exit_code)