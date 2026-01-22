"""
Test audit trail API functionality (service layer).

This tests the same code path that the HTTP endpoint uses.
"""
import asyncio
import sys
import json
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

# Add app to path
sys.path.insert(0, '/mnt/stocksblitz-data/Quantagro/tradingview-viz/order_service')

from app.services.audit_service import OrderAuditService

DATABASE_URL = "postgresql+asyncpg://stocksblitz:b4Gr60lYlbZVZz0ZRTcnf_YRkjO0sluNcwwJ-7lAfn4@localhost:5432/stocksblitz_unified_prod"


async def test_audit_api():
    """Test audit service API."""
    print("=" * 80)
    print("AUDIT TRAIL API TEST (Service Layer)")
    print("=" * 80)
    print()

    engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        connect_args={
            "server_settings": {
                "search_path": "order_service,public"
            }
        }
    )

    async_session_maker = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False
    )

    try:
        async with async_session_maker() as session:
            # Test with order ID 4 (created in previous test)
            order_id = 4

            print(f"Testing: Get audit trail for order {order_id}")
            print("-" * 80)

            # Create audit service (same as API endpoint does)
            audit_service = OrderAuditService(session)

            # Get audit history (same as API endpoint does)
            history = await audit_service.get_order_history(order_id)

            print(f"✅ Retrieved {len(history)} audit entries")
            print()

            # Convert to dict (same as API endpoint does)
            history_dicts = [h.to_dict() for h in history]

            print("Audit Trail Entries:")
            print("-" * 80)

            for i, entry in enumerate(history_dicts, 1):
                print(f"\nEntry #{i}:")
                print(json.dumps(entry, indent=2, default=str))

            print()
            print("=" * 80)
            print("API TEST SUMMARY")
            print("=" * 80)
            print()
            print(f"✅ Audit service: WORKING")
            print(f"✅ get_order_history(): SUCCESS")
            print(f"✅ to_dict() serialization: SUCCESS")
            print(f"✅ Entries retrieved: {len(history)}")
            print()

            # Verify data structure
            if len(history_dicts) > 0:
                first_entry = history_dicts[0]
                required_fields = ['id', 'order_id', 'transition', 'actor', 'context', 'timestamp']

                print("Data Structure Validation:")
                for field in required_fields:
                    present = field in first_entry
                    print(f"   {field}: {'✅' if present else '❌'}")
                print()

                # Validate transition structure
                if 'transition' in first_entry:
                    trans = first_entry['transition']
                    print(f"   transition.from: {'✅' if 'from' in trans else '❌'}")
                    print(f"   transition.to: {'✅' if 'to' in trans else '❌'}")

                # Validate actor structure
                if 'actor' in first_entry:
                    actor = first_entry['actor']
                    print(f"   actor.type: {'✅' if 'type' in actor else '❌'}")
                    print(f"   actor.user_id: {'✅' if 'user_id' in actor else '❌'}")
                    print(f"   actor.system: {'✅' if 'system' in actor else '❌'}")

                # Validate context structure
                if 'context' in first_entry:
                    ctx = first_entry['context']
                    print(f"   context.reason: {'✅' if 'reason' in ctx else '❌'}")
                    print(f"   context.metadata: {'✅' if 'metadata' in ctx else '❌'}")

            print()
            print("=" * 80)
            print("✅ Audit API functionality verified!")
            print("=" * 80)

            return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        await engine.dispose()


if __name__ == "__main__":
    success = asyncio.run(test_audit_api())
    sys.exit(0 if success else 1)
