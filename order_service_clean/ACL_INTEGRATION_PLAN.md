# Order Service ACL Integration Plan

## Overview

Integrate comprehensive ACL permission checks into order_service endpoints to ensure users can only access and manipulate orders, positions, and trades they have permission for.

## Resource Types

The order_service manages three resource types:
1. **order** - Individual trading orders
2. **position** - Open trading positions
3. **trade** - Completed trades

## Permission Levels

Based on `PermissionLevel` in ACL system:

| Level | Priority | Actions Allowed |
|-------|----------|-----------------|
| OWNER | 4 | All actions (irrevocable) |
| ADMIN | 3 | view, edit, cancel, share, admin, delete |
| FULL_ACCESS | 2 | view, edit, cancel, trade |
| VIEW_ONLY | 1 | view only |

## Action Mappings

### Orders
- **view** - View order details, list orders
- **trade** - Place new orders
- **edit** - Modify existing orders
- **cancel** - Cancel orders
- **admin** - View all orders for trading account

### Positions
- **view** - View position details, list positions
- **squareoff** - Close/square off positions
- **admin** - View all positions for trading account

### Trades
- **view** - View trade details, list trades
- **admin** - View all trades for trading account

## Endpoints to Protect

### Order Endpoints (orders.py)

| Endpoint | Method | Action Required | Resource Type |
|----------|--------|-----------------|---------------|
| `/orders` | POST | `trade` on `trading_account` | trading_account |
| `/orders/batch` | POST | `trade` on `trading_account` | trading_account |
| `/orders/{order_id}` | PUT | `edit` on `order` | order |
| `/orders/{order_id}` | DELETE | `cancel` on `order` | order |
| `/orders/summary` | GET | `view` on `trading_account` | trading_account |
| `/orders` | GET | `view` on orders | order (filter) |
| `/orders/{order_id}` | GET | `view` on `order` | order |
| `/orders/count` | GET | `view` on `trading_account` | trading_account |
| `/orders/pending-count` | GET | `view` on `trading_account` | trading_account |
| `/orders/bulk-cancel` | POST | `cancel` on orders | order (bulk) |

### Position Endpoints (positions.py)

| Endpoint | Method | Action Required | Resource Type |
|----------|--------|-----------------|---------------|
| `/positions` | GET | `view` on positions | position (filter) |
| `/positions/{position_id}` | GET | `view` on `position` | position |
| `/positions/{position_id}/squareoff` | POST | `squareoff` on `position` | position |
| `/positions/summary` | GET | `view` on `trading_account` | trading_account |

### Trade Endpoints (trades.py)

| Endpoint | Method | Action Required | Resource Type |
|----------|--------|-----------------|---------------|
| `/trades` | GET | `view` on trades | trade (filter) |
| `/trades/{trade_id}` | GET | `view` on `trade` | trade |
| `/trades/summary` | GET | `view` on `trading_account` | trading_account |

## Implementation Strategy

### Phase 1: Add ACL Dependencies
1. Import ACL client library
2. Import FastAPI decorators
3. Create helper functions for resource filtering

### Phase 2: Protect Individual Resource Endpoints
1. Add `require_permission()` decorator to single resource endpoints
2. Test permission denial scenarios
3. Update error messages

### Phase 3: Protect List/Filter Endpoints
1. Add ACL filtering to list endpoints
2. Use `get_user_resources()` to pre-filter queries
3. Ensure pagination works with ACL filtering

### Phase 4: Protect Bulk Operations
1. Add bulk permission checks
2. Use `bulk_check_permissions()` for efficiency
3. Return partial success with denied items

### Phase 5: Add Cache Invalidation Hooks
1. Invalidate cache when order status changes
2. Invalidate cache when position closes
3. Invalidate cache when ACL entries change

## Code Examples

### Single Resource Protection

```python
from common.acl_client.fastapi_decorators import require_permission

@router.delete("/orders/{order_id}")
async def cancel_order(
    order_id: int,
    user_id: int = Depends(require_permission("order", "order_id", "cancel")),
    trading_account_id: int = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db)
):
    # User has 'cancel' permission on this order
    service = OrderService(db, user_id, trading_account_id)
    order = await service.cancel_order(order_id)
    return OrderResponse.from_orm(order)
```

### List Filtering with ACL

```python
from common.acl_client import ACLClient

@router.get("/orders")
async def list_orders(
    current_user: dict = Depends(get_current_user_optional),
    trading_account_id: int = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db)
):
    user_id = extract_user_id(current_user)

    # Get accessible order IDs from ACL
    acl_client = ACLClient()
    accessible_order_ids = await acl_client.get_user_resources(
        user_id=user_id,
        resource_type="order",
        min_action="view"
    )

    # Filter query by accessible IDs
    service = OrderService(db, user_id, trading_account_id)
    orders = await service.list_orders(
        accessible_order_ids=accessible_order_ids
    )

    return OrderListResponse(orders=orders, total=len(orders))
```

### Bulk Permission Check

```python
@router.post("/orders/bulk-cancel")
async def bulk_cancel_orders(
    order_ids: List[int],
    current_user: dict = Depends(get_current_user_optional),
    db: AsyncSession = Depends(get_db)
):
    user_id = extract_user_id(current_user)

    # Check permissions for all orders
    acl_client = ACLClient()
    checks = [
        {"resource_type": "order", "resource_id": oid, "action": "cancel"}
        for oid in order_ids
    ]
    results = await acl_client.bulk_check_permissions(user_id, checks)

    # Separate allowed and denied
    allowed_ids = [oid for oid, allowed in zip(order_ids, results) if allowed]
    denied_ids = [oid for oid, allowed in zip(order_ids, results) if not allowed]

    # Cancel allowed orders
    service = OrderService(db, user_id, trading_account_id)
    cancelled = await service.bulk_cancel(allowed_ids)

    return {
        "cancelled": cancelled,
        "denied": denied_ids
    }
```

## Testing Strategy

### Positive Tests
- [ ] User with OWNER permission can perform all actions
- [ ] User with ADMIN permission can view, edit, cancel, share
- [ ] User with FULL_ACCESS can view, edit, cancel, trade
- [ ] User with VIEW_ONLY can only view
- [ ] User can access resources shared via group membership
- [ ] User can access resources shared via organization membership
- [ ] User can access resources via subscription

### Negative Tests
- [ ] User without permission gets 403 Forbidden
- [ ] User cannot cancel order they don't have permission for
- [ ] User cannot view orders they don't have permission for
- [ ] List endpoints exclude unauthorized resources
- [ ] Bulk operations handle partial permissions correctly

### Performance Tests
- [ ] List endpoints with 1000+ orders perform well with ACL filtering
- [ ] Bulk operations with 100+ items use efficient bulk checks
- [ ] Cache hit rate > 80% for repeated permission checks

## Rollout Plan

### Step 1: Add ACL Dependencies (No Breaking Changes)
- Install common.acl_client
- Add helper functions
- No endpoint behavior changes

### Step 2: Enable ACL for New Resources (Opt-in)
- New orders automatically get ACL entries
- Existing orders continue working without ACL

### Step 3: Backfill ACL Entries
- Create ACL entries for existing orders/positions/trades
- Owner: user who created the order
- Permission level: OWNER

### Step 4: Enable ACL Enforcement (Breaking Change)
- Turn on ACL checks for all endpoints
- Monitor logs for permission denials
- Fix any false positives

### Step 5: Full Rollout
- ACL fully enforced
- Cache invalidation hooks active
- Monitoring and alerting

## Migration Script

```python
# backfill_order_acl.py
from user_service.app.services.enhanced_acl_service import EnhancedACLService

async def backfill_order_acl():
    """Create ACL entries for existing orders"""

    # Get all orders without ACL entries
    orders = db.query(Order).all()

    for order in orders:
        # Grant OWNER permission to order creator
        acl_service.grant_with_level(
            resource_type="order",
            resource_id=order.order_id,
            principal_type="user",
            principal_id=order.user_id,
            permission_level="owner",
            granted_by=1  # system user
        )

    print(f"Created ACL entries for {len(orders)} orders")
```

## Metrics to Track

- Permission check latency (p50, p95, p99)
- Cache hit rate
- Permission denial rate
- False positive rate (users denied incorrectly)
- Database query count (should decrease with caching)

## Rollback Plan

If issues arise:
1. Disable ACL checks (feature flag)
2. Continue using existing auth (trading_account_id only)
3. Investigate issues
4. Fix and re-enable
