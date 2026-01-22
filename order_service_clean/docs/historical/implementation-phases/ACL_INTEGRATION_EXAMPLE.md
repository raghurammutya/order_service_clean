# Order Service ACL Integration - Implementation Example

This document shows how to integrate ACL permission checks into order_service endpoints.

## Approach 1: Using FastAPI Decorators (Recommended)

### Step 1: Import ACL Decorators

```python
# At top of orders.py
from common.acl_client.fastapi_decorators import require_permission, require_any_permission
```

### Step 2: Add Permission Check to Endpoint

**Before (No ACL):**
```python
@router.delete("/orders/{order_id}")
async def cancel_order(
    order_id: int,
    current_user: dict = Depends(get_current_user_optional),
    trading_account_id: int = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db)
):
    user_id = extract_user_id(current_user)
    service = OrderService(db, user_id, trading_account_id)
    order = await service.cancel_order(order_id)
    return OrderResponse.from_orm(order)
```

**After (With ACL):**
```python
@router.delete("/orders/{order_id}")
async def cancel_order(
    order_id: int,
    user_id: int = Depends(require_permission("order", "order_id", "cancel")),
    trading_account_id: int = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db)
):
    # user_id is now validated by ACL decorator
    # If user lacks 'cancel' permission on this order, raises HTTP 403
    service = OrderService(db, user_id, trading_account_id)
    order = await service.cancel_order(order_id)
    return OrderResponse.from_orm(order)
```

**Changes:**
- ✅ Replaced `current_user: dict = Depends(get_current_user_optional)` with `user_id: int = Depends(require_permission(...))`
- ✅ Removed `user_id = extract_user_id(current_user)`
- ✅ Automatic 403 response if permission denied
- ✅ No code changes in service layer

### Example: Modify Order Endpoint

```python
@router.put("/orders/{order_id}")
async def modify_order(
    order_id: int,
    modify_request: ModifyOrderRequest,
    user_id: int = Depends(require_permission("order", "order_id", "edit")),
    trading_account_id: int = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db)
):
    # User needs 'edit' permission on this order
    service = OrderService(db, user_id, trading_account_id)
    order = await service.modify_order(order_id, modify_request)
    return OrderResponse.from_orm(order)
```

### Example: View Order (Multiple Permissions)

```python
@router.get("/orders/{order_id}")
async def get_order(
    order_id: int,
    user_id: int = Depends(require_any_permission(
        "order", "order_id", ["view", "edit", "cancel"]
    )),
    db: AsyncSession = Depends(get_db)
):
    # User needs at least one of: view, edit, or cancel permission
    service = OrderService(db, user_id, trading_account_id)
    order = await service.get_order(order_id)
    return OrderResponse.from_orm(order)
```

## Approach 2: Manual ACL Checks (For Complex Logic)

For endpoints with complex filtering or multiple permission checks, use manual checks:

### Step 1: Import ACL Helpers

```python
from app.utils.acl_helpers import (
    ACLHelper,
    check_order_permission,
    get_accessible_orders
)
```

### Step 2: List Orders with ACL Filtering

**Before (No ACL):**
```python
@router.get("/orders")
async def list_orders(
    status: Optional[str] = None,
    current_user: dict = Depends(get_current_user_optional),
    trading_account_id: int = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db)
):
    user_id = extract_user_id(current_user)
    service = OrderService(db, user_id, trading_account_id)

    # Returns ALL orders for trading account
    orders = await service.list_orders(status=status)

    return OrderListResponse(orders=orders, total=len(orders))
```

**After (With ACL Filtering):**
```python
@router.get("/orders")
async def list_orders(
    status: Optional[str] = None,
    current_user: dict = Depends(get_current_user_optional),
    trading_account_id: int = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db)
):
    user_id = extract_user_id(current_user)

    # Get accessible order IDs from ACL
    accessible_order_ids = await get_accessible_orders(user_id, min_action="view")

    # If no accessible orders, return empty list
    if not accessible_order_ids:
        return OrderListResponse(orders=[], total=0)

    # Filter orders by accessible IDs
    service = OrderService(db, user_id, trading_account_id)
    orders = await service.list_orders(
        status=status,
        order_ids=accessible_order_ids  # NEW: Filter by ACL
    )

    return OrderListResponse(orders=orders, total=len(orders))
```

### Step 3: Update Service Layer to Accept order_ids Filter

```python
# In order_service/app/services/order_service.py

class OrderService:
    async def list_orders(
        self,
        status: Optional[str] = None,
        order_ids: Optional[List[int]] = None  # NEW parameter
    ) -> List[Order]:
        """
        List orders with optional filtering.

        Args:
            status: Filter by order status
            order_ids: Filter by specific order IDs (for ACL)
        """
        query = self.db.query(Order).filter(
            Order.trading_account_id == self.trading_account_id
        )

        if status:
            query = query.filter(Order.status == status)

        # NEW: Filter by accessible order IDs
        if order_ids is not None:
            query = query.filter(Order.order_id.in_(order_ids))

        return query.all()
```

## Approach 3: Bulk Permission Checks

For bulk operations (cancel multiple orders, etc.):

```python
from app.utils.acl_helpers import ACLHelper

@router.post("/orders/bulk-cancel")
async def bulk_cancel_orders(
    order_ids: List[int],
    current_user: dict = Depends(get_current_user_optional),
    trading_account_id: int = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db)
):
    user_id = extract_user_id(current_user)

    # Check permissions for all orders in one call
    checks = [
        {"resource_type": "order", "resource_id": oid, "action": "cancel"}
        for oid in order_ids
    ]
    results = await ACLHelper.bulk_check_permissions(user_id, checks)

    # Separate allowed and denied
    allowed_ids = [oid for oid, allowed in zip(order_ids, results) if allowed]
    denied_ids = [oid for oid, allowed in zip(order_ids, results) if not allowed]

    # Cancel allowed orders only
    service = OrderService(db, user_id, trading_account_id)
    cancelled_orders = []
    for oid in allowed_ids:
        order = await service.cancel_order(oid)
        cancelled_orders.append(order)

    return {
        "success": True,
        "cancelled": [OrderResponse.from_orm(o) for o in cancelled_orders],
        "denied": denied_ids,
        "message": f"Cancelled {len(cancelled_orders)} orders, denied {len(denied_ids)}"
    }
```

## Position Endpoints

### Squareoff Position

```python
@router.post("/positions/{position_id}/squareoff")
async def squareoff_position(
    position_id: int,
    user_id: int = Depends(require_permission("position", "position_id", "squareoff")),
    trading_account_id: int = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db)
):
    # User needs 'squareoff' permission on this position
    service = PositionService(db, user_id, trading_account_id)
    position = await service.squareoff_position(position_id)
    return PositionResponse.from_orm(position)
```

### List Positions with ACL

```python
from app.utils.acl_helpers import get_accessible_positions

@router.get("/positions")
async def list_positions(
    current_user: dict = Depends(get_current_user_optional),
    trading_account_id: int = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db)
):
    user_id = extract_user_id(current_user)

    # Get accessible position IDs
    accessible_position_ids = await get_accessible_positions(user_id, min_action="view")

    if not accessible_position_ids:
        return PositionListResponse(positions=[], total=0)

    # Filter positions
    service = PositionService(db, user_id, trading_account_id)
    positions = await service.list_positions(position_ids=accessible_position_ids)

    return PositionListResponse(positions=positions, total=len(positions))
```

## Trade Endpoints

### View Trade

```python
@router.get("/trades/{trade_id}")
async def get_trade(
    trade_id: int,
    user_id: int = Depends(require_permission("trade", "trade_id", "view")),
    db: AsyncSession = Depends(get_db)
):
    # User needs 'view' permission on this trade
    service = TradeService(db, user_id, trading_account_id)
    trade = await service.get_trade(trade_id)
    return TradeResponse.from_orm(trade)
```

### List Trades with ACL

```python
from app.utils.acl_helpers import get_accessible_trades

@router.get("/trades")
async def list_trades(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user: dict = Depends(get_current_user_optional),
    trading_account_id: int = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db)
):
    user_id = extract_user_id(current_user)

    # Get accessible trade IDs
    accessible_trade_ids = await get_accessible_trades(user_id, min_action="view")

    if not accessible_trade_ids:
        return TradeListResponse(trades=[], total=0)

    # Filter trades
    service = TradeService(db, user_id, trading_account_id)
    trades = await service.list_trades(
        start_date=start_date,
        end_date=end_date,
        trade_ids=accessible_trade_ids
    )

    return TradeListResponse(trades=trades, total=len(trades))
```

## Trading Account Permission Check

For endpoints that create new resources (place order), check trading account permission:

```python
from app.utils.acl_helpers import ACLHelper

@router.post("/orders")
async def place_order(
    order_request: PlaceOrderRequest,
    current_user: dict = Depends(get_current_user_optional),
    trading_account_id: int = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db)
):
    user_id = extract_user_id(current_user)

    # Check if user has 'trade' permission on trading account
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=trading_account_id,
        action="trade"
    )

    if not has_permission:
        raise HTTPException(
            status_code=403,
            detail=ACLHelper.get_permission_error_response(
                "trading_account", trading_account_id, "trade"
            )
        )

    # Place order
    service = OrderService(db, user_id, trading_account_id)
    order = await service.place_order(order_request)

    # Create ACL entry for new order (user is OWNER)
    await create_order_acl(user_id, order.order_id)

    return OrderResponse.from_orm(order)
```

## Creating ACL Entries for New Resources

When creating new orders/positions/trades, automatically grant OWNER permission:

```python
from common.acl_client import ACLClient

async def create_order_acl(user_id: int, order_id: int):
    """Grant OWNER permission on new order"""
    try:
        # Call user_service ACL API to grant permission
        acl_client = ACLClient()
        # Note: This would need to be implemented in ACLClient
        # For now, call user_service grant endpoint directly
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "http://user-service:8011/api/v1/acl/grant",
                json={
                    "resource_type": "order",
                    "resource_id": order_id,
                    "principal_type": "user",
                    "principal_id": user_id,
                    "permission_level": "owner",
                    "permissions": ["view", "edit", "cancel", "share", "admin", "delete"]
                }
            ) as resp:
                if resp.status != 201:
                    logger.error(f"Failed to create ACL for order {order_id}")

    except Exception as e:
        logger.error(f"Error creating ACL for order {order_id}: {e}")
```

## Error Handling

ACL checks can fail for various reasons:

```python
from common.acl_client import ACLServiceUnavailableException
from fastapi import HTTPException

try:
    accessible_ids = await get_accessible_orders(user_id)
except ACLServiceUnavailableException:
    # ACL service is down
    raise HTTPException(
        status_code=503,
        detail="Permission check service temporarily unavailable"
    )
except Exception as e:
    logger.error(f"Unexpected ACL error: {e}")
    raise HTTPException(
        status_code=500,
        detail="Permission check failed"
    )
```

## Performance Considerations

### 1. Use Bulk Checks

❌ **Bad** (N individual checks):
```python
for order_id in order_ids:
    if await check_order_permission(user_id, order_id, "view"):
        accessible.append(order_id)
```

✅ **Good** (1 bulk check):
```python
checks = [{"resource_type": "order", "resource_id": oid, "action": "view"}
          for oid in order_ids]
results = await ACLHelper.bulk_check_permissions(user_id, checks)
accessible = [oid for oid, allowed in zip(order_ids, results) if allowed]
```

### 2. Use get_accessible_resources for Lists

❌ **Bad** (Query all then filter):
```python
all_orders = await service.list_all_orders()
accessible = [o for o in all_orders if await check_order_permission(user_id, o.id, "view")]
```

✅ **Good** (Filter in database):
```python
accessible_ids = await get_accessible_orders(user_id, "view")
orders = await service.list_orders(order_ids=accessible_ids)
```

### 3. Cache Permission Checks

The ACL client has built-in caching (60s TTL), but for tight loops, cache locally:

```python
# Cache permission results
permission_cache = {}

for order_id in order_ids:
    if order_id not in permission_cache:
        permission_cache[order_id] = await check_order_permission(user_id, order_id, "cancel")

    if permission_cache[order_id]:
        # Process order
        pass
```

## Migration Strategy

### Phase 1: Add ACL Checks (Non-Breaking)
- Add ACL checks but don't enforce
- Log permission denials
- Monitor false positives

### Phase 2: Enforce for New Resources
- New orders get ACL entries
- ACL enforced for new resources only
- Existing resources continue working

### Phase 3: Backfill ACL Entries
- Create ACL entries for existing resources
- Owner: original creator
- Permission level: OWNER

### Phase 4: Full Enforcement
- ACL enforced for all resources
- Remove bypass logic
- Full rollout

## Testing

### Unit Tests

```python
import pytest
from unittest.mock import AsyncMock, patch

@pytest.mark.asyncio
async def test_cancel_order_with_permission():
    """Test cancelling order with proper permission"""
    with patch('app.utils.acl_helpers.ACLClient') as mock_acl:
        mock_acl.return_value.check_permission = AsyncMock(return_value=True)

        # User has permission, should succeed
        response = await client.delete("/api/v1/orders/123")
        assert response.status_code == 200

@pytest.mark.asyncio
async def test_cancel_order_without_permission():
    """Test cancelling order without permission"""
    with patch('app.utils.acl_helpers.ACLClient') as mock_acl:
        mock_acl.return_value.check_permission = AsyncMock(return_value=False)

        # User lacks permission, should fail
        response = await client.delete("/api/v1/orders/123")
        assert response.status_code == 403
        assert "permission" in response.json()["detail"].lower()
```

### Integration Tests

```python
@pytest.mark.asyncio
async def test_list_orders_filters_by_acl():
    """Test list orders only returns accessible orders"""
    # Create 3 orders
    order1 = await create_order(user_id=1)
    order2 = await create_order(user_id=1)
    order3 = await create_order(user_id=2)

    # Grant view permission on order1 and order2 only
    await grant_permission(user_id=1, resource_id=order1.id, action="view")
    await grant_permission(user_id=1, resource_id=order2.id, action="view")

    # List orders
    response = await client.get("/api/v1/orders")

    # Should only see order1 and order2, not order3
    assert len(response.json()["orders"]) == 2
    order_ids = [o["order_id"] for o in response.json()["orders"]]
    assert order1.id in order_ids
    assert order2.id in order_ids
    assert order3.id not in order_ids
```

## Summary

**Recommended Integration Steps:**

1. ✅ Create ACL helper module (`acl_helpers.py`)
2. ✅ Add FastAPI decorators to single-resource endpoints
3. ✅ Add ACL filtering to list endpoints
4. ✅ Add bulk permission checks for batch operations
5. ✅ Create ACL entries for new resources
6. ✅ Add cache invalidation hooks
7. ✅ Write comprehensive tests
8. ✅ Deploy with monitoring

**Benefits:**
- Granular permission control (owner, admin, full_access, view_only)
- Multi-layer permission resolution (user, group, org, subscription)
- High performance with Redis caching
- Audit trail of permission changes
- Secure by default

**Next Steps:**
- Implement ACL for order endpoints
- Implement ACL for position endpoints
- Implement ACL for trade endpoints
- Run comprehensive tests (Sprint 4)
- Deploy to production (Sprint 5)
