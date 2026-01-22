"""
API v1 module
"""

from fastapi import APIRouter
from app.api.v1.endpoints import (
    orders, positions, positions_integration, trades, 
    gtt, accounts, pnl, strategies, portfolios,
    dashboard, instruments, internal, admin, manual_attribution,
    external_order_validation
)

api_router = APIRouter()

# Public endpoints
api_router.include_router(orders.router, prefix="/orders", tags=["Orders"])
api_router.include_router(positions.router, prefix="/positions", tags=["Positions"])
api_router.include_router(trades.router, prefix="/trades", tags=["Trades"])
api_router.include_router(gtt.router, prefix="/gtt", tags=["GTT Orders"])
api_router.include_router(accounts.router, prefix="/accounts", tags=["Accounts"])
api_router.include_router(pnl.router, prefix="/pnl", tags=["P&L"])
api_router.include_router(strategies.router, prefix="/strategies", tags=["Strategies"])
api_router.include_router(portfolios.router, prefix="/portfolios", tags=["Portfolios"])
api_router.include_router(dashboard.router, prefix="/dashboard", tags=["Dashboard"])
api_router.include_router(instruments.router, prefix="/instruments", tags=["Instruments"])
api_router.include_router(admin.router, prefix="/admin", tags=["Admin"])
api_router.include_router(manual_attribution.router, prefix="/manual-attribution", tags=["Manual Attribution"])
api_router.include_router(external_order_validation.router, tags=["External Order Validation"])

# Internal service-to-service endpoints
api_router.include_router(
    positions_integration.router, 
    prefix="/positions", 
    tags=["Positions Integration"]
)
api_router.include_router(internal.router, prefix="/internal", tags=["Internal"])
