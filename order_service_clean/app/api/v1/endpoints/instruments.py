"""
Instrument Lookup API

Provides endpoints for looking up instrument tokens from the instrument registry.
Used by frontend for WebSocket subscriptions.
"""
import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel
from sqlalchemy import text

from ....database.connection import get_db

logger = logging.getLogger(__name__)

router = APIRouter()


# ==========================================
# RESPONSE MODELS
# ==========================================

class InstrumentResponse(BaseModel):
    """Single instrument response"""
    instrument_token: int
    tradingsymbol: str
    name: Optional[str] = None
    exchange: Optional[str] = None
    segment: Optional[str] = None
    instrument_type: Optional[str] = None
    lot_size: Optional[int] = None
    tick_size: Optional[float] = None


class InstrumentLookupResponse(BaseModel):
    """Instrument lookup response"""
    instruments: List[InstrumentResponse]
    count: int


# ==========================================
# ENDPOINTS
# ==========================================

@router.get("/instruments/lookup", response_model=InstrumentLookupResponse)
async def lookup_instruments(
    symbols: str = Query(..., description="Comma-separated symbols to look up"),
    exchange: str = Query("NSE", description="Exchange code (NSE, NFO, BSE, BFO, MCX, CDS)"),
    db = Depends(get_db)
):
    """
    Look up instrument tokens for given symbols.

    Used by frontend to get tokens for WebSocket subscriptions.

    - **symbols**: Comma-separated list of trading symbols (e.g., "RELIANCE,INFY,TCS")
    - **exchange**: Exchange code (default: NSE)

    Returns list of instruments with their tokens.
    """
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]

    if not symbol_list:
        raise HTTPException(status_code=400, detail="No symbols provided")

    if len(symbol_list) > 100:
        raise HTTPException(status_code=400, detail="Maximum 100 symbols per request")

    logger.info(f"Looking up {len(symbol_list)} instruments on {exchange}")

    result = await db.execute(text("""
        SELECT
            instrument_token,
            symbol,
            name,
            exchange,
            segment,
            instrument_type,
            lot_size,
            tick_size
        FROM public.instrument_registry
        WHERE symbol = ANY(:symbols)
          AND exchange = :exchange
          AND is_active = true
    """), {"symbols": symbol_list, "exchange": exchange})

    rows = result.fetchall()

    instruments = [
        InstrumentResponse(
            instrument_token=row.instrument_token,
            tradingsymbol=row.tradingsymbol,
            name=row.name,
            exchange=row.exchange,
            segment=row.segment,
            instrument_type=row.instrument_type,
            lot_size=row.lot_size,
            tick_size=row.tick_size
        )
        for row in rows
    ]

    logger.info(f"Found {len(instruments)} instruments")

    return InstrumentLookupResponse(instruments=instruments, count=len(instruments))


@router.get("/instruments/{instrument_token}", response_model=InstrumentResponse)
async def get_instrument(
    instrument_token: int,
    db = Depends(get_db)
):
    """
    Get instrument details by token.

    - **instrument_token**: Numeric instrument token

    Returns instrument details if found.
    """
    result = await db.execute(text("""
        SELECT
            instrument_token,
            symbol,
            name,
            exchange,
            segment,
            instrument_type,
            lot_size,
            tick_size
        FROM public.instrument_registry
        WHERE instrument_token = :token
          AND is_active = true
    """), {"token": instrument_token})

    row = result.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Instrument not found")

    return InstrumentResponse(
        instrument_token=row.instrument_token,
        tradingsymbol=row.tradingsymbol,
        name=row.name,
        exchange=row.exchange,
        segment=row.segment,
        instrument_type=row.instrument_type,
        lot_size=row.lot_size,
        tick_size=row.tick_size
    )


@router.get("/instruments/search/{query}", response_model=InstrumentLookupResponse)
async def search_instruments(
    query: str,
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    limit: int = Query(20, ge=1, le=100, description="Maximum results"),
    db = Depends(get_db)
):
    """
    Search instruments by symbol or name.

    - **query**: Search query (min 2 characters)
    - **exchange**: Optional exchange filter
    - **limit**: Maximum results to return (default: 20)

    Returns matching instruments.
    """
    if len(query) < 2:
        raise HTTPException(status_code=400, detail="Query must be at least 2 characters")

    search_pattern = f"%{query.upper()}%"

    sql = """
        SELECT
            instrument_token,
            symbol,
            name,
            exchange,
            segment,
            instrument_type,
            lot_size,
            tick_size
        FROM public.instrument_registry
        WHERE (tradingsymbol ILIKE :pattern OR name ILIKE :pattern)
          AND is_active = true
    """

    params = {"pattern": search_pattern, "limit": limit}

    if exchange:
        sql += " AND exchange = :exchange"
        params["exchange"] = exchange

    sql += " ORDER BY tradingsymbol LIMIT :limit"

    result = await db.execute(text(sql), params)
    rows = result.fetchall()

    instruments = [
        InstrumentResponse(
            instrument_token=row.instrument_token,
            tradingsymbol=row.tradingsymbol,
            name=row.name,
            exchange=row.exchange,
            segment=row.segment,
            instrument_type=row.instrument_type,
            lot_size=row.lot_size,
            tick_size=row.tick_size
        )
        for row in rows
    ]

    return InstrumentLookupResponse(instruments=instruments, count=len(instruments))
