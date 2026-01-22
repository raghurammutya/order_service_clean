"""
Instrument Lookup API

Provides endpoints for looking up instrument tokens from the instrument registry.
Used by frontend for WebSocket subscriptions.

CRITICAL: Uses Market Data Service API instead of public.instrument_registry 
since that table doesn't exist in order_service database.
"""
import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel
from sqlalchemy import text

from ....database.connection import get_db
from ....clients.market_data_service_client import get_market_data_client, MarketDataServiceError

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
    Look up instrument tokens for given symbols via Market Data Service API.

    CRITICAL: Uses Market Data Service API instead of public.instrument_registry
    since that table doesn't exist in order_service database.

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

    try:
        market_client = await get_market_data_client()
        instruments = []
        
        # Look up each symbol individually via Market Data Service API
        for symbol in symbol_list:
            try:
                instrument_info = await market_client.get_instrument_info(symbol, exchange)
                if instrument_info:
                    instruments.append(InstrumentResponse(
                        instrument_token=instrument_info.get("instrument_token"),
                        tradingsymbol=instrument_info.get("symbol", symbol),
                        name=instrument_info.get("name"),
                        exchange=instrument_info.get("exchange", exchange),
                        segment=instrument_info.get("segment"),
                        instrument_type=instrument_info.get("instrument_type"),
                        lot_size=instrument_info.get("lot_size"),
                        tick_size=instrument_info.get("tick_size")
                    ))
            except MarketDataServiceError as e:
                logger.warning(f"Failed to lookup {symbol}: {e}")
                # Continue with other symbols
                continue

        logger.info(f"Found {len(instruments)} instruments")
        return InstrumentLookupResponse(instruments=instruments, count=len(instruments))
        
    except MarketDataServiceError as e:
        logger.error(f"Market Data Service failed: {e}")
        raise HTTPException(status_code=503, detail="Market data service unavailable")


@router.get("/instruments/{instrument_token}", response_model=InstrumentResponse)
async def get_instrument(
    instrument_token: int,
    db = Depends(get_db)
):
    """
    Get instrument details by token via Market Data Service API.

    CRITICAL: Uses Market Data Service API instead of public.instrument_registry
    since that table doesn't exist in order_service database.

    - **instrument_token**: Numeric instrument token

    Returns instrument details if found.
    """
    try:
        market_client = await get_market_data_client()
        
        # Note: This assumes the market data service has an endpoint to get by token
        # You may need to implement this in the market data service
        # For now, we'll return a 501 Not Implemented error
        raise HTTPException(
            status_code=501, 
            detail="Get by instrument token not yet implemented via Market Data Service API. Use symbol/exchange lookup instead."
        )
        
    except MarketDataServiceError as e:
        logger.error(f"Market Data Service failed: {e}")
        raise HTTPException(status_code=503, detail="Market data service unavailable")


@router.get("/instruments/search/{query}", response_model=InstrumentLookupResponse)
async def search_instruments(
    query: str,
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    limit: int = Query(20, ge=1, le=100, description="Maximum results"),
    db = Depends(get_db)
):
    """
    Search instruments by symbol or name via Market Data Service API.

    CRITICAL: Uses Market Data Service API instead of public.instrument_registry
    since that table doesn't exist in order_service database.

    - **query**: Search query (min 2 characters)
    - **exchange**: Optional exchange filter
    - **limit**: Maximum results to return (default: 20)

    Returns matching instruments.
    """
    if len(query) < 2:
        raise HTTPException(status_code=400, detail="Query must be at least 2 characters")

    try:
        market_client = await get_market_data_client()
        
        # Search instruments via Market Data Service API
        search_results = await market_client.search_instruments(
            query=query.upper(),
            exchange=exchange,
            limit=limit
        )
        
        instruments = [
            InstrumentResponse(
                instrument_token=result.get("instrument_token"),
                tradingsymbol=result.get("symbol"),
                name=result.get("name"),
                exchange=result.get("exchange"),
                segment=result.get("segment"),
                instrument_type=result.get("instrument_type"),
                lot_size=result.get("lot_size"),
                tick_size=result.get("tick_size")
            )
            for result in search_results
        ]

        return InstrumentLookupResponse(instruments=instruments, count=len(instruments))
        
    except MarketDataServiceError as e:
        logger.error(f"Market Data Service failed: {e}")
        raise HTTPException(status_code=503, detail="Market data service unavailable")
