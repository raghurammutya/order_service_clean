"""
Market Hours Service

Handles market state detection for all Indian market segments:
- Equity (NSE/BSE)
- Currency (Forex Derivatives)
- Commodity (MCX)

Includes pre-market, trading, post-market, and auto square-off timings.

NOTE: This module now supports optional integration with calendar_service
for dynamic holiday data. If calendar_service is unavailable, it falls
back to hardcoded HOLIDAYS_2025 list.

Integration:
    # Initialize with calendar service (optional)
    from common.calendar_service import CalendarClient
    client = CalendarClient()
    MarketHoursService.set_calendar_client(client)

    # Or use environment variable
    # CALENDAR_SERVICE_URL=http://calendar-service:8013
"""

import logging
from datetime import datetime, time, date, timedelta
from zoneinfo import ZoneInfo
from enum import Enum
from typing import Optional, Dict, Tuple, Set

logger = logging.getLogger(__name__)

# Indian market timezone
IST = ZoneInfo("Asia/Kolkata")

# Optional calendar service client
_calendar_client = None
_holiday_cache: Dict[str, Set[str]] = {}  # year -> set of holiday date strings
_holiday_cache_timestamp: Optional[datetime] = None
HOLIDAY_CACHE_TTL = 3600  # 1 hour


def set_calendar_client(client) -> None:
    """
    Set the calendar service client for dynamic holiday data.

    Args:
        client: CalendarClient instance from common.calendar_service

    Example:
        from common.calendar_service import CalendarClient
        client = CalendarClient(base_url="http://calendar-service:8013")
        set_calendar_client(client)
    """
    global _calendar_client
    _calendar_client = client
    logger.info("Calendar service client configured for market hours")


def get_calendar_client():
    """Get the current calendar client, or None if not configured."""
    return _calendar_client


async def _fetch_holidays_from_calendar(year: int) -> Set[str]:
    """
    Fetch holidays from calendar service for a given year.

    Args:
        year: Year to fetch holidays for

    Returns:
        Set of holiday date strings in YYYY-MM-DD format
    """
    global _holiday_cache, _holiday_cache_timestamp

    # Check cache
    cache_key = str(year)
    if cache_key in _holiday_cache:
        if _holiday_cache_timestamp and (datetime.now() - _holiday_cache_timestamp).total_seconds() < HOLIDAY_CACHE_TTL:
            return _holiday_cache[cache_key]

    # Try calendar service
    if _calendar_client:
        try:
            from datetime import date
            start_date = date(year, 1, 1)
            end_date = date(year, 12, 31)

            holidays = await _calendar_client.get_holidays(
                calendar="NSE",
                start_date=start_date,
                end_date=end_date
            )

            holiday_set = {h.date.strftime("%Y-%m-%d") for h in holidays}
            _holiday_cache[cache_key] = holiday_set
            _holiday_cache_timestamp = datetime.now()
            logger.debug(f"Fetched {len(holiday_set)} holidays from calendar service for {year}")
            return holiday_set

        except Exception as e:
            logger.warning(f"Failed to fetch holidays from calendar service: {e}, using fallback")

    # Fallback to static holidays for supported years
    year_str = str(year)
    if year_str in MarketHoursService.STATIC_HOLIDAYS:
        return set(MarketHoursService.STATIC_HOLIDAYS[year_str])

    logger.warning(f"No holiday data available for year {year}")
    return set()


async def is_holiday_async(check_date: date = None) -> bool:
    """
    Check if a date is a holiday using calendar service.

    Args:
        check_date: Date to check (defaults to today)

    Returns:
        True if the date is a holiday
    """
    if check_date is None:
        check_date = datetime.now(IST).date()

    year = check_date.year
    holidays = await _fetch_holidays_from_calendar(year)
    return check_date.strftime("%Y-%m-%d") in holidays


def is_holiday_sync(check_date: date = None) -> bool:
    """
    Synchronous holiday check using cached data or hardcoded fallback.

    Args:
        check_date: Date to check (defaults to today)

    Returns:
        True if the date is a holiday
    """
    if check_date is None:
        check_date = datetime.now(IST).date()

    date_str = check_date.strftime("%Y-%m-%d")
    year = check_date.year

    # Check cache first
    cache_key = str(year)
    if cache_key in _holiday_cache:
        return date_str in _holiday_cache[cache_key]

    # Fallback to static holidays
    year_str = str(year)
    if year_str in MarketHoursService.STATIC_HOLIDAYS:
        return date_str in MarketHoursService.STATIC_HOLIDAYS[year_str]

    return False


class MarketSegment(str, Enum):
    """Market segment types."""
    EQUITY = "equity"               # NSE/BSE stocks
    EQUITY_DERIVATIVES = "equity_derivatives"  # Index futures/options (Nifty, Bank Nifty)
    CURRENCY = "currency"           # Forex derivatives
    COMMODITY = "commodity"         # MCX commodities


class MarketState(str, Enum):
    """Market state."""
    PRE_MARKET_ORDER = "pre_market_order"      # 9:00-9:07: Order placement
    PRE_MARKET_DISCOVERY = "pre_market_discovery"  # 9:07-9:08: Price discovery
    PRE_MARKET_BUFFER = "pre_market_buffer"    # 9:08-9:15: Buffer (no changes)
    OPEN = "open"                               # Normal trading
    POST_MARKET_CLOSING = "post_market_closing"  # 3:30-3:40: Closing price calculation
    POST_MARKET_TRADE = "post_market_trade"    # 3:40-4:00: Market orders at closing price
    AUTO_SQUARE_OFF = "auto_square_off"        # 10 mins before close (intraday positions)
    CLOSED = "closed"                           # Market closed
    WEEKEND = "weekend"                         # Saturday/Sunday
    HOLIDAY = "holiday"                         # Exchange holiday


class MarketHoursService:
    """
    Comprehensive market hours service for all Indian market segments.

    Handles:
    - Multiple market segments with different timings
    - Pre-market, normal trading, post-market sessions
    - Auto square-off windows for intraday positions
    - Exchange holidays
    - Weekend handling
    """

    # ============================================================================
    # EQUITY SEGMENT (NSE/BSE)
    # ============================================================================
    EQUITY_PRE_MARKET_START = time(9, 0)       # 9:00 AM
    EQUITY_PRE_MARKET_ORDER_END = time(9, 7)   # 9:07 AM
    EQUITY_PRE_MARKET_DISCOVERY_END = time(9, 8)  # 9:08 AM
    EQUITY_PRE_MARKET_BUFFER_END = time(9, 15)  # 9:15 AM

    EQUITY_OPEN = time(9, 15)                  # 9:15 AM
    EQUITY_CLOSE = time(15, 30)                # 3:30 PM

    EQUITY_POST_MARKET_CLOSING_END = time(15, 40)  # 3:40 PM
    EQUITY_POST_MARKET_END = time(16, 0)       # 4:00 PM

    # Auto square-off times
    # IMPORTANT: NSE squares off MIS positions at 3:20 PM for ALL segments (equity and derivatives)
    # Source: NSE Circulars - Auto square-off for intraday positions
    EQUITY_CASH_SQUARE_OFF = time(15, 20)      # 3:20 PM (MIS equity positions)
    EQUITY_DERIVATIVES_SQUARE_OFF = time(15, 20)  # 3:20 PM (MIS derivatives - CORRECTED from 3:25 PM)

    # ============================================================================
    # CURRENCY SEGMENT (Forex Derivatives)
    # ============================================================================
    CURRENCY_OPEN = time(9, 0)                 # 9:00 AM
    CURRENCY_CLOSE = time(17, 0)               # 5:00 PM

    # ============================================================================
    # COMMODITY SEGMENT (MCX)
    # ============================================================================
    COMMODITY_OPEN = time(9, 0)                # 9:00 AM
    COMMODITY_CLOSE = time(23, 30)             # 11:30 PM

    # Auto square-off: 10 minutes before close
    COMMODITY_SQUARE_OFF = time(23, 20)        # 11:20 PM

    # ============================================================================
    # EXCHANGE HOLIDAYS (Multi-year support)
    # ============================================================================
    
    # Static holiday data - fallback when calendar service unavailable
    STATIC_HOLIDAYS = {
        "2024": [
            "2024-01-26", "2024-03-08", "2024-03-25", "2024-03-29",
            "2024-04-11", "2024-04-14", "2024-04-17", "2024-05-01",
            "2024-06-17", "2024-08-15", "2024-10-02", "2024-10-31",
            "2024-11-01", "2024-11-15", "2024-12-25"
        ],
        "2025": [
            "2025-01-26", "2025-03-14", "2025-03-31", "2025-04-10",
            "2025-04-14", "2025-04-18", "2025-05-01", "2025-06-07", 
            "2025-08-15", "2025-08-27", "2025-10-02", "2025-10-21",
            "2025-11-01", "2025-11-05", "2025-11-24", "2025-12-25"
        ],
        "2026": [
            "2026-01-26", "2026-03-03", "2026-03-20", "2026-03-30",
            "2026-04-02", "2026-04-06", "2026-04-14", "2026-05-01",
            "2026-05-27", "2026-08-15", "2026-09-16", "2026-10-02",
            "2026-10-19", "2026-11-04", "2026-11-21", "2026-12-25"
        ]
    }
    
    # Legacy compatibility
    HOLIDAYS_2025 = STATIC_HOLIDAYS["2025"]
    
    @classmethod
    def has_holiday_data_for_year(cls, year: int) -> bool:
        """Check if holiday data is available for given year"""
        return str(year) in cls.STATIC_HOLIDAYS
    
    @classmethod 
    def get_supported_years(cls) -> list:
        """Get list of years with holiday data"""
        return list(cls.STATIC_HOLIDAYS.keys())

    @classmethod
    def get_market_state(
        cls,
        segment: MarketSegment,
        now: Optional[datetime] = None
    ) -> MarketState:
        """
        Get current market state for a specific segment.

        Args:
            segment: Market segment (equity, currency, commodity)
            now: Current time (defaults to now in IST)

        Returns:
            MarketState enum

        Example:
            >>> state = MarketHoursService.get_market_state(MarketSegment.EQUITY)
            >>> if state == MarketState.OPEN:
            >>>     # Place orders
        """
        if now is None:
            now = datetime.now(IST)

        # Check if weekend
        if now.weekday() >= 5:  # Saturday=5, Sunday=6
            return MarketState.WEEKEND

        # Check if holiday
        date_str = now.strftime("%Y-%m-%d")
        year_str = str(now.year)
        holidays = cls.STATIC_HOLIDAYS.get(year_str, [])
        if date_str in holidays:
            return MarketState.HOLIDAY

        current_time = now.time()

        # Route to segment-specific logic
        if segment == MarketSegment.EQUITY:
            return cls._get_equity_state(current_time)
        elif segment == MarketSegment.EQUITY_DERIVATIVES:
            return cls._get_equity_derivatives_state(current_time)
        elif segment == MarketSegment.CURRENCY:
            return cls._get_currency_state(current_time)
        elif segment == MarketSegment.COMMODITY:
            return cls._get_commodity_state(current_time)
        else:
            return MarketState.CLOSED

    @classmethod
    def _get_equity_state(cls, current_time: time) -> MarketState:
        """Get market state for equity segment."""
        # Pre-market sessions
        if cls.EQUITY_PRE_MARKET_START <= current_time < cls.EQUITY_PRE_MARKET_ORDER_END:
            return MarketState.PRE_MARKET_ORDER

        if cls.EQUITY_PRE_MARKET_ORDER_END <= current_time < cls.EQUITY_PRE_MARKET_DISCOVERY_END:
            return MarketState.PRE_MARKET_DISCOVERY

        if cls.EQUITY_PRE_MARKET_DISCOVERY_END <= current_time < cls.EQUITY_PRE_MARKET_BUFFER_END:
            return MarketState.PRE_MARKET_BUFFER

        # Normal trading
        if cls.EQUITY_OPEN <= current_time < cls.EQUITY_CLOSE:
            return MarketState.OPEN

        # Post-market sessions
        if cls.EQUITY_CLOSE <= current_time < cls.EQUITY_POST_MARKET_CLOSING_END:
            return MarketState.POST_MARKET_CLOSING

        if cls.EQUITY_POST_MARKET_CLOSING_END <= current_time < cls.EQUITY_POST_MARKET_END:
            return MarketState.POST_MARKET_TRADE

        return MarketState.CLOSED

    @classmethod
    def _get_equity_derivatives_state(cls, current_time: time) -> MarketState:
        """Get market state for equity derivatives (same as equity but different square-off)."""
        return cls._get_equity_state(current_time)

    @classmethod
    def _get_currency_state(cls, current_time: time) -> MarketState:
        """Get market state for currency segment."""
        if cls.CURRENCY_OPEN <= current_time < cls.CURRENCY_CLOSE:
            return MarketState.OPEN

        return MarketState.CLOSED

    @classmethod
    def _get_commodity_state(cls, current_time: time) -> MarketState:
        """Get market state for commodity segment."""
        if cls.COMMODITY_OPEN <= current_time < cls.COMMODITY_CLOSE:
            return MarketState.OPEN

        return MarketState.CLOSED

    @classmethod
    def is_market_open(
        cls,
        segment: MarketSegment,
        now: Optional[datetime] = None
    ) -> bool:
        """
        Check if market is currently open for trading.

        Args:
            segment: Market segment
            now: Current time (optional)

        Returns:
            True if market is open for normal trading
        """
        state = cls.get_market_state(segment, now)
        return state == MarketState.OPEN

    @classmethod
    def can_place_orders(
        cls,
        segment: MarketSegment,
        now: Optional[datetime] = None
    ) -> bool:
        """
        Check if orders can be placed (includes pre-market order window).

        Args:
            segment: Market segment
            now: Current time (optional)

        Returns:
            True if orders can be placed
        """
        state = cls.get_market_state(segment, now)

        # Orders allowed during these states
        allowed_states = {
            MarketState.PRE_MARKET_ORDER,
            MarketState.OPEN,
            MarketState.POST_MARKET_TRADE,
        }

        return state in allowed_states

    @classmethod
    def is_auto_square_off_window(
        cls,
        segment: MarketSegment,
        now: Optional[datetime] = None
    ) -> bool:
        """
        Check if we're in the auto square-off window for intraday positions.

        Args:
            segment: Market segment
            now: Current time (optional)

        Returns:
            True if in square-off window (positions will be auto-closed)
        """
        if now is None:
            now = datetime.now(IST)

        current_time = now.time()

        if segment == MarketSegment.EQUITY:
            # Equity/Cash: 3:20 PM onwards
            return current_time >= cls.EQUITY_CASH_SQUARE_OFF

        elif segment == MarketSegment.EQUITY_DERIVATIVES:
            # Equity/Index Derivatives: 3:20 PM onwards (same as equity cash)
            return current_time >= cls.EQUITY_DERIVATIVES_SQUARE_OFF

        elif segment == MarketSegment.COMMODITY:
            # Commodities: 10 minutes before close (11:20 PM onwards)
            return current_time >= cls.COMMODITY_SQUARE_OFF

        elif segment == MarketSegment.CURRENCY:
            # Currency: No explicit square-off window mentioned
            # Use 10 minutes before close as safety measure
            currency_square_off = time(16, 50)  # 4:50 PM
            return current_time >= currency_square_off

        return False

    @classmethod
    def time_until_market_open(
        cls,
        segment: MarketSegment,
        now: Optional[datetime] = None
    ) -> int:
        """
        Calculate seconds until market opens for normal trading.

        Args:
            segment: Market segment
            now: Current time (optional)

        Returns:
            Seconds until market open, or 0 if market is open
        """
        if now is None:
            now = datetime.now(IST)

        if cls.is_market_open(segment, now):
            return 0

        # Determine market open time for segment
        if segment == MarketSegment.EQUITY or segment == MarketSegment.EQUITY_DERIVATIVES:
            open_time = cls.EQUITY_OPEN
        elif segment == MarketSegment.CURRENCY:
            open_time = cls.CURRENCY_OPEN
        elif segment == MarketSegment.COMMODITY:
            open_time = cls.COMMODITY_OPEN
        else:
            return 0

        # Calculate next market open
        next_open = now.replace(
            hour=open_time.hour,
            minute=open_time.minute,
            second=0,
            microsecond=0
        )

        # If past today's open time, move to next business day
        if now.time() >= open_time:
            next_open += timedelta(days=1)

        # Skip weekends
        while next_open.weekday() >= 5:
            next_open += timedelta(days=1)

        # Skip holidays
        while True:
            year_str = str(next_open.year)
            holidays = cls.STATIC_HOLIDAYS.get(year_str, [])
            if next_open.strftime("%Y-%m-%d") not in holidays:
                break
            next_open += timedelta(days=1)

        return int((next_open - now).total_seconds())

    @classmethod
    def time_until_market_close(
        cls,
        segment: MarketSegment,
        now: Optional[datetime] = None
    ) -> int:
        """
        Calculate seconds until market closes.

        Args:
            segment: Market segment
            now: Current time (optional)

        Returns:
            Seconds until market close, or 0 if market is closed
        """
        if now is None:
            now = datetime.now(IST)

        if not cls.is_market_open(segment, now):
            return 0

        # Determine market close time for segment
        if segment == MarketSegment.EQUITY or segment == MarketSegment.EQUITY_DERIVATIVES:
            close_time = cls.EQUITY_CLOSE
        elif segment == MarketSegment.CURRENCY:
            close_time = cls.CURRENCY_CLOSE
        elif segment == MarketSegment.COMMODITY:
            close_time = cls.COMMODITY_CLOSE
        else:
            return 0

        market_close = now.replace(
            hour=close_time.hour,
            minute=close_time.minute,
            second=0,
            microsecond=0
        )

        return int((market_close - now).total_seconds())

    @classmethod
    def time_until_square_off(
        cls,
        segment: MarketSegment,
        now: Optional[datetime] = None
    ) -> int:
        """
        Calculate seconds until auto square-off window.

        Args:
            segment: Market segment
            now: Current time (optional)

        Returns:
            Seconds until square-off, or 0 if already in square-off window
        """
        if now is None:
            now = datetime.now(IST)

        if cls.is_auto_square_off_window(segment, now):
            return 0

        # Determine square-off time for segment
        if segment == MarketSegment.EQUITY:
            square_off_time = cls.EQUITY_CASH_SQUARE_OFF
        elif segment == MarketSegment.EQUITY_DERIVATIVES:
            square_off_time = cls.EQUITY_DERIVATIVES_SQUARE_OFF
        elif segment == MarketSegment.COMMODITY:
            square_off_time = cls.COMMODITY_SQUARE_OFF
        elif segment == MarketSegment.CURRENCY:
            square_off_time = time(16, 50)  # 4:50 PM (10 mins before close)
        else:
            return 0

        square_off = now.replace(
            hour=square_off_time.hour,
            minute=square_off_time.minute,
            second=0,
            microsecond=0
        )

        # If past today's square-off, it's tomorrow
        if now.time() >= square_off_time:
            return 0

        return int((square_off - now).total_seconds())

    @classmethod
    def get_segment_from_symbol(cls, symbol: str) -> MarketSegment:
        """
        Infer market segment from symbol.

        Args:
            symbol: Trading symbol (e.g., "NIFTY25DEC24700CE", "USDINR25JANFUT")

        Returns:
            MarketSegment enum

        Examples:
            >>> MarketHoursService.get_segment_from_symbol("NIFTY25DEC24700CE")
            MarketSegment.EQUITY_DERIVATIVES

            >>> MarketHoursService.get_segment_from_symbol("USDINR25JANFUT")
            MarketSegment.CURRENCY

            >>> MarketHoursService.get_segment_from_symbol("GOLDM25FEBFUT")
            MarketSegment.COMMODITY
        """
        symbol_upper = symbol.upper()

        # Equity derivatives: NIFTY, BANKNIFTY, FINNIFTY
        if any(idx in symbol_upper for idx in ["NIFTY", "BANKNIFTY", "FINNIFTY"]):
            return MarketSegment.EQUITY_DERIVATIVES

        # Currency: USD, EUR, GBP, JPY, INR
        if any(curr in symbol_upper for curr in ["USD", "EUR", "GBP", "JPY", "INR"]) and "FUT" in symbol_upper:
            return MarketSegment.CURRENCY

        # Commodity: GOLD, SILVER, CRUDE, NATURALGAS, etc.
        if any(comm in symbol_upper for comm in ["GOLD", "SILVER", "CRUDE", "NATURALGAS", "COPPER", "ZINC"]):
            return MarketSegment.COMMODITY

        # Default to equity
        return MarketSegment.EQUITY

    @classmethod
    def get_market_info(
        cls,
        segment: MarketSegment,
        now: Optional[datetime] = None
    ) -> Dict:
        """
        Get comprehensive market information for a segment.

        Args:
            segment: Market segment
            now: Current time (optional)

        Returns:
            Dictionary with market state, timings, and flags
        """
        if now is None:
            now = datetime.now(IST)

        state = cls.get_market_state(segment, now)

        return {
            "segment": segment.value,
            "state": state.value,
            "is_open": cls.is_market_open(segment, now),
            "can_place_orders": cls.can_place_orders(segment, now),
            "is_square_off_window": cls.is_auto_square_off_window(segment, now),
            "seconds_until_open": cls.time_until_market_open(segment, now),
            "seconds_until_close": cls.time_until_market_close(segment, now),
            "seconds_until_square_off": cls.time_until_square_off(segment, now),
            "current_time_ist": now.isoformat(),
            "is_weekend": now.weekday() >= 5,
            "is_holiday": now.strftime("%Y-%m-%d") in cls.STATIC_HOLIDAYS.get(str(now.year), []),
        }

    @classmethod
    def should_close_intraday_positions(
        cls,
        segment: MarketSegment,
        now: Optional[datetime] = None
    ) -> Tuple[bool, str]:
        """
        Check if intraday positions should be closed.

        Args:
            segment: Market segment
            now: Current time (optional)

        Returns:
            (should_close: bool, reason: str)

        Example:
            >>> should_close, reason = MarketHoursService.should_close_intraday_positions(
            ...     MarketSegment.EQUITY_DERIVATIVES
            ... )
            >>> if should_close:
            >>>     logger.warning(f"Closing intraday positions: {reason}")
            >>>     close_all_intraday_positions()
        """
        if now is None:
            now = datetime.now(IST)

        # Check if in square-off window
        if cls.is_auto_square_off_window(segment, now):
            seconds_until_close = cls.time_until_market_close(segment, now)
            minutes_until_close = seconds_until_close // 60

            return (
                True,
                f"Auto square-off window active. Market closes in {minutes_until_close} minutes."
            )

        # Check if market is closing soon (5 minutes before square-off)
        seconds_until_square_off = cls.time_until_square_off(segment, now)

        if 0 < seconds_until_square_off <= 300:  # 5 minutes
            minutes_left = seconds_until_square_off // 60
            return (
                True,
                f"Approaching auto square-off in {minutes_left} minutes. Close positions proactively."
            )

        return (False, "")


    # ============================================================================
    # ASYNC METHODS (Calendar Service Integration)
    # ============================================================================

    @classmethod
    async def get_market_state_async(
        cls,
        segment: MarketSegment,
        now: Optional[datetime] = None
    ) -> MarketState:
        """
        Get current market state using calendar service for holiday data.

        This async version fetches holiday information from the calendar service
        for accurate, up-to-date holiday detection.

        Args:
            segment: Market segment (equity, currency, commodity)
            now: Current time (defaults to now in IST)

        Returns:
            MarketState enum
        """
        if now is None:
            now = datetime.now(IST)

        # Check if weekend
        if now.weekday() >= 5:  # Saturday=5, Sunday=6
            return MarketState.WEEKEND

        # Check if holiday (async)
        if await is_holiday_async(now.date()):
            return MarketState.HOLIDAY

        current_time = now.time()

        # Route to segment-specific logic
        if segment == MarketSegment.EQUITY:
            return cls._get_equity_state(current_time)
        elif segment == MarketSegment.EQUITY_DERIVATIVES:
            return cls._get_equity_derivatives_state(current_time)
        elif segment == MarketSegment.CURRENCY:
            return cls._get_currency_state(current_time)
        elif segment == MarketSegment.COMMODITY:
            return cls._get_commodity_state(current_time)
        else:
            return MarketState.CLOSED

    @classmethod
    async def is_market_open_async(
        cls,
        segment: MarketSegment,
        now: Optional[datetime] = None
    ) -> bool:
        """
        Async check if market is currently open for trading.

        Uses calendar service for holiday detection.
        """
        state = await cls.get_market_state_async(segment, now)
        return state == MarketState.OPEN

    @classmethod
    async def can_place_orders_async(
        cls,
        segment: MarketSegment,
        now: Optional[datetime] = None
    ) -> bool:
        """
        Async check if orders can be placed.

        Uses calendar service for holiday detection.
        """
        state = await cls.get_market_state_async(segment, now)
        allowed_states = {
            MarketState.PRE_MARKET_ORDER,
            MarketState.OPEN,
            MarketState.POST_MARKET_TRADE,
        }
        return state in allowed_states

    @classmethod
    async def get_market_info_async(
        cls,
        segment: MarketSegment,
        now: Optional[datetime] = None
    ) -> Dict:
        """
        Get comprehensive market information using calendar service.

        Args:
            segment: Market segment
            now: Current time (optional)

        Returns:
            Dictionary with market state, timings, and flags
        """
        if now is None:
            now = datetime.now(IST)

        state = await cls.get_market_state_async(segment, now)
        is_holiday = await is_holiday_async(now.date())

        return {
            "segment": segment.value,
            "state": state.value,
            "is_open": state == MarketState.OPEN,
            "can_place_orders": state in {MarketState.PRE_MARKET_ORDER, MarketState.OPEN, MarketState.POST_MARKET_TRADE},
            "is_square_off_window": cls.is_auto_square_off_window(segment, now),
            "seconds_until_open": cls.time_until_market_open(segment, now),
            "seconds_until_close": cls.time_until_market_close(segment, now),
            "seconds_until_square_off": cls.time_until_square_off(segment, now),
            "current_time_ist": now.isoformat(),
            "is_weekend": now.weekday() >= 5,
            "is_holiday": is_holiday,
            "calendar_service_active": _calendar_client is not None,
        }

    @classmethod
    async def get_next_trading_day_async(
        cls,
        calendar: str = "NSE",
        from_date: Optional[date] = None
    ) -> date:
        """
        Get the next trading day using calendar service.

        Args:
            calendar: Calendar code (NSE, BSE, MCX)
            from_date: Start date (defaults to today)

        Returns:
            Next trading day date
        """
        if from_date is None:
            from_date = datetime.now(IST).date()

        check_date = from_date + timedelta(days=1)

        # Check up to 10 days ahead
        for _ in range(10):
            # Skip weekends
            if check_date.weekday() < 5:  # Not weekend
                if not await is_holiday_async(check_date):
                    return check_date
            check_date += timedelta(days=1)

        # Fallback - just return next weekday
        return check_date

    @classmethod
    async def is_trading_day_async(
        cls,
        calendar: str = "NSE",
        check_date: Optional[date] = None
    ) -> bool:
        """
        Check if a date is a trading day using calendar service.

        Args:
            calendar: Calendar code (NSE, BSE, MCX)
            check_date: Date to check (defaults to today)

        Returns:
            True if it's a trading day
        """
        if check_date is None:
            check_date = datetime.now(IST).date()

        # Weekend check
        if check_date.weekday() >= 5:
            return False

        # Holiday check via calendar service
        return not await is_holiday_async(check_date)


# ============================================================================
# GLOBAL HELPER FUNCTIONS
# ============================================================================

def get_market_state_for_symbol(symbol: str, now: Optional[datetime] = None) -> MarketState:
    """
    Convenience function to get market state directly from symbol.

    Args:
        symbol: Trading symbol
        now: Current time (optional)

    Returns:
        MarketState enum

    Example:
        >>> state = get_market_state_for_symbol("NIFTY25DEC24700CE")
        >>> if state == MarketState.OPEN:
        >>>     # Place order
    """
    segment = MarketHoursService.get_segment_from_symbol(symbol)
    return MarketHoursService.get_market_state(segment, now)


async def get_market_state_for_symbol_async(symbol: str, now: Optional[datetime] = None) -> MarketState:
    """
    Async convenience function to get market state directly from symbol.

    Uses calendar service for holiday detection.

    Args:
        symbol: Trading symbol
        now: Current time (optional)

    Returns:
        MarketState enum
    """
    segment = MarketHoursService.get_segment_from_symbol(symbol)
    return await MarketHoursService.get_market_state_async(segment, now)


async def initialize_calendar_client(calendar_service_url: str = None) -> bool:
    """
    Initialize the calendar service client.

    Call this at application startup to enable calendar service integration.

    Args:
        calendar_service_url: URL of calendar service (default: from env or http://localhost:8013)

    Returns:
        True if client was initialized successfully

    Example:
        # In main.py lifespan:
        await initialize_calendar_client("http://calendar-service:8013")
    """
    if calendar_service_url is None:
        try:
            from ..config.settings import settings, _get_service_port
            
            # Try service discovery first
            try:
                port = await _get_service_port("calendar_service")
                url = f"http://calendar-service:{port}"
            except Exception:
                # Fallback to settings
                url = settings.calendar_service_url
        except Exception:
            # Final fallback - use default service name
            url = "http://calendar-service:8013"
    else:
        url = calendar_service_url

    try:
        # Try importing the calendar client
        from common.calendar_service import CalendarClient
        client = CalendarClient(base_url=url)

        # Test connection
        try:
            status = await client.get_market_status(segment="EQUITY")
            logger.info(f"Calendar service connected: {url}")
            set_calendar_client(client)
            return True
        except Exception as e:
            logger.warning(f"Calendar service not reachable at {url}: {e}")
            return False

    except ImportError:
        logger.warning("common.calendar_service not available - using hardcoded holidays")
        return False


def get_holidays_for_year(year: int = None) -> Set[str]:
    """
    Get holidays for a year (sync version using cache or fallback).

    Args:
        year: Year to get holidays for (defaults to current year)

    Returns:
        Set of holiday date strings in YYYY-MM-DD format
    """
    if year is None:
        year = datetime.now(IST).year

    cache_key = str(year)
    if cache_key in _holiday_cache:
        return _holiday_cache[cache_key]

    if year == 2025:
        return set(MarketHoursService.HOLIDAYS_2025)

    return set()


# Module-level exports
__all__ = [
    # Classes
    "MarketSegment",
    "MarketState",
    "MarketHoursService",
    # Sync functions
    "get_market_state_for_symbol",
    "is_holiday_sync",
    "get_holidays_for_year",
    # Async functions
    "get_market_state_for_symbol_async",
    "is_holiday_async",
    "initialize_calendar_client",
    # Client management
    "set_calendar_client",
    "get_calendar_client",
]
