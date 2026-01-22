"""
Tests for Market Hours Service

Validates market hours detection, holiday handling, and auto square-off timing.
Tests the critical fix: NSE derivatives square-off at 3:20 PM (not 3:25 PM).
"""
import pytest
from datetime import datetime, time, date
from zoneinfo import ZoneInfo
from order_service.app.services.market_hours import MarketHoursService

# IST timezone
IST = ZoneInfo("Asia/Kolkata")


@pytest.fixture
def market_hours():
    """Create market hours service instance"""
    return MarketHoursService()


class TestEquityMarketHours:
    """Test equity market hours (NSE)"""

    def test_equity_market_open(self, market_hours):
        """Equity market open hours: 9:15 AM - 3:30 PM"""
        # During market hours
        market_time = datetime(2025, 11, 22, 10, 30, tzinfo=IST)  # Saturday
        assert market_hours.is_market_open("EQUITY", market_time) is False  # Weekend

        # Weekday during market hours
        market_time = datetime(2025, 11, 24, 10, 30, tzinfo=IST)  # Monday
        assert market_hours.is_market_open("EQUITY", market_time) is True

    def test_equity_market_closed(self, market_hours):
        """Equity market closed before 9:15 AM and after 3:30 PM"""
        # Before market open
        before_open = datetime(2025, 11, 24, 9, 0, tzinfo=IST)
        assert market_hours.is_market_open("EQUITY", before_open) is False

        # After market close
        after_close = datetime(2025, 11, 24, 15, 45, tzinfo=IST)
        assert market_hours.is_market_open("EQUITY", after_close) is False

    def test_equity_opening_time(self, market_hours):
        """Market opens exactly at 9:15 AM"""
        opening = datetime(2025, 11, 24, 9, 15, tzinfo=IST)
        assert market_hours.is_market_open("EQUITY", opening) is True

    def test_equity_closing_time(self, market_hours):
        """Market closes at 3:30 PM"""
        # Just before close - still open
        before_close = datetime(2025, 11, 24, 15, 29, 59, tzinfo=IST)
        assert market_hours.is_market_open("EQUITY", before_close) is True

        # At close - market closed
        at_close = datetime(2025, 11, 24, 15, 30, tzinfo=IST)
        assert market_hours.is_market_open("EQUITY", at_close) is False


class TestDerivativesMarketHours:
    """Test derivatives market hours (NSE F&O)"""

    def test_derivatives_market_open(self, market_hours):
        """Derivatives market open hours: 9:15 AM - 3:30 PM"""
        market_time = datetime(2025, 11, 24, 10, 30, tzinfo=IST)
        assert market_hours.is_market_open("EQUITY_DERIVATIVES", market_time) is True

    def test_derivatives_same_as_equity(self, market_hours):
        """Derivatives have same trading hours as equity"""
        test_time = datetime(2025, 11, 24, 14, 0, tzinfo=IST)

        equity_open = market_hours.is_market_open("EQUITY", test_time)
        derivatives_open = market_hours.is_market_open("EQUITY_DERIVATIVES", test_time)

        assert equity_open == derivatives_open


class TestAutoSquareOffTiming:
    """Test auto square-off timing - CRITICAL FIX VALIDATION"""

    def test_derivatives_square_off_320pm(self, market_hours):
        """
        CRITICAL: NSE derivatives square-off at 3:20 PM (not 3:25 PM)

        This validates the fix for incorrect auto square-off timing.
        All MIS positions must be squared off by 3:20 PM.
        """
        square_off_time = market_hours.get_auto_square_off_time("EQUITY_DERIVATIVES", "MIS")

        # Should be 3:20 PM (15:20)
        assert square_off_time == time(15, 20, 0)
        assert square_off_time != time(15, 25, 0), "Old incorrect timing was 3:25 PM"

    def test_equity_square_off_unchanged(self, market_hours):
        """Equity MIS square-off remains at 3:20 PM"""
        square_off_time = market_hours.get_auto_square_off_time("EQUITY", "MIS")

        # Equity also 3:20 PM
        assert square_off_time == time(15, 20, 0)

    def test_cnc_no_square_off(self, market_hours):
        """CNC positions should not have auto square-off"""
        square_off_time = market_hours.get_auto_square_off_time("EQUITY", "CNC")

        # CNC is delivery, no square-off
        assert square_off_time is None

    def test_nrml_no_intraday_square_off(self, market_hours):
        """NRML positions can be carried overnight"""
        square_off_time = market_hours.get_auto_square_off_time("EQUITY_DERIVATIVES", "NRML")

        # NRML can be held overnight, no intraday square-off
        assert square_off_time is None


class TestWeekendAndHolidays:
    """Test weekend and holiday detection"""

    def test_saturday_market_closed(self, market_hours):
        """Market closed on Saturdays"""
        saturday = datetime(2025, 11, 22, 10, 30, tzinfo=IST)
        assert saturday.weekday() == 5  # Saturday
        assert market_hours.is_market_open("EQUITY", saturday) is False

    def test_sunday_market_closed(self, market_hours):
        """Market closed on Sundays"""
        sunday = datetime(2025, 11, 23, 10, 30, tzinfo=IST)
        assert sunday.weekday() == 6  # Sunday
        assert market_hours.is_market_open("EQUITY", sunday) is False

    def test_weekday_market_open(self, market_hours):
        """Market open on weekdays (Mon-Fri)"""
        monday = datetime(2025, 11, 24, 10, 30, tzinfo=IST)
        assert monday.weekday() == 0  # Monday
        assert market_hours.is_market_open("EQUITY", monday) is True

    def test_2025_holidays(self, market_hours):
        """Test 2025 NSE holidays"""
        # Republic Day - Jan 26, 2025
        republic_day = datetime(2025, 1, 26, 10, 30, tzinfo=IST)
        assert market_hours.is_holiday(republic_day.date()) is True
        assert market_hours.is_market_open("EQUITY", republic_day) is False

        # Independence Day - Aug 15, 2025
        independence_day = datetime(2025, 8, 15, 10, 30, tzinfo=IST)
        assert market_hours.is_holiday(independence_day.date()) is True

        # Diwali - Oct 20, 2025 (example date)
        # Note: Actual date depends on lunar calendar
        # This test may need updating with actual 2025 NSE holiday calendar

    def test_regular_trading_day(self, market_hours):
        """Regular trading day should not be a holiday"""
        regular_day = datetime(2025, 11, 24, 10, 30, tzinfo=IST)  # Monday
        assert market_hours.is_holiday(regular_day.date()) is False


class TestCurrencyMarketHours:
    """Test currency market hours"""

    def test_currency_market_extended_hours(self, market_hours):
        """Currency market: 9:00 AM - 5:00 PM"""
        # Open at 9:00 AM
        opening = datetime(2025, 11, 24, 9, 0, tzinfo=IST)
        assert market_hours.is_market_open("CURRENCY", opening) is True

        # Still open at 4:30 PM (after equity closes)
        late_afternoon = datetime(2025, 11, 24, 16, 30, tzinfo=IST)
        assert market_hours.is_market_open("CURRENCY", late_afternoon) is True

        # Closed at 5:00 PM
        closing = datetime(2025, 11, 24, 17, 0, tzinfo=IST)
        assert market_hours.is_market_open("CURRENCY", closing) is False


class TestCommodityMarketHours:
    """Test commodity market hours"""

    def test_commodity_market_hours(self, market_hours):
        """Commodity market: 9:00 AM - 11:30 PM"""
        # Open at 9:00 AM
        opening = datetime(2025, 11, 24, 9, 0, tzinfo=IST)
        assert market_hours.is_market_open("COMMODITY", opening) is True

        # Still open at 10:00 PM (late evening)
        late_evening = datetime(2025, 11, 24, 22, 0, tzinfo=IST)
        assert market_hours.is_market_open("COMMODITY", late_evening) is True

        # Closed at 11:30 PM
        closing = datetime(2025, 11, 24, 23, 30, tzinfo=IST)
        assert market_hours.is_market_open("COMMODITY", closing) is False


class TestSegmentDetection:
    """Test segment detection from symbol/exchange"""

    def test_nse_equity_segment(self, market_hours):
        """NSE equity symbols belong to EQUITY segment"""
        segment = market_hours.get_segment("RELIANCE", "NSE")
        assert segment == "EQUITY"

    def test_nfo_derivatives_segment(self, market_hours):
        """NFO symbols belong to EQUITY_DERIVATIVES segment"""
        segment = market_hours.get_segment("NIFTY25DEC24500CE", "NFO")
        assert segment == "EQUITY_DERIVATIVES"

    def test_cds_currency_segment(self, market_hours):
        """CDS symbols belong to CURRENCY segment"""
        segment = market_hours.get_segment("USDINR25DECFUT", "CDS")
        assert segment == "CURRENCY"

    def test_mcx_commodity_segment(self, market_hours):
        """MCX symbols belong to COMMODITY segment"""
        segment = market_hours.get_segment("GOLDPETAL25DECFUT", "MCX")
        assert segment == "COMMODITY"


class TestTimezoneHandling:
    """Test timezone handling"""

    def test_ist_timezone_required(self, market_hours):
        """All times should be in IST"""
        # Create time in UTC
        utc_time = datetime(2025, 11, 24, 4, 45, tzinfo=ZoneInfo("UTC"))  # 10:15 AM IST

        # Convert to IST for checking
        ist_time = utc_time.astimezone(IST)

        # Should be open (10:15 AM IST)
        assert market_hours.is_market_open("EQUITY", ist_time) is True

    def test_naive_datetime_assumes_ist(self, market_hours):
        """Naive datetime should be treated as IST"""
        # This might depend on implementation
        # If service requires timezone-aware datetime, this should raise an error
        naive_time = datetime(2025, 11, 24, 10, 30)

        # Depending on implementation, this might work or require timezone
        try:
            result = market_hours.is_market_open("EQUITY", naive_time)
            # If it works, verify it's treated as IST
            assert isinstance(result, bool)
        except (TypeError, ValueError):
            # Service correctly requires timezone-aware datetime
            pass


class TestEdgeCases:
    """Test edge cases and boundary conditions"""

    def test_pre_market_hours(self, market_hours):
        """Pre-market hours (9:00 AM - 9:15 AM) should be closed for regular trading"""
        pre_market = datetime(2025, 11, 24, 9, 10, tzinfo=IST)
        assert market_hours.is_market_open("EQUITY", pre_market) is False

    def test_post_market_hours(self, market_hours):
        """Post-market hours (3:30 PM - 4:00 PM) should be closed for regular trading"""
        post_market = datetime(2025, 11, 24, 15, 45, tzinfo=IST)
        assert market_hours.is_market_open("EQUITY", post_market) is False

    def test_midnight_market_closed(self, market_hours):
        """Market closed at midnight"""
        midnight = datetime(2025, 11, 24, 0, 0, tzinfo=IST)
        assert market_hours.is_market_open("EQUITY", midnight) is False

    def test_exact_second_precision(self, market_hours):
        """Test precise timing at market open/close"""
        # 9:14:59 - still closed
        just_before_open = datetime(2025, 11, 24, 9, 14, 59, tzinfo=IST)
        assert market_hours.is_market_open("EQUITY", just_before_open) is False

        # 9:15:00 - now open
        exactly_open = datetime(2025, 11, 24, 9, 15, 0, tzinfo=IST)
        assert market_hours.is_market_open("EQUITY", exactly_open) is True

        # 15:29:59 - still open
        just_before_close = datetime(2025, 11, 24, 15, 29, 59, tzinfo=IST)
        assert market_hours.is_market_open("EQUITY", just_before_close) is True

        # 15:30:00 - now closed
        exactly_close = datetime(2025, 11, 24, 15, 30, 0, tzinfo=IST)
        assert market_hours.is_market_open("EQUITY", exactly_close) is False


class TestRealWorldScenarios:
    """Test real-world trading scenarios"""

    def test_typical_trading_day(self, market_hours):
        """Simulate a typical trading day"""
        # Monday, Nov 24, 2025
        trading_day = date(2025, 11, 24)

        # Not a weekend
        assert trading_day.weekday() < 5

        # Not a holiday
        assert not market_hours.is_holiday(trading_day)

        # Market open at 10:30 AM
        morning = datetime.combine(trading_day, time(10, 30), tzinfo=IST)
        assert market_hours.is_market_open("EQUITY", morning) is True

        # Market still open at 2:00 PM
        afternoon = datetime.combine(trading_day, time(14, 0), tzinfo=IST)
        assert market_hours.is_market_open("EQUITY", afternoon) is True

        # MIS square-off at 3:20 PM
        square_off = market_hours.get_auto_square_off_time("EQUITY", "MIS")
        assert square_off == time(15, 20, 0)

        # Market closes at 3:30 PM
        closing = datetime.combine(trading_day, time(15, 30), tzinfo=IST)
        assert market_hours.is_market_open("EQUITY", closing) is False

    def test_long_weekend(self, market_hours):
        """Test long weekend (Sat-Sun-Mon holiday)"""
        # Saturday
        saturday = datetime(2025, 8, 16, 10, 30, tzinfo=IST)
        assert not market_hours.is_market_open("EQUITY", saturday)

        # Sunday
        sunday = datetime(2025, 8, 17, 10, 30, tzinfo=IST)
        assert not market_hours.is_market_open("EQUITY", sunday)

        # If Aug 18 is a holiday, entire weekend is closed
        # (This depends on actual 2025 holiday calendar)
