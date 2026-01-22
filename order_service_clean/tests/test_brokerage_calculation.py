"""
Tests for Brokerage Calculation Service

Validates that brokerage and charges are calculated correctly
according to Zerodha's fee structure.
"""
import pytest
from decimal import Decimal
from order_service.app.services.brokerage_service import BrokerageService


@pytest.fixture
def brokerage_service():
    """Create brokerage service instance"""
    return BrokerageService()


class TestEquityDeliveryBrokerage:
    """Test equity delivery (CNC) brokerage calculation"""

    def test_cnc_zero_brokerage(self, brokerage_service):
        """CNC orders should have 0% brokerage"""
        result = brokerage_service.calculate_brokerage(
            exchange="NSE",
            product_type="CNC",
            transaction_type="BUY",
            quantity=100,
            price=Decimal("2500.00"),
            instrument_type="EQ"
        )

        assert result['brokerage'] == Decimal('0.00')
        assert result['trade_value'] == Decimal('250000.00')

    def test_cnc_stt_both_sides(self, brokerage_service):
        """CNC should charge 0.1% STT on both buy and sell"""
        buy_result = brokerage_service.calculate_brokerage(
            exchange="NSE",
            product_type="CNC",
            transaction_type="BUY",
            quantity=100,
            price=Decimal("2500.00"),
            instrument_type="EQ"
        )

        sell_result = brokerage_service.calculate_brokerage(
            exchange="NSE",
            product_type="CNC",
            transaction_type="SELL",
            quantity=100,
            price=Decimal("2500.00"),
            instrument_type="EQ"
        )

        # 0.1% STT = 250,000 * 0.001 = 250
        expected_stt = Decimal('250.00')
        assert buy_result['stt'] == expected_stt
        assert sell_result['stt'] == expected_stt

    def test_cnc_stamp_duty_buy_only(self, brokerage_service):
        """Stamp duty should apply only on buy side"""
        buy_result = brokerage_service.calculate_brokerage(
            exchange="NSE",
            product_type="CNC",
            transaction_type="BUY",
            quantity=100,
            price=Decimal("2500.00"),
            instrument_type="EQ"
        )

        sell_result = brokerage_service.calculate_brokerage(
            exchange="NSE",
            product_type="CNC",
            transaction_type="SELL",
            quantity=100,
            price=Decimal("2500.00"),
            instrument_type="EQ"
        )

        # 0.003% stamp duty on buy
        assert buy_result['stamp_duty'] > 0
        assert sell_result['stamp_duty'] == 0


class TestEquityIntradayBrokerage:
    """Test equity intraday (MIS) brokerage calculation"""

    def test_mis_brokerage_flat_rate(self, brokerage_service):
        """MIS should charge ₹20 flat for small orders"""
        result = brokerage_service.calculate_brokerage(
            exchange="NSE",
            product_type="MIS",
            transaction_type="BUY",
            quantity=10,
            price=Decimal("1000.00"),
            instrument_type="EQ"
        )

        # Trade value = 10,000
        # 0.03% = 3, but max ₹20, so should be ₹3
        assert result['brokerage'] == Decimal('3.00')

    def test_mis_brokerage_capped_at_20(self, brokerage_service):
        """MIS brokerage should cap at ₹20"""
        result = brokerage_service.calculate_brokerage(
            exchange="NSE",
            product_type="MIS",
            transaction_type="BUY",
            quantity=1000,
            price=Decimal("1000.00"),
            instrument_type="EQ"
        )

        # Trade value = 1,000,000
        # 0.03% = 300, but max ₹20, so should be ₹20
        assert result['brokerage'] == Decimal('20.00')

    def test_mis_stt_sell_only(self, brokerage_service):
        """MIS should charge 0.025% STT on sell side only"""
        buy_result = brokerage_service.calculate_brokerage(
            exchange="NSE",
            product_type="MIS",
            transaction_type="BUY",
            quantity=100,
            price=Decimal("2500.00"),
            instrument_type="EQ"
        )

        sell_result = brokerage_service.calculate_brokerage(
            exchange="NSE",
            product_type="MIS",
            transaction_type="SELL",
            quantity=100,
            price=Decimal("2500.00"),
            instrument_type="EQ"
        )

        # Buy should have no STT for MIS
        assert buy_result['stt'] == Decimal('0.00')

        # Sell should have 0.025% STT = 250,000 * 0.00025 = 62.50
        assert sell_result['stt'] == Decimal('62.50')


class TestFuturesAndOptionsBrokerage:
    """Test F&O brokerage calculation"""

    def test_fo_flat_brokerage(self, brokerage_service):
        """F&O should charge flat ₹20 per order"""
        result = brokerage_service.calculate_brokerage(
            exchange="NFO",
            product_type="NRML",
            transaction_type="BUY",
            quantity=50,  # 1 lot of NIFTY
            price=Decimal("24500.00"),
            instrument_type="FUT"
        )

        assert result['brokerage'] == Decimal('20.00')

    def test_options_stt_sell_only(self, brokerage_service):
        """Options should charge 0.05% STT on sell side only"""
        buy_result = brokerage_service.calculate_brokerage(
            exchange="NFO",
            product_type="NRML",
            transaction_type="BUY",
            quantity=50,
            price=Decimal("100.00"),  # Premium
            instrument_type="CE"
        )

        sell_result = brokerage_service.calculate_brokerage(
            exchange="NFO",
            product_type="NRML",
            transaction_type="SELL",
            quantity=50,
            price=Decimal("100.00"),
            instrument_type="CE"
        )

        # Buy should have no STT
        assert buy_result['stt'] == Decimal('0.00')

        # Sell should have 0.05% STT = 5,000 * 0.0005 = 2.50
        assert sell_result['stt'] == Decimal('2.50')

    def test_futures_stt_sell_only(self, brokerage_service):
        """Futures should charge 0.01% STT on sell side only"""
        sell_result = brokerage_service.calculate_brokerage(
            exchange="NFO",
            product_type="NRML",
            transaction_type="SELL",
            quantity=50,
            price=Decimal("24500.00"),
            instrument_type="FUT"
        )

        # 0.01% STT = 1,225,000 * 0.0001 = 122.50
        assert sell_result['stt'] == Decimal('122.50')


class TestGSTCalculation:
    """Test GST calculation on taxable charges"""

    def test_gst_18_percent(self, brokerage_service):
        """GST should be 18% on (brokerage + exchange charges)"""
        result = brokerage_service.calculate_brokerage(
            exchange="NSE",
            product_type="MIS",
            transaction_type="BUY",
            quantity=100,
            price=Decimal("1000.00"),
            instrument_type="EQ"
        )

        # Brokerage = 20, Exchange charges ~ 3.25
        # GST = 18% of (20 + 3.25) = 4.19 (approximately)
        taxable = result['brokerage'] + result['exchange_charges']
        expected_gst = taxable * Decimal('0.18')
        assert result['gst'] == expected_gst.quantize(Decimal('0.01'))


class TestTotalChargesCalculation:
    """Test total charges calculation"""

    def test_total_charges_sum(self, brokerage_service):
        """Total charges should be sum of all components"""
        result = brokerage_service.calculate_brokerage(
            exchange="NSE",
            product_type="MIS",
            transaction_type="BUY",
            quantity=100,
            price=Decimal("1000.00"),
            instrument_type="EQ"
        )

        expected_total = (
            result['brokerage'] +
            result['stt'] +
            result['exchange_charges'] +
            result['gst'] +
            result['sebi_charges'] +
            result['stamp_duty']
        )

        assert result['total_charges'] == expected_total


class TestPositionCharges:
    """Test calculate_position_charges method"""

    def test_position_charges_buy_and_sell(self, brokerage_service):
        """Calculate total charges for a position with buy and sell"""
        total_charges = brokerage_service.calculate_position_charges(
            exchange="NSE",
            product_type="MIS",
            buy_quantity=100,
            buy_value=Decimal("250000.00"),
            sell_quantity=100,
            sell_value=Decimal("251000.00"),
            instrument_type="EQ"
        )

        # Should include charges from both buy and sell sides
        assert total_charges > 0
        assert isinstance(total_charges, Decimal)


class TestRealWorldScenarios:
    """Test real-world trading scenarios"""

    def test_reliance_intraday_trade(self, brokerage_service):
        """Example from documentation: RELIANCE intraday trade"""
        result = brokerage_service.calculate_trade_charges(
            exchange="NSE",
            product_type="MIS",
            buy_quantity=100,
            buy_price=Decimal("2500.00"),
            sell_quantity=100,
            sell_price=Decimal("2510.00"),
            instrument_type="EQ"
        )

        # Gross P&L = (2510 - 2500) * 100 = 1000
        assert result['gross_pnl'] == Decimal('1000.00')

        # Total charges should be around 130-140 (brokerage + STT + exchange + GST)
        assert 100 < result['total_charges'] < 200

        # Net P&L should be gross - charges
        expected_net = result['gross_pnl'] - result['total_charges']
        assert result['net_pnl'] == expected_net

    def test_nifty_options_trade(self, brokerage_service):
        """NIFTY options trade"""
        result = brokerage_service.calculate_trade_charges(
            exchange="NFO",
            product_type="NRML",
            buy_quantity=50,  # 1 lot
            buy_price=Decimal("100.00"),
            sell_quantity=50,
            sell_price=Decimal("120.00"),
            instrument_type="CE"
        )

        # Gross P&L = (120 - 100) * 50 = 1000
        assert result['gross_pnl'] == Decimal('1000.00')

        # F&O charges: flat ₹20 brokerage per side = ₹40 total
        assert result['total_brokerage'] == Decimal('40.00')

        # Net P&L should account for all charges
        assert result['net_pnl'] < result['gross_pnl']

    def test_zero_quantity_position(self, brokerage_service):
        """Position with zero quantity should have zero charges"""
        total_charges = brokerage_service.calculate_position_charges(
            exchange="NSE",
            product_type="CNC",
            buy_quantity=0,
            buy_value=Decimal("0.00"),
            sell_quantity=0,
            sell_value=Decimal("0.00"),
            instrument_type="EQ"
        )

        assert total_charges == Decimal('0.00')


class TestEdgeCases:
    """Test edge cases and boundary conditions"""

    def test_very_small_trade(self, brokerage_service):
        """Very small trade value"""
        result = brokerage_service.calculate_brokerage(
            exchange="NSE",
            product_type="MIS",
            transaction_type="BUY",
            quantity=1,
            price=Decimal("10.00"),
            instrument_type="EQ"
        )

        # Even tiny trades should have valid charges
        assert result['total_charges'] > 0
        assert result['brokerage'] >= 0

    def test_very_large_trade(self, brokerage_service):
        """Very large trade value"""
        result = brokerage_service.calculate_brokerage(
            exchange="NSE",
            product_type="CNC",
            transaction_type="BUY",
            quantity=10000,
            price=Decimal("5000.00"),
            instrument_type="EQ"
        )

        # Large trades should have proportional charges
        assert result['total_charges'] > 0
        assert result['stt'] > 0  # STT should be significant

    def test_decimal_precision(self, brokerage_service):
        """All charge components should have 2 decimal precision"""
        result = brokerage_service.calculate_brokerage(
            exchange="NSE",
            product_type="MIS",
            transaction_type="BUY",
            quantity=100,
            price=Decimal("1234.56"),
            instrument_type="EQ"
        )

        # Check all components are rounded to 2 decimals
        for key in ['brokerage', 'stt', 'exchange_charges', 'gst', 'total_charges']:
            value = result[key]
            # Check that value has at most 2 decimal places
            assert value == value.quantize(Decimal('0.01'))
