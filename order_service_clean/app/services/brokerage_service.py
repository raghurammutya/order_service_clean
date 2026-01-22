"""
Brokerage Calculation Service

Calculates brokerage and charges for trades based on Zerodha's fee structure.
Ensures P&L reflects actual costs, not just price difference.

Zerodha Brokerage Structure (as of 2025):
- Equity Delivery (CNC): 0% brokerage
- Equity Intraday (MIS): ₹20 or 0.03% per executed order (whichever is lower)
- F&O: ₹20 per executed order (flat)

Additional Charges:
- STT (Securities Transaction Tax): Varies by segment
- Exchange Transaction Charges: ~0.00325% for NSE
- GST: 18% on (brokerage + transaction charges)
- SEBI Charges: ₹10 per crore
- Stamp Duty: 0.003% on buy side
"""
import logging
from decimal import Decimal
from typing import Dict, Any

logger = logging.getLogger(__name__)


class BrokerageService:
    """Calculate brokerage and transaction charges for trades."""

    # Zerodha brokerage rates
    EQUITY_DELIVERY_BROKERAGE_RATE = Decimal('0.0')  # 0% for CNC
    EQUITY_INTRADAY_BROKERAGE_MAX = Decimal('20.0')  # ₹20 max per order
    EQUITY_INTRADAY_BROKERAGE_RATE = Decimal('0.0003')  # 0.03%
    FO_BROKERAGE_FLAT = Decimal('20.0')  # ₹20 flat per order

    # STT (Securities Transaction Tax) rates
    STT_EQUITY_DELIVERY = Decimal('0.001')  # 0.1% on both buy and sell
    STT_EQUITY_INTRADAY = Decimal('0.00025')  # 0.025% on sell side only
    STT_FO_FUTURES = Decimal('0.0001')  # 0.01% on sell side only
    STT_FO_OPTIONS = Decimal('0.0005')  # 0.05% on sell side (premium)

    # Exchange transaction charges (NSE)
    EXCHANGE_CHARGES_EQUITY = Decimal('0.0000325')  # 0.00325%
    EXCHANGE_CHARGES_FO = Decimal('0.00005')  # 0.005%

    # GST on brokerage + transaction charges
    GST_RATE = Decimal('0.18')  # 18%

    # SEBI charges
    SEBI_CHARGES_PER_CRORE = Decimal('10.0')  # ₹10 per crore turnover

    # Stamp duty
    STAMP_DUTY_RATE = Decimal('0.00003')  # 0.003% on buy side

    def __init__(self):
        """Initialize brokerage service."""
        logger.info("BrokerageService initialized with Zerodha fee structure")

    def calculate_brokerage(
        self,
        exchange: str,
        product_type: str,
        transaction_type: str,
        quantity: int,
        price: Decimal,
        instrument_type: str = "EQ"  # EQ, FUT, CE, PE
    ) -> Dict[str, Decimal]:
        """
        Calculate total brokerage and charges for a trade.

        Args:
            exchange: Exchange code (NSE, NFO, BSE, etc.)
            product_type: CNC, MIS, NRML
            transaction_type: BUY or SELL
            quantity: Trade quantity
            price: Trade price
            instrument_type: EQ (equity), FUT (futures), CE/PE (options)

        Returns:
            Dict with detailed breakdown of charges
        """
        # Trade value
        trade_value = Decimal(quantity) * Decimal(price)

        # Initialize charges
        brokerage = Decimal('0')
        stt = Decimal('0')
        exchange_charges = Decimal('0')
        sebi_charges = Decimal('0')
        stamp_duty = Decimal('0')

        # Determine if F&O
        is_fo = exchange in ['NFO', 'BFO', 'MCX', 'CDS'] or instrument_type in ['FUT', 'CE', 'PE']

        # Calculate brokerage
        if is_fo:
            # F&O: Flat ₹20 per order
            brokerage = self.FO_BROKERAGE_FLAT

        elif product_type == 'CNC':
            # Equity Delivery: 0% brokerage
            brokerage = Decimal('0')

        elif product_type == 'MIS':
            # Equity Intraday: ₹20 or 0.03%, whichever is lower
            calculated_brokerage = trade_value * self.EQUITY_INTRADAY_BROKERAGE_RATE
            brokerage = min(calculated_brokerage, self.EQUITY_INTRADAY_BROKERAGE_MAX)

        # Calculate STT
        if is_fo:
            # F&O
            if instrument_type == 'FUT':
                # Futures: 0.01% on sell side only
                if transaction_type == 'SELL':
                    stt = trade_value * self.STT_FO_FUTURES
            else:
                # Options: 0.05% on sell side (on premium)
                if transaction_type == 'SELL':
                    stt = trade_value * self.STT_FO_OPTIONS

        elif product_type == 'CNC':
            # Equity Delivery: 0.1% on both buy and sell
            stt = trade_value * self.STT_EQUITY_DELIVERY

        elif product_type == 'MIS':
            # Equity Intraday: 0.025% on sell side only
            if transaction_type == 'SELL':
                stt = trade_value * self.STT_EQUITY_INTRADAY

        # Calculate exchange transaction charges
        if is_fo:
            exchange_charges = trade_value * self.EXCHANGE_CHARGES_FO
        else:
            exchange_charges = trade_value * self.EXCHANGE_CHARGES_EQUITY

        # Calculate SEBI charges (₹10 per crore)
        turnover_in_crores = trade_value / Decimal('10000000')  # 1 crore = 1,00,00,000
        sebi_charges = turnover_in_crores * self.SEBI_CHARGES_PER_CRORE

        # Calculate stamp duty (on buy side only)
        if transaction_type == 'BUY':
            stamp_duty = trade_value * self.STAMP_DUTY_RATE

        # Calculate GST (18% on brokerage + exchange charges)
        taxable_amount = brokerage + exchange_charges
        gst = taxable_amount * self.GST_RATE

        # Total charges
        total_charges = brokerage + stt + exchange_charges + gst + sebi_charges + stamp_duty

        return {
            'trade_value': trade_value,
            'brokerage': brokerage.quantize(Decimal('0.01')),
            'stt': stt.quantize(Decimal('0.01')),
            'exchange_charges': exchange_charges.quantize(Decimal('0.01')),
            'gst': gst.quantize(Decimal('0.01')),
            'sebi_charges': sebi_charges.quantize(Decimal('0.01')),
            'stamp_duty': stamp_duty.quantize(Decimal('0.01')),
            'total_charges': total_charges.quantize(Decimal('0.01'))
        }

    def calculate_trade_charges(
        self,
        exchange: str,
        product_type: str,
        buy_quantity: int,
        buy_price: Decimal,
        sell_quantity: int,
        sell_price: Decimal,
        instrument_type: str = "EQ"
    ) -> Dict[str, Any]:
        """
        Calculate charges for a complete trade (buy + sell).

        Args:
            exchange: Exchange code
            product_type: CNC, MIS, NRML
            buy_quantity: Buy quantity
            buy_price: Average buy price
            sell_quantity: Sell quantity
            sell_price: Average sell price
            instrument_type: EQ, FUT, CE, PE

        Returns:
            Dict with buy charges, sell charges, total charges, and net P&L
        """
        # Calculate charges for buy side
        buy_charges = self.calculate_brokerage(
            exchange=exchange,
            product_type=product_type,
            transaction_type='BUY',
            quantity=buy_quantity,
            price=buy_price,
            instrument_type=instrument_type
        )

        # Calculate charges for sell side
        sell_charges = self.calculate_brokerage(
            exchange=exchange,
            product_type=product_type,
            transaction_type='SELL',
            quantity=sell_quantity,
            price=sell_price,
            instrument_type=instrument_type
        )

        # Total charges
        total_brokerage = buy_charges['brokerage'] + sell_charges['brokerage']
        total_stt = buy_charges['stt'] + sell_charges['stt']
        total_exchange_charges = buy_charges['exchange_charges'] + sell_charges['exchange_charges']
        total_gst = buy_charges['gst'] + sell_charges['gst']
        total_sebi = buy_charges['sebi_charges'] + sell_charges['sebi_charges']
        total_stamp_duty = buy_charges['stamp_duty'] + sell_charges['stamp_duty']
        total_charges = buy_charges['total_charges'] + sell_charges['total_charges']

        # Gross P&L (before charges)
        closed_quantity = min(buy_quantity, sell_quantity)
        gross_pnl = (sell_price - buy_price) * Decimal(closed_quantity)

        # Net P&L (after charges)
        net_pnl = gross_pnl - total_charges

        return {
            'buy_charges': buy_charges,
            'sell_charges': sell_charges,
            'total_brokerage': total_brokerage.quantize(Decimal('0.01')),
            'total_stt': total_stt.quantize(Decimal('0.01')),
            'total_exchange_charges': total_exchange_charges.quantize(Decimal('0.01')),
            'total_gst': total_gst.quantize(Decimal('0.01')),
            'total_sebi_charges': total_sebi.quantize(Decimal('0.01')),
            'total_stamp_duty': total_stamp_duty.quantize(Decimal('0.01')),
            'total_charges': total_charges.quantize(Decimal('0.01')),
            'gross_pnl': gross_pnl.quantize(Decimal('0.01')),
            'net_pnl': net_pnl.quantize(Decimal('0.01')),
            'charges_impact': (total_charges / abs(gross_pnl) * Decimal('100')).quantize(Decimal('0.01')) if gross_pnl != 0 else Decimal('0')
        }

    def calculate_position_charges(
        self,
        exchange: str,
        product_type: str,
        buy_quantity: int,
        buy_value: Decimal,
        sell_quantity: int,
        sell_value: Decimal,
        instrument_type: str = "EQ"
    ) -> Decimal:
        """
        Calculate total charges for a position based on buy/sell values.

        This is a simplified version for position tracking where we have
        aggregate buy_value and sell_value instead of individual trades.

        Args:
            exchange: Exchange code
            product_type: CNC, MIS, NRML
            buy_quantity: Total buy quantity
            buy_value: Total buy value
            sell_quantity: Total sell quantity
            sell_value: Total sell value
            instrument_type: EQ, FUT, CE, PE

        Returns:
            Total charges as Decimal
        """
        total_charges = Decimal('0')

        # Calculate buy side charges
        if buy_quantity > 0 and buy_value > 0:
            avg_buy_price = buy_value / Decimal(buy_quantity)
            buy_charges = self.calculate_brokerage(
                exchange=exchange,
                product_type=product_type,
                transaction_type='BUY',
                quantity=buy_quantity,
                price=avg_buy_price,
                instrument_type=instrument_type
            )
            total_charges += buy_charges['total_charges']

        # Calculate sell side charges
        if sell_quantity > 0 and sell_value > 0:
            avg_sell_price = sell_value / Decimal(sell_quantity)
            sell_charges = self.calculate_brokerage(
                exchange=exchange,
                product_type=product_type,
                transaction_type='SELL',
                quantity=sell_quantity,
                price=avg_sell_price,
                instrument_type=instrument_type
            )
            total_charges += sell_charges['total_charges']

        return total_charges.quantize(Decimal('0.01'))
