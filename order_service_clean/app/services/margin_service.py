"""
Margin Service

Provides margin calculation and validation for order placement.
"""
import logging
from typing import Dict, Any
from decimal import Decimal

logger = logging.getLogger(__name__)

class MarginService:
    """Service for managing margin requirements and calculations."""
    
    def __init__(self):
        """Initialize the margin service."""
        # Default margin requirements (percentage of value)
        self._default_margins = {
            'EQ': Decimal('0.20'),      # 20% margin for equity
            'FUT': Decimal('0.10'),     # 10% margin for futures
            'OPT': Decimal('1.00'),     # 100% margin for options (premium)
            'CE': Decimal('1.00'),      # Call options
            'PE': Decimal('1.00')       # Put options
        }
        
        # Margin multipliers for different product types
        self._product_multipliers = {
            'MIS': Decimal('0.5'),      # Intraday - 50% of normal margin
            'CNC': Decimal('1.0'),      # Cash and Carry - full margin
            'NRML': Decimal('1.0')      # Normal - full margin
        }
        
    def calculate_margin_required(
        self, 
        symbol: str, 
        quantity: int, 
        price: Decimal, 
        instrument_type: str = 'EQ',
        product_type: str = 'CNC'
    ) -> Decimal:
        """
        Calculate margin required for an order.
        
        Args:
            symbol: Instrument symbol
            quantity: Number of shares/lots
            price: Price per unit
            instrument_type: Type of instrument ('EQ', 'FUT', 'OPT', etc.)
            product_type: Product type ('MIS', 'CNC', 'NRML')
            
        Returns:
            Margin required as Decimal
        """
        try:
            # Calculate gross value
            gross_value = Decimal(str(quantity)) * price
            
            # Get base margin percentage
            base_margin_pct = self._default_margins.get(instrument_type, Decimal('0.20'))
            
            # Get product multiplier
            product_multiplier = self._product_multipliers.get(product_type, Decimal('1.0'))
            
            # Calculate margin
            base_margin = gross_value * base_margin_pct
            final_margin = base_margin * product_multiplier
            
            logger.debug(f"Margin calculation: {symbol} qty={quantity} price={price} "
                        f"type={instrument_type} product={product_type} -> {final_margin}")
            
            return final_margin
            
        except Exception as e:
            logger.error(f"Error calculating margin for {symbol}: {e}")
            # Conservative fallback - require 100% margin
            return Decimal(str(quantity)) * price
            
    def validate_margin_available(
        self, 
        required_margin: Decimal, 
        available_margin: Decimal
    ) -> bool:
        """
        Validate if sufficient margin is available.
        
        Args:
            required_margin: Margin required for order
            available_margin: Available margin in account
            
        Returns:
            True if sufficient margin available, False otherwise
        """
        try:
            return available_margin >= required_margin
        except Exception as e:
            logger.error(f"Error validating margin: {e}")
            return False
            
    def get_margin_shortage(
        self, 
        required_margin: Decimal, 
        available_margin: Decimal
    ) -> Decimal:
        """
        Calculate margin shortage amount.
        
        Args:
            required_margin: Margin required for order
            available_margin: Available margin in account
            
        Returns:
            Shortage amount (0 if sufficient margin available)
        """
        try:
            shortage = required_margin - available_margin
            return max(shortage, Decimal('0'))
        except Exception as e:
            logger.error(f"Error calculating margin shortage: {e}")
            return required_margin  # Conservative fallback
            
    def calculate_exposure_limit(
        self, 
        available_funds: Decimal, 
        instrument_type: str = 'EQ',
        product_type: str = 'CNC'
    ) -> Decimal:
        """
        Calculate maximum exposure possible with available funds.
        
        Args:
            available_funds: Available funds in account
            instrument_type: Type of instrument
            product_type: Product type
            
        Returns:
            Maximum exposure amount
        """
        try:
            # Get margin requirement percentage
            margin_pct = self._default_margins.get(instrument_type, Decimal('0.20'))
            product_multiplier = self._product_multipliers.get(product_type, Decimal('1.0'))
            
            effective_margin_pct = margin_pct * product_multiplier
            
            # Calculate maximum exposure
            if effective_margin_pct > 0:
                max_exposure = available_funds / effective_margin_pct
            else:
                max_exposure = available_funds
                
            return max_exposure
            
        except Exception as e:
            logger.error(f"Error calculating exposure limit: {e}")
            return available_funds  # Conservative fallback
            
    def get_margin_info(
        self, 
        symbol: str,
        instrument_type: str = 'EQ'
    ) -> Dict[str, Any]:
        """
        Get margin information for an instrument.
        
        Args:
            symbol: Instrument symbol
            instrument_type: Type of instrument
            
        Returns:
            Dictionary with margin information
        """
        try:
            base_margin_pct = self._default_margins.get(instrument_type, Decimal('0.20'))
            
            return {
                'symbol': symbol,
                'instrument_type': instrument_type,
                'base_margin_percentage': float(base_margin_pct * 100),
                'product_multipliers': {
                    k: float(v) for k, v in self._product_multipliers.items()
                },
                'effective_margins': {
                    product: float(base_margin_pct * multiplier * 100)
                    for product, multiplier in self._product_multipliers.items()
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting margin info for {symbol}: {e}")
            return {
                'symbol': symbol,
                'error': str(e)
            }

# Global instance
margin_service = MarginService()