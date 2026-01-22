"""
Lot Size Service

Provides lot size and multiplier information for instruments.
"""
import logging
from decimal import Decimal

logger = logging.getLogger(__name__)

class LotSizeService:
    """Service for managing lot sizes and multipliers for instruments."""
    
    def __init__(self):
        """Initialize the lot size service."""
        # Default lot sizes for common instruments
        self._default_lot_sizes = {
            'EQ': 1,      # Equity
            'FUT': 25,    # Futures (NSE default)  
            'OPT': 25,    # Options (NSE default)
            'CE': 25,     # Call options
            'PE': 25      # Put options
        }
        
        # Common NSE F&O lot sizes
        self._instrument_lot_sizes = {
            'NIFTY': 25,
            'BANKNIFTY': 15,
            'FINNIFTY': 25,
            'RELIANCE': 250,
            'TCS': 150,
            'INFY': 300,
            'HDFCBANK': 550,
            'ICICIBANK': 1375,
            'SBIN': 3000,
            'ITC': 3200,
            'LT': 300,
            'HCLTECH': 700,
            'WIPRO': 3000,
            'AXISBANK': 1200,
            'MARUTI': 100,
            'ASIANPAINT': 300,
            'BAJFINANCE': 125,
            'BHARTIARTL': 1800,
            'COALINDIA': 4200,
            'DRREDDY': 125,
            'EICHERMOT': 350,
            'GRASIM': 600,
            'HDFCLIFE': 1800,
            'HEROMOTOCO': 300,
            'HINDALCO': 2400,
            'HINDUNILVR': 300,
            'INDUSINDBK': 1800,
            'JSWSTEEL': 2400,
            'KOTAKBANK': 400,
            'M&M': 900,
            'NESTLEIND': 50,
            'NTPC': 7000,
            'ONGC': 4550,
            'POWERGRID': 4200,
            'SBILIFE': 1400,
            'SUNPHARMA': 1000,
            'TATACONSUM': 1500,
            'TATAMOTORS': 1800,
            'TATASTEEL': 800,
            'TECHM': 600,
            'TITAN': 300,
            'ULTRACEMCO': 200,
            'UPL': 1500
        }
        
    def get_lot_size(self, symbol: str, instrument_type: str = 'EQ') -> int:
        """
        Get lot size for a symbol.
        
        Args:
            symbol: Instrument symbol (e.g., 'RELIANCE', 'NIFTY')
            instrument_type: Type of instrument ('EQ', 'FUT', 'OPT', 'CE', 'PE')
            
        Returns:
            Lot size as integer
        """
        try:
            # For equity, lot size is always 1
            if instrument_type == 'EQ':
                return 1
                
            # For F&O instruments, check specific symbol mappings
            if symbol.upper() in self._instrument_lot_sizes:
                return self._instrument_lot_sizes[symbol.upper()]
                
            # Fall back to default lot size for instrument type
            return self._default_lot_sizes.get(instrument_type, 1)
            
        except Exception as e:
            logger.error(f"Error getting lot size for {symbol} ({instrument_type}): {e}")
            return 1  # Safe default
            
    def get_multiplier(self, symbol: str, instrument_type: str = 'EQ') -> Decimal:
        """
        Get multiplier for quantity calculation.
        
        Args:
            symbol: Instrument symbol
            instrument_type: Type of instrument
            
        Returns:
            Multiplier as Decimal
        """
        try:
            lot_size = self.get_lot_size(symbol, instrument_type)
            return Decimal(str(lot_size))
        except Exception as e:
            logger.error(f"Error getting multiplier for {symbol}: {e}")
            return Decimal('1')
            
    def calculate_quantity(self, symbol: str, lots: int, instrument_type: str = 'EQ') -> int:
        """
        Calculate actual quantity from number of lots.
        
        Args:
            symbol: Instrument symbol
            lots: Number of lots to trade
            instrument_type: Type of instrument
            
        Returns:
            Actual quantity to trade
        """
        try:
            lot_size = self.get_lot_size(symbol, instrument_type)
            return lots * lot_size
        except Exception as e:
            logger.error(f"Error calculating quantity for {symbol}: {e}")
            return lots  # Fallback to input lots
            
    def validate_quantity(self, symbol: str, quantity: int, instrument_type: str = 'EQ') -> bool:
        """
        Validate if quantity is in valid lot multiples.
        
        Args:
            symbol: Instrument symbol
            quantity: Quantity to validate
            instrument_type: Type of instrument
            
        Returns:
            True if valid, False otherwise
        """
        try:
            if quantity <= 0:
                return False
                
            lot_size = self.get_lot_size(symbol, instrument_type)
            
            # For equity, any positive quantity is valid
            if instrument_type == 'EQ':
                return True
                
            # For F&O, quantity must be multiple of lot size
            return quantity % lot_size == 0
            
        except Exception as e:
            logger.error(f"Error validating quantity for {symbol}: {e}")
            return False

# Global instance
lot_size_service = LotSizeService()