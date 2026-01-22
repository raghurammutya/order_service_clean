"""
Order Service Specific Exception Classes

Provides structured exception hierarchy for proper error handling
and eliminates dangerous catch-all exception blocks.
"""

from typing import Optional, Dict, Any


class OrderServiceError(Exception):
    """Base exception for order service errors"""
    pass


class ServiceUnavailableError(OrderServiceError):
    """External service unavailable or timing out"""
    pass


class BrokerAPIError(OrderServiceError):
    """Broker API specific errors (Kite, etc.)"""
    
    def __init__(self, message: str, error_code: Optional[str] = None, status_code: Optional[int] = None):
        super().__init__(message)
        self.error_code = error_code
        self.status_code = status_code


class AuthenticationError(BrokerAPIError):
    """Authentication/token related errors"""
    pass


class ValidationError(OrderServiceError):
    """Data validation errors"""
    
    def __init__(self, message: str, field: Optional[str] = None, value: Optional[Any] = None):
        super().__init__(message)
        self.field = field
        self.value = value


class OrderProcessingError(OrderServiceError):
    """Order processing business logic errors"""
    pass


class DataFormatError(OrderServiceError):
    """Data parsing/format errors"""
    pass


class DatabaseError(OrderServiceError):
    """Database operation errors"""
    pass


class ConfigurationError(OrderServiceError):
    """Configuration/settings errors"""
    pass


class RateLimitError(BrokerAPIError):
    """Rate limit exceeded errors"""
    pass


class InsufficientFundsError(OrderProcessingError):
    """Insufficient funds for order"""
    pass


class InvalidSymbolError(ValidationError):
    """Invalid trading symbol"""
    pass