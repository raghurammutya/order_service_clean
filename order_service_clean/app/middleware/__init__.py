"""Middleware Module for Order Service

Standardized middleware stack for request tracking and error handling.

MIDDLEWARE ORDER (outermost to innermost):
1. CorrelationIDMiddleware - Add correlation IDs (X-Request-ID, X-Trace-ID)
2. ErrorHandlerMiddleware - Catch exceptions, standardize error responses
3. CORSMiddleware - Handle CORS (added in main.py)

Usage in main.py:
    from .middleware import (
        CorrelationIDMiddleware,
        ErrorHandlerMiddleware,
    )

    # Add in REVERSE order (innermost to outermost)
    app.add_middleware(CORSMiddleware, ...)
    app.add_middleware(ErrorHandlerMiddleware, environment=settings.ENVIRONMENT)
    app.add_middleware(CorrelationIDMiddleware)
"""
from .correlation_id import (
    CorrelationIDMiddleware,
    CorrelationIDFilter,
    set_correlation_ids,
    get_correlation_ids,
    clear_correlation_ids,
)
from .error_handler import (
    ErrorHandlerMiddleware,
    http_exception_handler,
    validation_exception_handler,
    general_exception_handler,
)

__all__ = [
    # Middleware classes
    "CorrelationIDMiddleware",
    "ErrorHandlerMiddleware",
    # Correlation ID utilities
    "CorrelationIDFilter",
    "set_correlation_ids",
    "get_correlation_ids",
    "clear_correlation_ids",
    # Exception handlers
    "http_exception_handler",
    "validation_exception_handler",
    "general_exception_handler",
]
