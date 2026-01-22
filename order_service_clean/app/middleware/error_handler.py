"""Error Handler Middleware

ARCHITECTURE COMPLIANCE:
- Standardized error response format across all services
- Environment-aware error details (hide stack traces in production)
- Correlation IDs in all error responses
- Global exception handling

Based on best patterns from:
- backend: Environment-aware error responses
- api_gateway: Standardized error format with correlation IDs
- order_service: Comprehensive error codes

This middleware catches all unhandled exceptions and returns standardized
error responses with correlation IDs.
"""
import logging
import traceback
from typing import Union

from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from ..config import settings

logger = logging.getLogger(__name__)


class ErrorHandlerMiddleware:
    """
    Global error handler middleware.

    Catches all unhandled exceptions and returns standardized error responses.

    Error Response Format:
    {
        "error": {
            "code": "INTERNAL_SERVER_ERROR",
            "message": "An error occurred",
            "details": {...},  // Only in development
            "request_id": "abc123",
            "trace_id": "0af76519...",
            "timestamp": "2025-12-07T10:30:45.123Z"
        }
    }

    Environment-Aware:
    - Production: Hides stack traces, shows generic error messages
    - Development: Shows full stack traces and error details
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        """ASGI middleware interface."""
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_wrapper(message):
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception as exc:
            # Get request from scope
            request = Request(scope, receive)

            # Handle exception
            response = await self.handle_exception(request, exc)

            # Send response
            await response(scope, receive, send)

    async def handle_exception(
        self,
        request: Request,
        exc: Exception
    ) -> JSONResponse:
        """
        Handle exception and return standardized error response.

        Args:
            request: HTTP request
            exc: Exception that was raised

        Returns:
            JSONResponse with error details
        """
        # Extract correlation IDs
        request_id = getattr(request.state, "request_id", "-")
        trace_id = getattr(request.state, "trace_id", "-")
        span_id = getattr(request.state, "span_id", "-")

        # Import here to avoid circular dependency
        from datetime import datetime, timezone
        timestamp = datetime.now(timezone.utc).isoformat()

        # =====================================================================
        # CASE 1: HTTPException (FastAPI or Starlette)
        # =====================================================================

        if isinstance(exc, (HTTPException, StarletteHTTPException)):
            status_code = exc.status_code
            error_code = self._get_error_code_from_status(status_code)

            # Use exception detail as message
            message = str(exc.detail) if exc.detail else self._get_default_message(status_code)

            # Log based on severity
            if status_code >= 500:
                logger.error(
                    f"[{request_id}] HTTP {status_code} - {error_code}: {message}",
                    exc_info=True if not settings.is_production else False
                )
            elif status_code >= 400:
                logger.warning(f"[{request_id}] HTTP {status_code} - {error_code}: {message}")

            error_response = {
                "error": {
                    "code": error_code,
                    "message": message,
                    "request_id": request_id,
                    "trace_id": trace_id,
                    "timestamp": timestamp,
                }
            }

            # Add details in development
            if settings.is_development and hasattr(exc, 'detail') and isinstance(exc.detail, dict):
                error_response["error"]["details"] = exc.detail

            return JSONResponse(
                status_code=status_code,
                content=error_response
            )

        # =====================================================================
        # CASE 2: RequestValidationError (Pydantic validation)
        # =====================================================================

        if isinstance(exc, RequestValidationError):
            status_code = 422
            error_code = "VALIDATION_ERROR"

            # Extract validation errors
            validation_errors = exc.errors()

            logger.warning(
                f"[{request_id}] Validation error: {validation_errors}"
            )

            error_response = {
                "error": {
                    "code": error_code,
                    "message": "Request validation failed",
                    "request_id": request_id,
                    "trace_id": trace_id,
                    "timestamp": timestamp,
                }
            }

            # Always show validation errors (needed for client debugging)
            error_response["error"]["validation_errors"] = validation_errors

            return JSONResponse(
                status_code=status_code,
                content=error_response
            )

        # =====================================================================
        # CASE 3: Unhandled Exception (500 Internal Server Error)
        # =====================================================================

        status_code = 500
        error_code = "INTERNAL_SERVER_ERROR"

        # Log with full traceback
        logger.error(
            f"[{request_id}] Unhandled exception: {exc}",
            exc_info=True
        )

        # Environment-aware response
        if settings.is_production:
            # Production: Hide error details
            error_response = {
                "error": {
                    "code": error_code,
                    "message": "An internal server error occurred. Please try again later.",
                    "request_id": request_id,
                    "trace_id": trace_id,
                    "timestamp": timestamp,
                }
            }
        else:
            # Development: Show full error details
            error_response = {
                "error": {
                    "code": error_code,
                    "message": str(exc),
                    "request_id": request_id,
                    "trace_id": trace_id,
                    "timestamp": timestamp,
                    "details": {
                        "type": exc.__class__.__name__,
                        "traceback": traceback.format_exc(),
                    }
                }
            }

        return JSONResponse(
            status_code=status_code,
            content=error_response
        )

    def _get_error_code_from_status(self, status_code: int) -> str:
        """
        Get error code from HTTP status code.

        Args:
            status_code: HTTP status code

        Returns:
            Error code string
        """
        error_codes = {
            400: "BAD_REQUEST",
            401: "UNAUTHORIZED",
            403: "FORBIDDEN",
            404: "NOT_FOUND",
            405: "METHOD_NOT_ALLOWED",
            409: "CONFLICT",
            422: "VALIDATION_ERROR",
            429: "RATE_LIMIT_EXCEEDED",
            500: "INTERNAL_SERVER_ERROR",
            502: "BAD_GATEWAY",
            503: "SERVICE_UNAVAILABLE",
            504: "GATEWAY_TIMEOUT",
        }

        return error_codes.get(status_code, f"HTTP_{status_code}")

    def _get_default_message(self, status_code: int) -> str:
        """
        Get default error message for HTTP status code.

        Args:
            status_code: HTTP status code

        Returns:
            Default error message
        """
        messages = {
            400: "Bad request",
            401: "Authentication required",
            403: "Access forbidden",
            404: "Resource not found",
            405: "Method not allowed",
            409: "Resource conflict",
            422: "Validation error",
            429: "Rate limit exceeded",
            500: "Internal server error",
            502: "Bad gateway",
            503: "Service unavailable",
            504: "Gateway timeout",
        }

        return messages.get(status_code, f"HTTP {status_code}")


# =============================================================================
# EXCEPTION HANDLER FUNCTIONS (Alternative to middleware)
# =============================================================================

async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """
    Handle HTTPException.

    Alternative to middleware - can be registered with FastAPI:
        app.add_exception_handler(HTTPException, http_exception_handler)

    Args:
        request: HTTP request
        exc: HTTPException

    Returns:
        JSONResponse with error details
    """
    middleware = ErrorHandlerMiddleware(None)
    return await middleware.handle_exception(request, exc)


async def validation_exception_handler(
    request: Request,
    exc: RequestValidationError
) -> JSONResponse:
    """
    Handle RequestValidationError.

    Alternative to middleware - can be registered with FastAPI:
        app.add_exception_handler(RequestValidationError, validation_exception_handler)

    Args:
        request: HTTP request
        exc: RequestValidationError

    Returns:
        JSONResponse with validation error details
    """
    middleware = ErrorHandlerMiddleware(None)
    return await middleware.handle_exception(request, exc)


async def general_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Handle general exceptions.

    Alternative to middleware - can be registered with FastAPI:
        app.add_exception_handler(Exception, general_exception_handler)

    Args:
        request: HTTP request
        exc: Exception

    Returns:
        JSONResponse with error details
    """
    middleware = ErrorHandlerMiddleware(None)
    return await middleware.handle_exception(request, exc)
