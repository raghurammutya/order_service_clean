"""Correlation ID Middleware

ARCHITECTURE COMPLIANCE:
- W3C Trace Context standard (traceparent header format)
- Request tracing across distributed services
- Correlation IDs in all logs and error responses

Based on best patterns from:
- api_gateway: W3C Trace Context implementation
- order_service: Request ID in error responses

This middleware adds trace context to all requests for distributed tracing.
"""
import uuid
import re
import logging
from typing import Optional, Tuple, Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

logger = logging.getLogger(__name__)

# W3C Traceparent format: version-trace_id-parent_id-flags
# Example: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
TRACEPARENT_REGEX = re.compile(
    r"^(?P<version>[0-9a-f]{2})-"
    r"(?P<trace_id>[0-9a-f]{32})-"
    r"(?P<parent_id>[0-9a-f]{16})-"
    r"(?P<flags>[0-9a-f]{2})$"
)


def generate_trace_id() -> str:
    """Generate 32-character trace ID (W3C format)."""
    return uuid.uuid4().hex


def generate_span_id() -> str:
    """Generate 16-character span ID (W3C format)."""
    return uuid.uuid4().hex[:16]


def generate_request_id() -> str:
    """Generate short request ID for logging (8 characters)."""
    return uuid.uuid4().hex[:8]


def parse_traceparent(value: str) -> Optional[Tuple[str, str]]:
    """
    Parse W3C traceparent header.

    Args:
        value: Traceparent header value

    Returns:
        Tuple of (trace_id, parent_span_id) or None if invalid
    """
    if not value:
        return None

    match = TRACEPARENT_REGEX.match(value.lower().strip())
    if match:
        return match.group("trace_id"), match.group("parent_id")

    return None


def format_traceparent(trace_id: str, span_id: str) -> str:
    """
    Format W3C traceparent header.

    Format: version-trace_id-span_id-flags
    - version: 00 (current version)
    - trace_id: 32 hex chars
    - span_id: 16 hex chars
    - flags: 01 (sampled)

    Args:
        trace_id: Trace ID (32 hex chars)
        span_id: Span ID (16 hex chars)

    Returns:
        Formatted traceparent header
    """
    return f"00-{trace_id}-{span_id}-01"


class CorrelationIDMiddleware(BaseHTTPMiddleware):
    """
    Correlation ID middleware for distributed tracing.

    Adds trace context to all requests:
    - X-Request-ID: Short request ID for logging (8 chars)
    - X-Trace-ID: Trace ID for distributed tracing (32 chars)
    - X-Span-ID: Span ID for this service (16 chars)
    - Traceparent: W3C Trace Context format

    Trace Context Propagation:
    1. Incoming request has traceparent header:
       - Extract trace_id and parent_span_id
       - Generate new span_id for this service
       - Store in request.state for downstream use

    2. Incoming request has X-Trace-ID header (legacy):
       - Use provided trace_id
       - Generate new span_id

    3. No trace context:
       - Generate new trace_id and span_id

    All IDs are added to:
    - request.state (for route handlers)
    - Response headers (for client)
    - Logs (via logging filter)
    """

    async def dispatch(self, request: Request, call_next: Callable):
        """
        Add correlation IDs to request and response.

        Args:
            request: Incoming HTTP request
            call_next: Next middleware/route handler

        Returns:
            Response with correlation ID headers
        """
        # =====================================================================
        # STEP 1: Extract or generate trace context
        # =====================================================================

        # Try W3C Traceparent header (preferred)
        traceparent = request.headers.get("traceparent") or request.headers.get("Traceparent")
        parsed = parse_traceparent(traceparent) if traceparent else None

        if parsed:
            # Use trace_id from upstream, generate new span_id
            trace_id, parent_span_id = parsed
            span_id = generate_span_id()
            logger.debug(f"Trace context from traceparent: trace={trace_id[:8]}..., parent={parent_span_id[:8]}...")
        else:
            # Try legacy X-Trace-ID header
            trace_id = request.headers.get("x-trace-id") or request.headers.get("X-Trace-ID")
            parent_span_id = None

            if trace_id:
                # Use provided trace_id, generate span_id
                span_id = generate_span_id()
                logger.debug(f"Trace context from X-Trace-ID: trace={trace_id[:8]}...")
            else:
                # No trace context - generate new
                trace_id = generate_trace_id()
                span_id = generate_span_id()
                parent_span_id = None
                logger.debug(f"New trace context: trace={trace_id[:8]}..., span={span_id[:8]}...")

        # Generate request ID (short, for logging)
        request_id = request.headers.get("x-request-id") or generate_request_id()

        # =====================================================================
        # STEP 2: Store in request.state
        # =====================================================================

        request.state.request_id = request_id
        request.state.trace_id = trace_id
        request.state.span_id = span_id
        request.state.parent_span_id = parent_span_id

        # =====================================================================
        # STEP 3: Process request
        # =====================================================================

        response = await call_next(request)

        # =====================================================================
        # STEP 4: Add correlation IDs to response headers
        # =====================================================================

        response.headers["X-Request-ID"] = request_id
        response.headers["X-Trace-ID"] = trace_id
        response.headers["X-Span-ID"] = span_id
        response.headers["Traceparent"] = format_traceparent(trace_id, span_id)

        if parent_span_id:
            response.headers["X-Parent-Span-ID"] = parent_span_id

        return response


# =============================================================================
# LOGGING FILTER (Add correlation IDs to log records)
# =============================================================================

class CorrelationIDFilter(logging.Filter):
    """
    Logging filter to add correlation IDs to log records.

    Extracts correlation IDs from contextvars and adds them to log records.
    This allows all logs to include request_id, trace_id, span_id.

    Usage:
        import logging
        logger = logging.getLogger(__name__)
        logger.addFilter(CorrelationIDFilter())

        # Logs will include correlation IDs
        logger.info("Processing order")
        # Output: [request_id=abc123] [trace_id=0af7651...] Processing order
    """

    def filter(self, record):
        """
        Add correlation IDs to log record.

        Args:
            record: Log record

        Returns:
            True (always pass)
        """
        # Try to get correlation IDs from contextvars
        # (set by CorrelationIDMiddleware or manually)
        try:
            from contextvars import ContextVar

            request_id_var: ContextVar[str] = ContextVar("request_id", default="-")
            trace_id_var: ContextVar[str] = ContextVar("trace_id", default="-")
            span_id_var: ContextVar[str] = ContextVar("span_id", default="-")

            record.request_id = request_id_var.get()
            record.trace_id = trace_id_var.get()[:8] if trace_id_var.get() != "-" else "-"
            record.span_id = span_id_var.get()[:8] if span_id_var.get() != "-" else "-"

        except Exception:
            # Fallback if contextvars not available
            record.request_id = "-"
            record.trace_id = "-"
            record.span_id = "-"

        return True


# =============================================================================
# CONTEXT VAR HELPERS
# =============================================================================

from contextvars import ContextVar

# Context variables for correlation IDs
request_id_var: ContextVar[Optional[str]] = ContextVar("request_id", default=None)
trace_id_var: ContextVar[Optional[str]] = ContextVar("trace_id", default=None)
span_id_var: ContextVar[Optional[str]] = ContextVar("span_id", default=None)


def set_correlation_ids(request_id: str, trace_id: str, span_id: str):
    """
    Set correlation IDs in context vars.

    Called by CorrelationIDMiddleware to make IDs available to logging filter.

    Args:
        request_id: Request ID
        trace_id: Trace ID
        span_id: Span ID
    """
    request_id_var.set(request_id)
    trace_id_var.set(trace_id)
    span_id_var.set(span_id)


def get_correlation_ids() -> dict:
    """
    Get current correlation IDs from context vars.

    Returns:
        Dict with request_id, trace_id, span_id
    """
    return {
        "request_id": request_id_var.get(),
        "trace_id": trace_id_var.get(),
        "span_id": span_id_var.get(),
    }


def clear_correlation_ids():
    """Clear correlation IDs from context vars."""
    request_id_var.set(None)
    trace_id_var.set(None)
    span_id_var.set(None)
