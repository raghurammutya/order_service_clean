"""
Circuit Breaker Pattern Implementation

Prevents cascading failures by stopping requests to a failing service temporarily.
"""
import logging
import time
from typing import Callable, Any, Optional
from enum import Enum
from dataclasses import dataclass
from threading import Lock

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Circuit tripped, blocking calls
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration"""
    failure_threshold: int = 5  # Failures before opening circuit
    recovery_timeout: int = 60  # Seconds before attempting recovery
    expected_exception: type = Exception  # Exception type to catch
    name: str = "default"  # Circuit breaker name for logging


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open"""
    pass


class CircuitBreaker:
    """
    Circuit breaker implementation to prevent cascading failures.

    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Circuit tripped, requests are rejected immediately
    - HALF_OPEN: Testing if service recovered, limited requests allowed

    Example:
        breaker = CircuitBreaker(
            failure_threshold=3,
            recovery_timeout=30,
            name="kite_api"
        )

        try:
            result = await breaker.call(some_async_function, arg1, arg2)
        except CircuitBreakerError:
            # Circuit is open, service is down
            logger.error("Service is unavailable")
    """

    def __init__(self, config: Optional[CircuitBreakerConfig] = None):
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.lock = Lock()

        logger.info(
            f"Circuit breaker '{self.config.name}' initialized: "
            f"threshold={self.config.failure_threshold}, "
            f"recovery_timeout={self.config.recovery_timeout}s"
        )

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt recovery"""
        if self.last_failure_time is None:
            return False

        return (time.time() - self.last_failure_time) >= self.config.recovery_timeout

    def _record_success(self):
        """Record successful call"""
        with self.lock:
            self.failure_count = 0

            if self.state == CircuitState.HALF_OPEN:
                logger.info(f"Circuit breaker '{self.config.name}' recovered, closing circuit")
                self.state = CircuitState.CLOSED

    def _record_failure(self):
        """Record failed call"""
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.config.failure_threshold:
                if self.state != CircuitState.OPEN:
                    logger.error(
                        f"Circuit breaker '{self.config.name}' OPENED after "
                        f"{self.failure_count} failures"
                    )
                    self.state = CircuitState.OPEN
            else:
                logger.warning(
                    f"Circuit breaker '{self.config.name}' failure count: "
                    f"{self.failure_count}/{self.config.failure_threshold}"
                )

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Call a function through the circuit breaker.

        Args:
            func: Async function to call
            *args: Positional arguments for func
            **kwargs: Keyword arguments for func

        Returns:
            Function result

        Raises:
            CircuitBreakerError: If circuit is open
            Exception: If function raises exception
        """
        with self.lock:
            # Check if circuit should transition from OPEN to HALF_OPEN
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    logger.info(
                        f"Circuit breaker '{self.config.name}' attempting recovery "
                        "(HALF_OPEN)"
                    )
                    self.state = CircuitState.HALF_OPEN
                else:
                    raise CircuitBreakerError(
                        f"Circuit breaker '{self.config.name}' is OPEN. "
                        f"Service unavailable."
                    )

        # Execute the function
        try:
            result = await func(*args, **kwargs)
            self._record_success()
            return result

        except self.config.expected_exception as e:
            self._record_failure()
            raise

    def get_state(self) -> dict:
        """
        Get current circuit breaker state.

        Returns:
            Dictionary with state information
        """
        with self.lock:
            return {
                "name": self.config.name,
                "state": self.state.value,
                "failure_count": self.failure_count,
                "failure_threshold": self.config.failure_threshold,
                "last_failure_time": self.last_failure_time,
                "recovery_timeout": self.config.recovery_timeout
            }

    def reset(self):
        """Manually reset circuit breaker to CLOSED state"""
        with self.lock:
            logger.info(f"Circuit breaker '{self.config.name}' manually reset")
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.last_failure_time = None


class RetryConfig:
    """Retry configuration for exponential backoff"""
    def __init__(
        self,
        max_attempts: int = 3,
        initial_delay: float = 1.0,
        max_delay: float = 10.0,
        exponential_base: float = 2.0,
        jitter: bool = True
    ):
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter


async def retry_with_backoff(
    func: Callable,
    config: RetryConfig,
    *args,
    **kwargs
) -> Any:
    """
    Retry an async function with exponential backoff.

    Args:
        func: Async function to retry
        config: Retry configuration
        *args: Positional arguments for func
        **kwargs: Keyword arguments for func

    Returns:
        Function result

    Raises:
        Exception: Last exception if all retries fail
    """
    import asyncio
    import random

    last_exception = None

    for attempt in range(config.max_attempts):
        try:
            return await func(*args, **kwargs)

        except Exception as e:
            last_exception = e

            if attempt == config.max_attempts - 1:
                # Last attempt, don't retry
                logger.error(
                    f"All {config.max_attempts} retry attempts failed for {func.__name__}"
                )
                raise

            # Calculate delay with exponential backoff
            delay = min(
                config.initial_delay * (config.exponential_base ** attempt),
                config.max_delay
            )

            # Add jitter to prevent thundering herd
            if config.jitter:
                delay = delay * (0.5 + random.random() * 0.5)

            logger.warning(
                f"Retry attempt {attempt + 1}/{config.max_attempts} failed for "
                f"{func.__name__}: {e}. Retrying in {delay:.2f}s"
            )

            await asyncio.sleep(delay)

    # Should never reach here, but just in case
    raise last_exception
