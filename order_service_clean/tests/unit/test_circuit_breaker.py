import asyncio
import time

import pytest

from order_service.app.services import circuit_breaker as module


@pytest.mark.asyncio
async def test_circuit_breaker_opens_then_recovers(monkeypatch):
    breaker = module.CircuitBreaker(
        module.CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0.1)
    )

    async def failing():
        raise RuntimeError("fail")

    # First failure opens breaker
    with pytest.raises(RuntimeError):
        await breaker.call(failing)
    assert breaker.state == module.CircuitState.OPEN

    # Before timeout, calls are blocked
    with pytest.raises(module.CircuitBreakerError):
        await breaker.call(asyncio.sleep, 0)

    # After timeout, breaker transitions to half-open and allows a success to close
    time.sleep(0.11)
    async def success():
        return "ok"

    result = await breaker.call(success)
    assert result == "ok"
    assert breaker.state == module.CircuitState.CLOSED


@pytest.mark.asyncio
async def test_retry_with_backoff_retries_then_raises(monkeypatch):
    attempts = {"count": 0}

    async def flaky():
        attempts["count"] += 1
        raise RuntimeError("boom")

    sleep_calls = []
    monkeypatch.setattr(asyncio, "sleep", lambda d: sleep_calls.append(d) or asyncio.Future())

    cfg = module.RetryConfig(max_attempts=3, initial_delay=0.1, max_delay=0.2, exponential_base=2, jitter=False)

    with pytest.raises(RuntimeError):
        await module.retry_with_backoff(flaky, cfg)

    assert attempts["count"] == 3
    # First two sleeps logged (third attempt fails and exits)
    assert sleep_calls == [0.1, 0.2]
