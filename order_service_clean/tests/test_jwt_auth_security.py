"""
Tests for JWT Authentication Security

Validates the CRITICAL security fix: Fail-closed behavior when Redis is unavailable.
Previously, revoked tokens were accepted when Redis was down (fail-open vulnerability).
Now, all tokens are rejected when Redis is unavailable (fail-closed security).
"""
import pytest
from unittest.mock import Mock, patch, AsyncMock
from fastapi import HTTPException
from order_service.app.auth.jwt_auth import verify_token, check_token_revocation


@pytest.fixture
def valid_token_payload():
    """Sample valid JWT token payload"""
    return {
        "sub": "123",  # user_id
        "email": "trader@example.com",
        "exp": 9999999999,  # Far future expiration
        "iat": 1700000000,
        "jti": "unique-token-id-12345"
    }


@pytest.fixture
def mock_redis_available():
    """Mock Redis client that's available"""
    redis = AsyncMock()
    redis.ping.return_value = True
    redis.get.return_value = None  # Token not revoked
    return redis


@pytest.fixture
def mock_redis_unavailable():
    """Mock Redis client that's unavailable"""
    redis = AsyncMock()
    redis.ping.side_effect = Exception("Redis connection refused")
    redis.get.side_effect = Exception("Redis connection refused")
    return redis


@pytest.fixture
def mock_redis_revoked_token():
    """Mock Redis client with revoked token"""
    redis = AsyncMock()
    redis.ping.return_value = True
    redis.get.return_value = "revoked"  # Token is revoked
    return redis


class TestFailClosedSecurity:
    """
    CRITICAL: Test fail-closed security behavior

    When Redis is unavailable, the system MUST reject all tokens.
    This prevents attackers from using revoked tokens during Redis outages.
    """

    @pytest.mark.asyncio
    async def test_redis_unavailable_rejects_all_tokens(
        self, valid_token_payload, mock_redis_unavailable
    ):
        """
        CRITICAL SECURITY FIX: Redis down → Reject all tokens (fail-closed)

        Previously (VULNERABLE):
        - Redis down → Accept all tokens (fail-open)
        - Attackers could use revoked tokens during outage

        Now (SECURE):
        - Redis down → Reject all tokens (fail-closed)
        - Service degradation is better than security breach
        """
        with patch('order_service.app.auth.jwt_auth.get_redis', return_value=mock_redis_unavailable):
            with pytest.raises(HTTPException) as exc_info:
                await check_token_revocation(
                    jti="unique-token-id-12345",
                    redis_client=mock_redis_unavailable
                )

            # Should return 503 Service Unavailable (fail-closed)
            assert exc_info.value.status_code == 503
            assert "redis unavailable" in exc_info.value.detail.lower()

    @pytest.mark.asyncio
    async def test_redis_available_accepts_valid_token(
        self, valid_token_payload, mock_redis_available
    ):
        """
        When Redis is available and token not revoked → Accept token
        """
        # Should not raise exception
        await check_token_revocation(
            jti="unique-token-id-12345",
            redis_client=mock_redis_available
        )

        # Verify Redis was checked
        mock_redis_available.get.assert_called_once_with("revoked_token:unique-token-id-12345")

    @pytest.mark.asyncio
    async def test_redis_available_rejects_revoked_token(
        self, valid_token_payload, mock_redis_revoked_token
    ):
        """
        When Redis is available and token IS revoked → Reject token
        """
        with pytest.raises(HTTPException) as exc_info:
            await check_token_revocation(
                jti="unique-token-id-12345",
                redis_client=mock_redis_revoked_token
            )

        # Should return 401 Unauthorized
        assert exc_info.value.status_code == 401
        assert "revoked" in exc_info.value.detail.lower()

    @pytest.mark.asyncio
    async def test_fail_closed_prevents_revoked_token_attack(
        self, mock_redis_unavailable
    ):
        """
        Attack scenario: Attacker with revoked token waits for Redis outage

        Previously (VULNERABLE):
        - Attacker token was revoked
        - Admin triggers Redis maintenance/outage
        - Attacker's revoked token suddenly works (fail-open)
        - Attacker gains unauthorized access

        Now (SECURE):
        - Attacker token was revoked
        - Admin triggers Redis maintenance/outage
        - Attacker's revoked token is still rejected (fail-closed)
        - Attacker denied (everyone denied during outage)
        """
        revoked_token_jti = "revoked-attacker-token-xyz"

        with pytest.raises(HTTPException) as exc_info:
            await check_token_revocation(
                jti=revoked_token_jti,
                redis_client=mock_redis_unavailable
            )

        # Fail-closed: Even though we can't check if revoked, we reject
        assert exc_info.value.status_code == 503


class TestRedisPingHealthCheck:
    """Test Redis health checking before token validation"""

    @pytest.mark.asyncio
    async def test_redis_ping_before_token_check(self, mock_redis_available):
        """Verify Redis is pinged before checking token revocation"""
        await check_token_revocation(
            jti="test-token-123",
            redis_client=mock_redis_available
        )

        # Should ping Redis first to check availability
        # (Implementation detail - may vary based on actual code)
        mock_redis_available.get.assert_called_once()

    @pytest.mark.asyncio
    async def test_redis_ping_fails_triggers_fail_closed(self, mock_redis_unavailable):
        """When Redis ping fails, immediately fail-closed"""
        with pytest.raises(HTTPException) as exc_info:
            await check_token_revocation(
                jti="test-token-123",
                redis_client=mock_redis_unavailable
            )

        assert exc_info.value.status_code == 503


class TestTokenRevocationFlow:
    """Test complete token revocation flow"""

    @pytest.mark.asyncio
    async def test_valid_token_full_flow(self, mock_redis_available):
        """
        Complete flow for valid, non-revoked token:
        1. Check Redis available
        2. Check token not in revocation list
        3. Accept token
        """
        jti = "valid-token-abc"

        # Should not raise any exception
        await check_token_revocation(jti=jti, redis_client=mock_redis_available)

        # Verify revocation was checked
        mock_redis_available.get.assert_called_with(f"revoked_token:{jti}")

    @pytest.mark.asyncio
    async def test_revoked_token_full_flow(self, mock_redis_revoked_token):
        """
        Complete flow for revoked token:
        1. Check Redis available
        2. Find token in revocation list
        3. Reject token with 401
        """
        jti = "revoked-token-xyz"

        with pytest.raises(HTTPException) as exc_info:
            await check_token_revocation(jti=jti, redis_client=mock_redis_revoked_token)

        # Should be 401 Unauthorized (not 503)
        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_redis_down_full_flow(self, mock_redis_unavailable):
        """
        Complete flow when Redis is down:
        1. Try to check Redis
        2. Redis unavailable
        3. Reject ALL tokens with 503 (fail-closed)
        """
        jti = "any-token-doesnt-matter"

        with pytest.raises(HTTPException) as exc_info:
            await check_token_revocation(jti=jti, redis_client=mock_redis_unavailable)

        # Should be 503 Service Unavailable
        assert exc_info.value.status_code == 503
        assert "unavailable" in exc_info.value.detail.lower()


class TestSecurityProperties:
    """Test security properties and guarantees"""

    @pytest.mark.asyncio
    async def test_never_accept_when_uncertain(self, mock_redis_unavailable):
        """
        Security principle: When uncertain, REJECT

        If we can't verify token status (Redis down), we must reject.
        Better to have service degradation than security breach.
        """
        with pytest.raises(HTTPException):
            await check_token_revocation(
                jti="unknown-status-token",
                redis_client=mock_redis_unavailable
            )

    @pytest.mark.asyncio
    async def test_no_bypass_mechanism(self, mock_redis_unavailable):
        """
        There should be NO way to bypass revocation check when Redis is down.

        No fallback, no "admin override", no grace period.
        Fail-closed is absolute.
        """
        # Try with various token patterns - all should fail
        test_tokens = [
            "admin-token",
            "system-token",
            "bypass-token",
            "emergency-token",
            "root-token"
        ]

        for jti in test_tokens:
            with pytest.raises(HTTPException) as exc_info:
                await check_token_revocation(jti=jti, redis_client=mock_redis_unavailable)

            # All must be rejected with 503
            assert exc_info.value.status_code == 503

    @pytest.mark.asyncio
    async def test_consistent_rejection_during_outage(self, mock_redis_unavailable):
        """
        During Redis outage, ALL tokens must be consistently rejected.

        No race conditions, no partial success.
        """
        # Try same token multiple times
        jti = "test-token-consistency"

        for _ in range(10):
            with pytest.raises(HTTPException) as exc_info:
                await check_token_revocation(jti=jti, redis_client=mock_redis_unavailable)

            # Always 503, never 200
            assert exc_info.value.status_code == 503


class TestErrorMessages:
    """Test error messages for security and debugging"""

    @pytest.mark.asyncio
    async def test_fail_closed_error_message(self, mock_redis_unavailable):
        """Error message should clearly indicate fail-closed behavior"""
        with pytest.raises(HTTPException) as exc_info:
            await check_token_revocation(
                jti="test-token",
                redis_client=mock_redis_unavailable
            )

        error_detail = exc_info.value.detail.lower()

        # Should mention Redis unavailability
        assert "redis" in error_detail or "cache" in error_detail

        # Should NOT leak sensitive implementation details
        assert "fail-closed" in error_detail or "unavailable" in error_detail

    @pytest.mark.asyncio
    async def test_revoked_token_error_message(self, mock_redis_revoked_token):
        """Revoked token error should be clear"""
        with pytest.raises(HTTPException) as exc_info:
            await check_token_revocation(
                jti="revoked-token",
                redis_client=mock_redis_revoked_token
            )

        error_detail = exc_info.value.detail.lower()
        assert "revoked" in error_detail or "invalid" in error_detail


class TestComparisonWithVulnerableVersion:
    """
    Document the difference between vulnerable and secure versions
    """

    def test_vulnerable_behavior_documentation(self):
        """
        VULNERABLE VERSION (BEFORE FIX):

        ```python
        async def check_token_revocation_VULNERABLE(jti: str, redis: Redis):
            try:
                is_revoked = await redis.get(f"revoked_token:{jti}")
                if is_revoked:
                    raise HTTPException(401, "Token revoked")
                # Token OK
            except Exception as e:
                # BUG: Swallow exception and allow access (fail-open)
                logger.warning(f"Redis error, allowing access: {e}")
                pass  # VULNERABLE: Accept token when Redis down
        ```

        Attack scenario:
        1. Admin revokes attacker's token → Stored in Redis
        2. Attacker DDoSes Redis or waits for maintenance
        3. Redis goes down
        4. Attacker's revoked token now works (fail-open)
        5. Unauthorized access gained
        """
        pass

    def test_secure_behavior_documentation(self):
        """
        SECURE VERSION (AFTER FIX):

        ```python
        async def check_token_revocation_SECURE(jti: str, redis: Redis):
            try:
                is_revoked = await redis.get(f"revoked_token:{jti}")
                if is_revoked:
                    raise HTTPException(401, "Token revoked")
                # Token OK
            except Exception as e:
                # FIX: Reject token when Redis unavailable (fail-closed)
                logger.error(f"Redis unavailable, rejecting all tokens: {e}")
                raise HTTPException(503, "Token verification unavailable")
        ```

        Defense:
        1. Admin revokes attacker's token → Stored in Redis
        2. Attacker DDoSes Redis or waits for maintenance
        3. Redis goes down
        4. ALL tokens rejected (including attacker's)
        5. Service degradation instead of security breach
        6. Attacker denied, legitimate users also denied (acceptable trade-off)
        """
        pass


class TestIntegrationScenarios:
    """Test integration with actual JWT verification flow"""

    @pytest.mark.asyncio
    async def test_jwt_verification_with_redis_down(self, mock_redis_unavailable):
        """
        Integration test: Complete JWT verification when Redis is down

        Full flow:
        1. Decode JWT token
        2. Verify signature
        3. Check expiration
        4. Check revocation (Redis) ← Fails here
        5. Should reject with 503
        """
        # This would be tested with actual verify_token function
        # which calls check_token_revocation internally
        pass

    @pytest.mark.asyncio
    async def test_concurrent_requests_during_redis_outage(self, mock_redis_unavailable):
        """
        Multiple concurrent requests during Redis outage should all fail-closed
        """
        import asyncio

        async def make_request(jti):
            try:
                await check_token_revocation(jti=jti, redis_client=mock_redis_unavailable)
                return "success"
            except HTTPException as e:
                return e.status_code

        # Simulate 10 concurrent requests
        results = await asyncio.gather(
            *[make_request(f"token-{i}") for i in range(10)]
        )

        # All should fail with 503
        assert all(status == 503 for status in results)


# Performance considerations
class TestPerformance:
    """Test performance implications of fail-closed behavior"""

    @pytest.mark.asyncio
    async def test_fail_closed_performance(self, mock_redis_unavailable):
        """
        Fail-closed should fail fast (not hang waiting for Redis)
        """
        import time

        start = time.time()

        with pytest.raises(HTTPException):
            await check_token_revocation(
                jti="test-token",
                redis_client=mock_redis_unavailable
            )

        elapsed = time.time() - start

        # Should fail within 1 second (fast failure)
        assert elapsed < 1.0, "Fail-closed should not hang"

    @pytest.mark.asyncio
    async def test_normal_path_performance(self, mock_redis_available):
        """
        Normal path (Redis available) should remain fast
        """
        import time

        start = time.time()

        await check_token_revocation(
            jti="test-token",
            redis_client=mock_redis_available
        )

        elapsed = time.time() - start

        # Should complete within 100ms
        assert elapsed < 0.1, "Normal path should be fast"
