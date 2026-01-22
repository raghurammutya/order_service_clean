# Order Service

Order execution API handling placement, modification, cancellation, and account state with idempotency and reconciliation.

## Quick Start
```bash
pip install -r requirements.txt
AUTH_ENABLED=true PORT=8087 uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8087}
```

## Required Environment
- `DATABASE_URL` – Postgres/Timescale connection
- `REDIS_URL` – Redis for idempotency and caching
- `AUTH_ENABLED` – `true` to enforce JWT on all API routes
- `JWKS_URL`, `JWT_ISSUER`, `JWT_AUDIENCE` – JWT verification settings
- `USE_CONFIG_SERVICE` – `true` to pull secrets from config_service

## Key Endpoints (JWT unless noted)
- `GET /health` – health
- `GET /metrics` – Prometheus
- `POST /api/v1/orders` – place order
- `PUT /api/v1/orders/{id}` – modify
- `DELETE /api/v1/orders/{id}` – cancel
- `GET /api/v1/positions` – positions
- `GET /api/v1/trades` – trades
- `GET /api/v1/accounts` – account info
- `POST /api/v1/gtt` – GTT orders

## Notes
- Redis is fail-closed when `REDIS_REQUIRED=true` to prevent duplicate orders.
- Idempotency is enabled via Redis keys; keep TTL aligned with business requirements.
