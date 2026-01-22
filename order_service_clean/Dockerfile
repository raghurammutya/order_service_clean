# Order Execution Service Dockerfile - With Config Client Support
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user (Security: Non-root container)
RUN groupadd -r orderservice && \
    useradd -r -g orderservice -u 1000 -s /bin/bash -m orderservice

# Set working directory
WORKDIR /app

# Copy requirements (context is project root)
COPY order_service/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code (context is project root)
COPY order_service/app/ ./app/

# Copy config service client
COPY common/ ./common/

# Set ownership to non-root user
RUN chown -R orderservice:orderservice /app

# Switch to non-root user
USER orderservice

# Set default port via ARG (can be overridden at build time)
ARG PORT=8087
ENV PORT=${PORT}

# Expose port
EXPOSE $PORT

# Health check using environment variable
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8087}/health || exit 1

# Run application using environment variable
CMD ["sh", "-c", "python -m uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8087}"]
