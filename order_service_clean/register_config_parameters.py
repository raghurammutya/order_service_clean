#!/usr/bin/env python3
"""
Config Service Parameter Registration Script

Registers all required parameters for order service config-service compliance.
Run this script to populate the config service with necessary parameters.
"""
import os
import sys
import json
from typing import Dict, Any

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

try:
    from common.config_service.client import ConfigServiceClient
    from common.config_service.admin import ConfigServiceAdmin
except ImportError as e:
    print(f"WARNING: Config service modules not available: {e}")
    print("This script requires the config service client to be available")
    print("Run this script from the config service environment")
    sys.exit(1)


def register_order_service_parameters():
    """Register all order service parameters in config service"""
    
    # Initialize config service admin client
    admin = ConfigServiceAdmin(
        environment=os.getenv("ENVIRONMENT", "prod"),
        timeout=30
    )
    
    print("🔧 Registering Order Service Parameters in Config Service")
    print("=" * 60)
    
    # Order service specific configurations
    order_service_configs = {
        # Core Application
        "ORDER_SERVICE_PORT": {"value": 8087, "type": "int", "description": "Order service port"},
        
        # Database  
        "ORDER_SERVICE_DATABASE_POOL_SIZE": {"value": 20, "type": "int", "description": "Database connection pool size"},
        "ORDER_SERVICE_DATABASE_MAX_OVERFLOW": {"value": 10, "type": "int", "description": "Database pool max overflow"},
        
        # Redis
        "ORDER_SERVICE_REDIS_ORDER_TTL": {"value": 86400, "type": "int", "description": "Order TTL in Redis (seconds)"},
        "ORDER_SERVICE_REDIS_REQUIRED": {"value": True, "type": "bool", "description": "Redis required for production"},
        
        # Authentication
        "ORDER_SERVICE_AUTH_ENABLED": {"value": True, "type": "bool", "description": "Enable JWT authentication"},
        "ORDER_SERVICE_JWKS_URL": {"value": "", "type": "string", "description": "JWKS URL for JWT validation"},
        
        # Rate Limiting  
        "ORDER_SERVICE_RATE_LIMIT_ENABLED": {"value": True, "type": "bool", "description": "Enable rate limiting"},
        "ORDER_SERVICE_RATE_LIMIT_DEFAULT": {"value": "100/minute", "type": "string", "description": "Default rate limit"},
        "ORDER_SERVICE_RATE_LIMIT_ORDER_PLACEMENT": {"value": "10/minute", "type": "string", "description": "Order placement rate limit"},
        "ORDER_SERVICE_DAILY_ORDER_LIMIT": {"value": 100, "type": "int", "description": "Daily order limit per user"},
        "ORDER_SERVICE_DAILY_RESET_TIME": {"value": "09:15", "type": "string", "description": "Daily counter reset time"},
        "ORDER_SERVICE_HARD_REFRESH_RATE_LIMIT_SECONDS": {"value": 10, "type": "int", "description": "Hard refresh rate limit"},
        
        # CORS
        "ORDER_SERVICE_CORS_ENABLED": {"value": True, "type": "bool", "description": "Enable CORS"},
        "ORDER_SERVICE_CORS_ORIGINS": {"value": "https://app.stocksblitz.com,https://trading.stocksblitz.com", "type": "string", "description": "Allowed CORS origins"},
        
        # Broker Integration
        "ORDER_SERVICE_KITE_API_KEY": {"value": "", "type": "secret", "description": "Primary Kite API key"},
        "ORDER_SERVICE_KITE_ACCOUNT_ID": {"value": "primary", "type": "string", "description": "Primary Kite account ID"},
        "ORDER_SERVICE_KITE_PRIMARY_API_KEY": {"value": "", "type": "secret", "description": "Primary account Kite API key"},
        "ORDER_SERVICE_KITE_PERSONAL_API_KEY": {"value": "", "type": "secret", "description": "Personal account Kite API key"},
        
        # Order Execution
        "ORDER_SERVICE_MAX_ORDER_QUANTITY": {"value": 10000, "type": "int", "description": "Maximum order quantity"},
        "ORDER_SERVICE_MAX_ORDER_VALUE": {"value": 10000000.0, "type": "float", "description": "Maximum order value (INR)"},
        "ORDER_SERVICE_ENABLE_ORDER_VALIDATION": {"value": True, "type": "bool", "description": "Enable order validation"},
        "ORDER_SERVICE_ENABLE_RISK_CHECKS": {"value": True, "type": "bool", "description": "Enable risk checks"},
        "ORDER_SERVICE_RISK_MARGIN_MULTIPLIER": {"value": 1.25, "type": "float", "description": "Risk margin multiplier"},
        "ORDER_SERVICE_MAX_POSITION_EXPOSURE_VALUE": {"value": 10000000.0, "type": "float", "description": "Max position exposure"},
        "ORDER_SERVICE_MAX_POSITION_CONCENTRATION_PCT": {"value": 0.6, "type": "float", "description": "Max position concentration %"},
        "ORDER_SERVICE_DAILY_LOSS_LIMIT": {"value": -50000.0, "type": "float", "description": "Daily loss limit (negative)"},
        
        # Position Tracking
        "ORDER_SERVICE_ENABLE_POSITION_TRACKING": {"value": True, "type": "bool", "description": "Enable position tracking"},
        "ORDER_SERVICE_POSITION_SYNC_INTERVAL": {"value": 60, "type": "int", "description": "Position sync interval (seconds)"},
        
        # System
        "ORDER_SERVICE_SYSTEM_USER_ID": {"value": 1, "type": "int", "description": "System user ID for background workers"},
        "ORDER_SERVICE_METRICS_ENABLED": {"value": True, "type": "bool", "description": "Enable metrics collection"},
        "ORDER_SERVICE_LOG_LEVEL": {"value": "INFO", "type": "string", "description": "Logging level"},
        
        # Security
        "ORDER_SERVICE_IDEMPOTENCY_FAIL_CLOSED": {"value": True, "type": "bool", "description": "Idempotency fail-closed mode"},
        "ORDER_SERVICE_TEST_AUTH_MODE": {"value": False, "type": "bool", "description": "Test auth mode (dev only)"},
        "ORDER_SERVICE_GATEWAY_SECRET": {"value": "", "type": "secret", "description": "API gateway secret"},
        "ORDER_SERVICE_TRUST_GATEWAY_HEADERS": {"value": False, "type": "bool", "description": "Trust gateway headers"},
        
        # Production Hosts
        "ORDER_SERVICE_PRODUCTION_HOST_HTTP": {"value": "http://5.223.52.98", "type": "string", "description": "Production HTTP host"},
        "ORDER_SERVICE_PRODUCTION_DOMAIN_HTTPS": {"value": "https://app.stocksblitz.com", "type": "string", "description": "Production HTTPS domain"},
        "ORDER_SERVICE_TRADING_DOMAIN_HTTPS": {"value": "https://trading.stocksblitz.com", "type": "string", "description": "Trading HTTPS domain"},
    }
    
    # Port registry entries
    port_registry = {
        "order_service": 8087,
        "token_manager": 8088, 
        "ticker_service": 8089,
        "ticker_service_v2": 8089,
        "user_service": 8011,
        "calendar_service": 8013,
    }
    
    success_count = 0
    error_count = 0
    
    # Register service configurations
    print("📝 Registering service configurations...")
    for key, config in order_service_configs.items():
        try:
            if config["type"] == "secret":
                # Register as secret
                admin.create_secret(
                    key=key,
                    value=config["value"], 
                    description=config["description"]
                )
            else:
                # Register as regular config
                admin.create_config(
                    key=key,
                    value=config["value"],
                    value_type=config["type"],
                    description=config["description"]
                )
            print(f"✅ {key}")
            success_count += 1
        except Exception as e:
            print(f"❌ {key}: {e}")
            error_count += 1
    
    # Register port registry entries  
    print("\n🔌 Registering port registry entries...")
    for service_name, port in port_registry.items():
        try:
            admin.create_port_entry(
                service_name=service_name,
                environment="prod", 
                port=port,
                description=f"{service_name} service port"
            )
            print(f"✅ port_registry[{service_name}][prod] = {port}")
            success_count += 1
        except Exception as e:
            print(f"❌ {service_name}: {e}")
            error_count += 1
    
    print(f"\n📊 Registration Summary:")
    print(f"✅ Successful: {success_count}")
    print(f"❌ Failed: {error_count}")
    
    if error_count > 0:
        print(f"\n⚠️  {error_count} parameters failed to register")
        print("Check if parameters already exist or config service is unavailable")
        return False
    
    print("\n🎉 All parameters registered successfully!")
    return True


def verify_registration():
    """Verify all parameters were registered correctly"""
    print("\n🔍 Verifying parameter registration...")
    
    client = ConfigServiceClient(
        service_name="order_service",
        environment="prod",
        timeout=30
    )
    
    # Test a few key parameters
    test_params = [
        "ORDER_SERVICE_PORT",
        "ORDER_SERVICE_CORS_ORIGINS", 
        "ORDER_SERVICE_MAX_ORDER_VALUE"
    ]
    
    for param in test_params:
        try:
            value = client.get_config(param)
            print(f"✅ {param} = {value}")
        except Exception as e:
            print(f"❌ {param}: {e}")
            return False
    
    # Test port registry
    try:
        port = client.get_port("order_service")
        print(f"✅ port_registry[order_service] = {port}")
    except Exception as e:
        print(f"❌ port_registry[order_service]: {e}")
        return False
    
    print("✅ All test parameters verified!")
    return True


def main():
    """Main execution"""
    try:
        print("🚀 Order Service Config-Service Registration")
        print("=" * 50)
        
        # Check config service connectivity
        print("🔗 Testing config service connectivity...")
        client = ConfigServiceClient(service_name="test", environment="prod", timeout=10)
        if not client.health_check():
            print("❌ Config service health check failed")
            sys.exit(1)
        print("✅ Config service is healthy")
        
        # Register parameters
        if not register_order_service_parameters():
            sys.exit(1)
            
        # Verify registration
        if not verify_registration():
            sys.exit(1)
            
        print("\n🎉 Config service registration completed successfully!")
        print("📋 Next steps:")
        print("   1. Refactor Settings class to use config-service")
        print("   2. Update docker-compose.production.yml")
        print("   3. Run compliance tests")
        
    except KeyboardInterrupt:
        print("\n❌ Registration cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Registration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()