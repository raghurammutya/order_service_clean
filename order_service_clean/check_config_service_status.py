#!/usr/bin/env python3
"""
Check Config Service Parameter Status

Verifies which parameters already exist in the config service
and which ones need to be registered for order service compliance.
"""
import os

def check_config_service_status():
    """Check config service parameter status"""
    print("🔍 Config Service Parameter Status Check")
    print("=" * 50)
    
    # Set production environment
    os.environ['ENVIRONMENT'] = 'production'
    
    # Parameters we need for order service compliance
    required_params = {
        # Shared infrastructure (should already exist)
        "shared": [
            "DATABASE_URL",
            "REDIS_URL", 
            "CACHE_ENCRYPTION_KEY",
            "INTERNAL_API_KEY",
            "INTERNAL_SERVICE_SECRET",
            "JWT_SIGNING_KEY_ID",
            "JWT_ISSUER", 
            "JWT_AUDIENCE"
        ],
        
        # Order service specific (need to be registered)
        "order_service": [
            "ORDER_SERVICE_PORT",
            "ORDER_SERVICE_DATABASE_POOL_SIZE",
            "ORDER_SERVICE_DATABASE_MAX_OVERFLOW",
            "ORDER_SERVICE_REDIS_ORDER_TTL",
            "ORDER_SERVICE_REDIS_REQUIRED",
            "ORDER_SERVICE_AUTH_ENABLED",
            "ORDER_SERVICE_JWKS_URL",
            "ORDER_SERVICE_RATE_LIMIT_ENABLED",
            "ORDER_SERVICE_RATE_LIMIT_DEFAULT",
            "ORDER_SERVICE_RATE_LIMIT_ORDER_PLACEMENT",
            "ORDER_SERVICE_CORS_ENABLED",
            "ORDER_SERVICE_CORS_ORIGINS",
            "ORDER_SERVICE_KITE_API_KEY",
            "ORDER_SERVICE_KITE_ACCOUNT_ID",
            "ORDER_SERVICE_KITE_PRIMARY_API_KEY",
            "ORDER_SERVICE_KITE_PERSONAL_API_KEY",
            "ORDER_SERVICE_MAX_ORDER_QUANTITY",
            "ORDER_SERVICE_MAX_ORDER_VALUE",
            "ORDER_SERVICE_ENABLE_ORDER_VALIDATION",
            "ORDER_SERVICE_ENABLE_RISK_CHECKS",
            "ORDER_SERVICE_RISK_MARGIN_MULTIPLIER",
            "ORDER_SERVICE_MAX_POSITION_EXPOSURE_VALUE",
            "ORDER_SERVICE_MAX_POSITION_CONCENTRATION_PCT",
            "ORDER_SERVICE_DAILY_LOSS_LIMIT",
            "ORDER_SERVICE_ENABLE_POSITION_TRACKING",
            "ORDER_SERVICE_POSITION_SYNC_INTERVAL",
            "ORDER_SERVICE_SYSTEM_USER_ID",
            "ORDER_SERVICE_METRICS_ENABLED",
            "ORDER_SERVICE_LOG_LEVEL",
            "ORDER_SERVICE_DAILY_ORDER_LIMIT",
            "ORDER_SERVICE_DAILY_RESET_TIME",
            "ORDER_SERVICE_HARD_REFRESH_RATE_LIMIT_SECONDS",
            "ORDER_SERVICE_IDEMPOTENCY_FAIL_CLOSED",
            "ORDER_SERVICE_TEST_AUTH_MODE",
            "ORDER_SERVICE_GATEWAY_SECRET",
            "ORDER_SERVICE_TRUST_GATEWAY_HEADERS",
            "ORDER_SERVICE_PRODUCTION_HOST_HTTP",
            "ORDER_SERVICE_PRODUCTION_DOMAIN_HTTPS",
            "ORDER_SERVICE_TRADING_DOMAIN_HTTPS"
        ],
        
        # Port registry entries
        "ports": [
            "order_service",
            "token_manager",
            "ticker_service",
            "ticker_service_v2", 
            "user_service",
            "calendar_service"
        ]
    }
    
    try:
        # Try to import config service client
        try:
            from common.config_service.client import ConfigServiceClient
            
            client = ConfigServiceClient(
                service_name="order_service",
                environment="production", 
                timeout=10
            )
            
            print("🔗 Testing config service connectivity...")
            if client.health_check():
                print("✅ Config service is healthy")
                
                # Check existing parameters
                print("\\n📋 Checking existing parameters...")
                
                total_existing = 0
                total_missing = 0
                
                for category, params in required_params.items():
                    if category == "ports":
                        print(f"\\n🔌 Port Registry ({len(params)} services):")
                        for service in params:
                            try:
                                port = client.get_port(service)
                                print(f"  ✅ {service}: {port}")
                                total_existing += 1
                            except Exception as e:
                                print(f"  ❌ {service}: missing")
                                total_missing += 1
                    else:
                        print(f"\\n🔧 {category.title()} Parameters ({len(params)} items):")
                        for param in params:
                            try:
                                # Check if it's a secret or config
                                if "API_KEY" in param or "SECRET" in param or "URL" in param and param in ["DATABASE_URL", "REDIS_URL"]:
                                    value = client.get_secret(param)
                                    if value:
                                        print(f"  ✅ {param}: [SECRET]")
                                        total_existing += 1
                                    else:
                                        print(f"  ❌ {param}: empty/missing")
                                        total_missing += 1
                                else:
                                    value = client.get_config(param)
                                    print(f"  ✅ {param}: {value}")
                                    total_existing += 1
                            except Exception as e:
                                print(f"  ❌ {param}: missing")
                                total_missing += 1
                
                print(f"\\n📊 Summary:")
                print(f"✅ Existing: {total_existing}")
                print(f"❌ Missing: {total_missing}")
                print(f"📈 Coverage: {(total_existing / (total_existing + total_missing) * 100):.1f}%")
                
                if total_missing > 0:
                    print("\\n🔧 Action Required:")
                    print("1. Use config service admin interface to register missing parameters")
                    print("2. Use the parameter list above as reference")
                    print("3. Set appropriate values for production environment")
                    print("4. Re-run this script to verify registration")
                else:
                    print("\\n🎉 All parameters exist! Ready for config-service compliance migration.")
                
            else:
                print("❌ Config service health check failed")
                
        except ImportError:
            print("❌ Config service client not available")
            print("💡 This is expected if running outside config service environment")
            print("\\n📋 Manual Parameter Registration Required:")
            print("\\nUse config service admin CLI to register these parameters:")
            print("\\n🔧 Shared Infrastructure (may already exist):")
            for param in required_params["shared"]:
                print(f"  - {param}")
            print("\\n🔧 Order Service Specific:")
            for param in required_params["order_service"]:
                print(f"  - {param}")
            print("\\n🔌 Port Registry:")
            for service in required_params["ports"]:
                print(f"  - port_registry[{service}][production]")
                
    except Exception as e:
        print(f"❌ Error checking config service: {e}")
        return False
    
    return True


if __name__ == "__main__":
    check_config_service_status()