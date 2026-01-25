#!/usr/bin/env python3
"""
Concrete Config Service Verification Script

Provides concrete evidence of config service state, not aspirational claims.
Actually retrieves and lists all registered secrets with their values (redacted).
"""

import asyncio
import httpx
import json
import sys
from typing import Dict, Any

CONFIG_SERVICE_URL = "http://localhost:8100"
INTERNAL_API_KEY = "AShhRzWhfXd6IomyzZnE3d-lCcAvT1L5GDCCZRSXZGsJq7_eAJGxeMi-4AlfTeOc"

async def verify_actual_config_state():
    """Verify actual config service state with concrete evidence"""
    
    print("🔍 CONCRETE CONFIG SERVICE VERIFICATION")
    print("=" * 50)
    print("This script provides ACTUAL evidence, not aspirational claims.")
    print()
    
    async with httpx.AsyncClient(timeout=30.0, headers={"X-Internal-API-Key": INTERNAL_API_KEY}) as client:
        
        # 1. Health check
        try:
            health_response = await client.get(f"{CONFIG_SERVICE_URL}/health")
            if health_response.status_code == 200:
                health = health_response.json()
                print(f"✅ Config Service Health: {health['status']} (encryption: {health.get('encryption_status')})")
            else:
                print(f"❌ Config Service Unhealthy: {health_response.status_code}")
                return 1
        except Exception as e:
            print(f"💥 Cannot reach config service: {e}")
            return 1
        
        # 2. List ALL secrets
        try:
            list_response = await client.get(f"{CONFIG_SERVICE_URL}/api/v1/secrets", params={"environment": "prod"})
            if list_response.status_code == 200:
                all_secrets = list_response.json()
                print(f"\n📋 FOUND {len(all_secrets)} TOTAL SECRETS IN CONFIG SERVICE")
                print("-" * 50)
                
                # Group by category
                infrastructure = []
                service_specific = []
                global_configs = []
                other = []
                
                for secret in all_secrets:
                    key = secret['secret_key']
                    if key in ['DATABASE_URL', 'REDIS_URL', 'INTERNAL_API_KEY']:
                        infrastructure.append(key)
                    elif key.startswith('INSTRUMENT_REGISTRY_'):
                        service_specific.append(key)
                    elif key in ['LOG_LEVEL', 'CORS_ORIGINS', 'REQUEST_TIMEOUT', 'SHUTDOWN_TIMEOUT', 'DEBUG']:
                        global_configs.append(key)
                    else:
                        other.append(key)
                
                print(f"🔧 INFRASTRUCTURE SECRETS ({len(infrastructure)}):")
                for key in infrastructure:
                    print(f"   ✅ {key}")
                
                print(f"\n⚙️  SERVICE-SPECIFIC CONFIGS ({len(service_specific)}):")
                for key in service_specific:
                    print(f"   ✅ {key}")
                
                print(f"\n🌐 GLOBAL CONFIGURATIONS ({len(global_configs)}):")
                for key in global_configs:
                    print(f"   ✅ {key}")
                
                print(f"\n📦 OTHER SECRETS ({len(other)}):")
                for key in other[:10]:  # Show first 10 only
                    print(f"   • {key}")
                if len(other) > 10:
                    print(f"   ... and {len(other) - 10} more")
                
            else:
                print(f"❌ Cannot list secrets: {list_response.status_code}")
                return 1
        except Exception as e:
            print(f"💥 Error listing secrets: {e}")
            return 1
        
        # 3. Test specific required configs for main.py
        print(f"\n🧪 TESTING ACTUAL CONFIG RETRIEVAL (as main.py would)")
        print("-" * 50)
        
        required_for_startup = ["DATABASE_URL", "REDIS_URL", "INTERNAL_API_KEY"]
        startup_configs = {}
        
        for key in required_for_startup:
            try:
                response = await client.get(
                    f"{CONFIG_SERVICE_URL}/api/v1/secrets/{key}/value",
                    params={"environment": "prod"}
                )
                if response.status_code == 200:
                    data = response.json()
                    value = data.get("secret_value") or data.get("value", "")
                    # Redact for security but show it exists
                    redacted = value[:10] + "..." + value[-5:] if len(value) > 15 else "***"
                    print(f"   ✅ {key} = {redacted}")
                    startup_configs[key] = True
                else:
                    print(f"   ❌ {key} NOT ACCESSIBLE ({response.status_code})")
                    startup_configs[key] = False
            except Exception as e:
                print(f"   💥 {key} ERROR: {e}")
                startup_configs[key] = False
        
        # 4. Summary and startup readiness assessment
        print(f"\n📊 STARTUP READINESS ASSESSMENT")
        print("-" * 50)
        
        can_start = all(startup_configs.values())
        total_required = len(required_for_startup)
        accessible_required = sum(startup_configs.values())
        
        print(f"Required for startup: {accessible_required}/{total_required}")
        print(f"Service can start: {'✅ YES' if can_start else '❌ NO'}")
        
        if can_start:
            print("\n✅ CONCRETE EVIDENCE: Config service ready for instrument registry startup")
            print("   All critical configs accessible via API")
            print("   main.py lifespan will succeed")
            return 0
        else:
            print(f"\n❌ CONCRETE EVIDENCE: Config service NOT ready")
            print("   main.py lifespan will fail with missing configs")
            missing = [k for k, v in startup_configs.items() if not v]
            print(f"   Missing: {missing}")
            return 1

if __name__ == "__main__":
    exit_code = asyncio.run(verify_actual_config_state())
    sys.exit(exit_code)