#!/usr/bin/env python3
"""
Bootstrap Script for Instrument Registry Service Configuration

Registers ALL required configuration parameters in config service as identified
in main.py:99-112 and config_client.py:66-135. Addresses critical review finding #1.

Usage:
    python bootstrap_instrument_registry_config.py
"""

import asyncio
import httpx
import json
import sys
from typing import Dict, Any, List

# Config service details
CONFIG_SERVICE_URL = "http://localhost:8100"
INTERNAL_API_KEY = "AShhRzWhfXd6IomyzZnE3d-lCcAvT1L5GDCCZRSXZGsJq7_eAJGxeMi-4AlfTeOc"
ENVIRONMENT = "prod"

class ConfigBootstrapper:
    """Bootstrap configuration parameters for instrument registry service"""
    
    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers={"X-Internal-API-Key": INTERNAL_API_KEY}
        )
        self.registered_count = 0
        self.skipped_count = 0
        self.failed_count = 0
    
    async def close(self):
        await self.client.aclose()
    
    async def register_secret(self, key: str, value: str, description: str, secret_type: str = None) -> bool:
        """Register a secret in config service"""
        try:
            # Check if secret already exists
            check_response = await self.client.get(
                f"{CONFIG_SERVICE_URL}/api/v1/secrets/{key}/value",
                params={"environment": ENVIRONMENT}
            )
            
            if check_response.status_code == 200:
                print(f"✓ Secret '{key}' already exists, skipping")
                self.skipped_count += 1
                return True
            
            # Create new secret
            payload = {
                "secret_key": key,
                "environment": ENVIRONMENT,
                "description": description,
                "secret_value": value
            }
            
            if secret_type:
                payload["secret_type"] = secret_type
            
            response = await self.client.post(
                f"{CONFIG_SERVICE_URL}/api/v1/secrets",
                json=payload
            )
            
            if response.status_code == 201:
                print(f"✓ Registered secret '{key}'")
                self.registered_count += 1
                return True
            else:
                print(f"✗ Failed to register '{key}': {response.status_code} - {response.text}")
                self.failed_count += 1
                return False
                
        except Exception as e:
            print(f"✗ Error registering '{key}': {e}")
            self.failed_count += 1
            return False
    
    async def bootstrap_all_configs(self) -> Dict[str, Any]:
        """Bootstrap all required configuration parameters"""
        print("🚀 Starting Instrument Registry Configuration Bootstrap...")
        print(f"📡 Config Service URL: {CONFIG_SERVICE_URL}")
        print(f"🌍 Environment: {ENVIRONMENT}")
        print()
        
        # 1. Infrastructure secrets (main.py:99-112) - CRITICAL for startup
        print("📦 Registering Infrastructure Secrets...")
        infrastructure_configs = [
            ("DATABASE_URL", 
             "postgresql://stocksblitz:b4Gr60lYlbZVZz0ZRTcnf_YRkjO0sluNcwwJ-7lAfn4@localhost:5432/stocksblitz_unified_prod",
             "Database connection URL for instrument registry service",
             "db_credential"),
            ("REDIS_URL", 
             "redis://localhost:6379/0", 
             "Redis connection URL for instrument registry caching",
             "db_credential"),
            ("INTERNAL_API_KEY", 
             "AShhRzWhfXd6IomyzZnE3d-lCcAvT1L5GDCCZRSXZGsJq7_eAJGxeMi-4AlfTeOc",
             "Internal API key for service-to-service communication",
             "api_key")
        ]
        
        for key, value, description, secret_type in infrastructure_configs:
            await self.register_secret(key, value, description, secret_type)
        
        print()
        
        # 2. Global configuration (config_client.py:66-69)
        print("🌐 Registering Global Configuration...")
        global_configs = [
            ("LOG_LEVEL", "INFO", "Application logging level", None),
            ("CORS_ORIGINS", "http://localhost:3000,http://localhost:8080,https://stocksblitz.in", "Allowed CORS origins", None),
            ("REQUEST_TIMEOUT", "30", "Default API request timeout in seconds", None),
            ("SHUTDOWN_TIMEOUT", "25", "Graceful shutdown timeout in seconds", None),
            ("DEBUG", "false", "Debug mode flag", None)
        ]
        
        for key, value, description, secret_type in global_configs:
            await self.register_secret(key, value, description, secret_type)
        
        print()
        
        # 3. Service-specific configuration (config_client.py:90-99)
        print("🔧 Registering Service-Specific Configuration...")
        service_configs = [
            ("INSTRUMENT_REGISTRY_PORT", "8086", "Instrument registry service port", None),
            ("INSTRUMENT_REGISTRY_CACHE_TTL_SECONDS", "300", "Cache TTL in seconds (5 minutes)", None),
            ("INSTRUMENT_REGISTRY_HEALTH_CRITICAL_MINUTES", "60", "Critical health threshold in minutes", None),
            ("INSTRUMENT_REGISTRY_HEALTH_WARNING_MINUTES", "15", "Warning health threshold in minutes", None),
            ("INSTRUMENT_REGISTRY_HEALTH_HEALTHY_MINUTES", "5", "Healthy threshold in minutes", None),
            ("INSTRUMENT_REGISTRY_INGESTION_QUEUE_NAME", "instrument_ingestion", "Message queue name for data ingestion", None),
            ("INSTRUMENT_REGISTRY_INGESTION_BATCH_SIZE", "1000", "Batch size for ingestion operations", None),
            ("INSTRUMENT_REGISTRY_INGESTION_WORKER_COUNT", "3", "Number of ingestion worker processes", None)
        ]
        
        for key, value, description, secret_type in service_configs:
            await self.register_secret(key, value, description, secret_type)
        
        print()
        
        # 4. Additional operational parameters
        print("⚙️  Registering Operational Parameters...")
        operational_configs = [
            ("INSTRUMENT_REGISTRY_DATABASE_SCHEMA", "instrument_registry", "Database schema name for instrument tables", None),
            ("INSTRUMENT_REGISTRY_EVENT_RETENTION_DAYS", "90", "Event store data retention period in days", None),
            ("INSTRUMENT_REGISTRY_AUDIT_RETENTION_DAYS", "365", "Audit trail data retention period in days", None),
            ("INSTRUMENT_REGISTRY_METRICS_PORT", "9086", "Prometheus metrics port", None),
            ("INSTRUMENT_REGISTRY_MAX_CONNECTIONS", "100", "Maximum database connections", None),
            ("INSTRUMENT_REGISTRY_CONNECTION_TIMEOUT", "30", "Database connection timeout in seconds", None)
        ]
        
        for key, value, description, secret_type in operational_configs:
            await self.register_secret(key, value, description, secret_type)
        
        # Summary
        print()
        print("📊 Bootstrap Summary:")
        print(f"   ✓ Registered: {self.registered_count}")
        print(f"   ⏭️  Skipped: {self.skipped_count}")
        print(f"   ✗ Failed: {self.failed_count}")
        
        return {
            "registered": self.registered_count,
            "skipped": self.skipped_count,
            "failed": self.failed_count,
            "total_attempted": self.registered_count + self.skipped_count + self.failed_count
        }
    
    async def verify_config_health(self) -> Dict[str, Any]:
        """Verify config service is accessible and all required configs exist"""
        print("\n🔍 Verifying Configuration Health...")
        
        try:
            # Check config service health
            health_response = await self.client.get(f"{CONFIG_SERVICE_URL}/health")
            if health_response.status_code != 200:
                return {"status": "error", "message": "Config service not healthy"}
            
            health_data = health_response.json()
            print(f"✓ Config service health: {health_data.get('status', 'unknown')}")
            
            # Verify critical infrastructure configs exist
            critical_configs = ["DATABASE_URL", "REDIS_URL", "INTERNAL_API_KEY"]
            missing_configs = []
            
            for config_key in critical_configs:
                try:
                    response = await self.client.get(
                        f"{CONFIG_SERVICE_URL}/api/v1/secrets/{config_key}/value",
                        params={"environment": ENVIRONMENT}
                    )
                    if response.status_code == 200:
                        print(f"✓ Critical config '{config_key}' exists and accessible")
                    else:
                        missing_configs.append(config_key)
                        print(f"✗ Critical config '{config_key}' missing or inaccessible")
                except Exception as e:
                    missing_configs.append(config_key)
                    print(f"✗ Error checking '{config_key}': {e}")
            
            if missing_configs:
                return {
                    "status": "error", 
                    "message": f"Missing critical configs: {missing_configs}"
                }
            
            return {
                "status": "healthy", 
                "message": "All critical configurations verified",
                "config_service_health": health_data
            }
            
        except Exception as e:
            return {"status": "error", "message": f"Verification failed: {e}"}

async def main():
    """Main bootstrap execution"""
    bootstrapper = ConfigBootstrapper()
    
    try:
        # Verify config service is accessible first
        health_check = await bootstrapper.verify_config_health()
        if health_check["status"] == "error":
            print(f"❌ Pre-bootstrap health check failed: {health_check['message']}")
            # Continue anyway to register missing configs
        
        # Bootstrap all configurations
        result = await bootstrapper.bootstrap_all_configs()
        
        # Post-bootstrap verification
        print("\n🔍 Post-Bootstrap Verification...")
        verification = await bootstrapper.verify_config_health()
        
        print(f"\n🎯 Final Status: {verification['status'].upper()}")
        print(f"   Message: {verification['message']}")
        
        if verification["status"] == "healthy":
            print("\n✅ INSTRUMENT REGISTRY CONFIG BOOTSTRAP SUCCESSFUL")
            print("   Service is ready to start with all required configuration parameters.")
            return 0
        else:
            print("\n❌ BOOTSTRAP COMPLETED WITH ISSUES")
            print("   Some configurations may be missing. Check service startup logs.")
            return 1
            
    except Exception as e:
        print(f"\n💥 Bootstrap failed with exception: {e}")
        return 1
    finally:
        await bootstrapper.close()

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)