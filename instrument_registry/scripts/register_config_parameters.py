#!/usr/bin/env python3
"""
Register instrument registry configuration parameters with config service
Follows existing patterns and avoids duplicates
"""

import os
import sys
import asyncio
import asyncpg
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ConfigServiceRegistration:
    """Handles registration of instrument registry configuration"""
    
    def __init__(self, database_url: str):
        """
        Initialize with config service database URL
        
        Args:
            database_url: PostgreSQL connection string for config service DB
        """
        self.database_url = database_url
        self.service_name = "instrument_registry"  # New production name
        self.old_service_name = "instrument_registry_experiments"  # Legacy name
        
    async def analyze_existing_config(self) -> Dict[str, Any]:
        """Analyze existing configuration to avoid duplicates"""
        conn = await asyncpg.connect(self.database_url)
        
        try:
            # Check for existing INSTRUMENT_REGISTRY_* keys
            existing_configs = await conn.fetch("""
                SELECT config_key, config_value, service_name, environment
                FROM service_configs 
                WHERE config_key LIKE 'INSTRUMENT_REGISTRY%'
                OR service_name IN ($1, $2)
                ORDER BY config_key
            """, self.service_name, self.old_service_name)
            
            # Check for existing secrets
            existing_secrets = await conn.fetch("""
                SELECT secret_key, environment, description
                FROM secrets
                WHERE secret_key LIKE 'INSTRUMENT_REGISTRY%'
                OR secret_key IN ('KITE_API_KEY', 'TOKEN_MANAGER_INTERNAL_API_KEY')
            """)
            
            # Check port allocations
            existing_ports = await conn.fetch("""
                SELECT service_name, environment, port, description
                FROM port_registry
                WHERE service_name LIKE '%instrument%'
                OR port IN (8084, 8085, 8086, 8087)
            """)
            
            # Check existing service URLs (reusable patterns)
            service_urls = await conn.fetch("""
                SELECT config_key, config_value
                FROM service_configs
                WHERE config_key LIKE '%_SERVICE_URL'
                OR config_key LIKE '%_URL'
                AND config_key IN (
                    'MESSAGE_SERVICE_URL', 'CALENDAR_SERVICE_URL', 
                    'ALERT_SERVICE_URL', 'COMMS_SERVICE_URL',
                    'TOKEN_MANAGER_URL', 'CONFIG_SERVICE_URL'
                )
                AND environment = 'prod'
            """)
            
            return {
                "existing_configs": existing_configs,
                "existing_secrets": existing_secrets,
                "existing_ports": existing_ports,
                "service_urls": service_urls
            }
            
        finally:
            await conn.close()
    
    async def find_available_port(self) -> int:
        """Find next available port for instrument registry"""
        conn = await asyncpg.connect(self.database_url)
        
        try:
            # Get all allocated ports
            allocated_ports = await conn.fetch("""
                SELECT port FROM port_registry
                WHERE environment = 'prod'
                ORDER BY port
            """)
            
            used_ports = {row['port'] for row in allocated_ports}
            
            # Instrument registry should be around 8084-8090 range
            for port in range(8084, 8100):
                if port not in used_ports:
                    return port
            
            # Fallback to higher range
            for port in range(8100, 8200):
                if port not in used_ports:
                    return port
                    
            raise ValueError("No available ports found")
            
        finally:
            await conn.close()
    
    async def register_port(self, port: int) -> bool:
        """Register port in port registry"""
        conn = await asyncpg.connect(self.database_url)
        
        try:
            # Insert for all environments
            for env in ['dev', 'staging', 'prod']:
                staging_port = port + 100 if env == 'staging' else port
                
                await conn.execute("""
                    INSERT INTO port_registry (service_name, environment, port, protocol, description)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (service_name, environment) DO UPDATE SET
                        port = EXCLUDED.port,
                        description = EXCLUDED.description
                """, 
                    self.service_name, 
                    env, 
                    staging_port,
                    'http',
                    'Instrument Registry - Broker tokens and metadata service'
                )
            
            logger.info(f"Registered port {port} for {self.service_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register port: {e}")
            return False
        finally:
            await conn.close()
    
    async def register_minimal_config(self) -> Dict[str, Any]:
        """Register minimal, non-duplicate configuration"""
        conn = await asyncpg.connect(self.database_url)
        
        # Configuration that should be centralized (following token-manager patterns)
        configs_to_register = {
            
            # Ingestion Queue Configuration
            "INGESTION_QUEUE_NAME": {
                "value": "instrument_ingestion",
                "type": "string",
                "description": "Redis queue name for ingestion jobs"
            },
            
            "INGESTION_BATCH_SIZE": {
                "value": "1000",
                "type": "int",
                "description": "Batch size for ingestion processing"
            },
            
            "INGESTION_WORKER_COUNT": {
                "value": "4",
                "type": "int",
                "description": "Number of parallel ingestion workers"
            },
            
            # Cache Configuration
            "CACHE_TTL_SECONDS": {
                "value": "300",
                "type": "int",
                "description": "Cache TTL for instrument metadata (5 minutes)"
            },
            
            # Health Thresholds (previously hardcoded)
            "HEALTH_CRITICAL_MINUTES": {
                "value": "60",
                "type": "int",
                "description": "Minutes before marking service critical"
            },
            
            "HEALTH_WARNING_MINUTES": {
                "value": "15", 
                "type": "int",
                "description": "Minutes before marking service warning"
            },
            
            "HEALTH_HEALTHY_MINUTES": {
                "value": "5",
                "type": "int",
                "description": "Minutes threshold for healthy status"
            },
            
            # Calendar-Aware Proxy Intervals
            "PROXY_MARKET_HOURS_INTERVAL": {
                "value": "60",
                "type": "int", 
                "description": "Proxy refresh interval during market hours (seconds)"
            },
            
            "PROXY_OFF_HOURS_INTERVAL": {
                "value": "1800",
                "type": "int",
                "description": "Proxy refresh interval during off hours (seconds)"
            },
            
            "PROXY_HOLIDAY_INTERVAL": {
                "value": "3600", 
                "type": "int",
                "description": "Proxy refresh interval on holidays (seconds)"
            },
            
            # Proxy Behavior Flags
            "SKIP_REFRESH_DURING_MARKET": {
                "value": "false",
                "type": "bool",
                "description": "Skip proxy refresh during active market hours"
            },
            
            "SKIP_REFRESH_ON_HOLIDAYS": {
                "value": "true",
                "type": "bool", 
                "description": "Skip proxy refresh on market holidays"
            },
            
            # Broker-Specific Configuration
            "KITE_BROKER_ID": {
                "value": "kite",
                "type": "string",
                "description": "Primary broker identifier for Kite Connect"
            },
            
            "KITE_SEGMENTS": {
                "value": "NSE,BSE,NFO,BFO,CDS,MCX",
                "type": "string",
                "description": "Supported trading segments for Kite broker"
            },
            
            "DEFAULT_EXCHANGE": {
                "value": "NSE",
                "type": "string",
                "description": "Default exchange for instrument resolution"
            },
            
            "MONITORED_EXCHANGES": {
                "value": "NSE,BSE",
                "type": "string",
                "description": "Exchanges monitored for market state"
            },
            
            # Messaging Channels
            "ADMIN_CHANNEL": {
                "value": "instrument-registry-admin",
                "type": "string",
                "description": "Admin notification channel"
            },
            
            "ALERT_CHANNEL": {
                "value": "instrument-registry-alerts", 
                "type": "string",
                "description": "Alert notification channel"
            },
            
            "EMAIL_RECIPIENTS_CRITICAL": {
                "value": "platform-oncall@company.com",
                "type": "string",
                "description": "Email recipients for critical alerts"
            },
            
            "EMAIL_RECIPIENTS_WARNING": {
                "value": "instrument-registry-team@company.com",
                "type": "string",
                "description": "Email recipients for warning alerts"
            },
            
            # Event Streaming Configuration
            "EVENT_BROKER_URL": {
                "value": "redis://localhost:6379/0",
                "type": "string",
                "description": "Event broker URL for streaming infrastructure"
            },
            
            "EVENT_RETRY_ATTEMPTS": {
                "value": "3",
                "type": "int",
                "description": "Maximum retry attempts for failed events"
            },
            
            "EVENT_BATCH_SIZE": {
                "value": "100",
                "type": "int", 
                "description": "Batch size for event processing"
            },
            
            "EVENT_ORDERING_GUARANTEE": {
                "value": "partition",
                "type": "string",
                "description": "Event ordering guarantee level (none|partition|global)"
            },
            
            "DLQ_RETENTION_HOURS": {
                "value": "72",
                "type": "int",
                "description": "Dead letter queue retention period in hours"
            }
        }
        
        # Service URLs - these will be resolved via port registry, not hardcoded
        
        try:
            registered_count = 0
            
            # Register service-specific configs
            for config_key, config_data in configs_to_register.items():
                full_key = f"INSTRUMENT_REGISTRY_{config_key}"
                
                # Check if already exists
                existing = await conn.fetchrow("""
                    SELECT config_value FROM service_configs
                    WHERE service_name = $1 AND config_key = $2 AND environment = 'prod'
                """, self.service_name, full_key)
                
                if existing:
                    logger.info(f"Config key {full_key} already exists, skipping")
                    continue
                
                # Register for prod environment
                await conn.execute("""
                    INSERT INTO service_configs (service_name, environment, config_key, config_value, value_type, description)
                    VALUES ($1, $2, $3, $4, $5, $6)
                """, 
                    self.service_name,
                    'prod',
                    full_key,
                    config_data["value"],
                    config_data["type"], 
                    config_data["description"]
                )
                
                registered_count += 1
                logger.info(f"Registered {full_key}")
            
            # Note: Service URLs will be resolved via port registry automatically
            logger.info("Service URLs will be resolved via port registry (no hardcoded URLs needed)")
            
            return {
                "registered_configs": registered_count,
                "service_name": self.service_name
            }
            
        except Exception as e:
            logger.error(f"Failed to register configs: {e}")
            raise
        finally:
            await conn.close()
    
    async def register_secrets(self) -> Dict[str, Any]:
        """Register required secrets (only if not exists)"""
        conn = await asyncpg.connect(self.database_url)
        
        secrets_to_check = {
            # Infrastructure secrets (config_service responsibility)
            "DATABASE_URL": "Database connection URL for instrument registry service",
            "REDIS_URL": "Redis connection URL for instrument registry service", 
            "INSTRUMENT_REGISTRY_API_KEY": "Service-specific API key for internal authentication",
            
            # Note: Broker credentials (KITE_API_KEY, etc.) come from user_service APIs, not config_service
            # Note: TOKEN_MANAGER_INTERNAL_API_KEY is a shared service key, should already exist in config_service
        }
        
        try:
            existing_secrets = []
            missing_secrets = []
            
            for secret_key, description in secrets_to_check.items():
                existing = await conn.fetchrow("""
                    SELECT secret_key FROM secrets
                    WHERE secret_key = $1 AND environment = 'prod'
                """, secret_key)
                
                if existing:
                    existing_secrets.append(secret_key)
                else:
                    missing_secrets.append({"key": secret_key, "description": description})
            
            return {
                "existing_secrets": existing_secrets,
                "missing_secrets": missing_secrets,
                "message": "Secrets must be manually created with proper encryption"
            }
            
        finally:
            await conn.close()
    
    async def create_service_dependencies(self) -> bool:
        """Register service dependencies"""
        conn = await asyncpg.connect(self.database_url)
        
        dependencies = [
            ("postgresql", "required"),
            ("redis", "required"), 
            ("token_manager", "required"),
            ("config_service", "required"),
            ("calendar_service", "required"),
            ("message_service", "optional"),
            ("alert_service", "optional")
        ]
        
        try:
            for dep_service, dep_type in dependencies:
                await conn.execute("""
                    INSERT INTO service_dependencies (service_name, depends_on_service, dependency_type, environment)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (service_name, depends_on_service, environment) 
                    DO UPDATE SET dependency_type = EXCLUDED.dependency_type
                """,
                    self.service_name,
                    dep_service, 
                    dep_type,
                    'prod'
                )
            
            logger.info(f"Registered {len(dependencies)} service dependencies")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register dependencies: {e}")
            return False
        finally:
            await conn.close()


async def main():
    """Main registration process"""
    database_url = os.getenv("CONFIG_SERVICE_DATABASE_URL")
    if not database_url:
        logger.error("CONFIG_SERVICE_DATABASE_URL environment variable required")
        sys.exit(1)
    
    registration = ConfigServiceRegistration(database_url)
    
    logger.info("Starting instrument registry configuration registration...")
    
    # Step 1: Analyze existing configuration
    logger.info("Analyzing existing configuration...")
    analysis = await registration.analyze_existing_config()
    
    logger.info(f"Found {len(analysis['existing_configs'])} existing configs")
    logger.info(f"Found {len(analysis['existing_secrets'])} existing secrets") 
    logger.info(f"Found {len(analysis['existing_ports'])} existing port allocations")
    
    # Step 2: Port already registered manually (8086)
    available_port = 8086
    logger.info(f"Using pre-registered port: {available_port}")
    port_success = True
    
    # Step 3: Register configuration
    logger.info("Registering service configuration...")
    config_result = await registration.register_minimal_config()
    logger.info(f"Registered {config_result['registered_configs']} configuration parameters")
    
    # Step 4: Check secrets
    logger.info("Checking required secrets...")
    secrets_result = await registration.register_secrets()
    
    if secrets_result["missing_secrets"]:
        logger.warning("Missing secrets that need manual creation:")
        for secret in secrets_result["missing_secrets"]:
            logger.warning(f"  - {secret['key']}: {secret['description']}")
    
    # Step 5: Register dependencies
    logger.info("Registering service dependencies...")
    deps_success = await registration.create_service_dependencies()
    
    if deps_success:
        logger.info("Configuration registration completed successfully!")
        logger.info(f"Service: {registration.service_name}")
        logger.info(f"Port: {available_port}")
        logger.info(f"Configs: {config_result['registered_configs']} registered")
        
        if secrets_result["missing_secrets"]:
            logger.info("Next steps:")
            logger.info("1. Create missing secrets using config service admin interface")
            logger.info("2. Update service startup to fetch config from config service")
            logger.info("3. Test service with new configuration")
    else:
        logger.error("Failed to register service dependencies")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())