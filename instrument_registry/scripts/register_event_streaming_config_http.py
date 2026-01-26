#!/usr/bin/env python3
"""
Register event streaming configuration parameters using HTTP API
Following production compliance patterns for config service integration
"""

import json
import asyncio
import aiohttp
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_KEY = "AShhRzWhfXd6IomyzZnE3d-lCcAvT1L5GDCCZRSXZGsJq7_eAJGxeMi-4AlfTeOc"
CONFIG_SERVICE_URL = "http://localhost:8100"
SERVICE_NAME = "instrument_registry"


async def register_config_parameter(session: aiohttp.ClientSession, config_key: str, value: str, 
                                   value_type: str, description: str) -> bool:
    """Register a single configuration parameter"""
    headers = {"X-Internal-API-Key": API_KEY, "Content-Type": "application/json"}
    
    payload = {
        "secret_key": config_key,
        "secret_value": value,
        "environment": "prod",
        "description": description
    }
    
    try:
        async with session.post(f"{CONFIG_SERVICE_URL}/api/v1/secrets", 
                               headers=headers, json=payload) as response:
            if response.status in [200, 201]:
                logger.info(f"✓ Registered {config_key} = {value}")
                return True
            elif response.status == 409:
                logger.info(f"⚠ {config_key} already exists")
                return True
            else:
                response_text = await response.text()
                logger.error(f"✗ Failed to register {config_key}: HTTP {response.status} - {response_text}")
                return False
    except Exception as e:
        logger.error(f"✗ Error registering {config_key}: {e}")
        return False


async def verify_config_retrieval(session: aiohttp.ClientSession, config_key: str) -> tuple:
    """Verify that a config parameter can be retrieved"""
    headers = {"X-Internal-API-Key": API_KEY}
    
    try:
        url = f"{CONFIG_SERVICE_URL}/api/v1/secrets/{config_key}/value?environment=prod"
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                return True, data.get('value')
            else:
                return False, f"HTTP {response.status}"
    except Exception as e:
        return False, str(e)


async def main():
    """Register event streaming configuration parameters"""
    
    # Event streaming configuration parameters
    event_configs = {
        "INSTRUMENT_REGISTRY_EVENT_BROKER_URL": {
            "value": "redis://localhost:6379/0",
            "type": "string",
            "description": "Event broker URL for streaming infrastructure"
        },
        
        "INSTRUMENT_REGISTRY_EVENT_RETRY_ATTEMPTS": {
            "value": "3",
            "type": "int",
            "description": "Maximum retry attempts for failed events"
        },
        
        "INSTRUMENT_REGISTRY_EVENT_BATCH_SIZE": {
            "value": "100",
            "type": "int", 
            "description": "Batch size for event processing"
        },
        
        "INSTRUMENT_REGISTRY_EVENT_ORDERING_GUARANTEE": {
            "value": "partition",
            "type": "string",
            "description": "Event ordering guarantee level (none|partition|global)"
        },
        
        "INSTRUMENT_REGISTRY_DLQ_RETENTION_HOURS": {
            "value": "72",
            "type": "int",
            "description": "Dead letter queue retention period in hours"
        }
    }
    
    logger.info("Registering event streaming configuration parameters...")
    
    async with aiohttp.ClientSession() as session:
        registered_count = 0
        
        # Register each configuration parameter
        for config_key, config_data in event_configs.items():
            success = await register_config_parameter(
                session, config_key, config_data["value"], 
                config_data["type"], config_data["description"]
            )
            if success:
                registered_count += 1
        
        logger.info(f"Registered {registered_count}/{len(event_configs)} event configuration parameters")
        
        # Verify retrieval of all parameters
        logger.info("\nVerifying parameter retrieval...")
        
        all_verified = True
        for config_key in event_configs.keys():
            success, value = await verify_config_retrieval(session, config_key)
            if success:
                logger.info(f"✓ {config_key} = {value}")
            else:
                logger.error(f"✗ Failed to retrieve {config_key}: {value}")
                all_verified = False
        
        if all_verified:
            logger.info(f"\n✅ All {len(event_configs)} event streaming configuration parameters registered and accessible!")
            logger.info("🚀 Ready to proceed with event streaming implementation")
            return True
        else:
            logger.error(f"\n❌ Some parameters could not be verified")
            return False


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)