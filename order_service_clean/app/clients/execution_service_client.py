"""
Execution Service Client

Handles all communication with the algo_engine service for execution management.
Replaces direct algo_engine.executions table access.
"""

import logging
import httpx
from typing import Optional, Dict, Any, List
from datetime import datetime
from ..config.settings import _get_service_port

logger = logging.getLogger(__name__)


class ExecutionServiceError(Exception):
    """Execution service communication error"""
    pass


class ExecutionNotFoundError(ExecutionServiceError):
    """Execution not found error"""
    pass


class ExecutionServiceClient:
    """
    Client for communicating with algo_engine service for execution operations.
    
    This replaces direct access to:
    - algo_engine.executions
    - algo_engine.execution_pnl_metrics (if accessed)
    """

    def __init__(self, base_url: Optional[str] = None, timeout: int = 30):
        """
        Initialize execution service client.
        
        Args:
            base_url: Execution service base URL (if None, uses service discovery)
            timeout: Request timeout in seconds
        """
        self.base_url = base_url
        self.timeout = timeout
        self._http_client: Optional[httpx.AsyncClient] = None

    async def _get_base_url(self) -> str:
        """Get execution service base URL via service discovery"""
        if self.base_url:
            return self.base_url

        try:
            # Try algo_engine service first
            port = await _get_service_port("algo_engine")
            return f"http://algo-engine:{port}"
        except Exception:
            try:
                # Fallback to backend service (may handle executions)
                port = await _get_service_port("backend")
                return f"http://backend:{port}"
            except Exception as e:
                logger.warning(f"Service discovery failed for execution services: {e}")
                return "http://algo-engine:8003"  # Default fallback

    async def _get_http_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client"""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=self.timeout)
        return self._http_client

    async def close(self):
        """Close HTTP client"""
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    async def get_user_managed_execution(
        self, 
        strategy_id: int,
        user_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Get existing user-managed execution for a strategy.
        
        Replaces: SELECT id FROM algo_engine.executions WHERE strategy_id = ? AND execution_type = 'user_managed'
        
        Args:
            strategy_id: Strategy ID
            user_id: User ID
            
        Returns:
            Execution info if found, None if not found
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            response = await client.get(
                f"{base_url}/api/v1/executions/user-managed",
                params={
                    "strategy_id": strategy_id,
                    "user_id": user_id,
                    "active_only": True
                }
            )
            
            if response.status_code == 200:
                executions = response.json().get("executions", [])
                return executions[0] if executions else None
            elif response.status_code == 404:
                return None  # No execution found
            else:
                logger.error(f"Get user managed execution failed: {response.status_code} {response.text}")
                raise ExecutionServiceError(f"Get user managed execution failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Execution service request failed: {e}")
            raise ExecutionServiceError(f"Execution service request failed: {e}")

    async def create_user_managed_execution(
        self, 
        strategy_id: int,
        user_id: int,
        name: str = "Manual Trading",
        capital_allocation_pct: float = 100.0
    ) -> Dict[str, Any]:
        """
        Create a new user-managed execution for manual trading.
        
        Replaces: INSERT INTO algo_engine.executions (strategy_id, user_id, execution_type, ...)
        
        Args:
            strategy_id: Strategy ID
            user_id: User ID
            name: Execution name
            capital_allocation_pct: Capital allocation percentage
            
        Returns:
            Created execution information
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            execution_data = {
                "strategy_id": strategy_id,
                "user_id": user_id,
                "execution_type": "user_managed",
                "managed_by_user_id": user_id,
                "name": name,
                "status": "running",
                "capital_allocation_pct": capital_allocation_pct,
                "parameters": {
                    "is_tracking_only": True,
                    "auto_created": True
                }
            }

            response = await client.post(
                f"{base_url}/api/v1/executions",
                json=execution_data
            )
            
            if response.status_code == 201:
                return response.json()
            else:
                logger.error(f"Create execution failed: {response.status_code} {response.text}")
                raise ExecutionServiceError(f"Create execution failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Execution service request failed: {e}")
            raise ExecutionServiceError(f"Execution service request failed: {e}")

    async def get_or_create_user_managed_execution(
        self, 
        strategy_id: int,
        user_id: int
    ) -> str:
        """
        Get or create user-managed execution for a strategy.
        
        Args:
            strategy_id: Strategy ID
            user_id: User ID
            
        Returns:
            Execution ID as string
        """
        try:
            # Try to get existing execution
            existing = await self.get_user_managed_execution(strategy_id, user_id)
            if existing:
                execution_id = str(existing.get("id") or existing.get("execution_id"))
                logger.debug(f"Found existing user-managed execution {execution_id} for strategy {strategy_id}")
                return execution_id

            # Create new execution
            logger.info(f"Creating user-managed execution for strategy {strategy_id}")
            new_execution = await self.create_user_managed_execution(strategy_id, user_id)
            execution_id = str(new_execution.get("id") or new_execution.get("execution_id"))
            
            logger.info(f"Created user-managed execution {execution_id} for strategy {strategy_id}")
            return execution_id
            
        except ExecutionServiceError:
            raise
        except Exception as e:
            logger.error(f"Failed to get/create execution for strategy {strategy_id}: {e}")
            raise ExecutionServiceError(f"Failed to get/create execution: {e}")

    async def get_execution_info(self, execution_id: str) -> Dict[str, Any]:
        """
        Get comprehensive execution information.
        
        Args:
            execution_id: Execution ID
            
        Returns:
            Execution information dictionary
            
        Raises:
            ExecutionNotFoundError: If execution doesn't exist
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            response = await client.get(f"{base_url}/api/v1/executions/{execution_id}")
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                raise ExecutionNotFoundError(f"Execution {execution_id} not found")
            else:
                logger.error(f"Get execution info failed: {response.status_code} {response.text}")
                raise ExecutionServiceError(f"Get execution info failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Execution service request failed: {e}")
            raise ExecutionServiceError(f"Execution service request failed: {e}")

    async def update_execution_status(
        self, 
        execution_id: str, 
        status: str
    ) -> bool:
        """
        Update execution status.
        
        Args:
            execution_id: Execution ID
            status: New status ('running', 'idle', 'paused', 'stopped')
            
        Returns:
            True if update successful
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            update_data = {"status": status}

            response = await client.put(
                f"{base_url}/api/v1/executions/{execution_id}/status",
                json=update_data
            )
            
            if response.status_code == 200:
                return True
            elif response.status_code == 404:
                raise ExecutionNotFoundError(f"Execution {execution_id} not found")
            else:
                logger.error(f"Update execution status failed: {response.status_code} {response.text}")
                raise ExecutionServiceError(f"Update execution status failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Execution service request failed: {e}")
            raise ExecutionServiceError(f"Execution service request failed: {e}")

    async def get_executions_by_strategy(
        self, 
        strategy_id: int
    ) -> List[Dict[str, Any]]:
        """
        Get all executions for a strategy.
        
        Args:
            strategy_id: Strategy ID
            
        Returns:
            List of execution information dictionaries
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            response = await client.get(
                f"{base_url}/api/v1/strategies/{strategy_id}/executions"
            )
            
            if response.status_code == 200:
                return response.json().get("executions", [])
            elif response.status_code == 404:
                return []  # No executions found
            else:
                logger.error(f"Get executions by strategy failed: {response.status_code} {response.text}")
                raise ExecutionServiceError(f"Get executions by strategy failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Execution service request failed: {e}")
            raise ExecutionServiceError(f"Execution service request failed: {e}")


# Singleton instance
_execution_client: Optional[ExecutionServiceClient] = None


async def get_execution_client() -> ExecutionServiceClient:
    """Get or create execution service client singleton"""
    global _execution_client
    if _execution_client is None:
        _execution_client = ExecutionServiceClient()
    return _execution_client


async def cleanup_execution_client():
    """Cleanup execution service client"""
    global _execution_client
    if _execution_client:
        await _execution_client.close()
        _execution_client = None