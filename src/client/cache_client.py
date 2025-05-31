import asyncio
import json
import logging
import time
from typing import Optional, Any, Dict, List, Union
from dataclasses import dataclass
import aiohttp
from aiohttp import ClientSession, ClientTimeout

logger = logging.getLogger(__name__)

@dataclass
class CacheClientConfig:
    """Configuration for the cache client."""
    timeout: float = 5.0  # seconds
    max_retries: int = 3
    retry_delay: float = 0.1  # seconds
    connection_pool_size: int = 10
    nodes: Optional[List[str]] = None  # List of node URLs (e.g., ["http://node1:8000", "http://node2:8000"])

class CacheError(Exception):
    """Base exception for cache client errors."""
    pass

class ConnectionError(CacheError):
    """Raised when unable to connect to cache nodes."""
    pass

class TimeoutError(CacheError):
    """Raised when operation times out."""
    pass

class CacheClient:
    """Python SDK client for the distributed cache."""
    
    def __init__(self, config: Optional[CacheClientConfig] = None):
        self.config = config or CacheClientConfig()
        self._session: Optional[ClientSession] = None
        self._nodes: List[str] = self.config.nodes or []
        self._current_node_idx = 0
        
    async def __aenter__(self):
        await self.connect()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        
    async def connect(self):
        """Connect to the cache cluster."""
        if self._session is not None:
            return
            
        timeout = ClientTimeout(total=self.config.timeout)
        self._session = ClientSession(
            timeout=timeout,
            connector=aiohttp.TCPConnector(
                limit=self.config.connection_pool_size,
                force_close=False
            )
        )
        
        # If no nodes provided, try to discover them
        if not self._nodes:
            # TODO: Implement node discovery
            raise ValueError("No cache nodes provided")
            
        logger.info(f"Connected to cache cluster with {len(self._nodes)} nodes")
        
    async def close(self):
        """Close the client connection."""
        if self._session:
            await self._session.close()
            self._session = None
            
    def _get_next_node(self) -> str:
        """Get the next node URL using round-robin."""
        if not self._nodes:
            raise ConnectionError("No cache nodes available")
        node = self._nodes[self._current_node_idx]
        self._current_node_idx = (self._current_node_idx + 1) % len(self._nodes)
        return node
        
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        retry: bool = True
    ) -> Any:
        """Make an HTTP request to a cache node with retry logic."""
        if not self._session:
            raise ConnectionError("Client not connected")
            
        last_error = None
        retries = self.config.max_retries if retry else 1
        
        for attempt in range(retries):
            try:
                node = self._get_next_node()
                url = f"{node}/{endpoint}"
                
                async with self._session.request(
                    method,
                    url,
                    json=data,
                    headers={"Content-Type": "application/json"}
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 404:
                        return None
                    else:
                        error_data = await response.json()
                        raise CacheError(f"Cache error: {error_data.get('error', 'Unknown error')}")
                        
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_error = e
                if attempt < retries - 1:
                    await asyncio.sleep(self.config.retry_delay * (2 ** attempt))
                continue
                
        raise last_error or ConnectionError("Failed to connect to cache nodes")
        
    async def get(self, key: str) -> Optional[Any]:
        """Get a value from the cache.
        
        Args:
            key: The key to look up
            
        Returns:
            The value if found, None otherwise
            
        Raises:
            CacheError: If the operation fails
            ConnectionError: If unable to connect to cache nodes
            TimeoutError: If the operation times out
        """
        return await self._make_request("GET", f"get/{key}")
        
    async def put(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None
    ) -> bool:
        """Put a value in the cache.
        
        Args:
            key: The key to store
            value: The value to store
            ttl: Optional time-to-live in seconds
            
        Returns:
            True if successful, False otherwise
            
        Raises:
            CacheError: If the operation fails
            ConnectionError: If unable to connect to cache nodes
            TimeoutError: If the operation times out
        """
        data = {
            "key": key,
            "value": value
        }
        if ttl is not None:
            data["ttl"] = ttl
            
        return await self._make_request("PUT", "put", data)
        
    async def delete(self, key: str) -> bool:
        """Delete a value from the cache.
        
        Args:
            key: The key to delete
            
        Returns:
            True if successful, False otherwise
            
        Raises:
            CacheError: If the operation fails
            ConnectionError: If unable to connect to cache nodes
            TimeoutError: If the operation times out
        """
        return await self._make_request("DELETE", f"delete/{key}")
        
    async def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics.
        
        Returns:
            Dictionary containing cache statistics
            
        Raises:
            CacheError: If the operation fails
            ConnectionError: If unable to connect to cache nodes
            TimeoutError: If the operation times out
        """
        return await self._make_request("GET", "stats")
        
    async def add_node(self, node_url: str) -> bool:
        """Add a node to the client's node list.
        
        Args:
            node_url: The URL of the node to add (e.g., "http://node1:8000")
            
        Returns:
            True if successful, False otherwise
        """
        if node_url not in self._nodes:
            self._nodes.append(node_url)
        return True
        
    async def remove_node(self, node_url: str) -> bool:
        """Remove a node from the client's node list.
        
        Args:
            node_url: The URL of the node to remove
            
        Returns:
            True if successful, False otherwise
        """
        if node_url in self._nodes:
            self._nodes.remove(node_url)
            if self._current_node_idx >= len(self._nodes):
                self._current_node_idx = 0
        return True

# Example usage
async def main():
    # Create a client
    config = CacheClientConfig(
        nodes=["http://localhost:8000", "http://localhost:8001"],
        timeout=5.0,
        max_retries=3
    )
    
    async with CacheClient(config) as client:
        # Put some values
        await client.put("key1", "value1")
        await client.put("key2", "value2", ttl=10)  # 10 second TTL
        
        # Get values
        value1 = await client.get("key1")
        print(f"key1 = {value1}")  # Should print "value1"
        
        # Get stats
        stats = await client.get_stats()
        print(f"Stats: {stats}")

if __name__ == "__main__":
    asyncio.run(main()) 