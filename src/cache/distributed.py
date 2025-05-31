import asyncio
import logging
from typing import Dict, Any, Optional, List, Set, Tuple
from cache.router import RouterConfig, CacheRouter
from cache.node import NodeInfo, CacheNode
from cache.membership import MembershipConfig, MembershipProtocol
from .cache import Cache, CacheConfig
from .rpc import RPCServer, RPCConfig
from .wal import WriteAheadLog, WALConfig, OperationType

logger = logging.getLogger(__name__)

class DistributedConfig:
    def __init__(
        self,
        membership_config: Optional[MembershipConfig] = None,
        router_config: Optional[RouterConfig] = None,
        cache_config: Optional[CacheConfig] = None,
        rpc_config: Optional[RPCConfig] = None,
        wal_config: Optional[WALConfig] = None,
        enable_wal: bool = True
    ):
        self.membership_config = membership_config or MembershipConfig()
        self.router_config = router_config or RouterConfig()
        self.cache_config = cache_config or CacheConfig()
        self.rpc_config = rpc_config or RPCConfig()
        self.wal_config = wal_config or WALConfig()
        self.enable_wal = enable_wal

class DistributedCache:
    """Distributed cache implementation using consistent hashing and replication."""
    
    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        config: Optional[DistributedConfig] = None
    ):
        """Initialize the distributed cache.
        
        Args:
            node_id: The node's ID
            host: The node's host
            port: The node's port
            config: Optional distributed configuration
        """
        self.node_id = node_id
        self.host = host
        self.port = port
        self.config = config or DistributedConfig()
        
        # Initialize components
        self.membership = MembershipProtocol(
            node_id=node_id,
            host=host,
            port=port,
            config=self.config.membership_config
        )
        
        self.router = CacheRouter(
            node_id=node_id,
            membership=self.membership,
            config=self.config.router_config
        )
        
        self.cache = Cache(config=self.config.cache_config)
        
        self.rpc_server = RPCServer(
            host=host,
            port=port,
            config=self.config.rpc_config
        )
        
        # Initialize WAL if enabled
        self.wal: Optional[WriteAheadLog] = None
        if self.config.enable_wal:
            self.wal = WriteAheadLog(
                node_id=node_id,
                config=self.config.wal_config
            )
        
        # Register RPC handlers
        self._register_handlers()
        
    def _register_handlers(self):
        """Register RPC handlers for cache operations."""
        self.rpc_server.register_handler('get', self._handle_get)
        self.rpc_server.register_handler('put', self._handle_put)
        self.rpc_server.register_handler('delete', self._handle_delete)
        self.rpc_server.register_handler('get_stats', self._handle_get_stats)
        self.rpc_server.register_handler('heartbeat', self._handle_heartbeat)
        
    async def start(self):
        """Start the distributed cache node."""
        try:
            # Start WAL if enabled
            if self.wal:
                await self.wal.start()
                # Replay WAL to restore state
                cache_data, ttl_data = await self.wal.replay(None)
                # Restore cache state
                for key, value in cache_data.items():
                    self.cache._data[key] = value
                for key, ttl in ttl_data.items():
                    self.cache._ttl[key] = ttl
            
            # Start other components
            await self.membership.start()
            await self.router.start()
            await self.rpc_server.start()
            
            logger.info(f"Distributed cache node {self.node_id} started")
            
        except Exception as e:
            logger.error(f"Error starting node {self.node_id}: {e}")
            await self.stop()
            raise
            
    async def stop(self):
        """Stop the distributed cache node."""
        try:
            # Stop WAL if enabled
            if self.wal:
                await self.wal.stop()
            
            # Stop other components
            await self.rpc_server.stop()
            await self.router.stop()
            await self.membership.stop()
            
            logger.info(f"Distributed cache node {self.node_id} stopped")
            
        except Exception as e:
            logger.error(f"Error stopping node {self.node_id}: {e}")
            raise
            
    async def _handle_get(self, request: Dict[str, Any]) -> Optional[Any]:
        """Handle GET request."""
        key = request['key']
        return await self.get(key)
        
    async def _handle_put(self, request: Dict[str, Any]) -> bool:
        """Handle PUT request."""
        key = request['key']
        value = request['value']
        ttl = request.get('ttl')
        return await self.put(key, value, ttl)
        
    async def _handle_delete(self, request: Dict[str, Any]) -> bool:
        """Handle DELETE request."""
        key = request['key']
        return await self.delete(key)
        
    async def _handle_get_stats(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle GET_STATS request."""
        return await self.get_stats()
        
    async def _handle_heartbeat(self, request: Dict[str, Any]) -> bool:
        """Handle HEARTBEAT request."""
        return True
        
    async def get(self, key: str) -> Optional[Any]:
        """Get a value from the cache."""
        # First try local cache
        value = await self.cache.get(key)
        if value is not None:
            return value
            
        # If not found locally, try other nodes
        return await self.router.get(key)
        
    async def put(self, key: str, value: Any, ttl: Optional[float] = None) -> bool:
        """Put a value in the cache."""
        # Log to WAL first if enabled
        if self.wal:
            await self.wal.log_operation(
                operation=OperationType.PUT,
                key=key,
                value=value,
                ttl=ttl
            )
        
        # Update local cache
        success = await self.cache.put(key, value, ttl)
        if not success:
            return False
            
        # Replicate to other nodes
        return await self.router.put(key, value, ttl)
        
    async def delete(self, key: str) -> bool:
        """Delete a value from the cache."""
        # Log to WAL first if enabled
        if self.wal:
            await self.wal.log_operation(
                operation=OperationType.DELETE,
                key=key
            )
        
        # Update local cache
        success = await self.cache.delete(key)
        if not success:
            return False
            
        # Replicate to other nodes
        return await self.router.delete(key)
        
    async def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        local_stats = await self.cache.get_stats()
        cluster_stats = await self.router.get_stats()
        
        return {
            'node_id': self.node_id,
            'local': local_stats,
            'cluster': cluster_stats
        }
        
    async def compact_wal(self):
        """Compact the WAL if enabled."""
        if self.wal:
            await self.wal.compact()
    
    @property
    def replication_factor(self) -> int:
        """Get the current replication factor."""
        return self.router.config.replication_factor
    
    @property
    def node_count(self) -> int:
        """Get the current number of nodes in the cluster."""
        return len(self.router.nodes)

async def create_local_cluster(num_nodes: int = 3, max_memory_mb: int = 100,
                             max_size: int = 1000) -> DistributedCache:
    """Create a local cluster of cache nodes for testing.
    
    Args:
        num_nodes: Number of nodes to create
        max_memory_mb: Maximum memory per node in MB
        max_size: Maximum entries per node
        
    Returns:
        DistributedCache instance with the local cluster
    """
    cache = DistributedCache()
    
    # Create nodes
    for i in range(num_nodes):
        node_id = f"node{i}"
        await cache.add_node(node_id, max_memory_mb=max_memory_mb, max_size=max_size)
    
    return cache

# Example usage
async def main():
    # Create a local cluster
    cache = await create_local_cluster(num_nodes=3)
    
    try:
        # Put some values
        await cache.put("key1", "value1")
        await cache.put("key2", "value2", ttl=10)  # 10 second TTL
        
        # Get values
        value1 = await cache.get("key1")
        print(f"key1 = {value1}")  # Should print "value1"
        
        # Get stats
        stats = await cache.get_stats()
        print(f"Stats: {stats}")
        
    finally:
        # Shutdown
        await cache.shutdown()

if __name__ == "__main__":
    asyncio.run(main()) 