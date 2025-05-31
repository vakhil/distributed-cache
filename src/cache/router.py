import asyncio
import logging
import time
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass
from cache.node import NodeInfo
from cache.membership import MembershipProtocol, MembershipConfig
from cache.rpc import RPCClient, RPCConfig, RPCError, NodeUnavailableError

logger = logging.getLogger(__name__)

@dataclass
class RouterConfig:
    """Configuration for the cache router."""
    max_retries: int = 3  # Maximum number of retries for failed operations
    retry_delay: float = 0.1  # Delay between retries in seconds
    operation_timeout: float = 1.0  # Timeout for cache operations
    virtual_nodes: int = 3  # Number of virtual nodes per physical node
    replication_factor: int = 2  # Number of replicas for each key

class CacheRouter:
    """Router for distributed cache operations using consistent hashing."""
    
    def __init__(
        self,
        local_node: NodeInfo,
        router_config: Optional[RouterConfig] = None,
        membership_config: Optional[MembershipConfig] = None
    ):
        """Initialize cache router.
        
        Args:
            local_node: Local node information
            router_config: Optional router configuration
            membership_config: Optional membership protocol configuration
        """
        self.local_node = local_node
        self.config = router_config or RouterConfig()
        
        # Initialize RPC client with appropriate config
        rpc_config = RPCConfig(
            connect_timeout=self.config.operation_timeout,
            read_timeout=self.config.operation_timeout,
            write_timeout=self.config.operation_timeout,
            max_retries=self.config.max_retries,
            retry_delay=self.config.retry_delay
        )
        self.rpc = RPCClient(rpc_config)
        
        # Initialize membership protocol
        self.membership = MembershipProtocol(
            local_node=local_node,
            config=membership_config
        )
        
        # Node management
        self._nodes: Dict[str, NodeInfo] = {}  # node_id -> NodeInfo
        self._virtual_nodes: Dict[str, Set[str]] = {}  # node_id -> set of virtual node hashes
        self._hash_ring: Dict[str, str] = {}  # virtual node hash -> node_id
        self._lock = asyncio.Lock()
    
    async def start(self) -> None:
        """Start the router and membership protocol."""
        await self.membership.start()
        # Start node update task
        asyncio.create_task(self._update_nodes())
        logger.info("Cache router started")
    
    async def stop(self) -> None:
        """Stop the router and membership protocol."""
        await self.membership.stop()
        # Disconnect from all nodes
        for node_id in list(self._nodes.keys()):
            await self.rpc.disconnect(node_id)
        logger.info("Cache router stopped")
    
    async def _update_nodes(self) -> None:
        """Update node list based on membership protocol."""
        while True:
            try:
                # Get active nodes from membership
                active_nodes = await self.membership.get_active_nodes()
                
                async with self._lock:
                    # Add new nodes
                    for node in active_nodes:
                        if node.node_id not in self._nodes:
                            await self._add_node(node)
                    
                    # Remove failed nodes
                    for node_id in list(self._nodes.keys()):
                        if node_id not in {node.node_id for node in active_nodes}:
                            await self._remove_node(node_id)
                
                await asyncio.sleep(1.0)  # Update interval
                
            except Exception as e:
                logger.error(f"Error updating nodes: {e}")
                await asyncio.sleep(1.0)
    
    async def _add_node(self, node: NodeInfo) -> None:
        """Add a node to the hash ring.
        
        Args:
            node: Node to add
        """
        if node.node_id in self._nodes:
            return
        
        self._nodes[node.node_id] = node
        self._virtual_nodes[node.node_id] = set()
        
        # Add virtual nodes
        for i in range(self.config.virtual_nodes):
            virtual_hash = f"{node.node_id}_{i}"
            self._hash_ring[virtual_hash] = node.node_id
            self._virtual_nodes[node.node_id].add(virtual_hash)
        
        logger.info(f"Added node {node.node_id} to hash ring")
    
    async def _remove_node(self, node_id: str) -> None:
        """Remove a node from the hash ring.
        
        Args:
            node_id: ID of node to remove
        """
        if node_id not in self._nodes:
            return
        
        # Remove virtual nodes
        for virtual_hash in self._virtual_nodes[node_id]:
            del self._hash_ring[virtual_hash]
        
        del self._virtual_nodes[node_id]
        del self._nodes[node_id]
        
        # Disconnect from node
        await self.rpc.disconnect(node_id)
        
        logger.info(f"Removed node {node_id} from hash ring")
    
    def _get_nodes_for_key(self, key: str) -> List[NodeInfo]:
        """Get nodes responsible for a key.
        
        Args:
            key: Cache key
            
        Returns:
            List of nodes responsible for the key
        """
        if not self._hash_ring:
            return []
        
        # Get virtual node hashes
        virtual_hashes = sorted(self._hash_ring.keys())
        if not virtual_hashes:
            return []
        
        # Find first virtual node
        key_hash = str(hash(key))
        first_idx = 0
        for i, vh in enumerate(virtual_hashes):
            if vh > key_hash:
                first_idx = i
                break
        
        # Get nodes for replication
        nodes: List[NodeInfo] = []
        seen_nodes: Set[str] = set()
        
        # Start from first node and wrap around
        for i in range(len(virtual_hashes)):
            idx = (first_idx + i) % len(virtual_hashes)
            node_id = self._hash_ring[virtual_hashes[idx]]
            
            if node_id not in seen_nodes:
                nodes.append(self._nodes[node_id])
                seen_nodes.add(node_id)
                
                if len(nodes) >= self.config.replication_factor:
                    break
        
        return nodes
    
    async def get(self, key: str) -> Optional[Any]:
        """Get a value from the cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found
            
        Raises:
            RPCError: If all nodes fail
        """
        nodes = self._get_nodes_for_key(key)
        if not nodes:
            return None
        
        last_error = None
        for node in nodes:
            try:
                return await self.rpc.call(node, 'get', {'key': key})
            except (RPCError, NodeUnavailableError) as e:
                last_error = e
                logger.warning(f"Failed to get from node {node.node_id}: {e}")
                continue
        
        if last_error:
            raise last_error
        return None
    
    async def put(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """Put a value into the cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Optional time-to-live in seconds
            
        Raises:
            RPCError: If all nodes fail
        """
        nodes = self._get_nodes_for_key(key)
        if not nodes:
            raise RPCError("No nodes available")
        
        # Try to write to all replicas
        errors = []
        for node in nodes:
            try:
                await self.rpc.call(node, 'put', {
                    'key': key,
                    'value': value,
                    'ttl': ttl
                })
            except (RPCError, NodeUnavailableError) as e:
                errors.append(e)
                logger.warning(f"Failed to put to node {node.node_id}: {e}")
                continue
        
        # If all nodes failed, raise the last error
        if len(errors) == len(nodes):
            raise errors[-1]
    
    async def delete(self, key: str) -> None:
        """Delete a value from the cache.
        
        Args:
            key: Cache key
            
        Raises:
            RPCError: If all nodes fail
        """
        nodes = self._get_nodes_for_key(key)
        if not nodes:
            return
        
        # Try to delete from all replicas
        errors = []
        for node in nodes:
            try:
                await self.rpc.call(node, 'delete', {'key': key})
            except (RPCError, NodeUnavailableError) as e:
                errors.append(e)
                logger.warning(f"Failed to delete from node {node.node_id}: {e}")
                continue
        
        # If all nodes failed, raise the last error
        if len(errors) == len(nodes):
            raise errors[-1]
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get router statistics.
        
        Returns:
            Dictionary of statistics
        """
        stats = {
            'node_count': len(self._nodes),
            'virtual_node_count': sum(len(vnodes) for vnodes in self._virtual_nodes.values()),
            'replication_factor': self.config.replication_factor,
            'nodes': {}
        }
        
        # Get stats from each node
        for node_id, node in self._nodes.items():
            try:
                node_stats = await self.rpc.call(node, 'get_stats', {})
                stats['nodes'][node_id] = node_stats
            except (RPCError, NodeUnavailableError) as e:
                stats['nodes'][node_id] = {'error': str(e)}
        
        return stats 