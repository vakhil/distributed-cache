import asyncio
import logging
import json
import time
import random
from typing import Dict, Set, List, Optional, Tuple
from dataclasses import dataclass
from cache.node import NodeInfo

logger = logging.getLogger(__name__)

@dataclass
class NodeState:
    """State of a node in the cluster."""
    node: NodeInfo
    status: str = 'active'  # active, failed, leaving
    last_seen: float = time.time()
    version: int = 0
    heartbeat_count: int = 0
    failed_heartbeats: int = 0

@dataclass
class MembershipConfig:
    """Configuration for the membership protocol."""
    gossip_interval: float = 1.0  # How often to gossip
    failure_timeout: float = 3.0  # How long to wait before marking a node as failed
    cleanup_interval: float = 5.0  # How often to clean up failed nodes
    gossip_fanout: int = 3  # Number of nodes to gossip with each interval
    heartbeat_interval: float = 1.0  # How often to send cluster heartbeats
    heartbeat_timeout: float = 3.0  # How long to wait for heartbeat response
    max_failed_heartbeats: int = 3  # Number of failed heartbeats before marking node as failed

class MembershipProtocol:
    """Gossip-based membership protocol with cluster-wide heartbeats."""
    
    def __init__(self, local_node: NodeInfo, config: Optional[MembershipConfig] = None):
        """Initialize membership protocol.
        
        Args:
            local_node: Local node information
            config: Optional membership configuration
        """
        self.local_node = local_node
        self.config = config or MembershipConfig()
        self.nodes: Dict[str, NodeState] = {}  # node_id -> NodeState
        self._lock = asyncio.Lock()
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._gossip_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._version = 0
    
    async def start(self) -> None:
        """Start the membership protocol."""
        # Add local node
        async with self._lock:
            self.nodes[self.local_node.node_id] = NodeState(
                node=self.local_node,
                last_seen=time.time(),
                version=self._version
            )
        
        # Start background tasks
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._gossip_task = asyncio.create_task(self._gossip_loop())
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("Membership protocol started")
    
    async def stop(self) -> None:
        """Stop the membership protocol."""
        # Cancel background tasks
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._gossip_task:
            self._gossip_task.cancel()
        if self._cleanup_task:
            self._cleanup_task.cancel()
        
        # Wait for tasks to finish
        tasks = [t for t in [self._heartbeat_task, self._gossip_task, self._cleanup_task] if t]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info("Membership protocol stopped")
    
    async def add_node(self, node: NodeInfo) -> None:
        """Add a node to the membership list.
        
        Args:
            node: Node to add
        """
        async with self._lock:
            if node.node_id not in self.nodes:
                self.nodes[node.node_id] = NodeState(
                    node=node,
                    last_seen=time.time(),
                    version=self._version
                )
                self._version += 1
                logger.info(f"Added node {node.node_id} to membership")
    
    async def remove_node(self, node_id: str) -> None:
        """Remove a node from the membership list.
        
        Args:
            node_id: ID of node to remove
        """
        async with self._lock:
            if node_id in self.nodes:
                del self.nodes[node_id]
                self._version += 1
                logger.info(f"Removed node {node_id} from membership")
    
    async def get_active_nodes(self) -> List[NodeInfo]:
        """Get list of active nodes.
        
        Returns:
            List of active nodes
        """
        async with self._lock:
            now = time.time()
            return [
                state.node for state in self.nodes.values()
                if state.status == 'active' and 
                now - state.last_seen <= self.config.failure_timeout
            ]
    
    async def _heartbeat_loop(self) -> None:
        """Cluster-wide heartbeat loop."""
        while True:
            try:
                await asyncio.sleep(self.config.heartbeat_interval)
                
                # Get all nodes except local node
                async with self._lock:
                    nodes_to_ping = [
                        state.node for state in self.nodes.values()
                        if state.node.node_id != self.local_node.node_id
                    ]
                
                # Send heartbeat to all nodes
                for node in nodes_to_ping:
                    try:
                        # Send heartbeat request
                        # This would be implemented by the RPC layer
                        # For now, we'll just update the state
                        async with self._lock:
                            if node.node_id in self.nodes:
                                state = self.nodes[node.node_id]
                                state.last_seen = time.time()
                                state.heartbeat_count += 1
                                state.failed_heartbeats = 0
                                state.version = self._version
                                self._version += 1
                        
                    except Exception as e:
                        logger.warning(f"Heartbeat to node {node.node_id} failed: {e}")
                        async with self._lock:
                            if node.node_id in self.nodes:
                                state = self.nodes[node.node_id]
                                state.failed_heartbeats += 1
                                
                                # Mark node as failed if too many heartbeats failed
                                if state.failed_heartbeats >= self.config.max_failed_heartbeats:
                                    state.status = 'failed'
                                    logger.error(f"Node {node.node_id} marked as failed after {state.failed_heartbeats} failed heartbeats")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")
                await asyncio.sleep(1.0)
    
    async def _gossip_loop(self) -> None:
        """Gossip loop for state synchronization."""
        while True:
            try:
                await asyncio.sleep(self.config.gossip_interval)
                
                # Get random subset of nodes to gossip with
                async with self._lock:
                    active_nodes = [
                        state.node for state in self.nodes.values()
                        if state.node.node_id != self.local_node.node_id and
                        state.status == 'active'
                    ]
                
                if not active_nodes:
                    continue
                
                # Select random nodes to gossip with
                nodes_to_gossip = random.sample(
                    active_nodes,
                    min(self.config.gossip_fanout, len(active_nodes))
                )
                
                # Exchange state with selected nodes
                for node in nodes_to_gossip:
                    try:
                        # This would be implemented by the RPC layer
                        # For now, we'll just update our state
                        async with self._lock:
                            if node.node_id in self.nodes:
                                state = self.nodes[node.node_id]
                                state.last_seen = time.time()
                                state.version = self._version
                                self._version += 1
                        
                    except Exception as e:
                        logger.warning(f"Gossip with node {node.node_id} failed: {e}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in gossip loop: {e}")
                await asyncio.sleep(1.0)
    
    async def _cleanup_loop(self) -> None:
        """Cleanup loop for removing failed nodes."""
        while True:
            try:
                await asyncio.sleep(self.config.cleanup_interval)
                
                async with self._lock:
                    now = time.time()
                    failed_nodes = [
                        node_id for node_id, state in self.nodes.items()
                        if state.status == 'failed' or
                        (state.status == 'active' and 
                         now - state.last_seen > self.config.failure_timeout)
                    ]
                    
                    for node_id in failed_nodes:
                        await self.remove_node(node_id)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
                await asyncio.sleep(1.0)
    
    async def get_cluster_state(self) -> Dict[str, Any]:
        """Get current cluster state.
        
        Returns:
            Dictionary containing cluster state information
        """
        async with self._lock:
            return {
                'local_node': self.local_node.node_id,
                'version': self._version,
                'nodes': {
                    node_id: {
                        'status': state.status,
                        'last_seen': state.last_seen,
                        'version': state.version,
                        'heartbeat_count': state.heartbeat_count,
                        'failed_heartbeats': state.failed_heartbeats
                    }
                    for node_id, state in self.nodes.items()
                }
            }
    
    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handle incoming membership protocol connections."""
        try:
            # Read message
            data = await reader.readline()
            if not data:
                return
            
            message = json.loads(data.decode())
            message_type = message.get('type')
            
            if message_type == 'gossip':
                # Handle gossip message
                await self._handle_gossip(message)
                
                # Send response with our state
                response = {
                    'type': 'gossip_response',
                    'nodes': {
                        node_id: {
                            'node': {
                                'node_id': state.node.node_id,
                                'host': state.node.host,
                                'port': state.node.port
                            },
                            'last_seen': state.last_seen,
                            'status': state.status,
                            'version': state.version
                        }
                        for node_id, state in self.nodes.items()
                    },
                    'version': self._version
                }
                
                writer.write(json.dumps(response).encode() + b'\n')
                await writer.drain()
            
        except Exception as e:
            logger.error(f"Error handling membership connection: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
    
    async def _handle_gossip(self, message: Dict) -> None:
        """Handle incoming gossip message.
        
        Args:
            message: The gossip message
        """
        async with self._lock:
            now = time.time()
            remote_version = message.get('version', 0)
            
            # Only process if remote version is newer
            if remote_version <= self._version:
                return
            
            # Update node states
            for node_id, node_data in message.get('nodes', {}).items():
                if node_id == self.local_node.node_id:
                    continue
                
                node_info = NodeInfo(
                    node_id=node_data['node']['node_id'],
                    host=node_data['node']['host'],
                    port=node_data['node']['port']
                )
                
                if node_id not in self.nodes:
                    self.nodes[node_id] = NodeState(node_info, now)
                else:
                    state = self.nodes[node_id]
                    if node_data['version'] > state.version:
                        state.node = node_info
                        state.version = node_data['version']
                    state.last_seen = now
                    state.status = node_data['status']
            
            self._version = remote_version
    
    async def _gossip_loop(self) -> None:
        """Background task to periodically gossip with other nodes."""
        while True:
            try:
                # Get active nodes to gossip with
                active_nodes = await self.get_active_nodes()
                if not active_nodes:
                    await asyncio.sleep(self.config.gossip_interval)
                    continue
                
                # Select random nodes to gossip with
                targets = random.sample(
                    [n for n in active_nodes if n.node_id != self.local_node.node_id],
                    min(self.config.gossip_fanout, len(active_nodes) - 1)
                )
                
                # Prepare gossip message
                message = {
                    'type': 'gossip',
                    'nodes': {
                        node_id: {
                            'node': {
                                'node_id': state.node.node_id,
                                'host': state.node.host,
                                'port': state.node.port
                            },
                            'last_seen': state.last_seen,
                            'status': state.status,
                            'version': state.version
                        }
                        for node_id, state in self.nodes.items()
                    },
                    'version': self._version
                }
                
                # Send gossip to selected nodes
                for target in targets:
                    try:
                        reader, writer = await asyncio.open_connection(
                            target.host, target.port + 1
                        )
                        writer.write(json.dumps(message).encode() + b'\n')
                        await writer.drain()
                        
                        # Read response
                        response = await reader.readline()
                        if response:
                            await self._handle_gossip(json.loads(response.decode()))
                        
                        writer.close()
                        await writer.wait_closed()
                        
                    except Exception as e:
                        logger.warning(f"Failed to gossip with node {target.node_id}: {e}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in gossip loop: {e}")
            
            await asyncio.sleep(self.config.gossip_interval)
    
    async def _cleanup_loop(self) -> None:
        """Background task to clean up failed nodes."""
        while True:
            try:
                async with self._lock:
                    now = time.time()
                    failed_nodes = [
                        node_id for node_id, state in self.nodes.items()
                        if state.status == 'active' and 
                        now - state.last_seen > self.config.failure_timeout
                    ]
                    
                    for node_id in failed_nodes:
                        self.nodes[node_id].status = 'failed'
                        self._version += 1
                        logger.warning(f"Node {node_id} marked as failed")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
            
            await asyncio.sleep(self.config.cleanup_interval) 