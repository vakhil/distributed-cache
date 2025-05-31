import asyncio
import logging
from typing import Optional, Dict, Any, Tuple
from dataclasses import dataclass
import time
import json
from cache.cache import Cache

logger = logging.getLogger(__name__)

@dataclass
class NodeInfo:
    """Information about a cache node."""
    node_id: str
    host: str
    port: int
    weight: int = 1  # For weighted consistent hashing if needed later

class CacheNode:
    """Represents a single node in the distributed cache system."""
    
    def __init__(self, node_id: str, host: str = "localhost", port: int = 0,
                 max_memory_mb: int = 100, max_size: int = 1000):
        """Initialize a cache node.
        
        Args:
            node_id: Unique identifier for this node
            host: Host address for this node
            port: Port number (0 for auto-assign)
            max_memory_mb: Maximum memory in MB for this node's cache
            max_size: Maximum number of entries for this node's cache
        """
        self.info = NodeInfo(node_id=node_id, host=host, port=port)
        self.cache = Cache(max_memory_mb=max_memory_mb, max_size=max_size)
        self._server: Optional[asyncio.Server] = None
        self._running = False
        
    async def start(self):
        """Start the cache node server."""
        if self._running:
            return
            
        async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            """Handle incoming client requests."""
            try:
                while True:
                    # Read request length (4 bytes)
                    length_bytes = await reader.read(4)
                    if not length_bytes:
                        break
                    length = int.from_bytes(length_bytes, 'big')
                    
                    # Read request data
                    data = await reader.read(length)
                    if not data:
                        break
                        
                    request = json.loads(data.decode())
                    response = await self._handle_request(request)
                    
                    # Send response
                    response_data = json.dumps(response).encode()
                    writer.write(len(response_data).to_bytes(4, 'big'))
                    writer.write(response_data)
                    await writer.drain()
                    
            except Exception as e:
                logger.error(f"Error handling client request: {e}")
            finally:
                writer.close()
                await writer.wait_closed()
        
        self._server = await asyncio.start_server(
            handle_client,
            self.info.host,
            self.info.port
        )
        
        # Update port if it was auto-assigned
        if self.info.port == 0:
            self.info.port = self._server.sockets[0].getsockname()[1]
            
        self._running = True
        logger.info(f"Cache node {self.info.node_id} started on {self.info.host}:{self.info.port}")
        
        # Keep the server running
        async with self._server:
            await self._server.serve_forever()
    
    async def stop(self):
        """Stop the cache node server."""
        if not self._running:
            return
            
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        self._running = False
        self.cache.shutdown()
        logger.info(f"Cache node {self.info.node_id} stopped")
    
    async def _handle_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle a cache operation request."""
        op = request.get('op')
        key = request.get('key')
        value = request.get('value')
        ttl = request.get('ttl')
        
        try:
            if op == 'get':
                result = self.cache.get(key)
                return {'status': 'success', 'value': result}
                
            elif op == 'put':
                success = self.cache.put(key, value, ttl=ttl)
                return {'status': 'success' if success else 'failed'}
                
            elif op == 'delete':
                success = self.cache.delete(key)
                return {'status': 'success' if success else 'failed'}
                
            elif op == 'stats':
                stats = self.cache.get_stats()
                return {'status': 'success', 'stats': stats}
                
            else:
                return {'status': 'error', 'error': f'Unknown operation: {op}'}
                
        except Exception as e:
            logger.error(f"Error processing request: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def get_stats(self) -> Dict[str, Any]:
        """Get node statistics."""
        stats = self.cache.get_stats()
        stats.update({
            'node_id': self.info.node_id,
            'host': self.info.host,
            'port': self.info.port,
            'running': self._running
        })
        return stats 