import asyncio
import json
import logging
import time
from typing import Dict, Any, Optional, Callable, Awaitable, TypeVar, Generic
from dataclasses import dataclass
from enum import Enum
from cache.node import NodeInfo

logger = logging.getLogger(__name__)

T = TypeVar('T')

class RPCError(Exception):
    """Base class for RPC errors."""
    pass

class TimeoutError(RPCError):
    """Raised when an RPC operation times out."""
    pass

class ConnectionError(RPCError):
    """Raised when there's a connection error."""
    pass

class NodeUnavailableError(RPCError):
    """Raised when a node is unavailable."""
    pass

class RPCMessageType(Enum):
    """Types of RPC messages."""
    REQUEST = 'request'
    RESPONSE = 'response'
    ERROR = 'error'
    HEARTBEAT = 'heartbeat'

@dataclass
class RPCMessage(Generic[T]):
    """RPC message with type and payload."""
    type: RPCMessageType
    id: str
    payload: T
    timestamp: float = time.time()

@dataclass
class RPCConfig:
    """Configuration for RPC operations."""
    connect_timeout: float = 1.0  # Timeout for establishing connection
    read_timeout: float = 1.0    # Timeout for reading response
    write_timeout: float = 1.0   # Timeout for writing request
    max_retries: int = 3         # Maximum number of retries
    retry_delay: float = 0.1     # Delay between retries
    heartbeat_interval: float = 1.0  # Interval for heartbeat messages
    heartbeat_timeout: float = 3.0   # Timeout for heartbeat responses
    max_message_size: int = 1024 * 1024  # 1MB max message size

class RPCClient:
    """Async RPC client with connection pooling and fault tolerance."""
    
    def __init__(self, config: Optional[RPCConfig] = None):
        """Initialize RPC client.
        
        Args:
            config: Optional RPC configuration
        """
        self.config = config or RPCConfig()
        self._connections: Dict[str, asyncio.StreamWriter] = {}
        self._connection_locks: Dict[str, asyncio.Lock] = {}
        self._message_handlers: Dict[str, asyncio.Future] = {}
        self._heartbeat_tasks: Dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()
    
    async def connect(self, node: NodeInfo) -> None:
        """Establish connection to a node.
        
        Args:
            node: Node to connect to
            
        Raises:
            ConnectionError: If connection fails
            TimeoutError: If connection times out
        """
        async with self._lock:
            if node.node_id not in self._connections:
                self._connection_locks[node.node_id] = asyncio.Lock()
        
        async with self._connection_locks[node.node_id]:
            try:
                # Try to establish connection with timeout
                async with asyncio.timeout(self.config.connect_timeout):
                    reader, writer = await asyncio.open_connection(
                        node.host, node.port
                    )
                
                self._connections[node.node_id] = writer
                self._start_heartbeat(node)
                logger.info(f"Connected to node {node.node_id}")
                
                # Start message handler
                asyncio.create_task(self._handle_messages(node, reader))
                
            except asyncio.TimeoutError:
                raise TimeoutError(f"Connection to node {node.node_id} timed out")
            except Exception as e:
                raise ConnectionError(f"Failed to connect to node {node.node_id}: {e}")
    
    async def disconnect(self, node_id: str) -> None:
        """Disconnect from a node.
        
        Args:
            node_id: ID of the node to disconnect from
        """
        async with self._lock:
            if node_id in self._connections:
                # Stop heartbeat
                if node_id in self._heartbeat_tasks:
                    self._heartbeat_tasks[node_id].cancel()
                    del self._heartbeat_tasks[node_id]
                
                # Close connection
                writer = self._connections.pop(node_id)
                writer.close()
                await writer.wait_closed()
                
                # Clean up locks and handlers
                if node_id in self._connection_locks:
                    del self._connection_locks[node_id]
                
                # Cancel any pending message handlers
                for future in self._message_handlers.values():
                    if not future.done():
                        future.cancel()
                
                logger.info(f"Disconnected from node {node_id}")
    
    async def call(self, node: NodeInfo, method: str, params: Dict[str, Any]) -> Any:
        """Make an RPC call to a node.
        
        Args:
            node: Node to call
            method: Method name
            params: Method parameters
            
        Returns:
            Response from the node
            
        Raises:
            NodeUnavailableError: If node is not available
            TimeoutError: If call times out
            RPCError: For other RPC errors
        """
        # Ensure connection
        if node.node_id not in self._connections:
            try:
                await self.connect(node)
            except (ConnectionError, TimeoutError) as e:
                raise NodeUnavailableError(f"Node {node.node_id} unavailable: {e}")
        
        message_id = f"{method}_{time.time()}"
        message = RPCMessage(
            type=RPCMessageType.REQUEST,
            id=message_id,
            payload={
                'method': method,
                'params': params
            }
        )
        
        # Create future for response
        future = asyncio.Future()
        self._message_handlers[message_id] = future
        
        try:
            # Send request with retries
            for attempt in range(self.config.max_retries):
                try:
                    async with self._connection_locks[node.node_id]:
                        writer = self._connections[node.node_id]
                        
                        # Write message with timeout
                        try:
                            async with asyncio.timeout(self.config.write_timeout):
                                writer.write(json.dumps(message.__dict__).encode() + b'\n')
                                await writer.drain()
                        except asyncio.TimeoutError:
                            raise TimeoutError(f"Write timeout for node {node.node_id}")
                        
                        # Wait for response with timeout
                        try:
                            async with asyncio.timeout(self.config.read_timeout):
                                response = await future
                                if response.type == RPCMessageType.ERROR:
                                    raise RPCError(response.payload.get('error', 'Unknown error'))
                                return response.payload
                        except asyncio.TimeoutError:
                            raise TimeoutError(f"Read timeout for node {node.node_id}")
                        
                except (ConnectionError, TimeoutError) as e:
                    if attempt == self.config.max_retries - 1:
                        raise
                    logger.warning(f"Retry {attempt + 1} for node {node.node_id}: {e}")
                    await asyncio.sleep(self.config.retry_delay)
                    # Try to reconnect
                    try:
                        await self.connect(node)
                    except Exception:
                        pass
                
        finally:
            # Clean up message handler
            if message_id in self._message_handlers:
                del self._message_handlers[message_id]
    
    def _start_heartbeat(self, node: NodeInfo) -> None:
        """Start heartbeat monitoring for a node."""
        async def heartbeat_loop():
            while True:
                try:
                    await asyncio.sleep(self.config.heartbeat_interval)
                    await self.call(node, 'heartbeat', {})
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Heartbeat failed for node {node.node_id}: {e}")
                    # Mark node as failed
                    await self.disconnect(node.node_id)
                    break
        
        self._heartbeat_tasks[node.node_id] = asyncio.create_task(heartbeat_loop())
    
    async def _handle_messages(self, node: NodeInfo, reader: asyncio.StreamReader) -> None:
        """Handle incoming messages from a node."""
        try:
            while True:
                # Read message with size limit
                data = await reader.readuntil(b'\n')
                if len(data) > self.config.max_message_size:
                    logger.error(f"Message too large from node {node.node_id}")
                    continue
                
                try:
                    message_dict = json.loads(data.decode())
                    message = RPCMessage(**message_dict)
                    
                    # Handle response messages
                    if message.type == RPCMessageType.RESPONSE:
                        if message.id in self._message_handlers:
                            future = self._message_handlers[message.id]
                            if not future.done():
                                future.set_result(message)
                    
                    # Handle error messages
                    elif message.type == RPCMessageType.ERROR:
                        if message.id in self._message_handlers:
                            future = self._message_handlers[message.id]
                            if not future.done():
                                future.set_exception(RPCError(message.payload.get('error', 'Unknown error')))
                    
                except json.JSONDecodeError:
                    logger.error(f"Invalid message from node {node.node_id}")
                except Exception as e:
                    logger.error(f"Error handling message from node {node.node_id}: {e}")
                
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Connection to node {node.node_id} closed: {e}")
        finally:
            # Clean up connection
            await self.disconnect(node.node_id)

class RPCServer:
    """Async RPC server with request handling and heartbeat support."""
    
    def __init__(
        self,
        node: NodeInfo,
        config: Optional[RPCConfig] = None,
        handlers: Optional[Dict[str, Callable[..., Awaitable[Any]]]] = None
    ):
        """Initialize RPC server.
        
        Args:
            node: Node information
            config: Optional RPC configuration
            handlers: Optional request handlers
        """
        self.node = node
        self.config = config or RPCConfig()
        self.handlers = handlers or {}
        self._server: Optional[asyncio.Server] = None
        self._clients: Dict[str, asyncio.StreamWriter] = {}
        self._lock = asyncio.Lock()
    
    async def start(self) -> None:
        """Start the RPC server."""
        self._server = await asyncio.start_server(
            self._handle_connection,
            self.node.host,
            self.node.port
        )
        logger.info(f"RPC server started on {self.node.host}:{self.node.port}")
    
    async def stop(self) -> None:
        """Stop the RPC server."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            
            # Close all client connections
            async with self._lock:
                for writer in self._clients.values():
                    writer.close()
                    await writer.wait_closed()
                self._clients.clear()
            
            logger.info("RPC server stopped")
    
    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handle incoming client connection."""
        client_id = f"{writer.get_extra_info('peername')}"
        
        async with self._lock:
            self._clients[client_id] = writer
        
        try:
            while True:
                # Read message with size limit
                data = await reader.readuntil(b'\n')
                if len(data) > self.config.max_message_size:
                    logger.error(f"Message too large from client {client_id}")
                    continue
                
                try:
                    message_dict = json.loads(data.decode())
                    message = RPCMessage(**message_dict)
                    
                    # Handle request messages
                    if message.type == RPCMessageType.REQUEST:
                        try:
                            # Get handler for method
                            handler = self.handlers.get(message.payload['method'])
                            if not handler:
                                raise RPCError(f"Unknown method: {message.payload['method']}")
                            
                            # Call handler
                            result = await handler(**message.payload['params'])
                            
                            # Send response
                            response = RPCMessage(
                                type=RPCMessageType.RESPONSE,
                                id=message.id,
                                payload=result
                            )
                            writer.write(json.dumps(response.__dict__).encode() + b'\n')
                            await writer.drain()
                            
                        except Exception as e:
                            # Send error response
                            error = RPCMessage(
                                type=RPCMessageType.ERROR,
                                id=message.id,
                                payload={'error': str(e)}
                            )
                            writer.write(json.dumps(error.__dict__).encode() + b'\n')
                            await writer.drain()
                    
                except json.JSONDecodeError:
                    logger.error(f"Invalid message from client {client_id}")
                except Exception as e:
                    logger.error(f"Error handling message from client {client_id}: {e}")
                
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Connection from client {client_id} closed: {e}")
        finally:
            # Clean up client connection
            async with self._lock:
                if client_id in self._clients:
                    del self._clients[client_id]
            writer.close()
            await writer.wait_closed() 