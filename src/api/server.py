import asyncio
import logging
import json
from typing import Optional, Dict, Any
from aiohttp import web
from cache.distributed import DistributedCache, DistributedConfig

logger = logging.getLogger(__name__)

class CacheAPIServer:
    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        cache_config: Optional[DistributedConfig] = None
    ):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.cache = DistributedCache(
            node_id=node_id,
            host=host,
            port=port,
            config=cache_config
        )
        self.app = web.Application()
        self._setup_routes()
        
    def _setup_routes(self):
        """Set up API routes."""
        self.app.router.add_get('/get/{key}', self._handle_get)
        self.app.router.add_put('/put', self._handle_put)
        self.app.router.add_delete('/delete/{key}', self._handle_delete)
        self.app.router.add_get('/stats', self._handle_stats)
        self.app.router.add_get('/health', self._handle_health)
        
    async def start(self):
        """Start the API server."""
        try:
            # Start the cache
            await self.cache.start()
            
            # Start the web server
            runner = web.AppRunner(self.app)
            await runner.setup()
            site = web.TCPSite(runner, self.host, self.port)
            await site.start()
            
            logger.info(f"Cache API server started at http://{self.host}:{self.port}")
            
        except Exception as e:
            logger.error(f"Error starting API server: {e}")
            await self.stop()
            raise
            
    async def stop(self):
        """Stop the API server."""
        try:
            # Stop the cache
            await self.cache.stop()
            
            # Stop the web server
            await self.app.shutdown()
            await self.app.cleanup()
            
            logger.info("Cache API server stopped")
            
        except Exception as e:
            logger.error(f"Error stopping API server: {e}")
            raise
            
    async def _handle_get(self, request: web.Request) -> web.Response:
        """Handle GET request."""
        try:
            key = request.match_info['key']
            value = await self.cache.get(key)
            
            if value is None:
                return web.json_response(
                    {"error": "Key not found"},
                    status=404
                )
                
            return web.json_response({"value": value})
            
        except Exception as e:
            logger.error(f"Error handling GET request: {e}")
            return web.json_response(
                {"error": str(e)},
                status=500
            )
            
    async def _handle_put(self, request: web.Request) -> web.Response:
        """Handle PUT request."""
        try:
            data = await request.json()
            key = data.get('key')
            value = data.get('value')
            ttl = data.get('ttl')
            
            if key is None or value is None:
                return web.json_response(
                    {"error": "Missing key or value"},
                    status=400
                )
                
            success = await self.cache.put(key, value, ttl)
            
            if not success:
                return web.json_response(
                    {"error": "Failed to put value"},
                    status=500
                )
                
            return web.json_response({"status": "success"})
            
        except json.JSONDecodeError:
            return web.json_response(
                {"error": "Invalid JSON"},
                status=400
            )
        except Exception as e:
            logger.error(f"Error handling PUT request: {e}")
            return web.json_response(
                {"error": str(e)},
                status=500
            )
            
    async def _handle_delete(self, request: web.Request) -> web.Response:
        """Handle DELETE request."""
        try:
            key = request.match_info['key']
            success = await self.cache.delete(key)
            
            if not success:
                return web.json_response(
                    {"error": "Failed to delete key"},
                    status=500
                )
                
            return web.json_response({"status": "success"})
            
        except Exception as e:
            logger.error(f"Error handling DELETE request: {e}")
            return web.json_response(
                {"error": str(e)},
                status=500
            )
            
    async def _handle_stats(self, request: web.Request) -> web.Response:
        """Handle GET /stats request."""
        try:
            stats = await self.cache.get_stats()
            return web.json_response(stats)
            
        except Exception as e:
            logger.error(f"Error handling stats request: {e}")
            return web.json_response(
                {"error": str(e)},
                status=500
            )
            
    async def _handle_health(self, request: web.Request) -> web.Response:
        """Handle GET /health request."""
        try:
            # Check if cache is healthy
            stats = await self.cache.get_stats()
            return web.json_response({
                "status": "healthy",
                "node_id": self.node_id,
                "stats": stats
            })
            
        except Exception as e:
            logger.error(f"Error handling health check: {e}")
            return web.json_response(
                {
                    "status": "unhealthy",
                    "error": str(e)
                },
                status=503
            )

async def main():
    # Create and start the API server
    server = CacheAPIServer(
        node_id="node1",
        host="0.0.0.0",
        port=8000,
        cache_config=DistributedConfig(
            enable_wal=True,
            wal_config=WALConfig(
                wal_dir="wal",
                sync_on_write=False
            )
        )
    )
    
    try:
        await server.start()
        
        # Keep the server running
        while True:
            await asyncio.sleep(3600)  # Sleep for an hour
            
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await server.stop()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main()) 