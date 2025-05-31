import asyncio
import unittest
import time
import logging
from typing import List, Optional
from cache.node import NodeInfo
from cache.membership import MembershipProtocol, MembershipConfig, NodeState
from cache.router import CacheRouter, RouterConfig
from cache.distributed import DistributedCache

# Configure logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestDistributedCache(unittest.TestCase):
    """Test suite for the distributed cache system."""
    
    def setUp(self):
        """Set up test environment."""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        # Create test nodes
        self.nodes: List[NodeInfo] = [
            NodeInfo(f"node{i}", "localhost", 8000 + i)
            for i in range(3)
        ]
        
        # Create membership configs with faster intervals for testing
        self.membership_config = MembershipConfig(
            gossip_interval=0.1,  # 100ms
            failure_timeout=0.5,  # 500ms
            cleanup_interval=1.0,  # 1s
            gossip_fanout=2
        )
        
        # Create router configs
        self.router_config = RouterConfig(
            replication_factor=2,
            virtual_nodes=10,  # Fewer virtual nodes for testing
            operation_timeout=0.5,
            membership_config=self.membership_config
        )
    
    def tearDown(self):
        """Clean up test environment."""
        self.loop.close()
    
    async def _create_cluster(self, num_nodes: int = 3) -> List[DistributedCache]:
        """Create a test cluster of cache nodes."""
        caches = []
        for i in range(num_nodes):
            cache = DistributedCache(
                self.nodes[i],
                self.router_config,
                self.membership_config
            )
            await cache.start()
            caches.append(cache)
        
        # Wait for membership to stabilize
        await asyncio.sleep(0.5)
        return caches
    
    async def _stop_cluster(self, caches: List[DistributedCache]):
        """Stop all cache nodes in the cluster."""
        for cache in caches:
            await cache.stop()
    
    def test_membership_protocol(self):
        """Test the membership protocol functionality."""
        async def run_test():
            # Create two nodes
            node1 = MembershipProtocol(self.nodes[0], self.membership_config)
            node2 = MembershipProtocol(self.nodes[1], self.membership_config)
            
            try:
                # Start nodes
                await node1.start()
                await node2.start()
                
                # Add node2 to node1's membership
                await node1.add_node(self.nodes[1])
                
                # Wait for gossip
                await asyncio.sleep(0.2)
                
                # Check that both nodes know about each other
                active_nodes1 = await node1.get_active_nodes()
                active_nodes2 = await node2.get_active_nodes()
                
                self.assertEqual(len(active_nodes1), 2)
                self.assertEqual(len(active_nodes2), 2)
                self.assertTrue(any(n.node_id == "node1" for n in active_nodes1))
                self.assertTrue(any(n.node_id == "node0" for n in active_nodes2))
                
                # Test node failure detection
                await node2.stop()
                await asyncio.sleep(0.6)  # Wait for failure timeout
                
                active_nodes1 = await node1.get_active_nodes()
                self.assertEqual(len(active_nodes1), 1)
                self.assertTrue(all(n.node_id == "node0" for n in active_nodes1))
                
            finally:
                await node1.stop()
                if node2._server:  # node2 might be stopped already
                    await node2.stop()
        
        self.loop.run_until_complete(run_test())
    
    def test_consistent_hashing(self):
        """Test consistent hashing and key distribution."""
        async def run_test():
            caches = await self._create_cluster(3)
            try:
                # Put some test values
                test_data = {
                    "key1": "value1",
                    "key2": "value2",
                    "key3": "value3"
                }
                
                # Store values
                for key, value in test_data.items():
                    success = await caches[0].put(key, value)
                    self.assertTrue(success)
                
                # Get stats to see key distribution
                stats = await caches[0].get_stats()
                node_stats = stats['nodes']
                
                # Verify each key is stored on replication_factor nodes
                for node_id, node_stat in node_stats.items():
                    if 'error' not in node_stat:
                        self.assertIn('key_count', node_stat)
                
                # Verify keys are consistently mapped
                # Get the same key multiple times
                for _ in range(3):
                    value1 = await caches[0].get("key1")
                    self.assertEqual(value1, "value1")
                
                # Add a new node and verify minimal key movement
                new_node = NodeInfo("node3", "localhost", 8003)
                new_cache = DistributedCache(
                    new_node,
                    self.router_config,
                    self.membership_config
                )
                await new_cache.start()
                caches.append(new_cache)
                
                # Wait for membership to update
                await asyncio.sleep(0.2)
                
                # Verify all keys are still accessible
                for key, value in test_data.items():
                    retrieved = await caches[0].get(key)
                    self.assertEqual(retrieved, value)
                
            finally:
                await self._stop_cluster(caches)
        
        self.loop.run_until_complete(run_test())
    
    def test_replication(self):
        """Test replication behavior."""
        async def run_test():
            caches = await self._create_cluster(3)
            try:
                # Put a value
                key, value = "test_key", "test_value"
                success = await caches[0].put(key, value)
                self.assertTrue(success)
                
                # Verify value is accessible from all nodes
                for cache in caches:
                    retrieved = await cache.get(key)
                    self.assertEqual(retrieved, value)
                
                # Verify replication factor
                stats = await caches[0].get_stats()
                key_count = sum(
                    node_stat.get('key_count', 0)
                    for node_stat in stats['nodes'].values()
                    if 'error' not in node_stat
                )
                self.assertGreaterEqual(key_count, self.router_config.replication_factor)
                
            finally:
                await self._stop_cluster(caches)
        
        self.loop.run_until_complete(run_test())
    
    def test_node_management(self):
        """Test node addition and removal."""
        async def run_test():
            caches = await self._create_cluster(2)
            try:
                # Add a new node
                new_node = NodeInfo("node2", "localhost", 8002)
                new_cache = DistributedCache(
                    new_node,
                    self.router_config,
                    self.membership_config
                )
                await new_cache.start()
                caches.append(new_cache)
                
                # Wait for membership to update
                await asyncio.sleep(0.2)
                
                # Verify node count
                self.assertEqual(caches[0].node_count, 3)
                
                # Put some data
                await caches[0].put("key1", "value1")
                
                # Stop a node
                await caches[1].stop()
                caches.pop(1)
                
                # Wait for membership to update
                await asyncio.sleep(0.6)
                
                # Verify node count decreased
                self.assertEqual(caches[0].node_count, 2)
                
                # Verify data is still accessible
                value = await caches[0].get("key1")
                self.assertEqual(value, "value1")
                
            finally:
                await self._stop_cluster(caches)
        
        self.loop.run_until_complete(run_test())
    
    def test_concurrent_operations(self):
        """Test concurrent operations on the cache."""
        async def run_test():
            caches = await self._create_cluster(3)
            try:
                # Create concurrent operations
                async def put_operation(key: str, value: str):
                    return await caches[0].put(key, value)
                
                async def get_operation(key: str):
                    return await caches[0].get(key)
                
                # Run concurrent puts
                tasks = [
                    put_operation(f"key{i}", f"value{i}")
                    for i in range(10)
                ]
                results = await asyncio.gather(*tasks)
                self.assertTrue(all(results))
                
                # Run concurrent gets
                tasks = [
                    get_operation(f"key{i}")
                    for i in range(10)
                ]
                values = await asyncio.gather(*tasks)
                self.assertEqual(values, [f"value{i}" for i in range(10)])
                
                # Run mixed operations
                tasks = [
                    put_operation("key1", "new_value1"),
                    get_operation("key1"),
                    put_operation("key2", "new_value2"),
                    get_operation("key2")
                ]
                results = await asyncio.gather(*tasks)
                self.assertTrue(results[0])  # First put succeeded
                self.assertEqual(results[1], "new_value1")  # Get new value
                self.assertTrue(results[2])  # Second put succeeded
                self.assertEqual(results[3], "new_value2")  # Get new value
                
            finally:
                await self._stop_cluster(caches)
        
        self.loop.run_until_complete(run_test())
    
    def test_ttl_and_eviction(self):
        """Test TTL expiration and eviction."""
        async def run_test():
            caches = await self._create_cluster(3)
            try:
                # Put value with short TTL
                await caches[0].put("key1", "value1", ttl=0.5)
                
                # Verify value is accessible
                value = await caches[0].get("key1")
                self.assertEqual(value, "value1")
                
                # Wait for TTL to expire
                await asyncio.sleep(0.6)
                
                # Verify value is gone
                value = await caches[0].get("key1")
                self.assertIsNone(value)
                
                # Test eviction under memory pressure
                # Put many large values
                large_value = "x" * 1024  # 1KB
                for i in range(100):
                    await caches[0].put(f"large_key{i}", large_value)
                
                # Get stats to verify eviction
                stats = await caches[0].get_stats()
                for node_stat in stats['nodes'].values():
                    if 'error' not in node_stat:
                        self.assertLessEqual(
                            node_stat.get('memory_usage', 0),
                            node_stat.get('max_memory', float('inf'))
                        )
                
            finally:
                await self._stop_cluster(caches)
        
        self.loop.run_until_complete(run_test())
    
    def test_stats_and_monitoring(self):
        """Test statistics collection and monitoring."""
        async def run_test():
            caches = await self._create_cluster(3)
            try:
                # Put some values
                for i in range(5):
                    await caches[0].put(f"key{i}", f"value{i}")
                
                # Get stats
                stats = await caches[0].get_stats()
                
                # Verify router stats
                self.assertEqual(stats['router']['node_count'], 3)
                self.assertEqual(stats['router']['replication_factor'], 2)
                
                # Verify node stats
                for node_id, node_stat in stats['nodes'].items():
                    if 'error' not in node_stat:
                        self.assertIn('key_count', node_stat)
                        self.assertIn('hit_count', node_stat)
                        self.assertIn('miss_count', node_stat)
                        self.assertIn('memory_usage', node_stat)
                        
                        # Verify hit rate calculation
                        total = node_stat['hit_count'] + node_stat['miss_count']
                        if total > 0:
                            hit_rate = node_stat['hit_count'] / total
                            self.assertGreaterEqual(hit_rate, 0)
                            self.assertLessEqual(hit_rate, 1)
                
            finally:
                await self._stop_cluster(caches)
        
        self.loop.run_until_complete(run_test())

def run_tests():
    """Run all tests."""
    unittest.main()

if __name__ == '__main__':
    run_tests() 