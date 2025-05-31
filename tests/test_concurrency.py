import unittest
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Set, Tuple
import logging
from threading import Event
import threading
from dataclasses import dataclass
from queue import Queue
import statistics

from cache.cache import Cache

logger = logging.getLogger(__name__)

@dataclass
class OperationResult:
    """Record the result of a cache operation for verification."""
    operation: str  # 'get', 'put', 'delete'
    key: str
    value: any = None
    success: bool = True
    error: str = None
    timestamp: float = 0.0
    thread_id: int = 0

class ConcurrencyTest(unittest.TestCase):
    def setUp(self):
        """Set up test environment."""
        self.cache = Cache(max_memory_mb=100, max_size=1000)
        self.results: List[OperationResult] = []
        self.results_lock = threading.Lock()
        self.stop_event = Event()
        self.operation_queue = Queue()
        
        # Enable debug logging
        logging.basicConfig(level=logging.DEBUG)
    
    def tearDown(self):
        """Clean up after tests."""
        self.cache.shutdown()
    
    def record_operation(self, result: OperationResult):
        """Thread-safe recording of operation results."""
        with self.results_lock:
            self.results.append(result)
    
    def verify_consistency(self):
        """Verify cache consistency after concurrent operations."""
        # Track the last successful operation for each key
        key_history: Dict[str, List[OperationResult]] = {}
        
        # Group operations by key
        for result in sorted(self.results, key=lambda x: x.timestamp):
            if result.key not in key_history:
                key_history[result.key] = []
            key_history[result.key].append(result)
        
        # Verify each key's history
        inconsistencies = []
        for key, history in key_history.items():
            # Skip keys that were only read
            if all(op.operation == 'get' for op in history):
                continue
                
            # Find the last successful write operation
            last_write = None
            for op in reversed(history):
                if op.operation in ('put', 'delete') and op.success:
                    last_write = op
                    break
            
            # Verify all subsequent reads match the last write
            if last_write:
                for op in history:
                    if op.timestamp > last_write.timestamp and op.operation == 'get':
                        if op.value != (None if last_write.operation == 'delete' else last_write.value):
                            inconsistencies.append({
                                'key': key,
                                'last_write': last_write,
                                'inconsistent_read': op
                            })
        
        return inconsistencies
    
    def run_concurrent_operations(
        self,
        num_threads: int,
        num_operations: int,
        key_space: int,
        ttl_range: Tuple[float, float] = (0.1, 1.0)
    ):
        """Run concurrent operations with specified parameters."""
        def worker(thread_id: int):
            while not self.stop_event.is_set():
                try:
                    # Randomly choose operation
                    op = random.choice(['get', 'put', 'delete'])
                    key = f"key_{random.randint(0, key_space)}"
                    value = f"value_{random.randint(0, 1000)}"
                    ttl = random.uniform(*ttl_range) if op == 'put' else None
                    
                    start_time = time.time()
                    try:
                        if op == 'get':
                            result = self.cache.get(key)
                            success = True
                        elif op == 'put':
                            success = self.cache.put(key, value, ttl)
                            result = value if success else None
                        else:  # delete
                            success = self.cache.delete(key)
                            result = None
                            
                        self.record_operation(OperationResult(
                            operation=op,
                            key=key,
                            value=result,
                            success=success,
                            timestamp=time.time(),
                            thread_id=thread_id
                        ))
                    except Exception as e:
                        self.record_operation(OperationResult(
                            operation=op,
                            key=key,
                            success=False,
                            error=str(e),
                            timestamp=time.time(),
                            thread_id=thread_id
                        ))
                        logger.error(f"Operation failed: {op} {key} - {str(e)}")
                        
                except Exception as e:
                    logger.error(f"Worker error: {str(e)}")
        
        # Start worker threads
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, i) for i in range(num_threads)]
            
            # Run for specified number of operations
            operations_completed = 0
            while operations_completed < num_operations and not self.stop_event.is_set():
                time.sleep(0.1)  # Check periodically
                operations_completed = len(self.results)
            
            # Stop workers
            self.stop_event.set()
            
            # Wait for completion
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Thread error: {str(e)}")
    
    def test_basic_concurrency(self):
        """Test basic concurrent GET/PUT/DEL operations."""
        self.run_concurrent_operations(
            num_threads=10,
            num_operations=1000,
            key_space=100
        )
        
        # Verify consistency
        inconsistencies = self.verify_consistency()
        self.assertEqual(len(inconsistencies), 0, 
            f"Found {len(inconsistencies)} inconsistencies: {inconsistencies}")
        
        # Check metrics
        stats = self.cache.get_stats()
        self.assertGreater(stats['hits'] + stats['misses'], 0)
        self.assertGreater(stats['puts'], 0)
        self.assertGreater(stats['deletes'], 0)
    
    def test_ttl_concurrency(self):
        """Test concurrent operations with TTL expiry."""
        self.run_concurrent_operations(
            num_threads=10,
            num_operations=1000,
            key_space=100,
            ttl_range=(0.1, 0.5)  # Short TTL to force expiry
        )
        
        # Verify consistency
        inconsistencies = self.verify_consistency()
        self.assertEqual(len(inconsistencies), 0,
            f"Found {len(inconsistencies)} inconsistencies: {inconsistencies}")
        
        # Check expiry metrics
        stats = self.cache.get_stats()
        self.assertGreater(stats['expiries'], 0)
    
    def test_high_contention(self):
        """Test high contention on a small key space."""
        self.run_concurrent_operations(
            num_threads=20,
            num_operations=2000,
            key_space=10  # Small key space for high contention
        )
        
        # Verify consistency
        inconsistencies = self.verify_consistency()
        self.assertEqual(len(inconsistencies), 0,
            f"Found {len(inconsistencies)} inconsistencies: {inconsistencies}")
        
        # Check throughput
        stats = self.cache.get_stats()
        for op_type in ['get', 'put', 'delete']:
            self.assertGreater(stats['throughput'][op_type], 0)
    
    def test_memory_pressure(self):
        """Test behavior under memory pressure."""
        # Use large values to force memory pressure
        def worker(thread_id: int):
            while not self.stop_event.is_set():
                key = f"key_{random.randint(0, 100)}"
                # Generate large value (1MB)
                value = "x" * (1024 * 1024)
                try:
                    self.cache.put(key, value)
                    time.sleep(0.1)  # Slow down to allow eviction
                except Exception as e:
                    logger.error(f"Memory pressure error: {str(e)}")
        
        # Start worker threads
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(worker, i) for i in range(5)]
            
            # Run for 10 seconds
            time.sleep(10)
            self.stop_event.set()
            
            # Wait for completion
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Thread error: {str(e)}")
        
        # Verify memory usage
        stats = self.cache.get_stats()
        self.assertLessEqual(stats['memory_usage']['usage_percent'], 100)
    
    def test_operation_timing(self):
        """Test operation timing under load."""
        self.run_concurrent_operations(
            num_threads=10,
            num_operations=1000,
            key_space=100
        )
        
        # Analyze operation timing
        stats = self.cache.get_stats()
        for op_type in ['get', 'put', 'delete']:
            throughput = stats['throughput'][op_type]
            self.assertGreater(throughput, 0, f"{op_type} throughput should be positive")
            logger.info(f"{op_type} throughput: {throughput:.2f} ops/sec")

if __name__ == '__main__':
    unittest.main() 