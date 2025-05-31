import unittest
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Set, Tuple
import logging
from threading import Event, Barrier
import threading
from dataclasses import dataclass
from queue import Queue
import statistics
from collections import defaultdict

from cache.cache import Cache

logger = logging.getLogger(__name__)

@dataclass
class AccessPattern:
    """Record a specific access pattern for analysis."""
    pattern: str  # 'get_during_put', 'del_then_get', 'ttl_expiry', etc.
    key: str
    thread_id: int
    start_time: float
    end_time: float
    success: bool
    error: str = None
    value_before: any = None
    value_after: any = None

class LockFreeTest(unittest.TestCase):
    def setUp(self):
        """Set up test environment with detailed logging."""
        self.cache = Cache(max_memory_mb=100, max_size=1000)
        self.access_patterns: List[AccessPattern] = []
        self.patterns_lock = threading.Lock()
        self.stop_event = Event()
        
        # Enable detailed logging
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(threadName)s - %(message)s'
        )
    
    def tearDown(self):
        """Clean up and analyze results."""
        self.cache.shutdown()
        self._analyze_patterns()
    
    def record_pattern(self, pattern: AccessPattern):
        """Thread-safe recording of access patterns."""
        with self.patterns_lock:
            self.access_patterns.append(pattern)
            logger.debug(f"Pattern: {pattern.pattern} - Key: {pattern.key} - "
                        f"Thread: {pattern.thread_id} - Success: {pattern.success}")
    
    def _analyze_patterns(self):
        """Analyze access patterns for inconsistencies."""
        patterns_by_type = defaultdict(list)
        for pattern in self.access_patterns:
            patterns_by_type[pattern.pattern].append(pattern)
        
        # Analyze each pattern type
        for pattern_type, patterns in patterns_by_type.items():
            success_rate = sum(1 for p in patterns if p.success) / len(patterns)
            avg_duration = statistics.mean(p.end_time - p.start_time for p in patterns)
            
            logger.info(f"\nPattern Analysis - {pattern_type}:")
            logger.info(f"Total occurrences: {len(patterns)}")
            logger.info(f"Success rate: {success_rate:.2%}")
            logger.info(f"Average duration: {avg_duration:.6f}s")
            
            # Analyze specific pattern types
            if pattern_type == 'get_during_put':
                self._analyze_get_during_put(patterns)
            elif pattern_type == 'del_then_get':
                self._analyze_del_then_get(patterns)
            elif pattern_type == 'ttl_expiry':
                self._analyze_ttl_expiry(patterns)
    
    def _analyze_get_during_put(self, patterns: List[AccessPattern]):
        """Analyze GET during PUT patterns for consistency."""
        for pattern in patterns:
            if pattern.value_before != pattern.value_after:
                logger.warning(
                    f"Inconsistent GET during PUT - Key: {pattern.key}\n"
                    f"Value before: {pattern.value_before}\n"
                    f"Value after: {pattern.value_after}"
                )
    
    def _analyze_del_then_get(self, patterns: List[AccessPattern]):
        """Analyze DELETE then GET patterns."""
        for pattern in patterns:
            if pattern.success and pattern.value_after is not None:
                logger.warning(
                    f"Unexpected value after DELETE - Key: {pattern.key}\n"
                    f"Value after delete: {pattern.value_after}"
                )
    
    def _analyze_ttl_expiry(self, patterns: List[AccessPattern]):
        """Analyze TTL expiry patterns."""
        for pattern in patterns:
            if pattern.success and pattern.value_after is not None:
                logger.warning(
                    f"Value not expired - Key: {pattern.key}\n"
                    f"Value after TTL: {pattern.value_after}"
                )
    
    def test_get_during_put(self):
        """Test concurrent GET and PUT operations on the same key."""
        def get_worker(key: str, barrier: Barrier):
            barrier.wait()  # Synchronize start
            start_time = time.time()
            value_before = self.cache.get(key)
            time.sleep(0.001)  # Ensure overlap
            value_after = self.cache.get(key)
            end_time = time.time()
            
            self.record_pattern(AccessPattern(
                pattern='get_during_put',
                key=key,
                thread_id=threading.get_ident(),
                start_time=start_time,
                end_time=end_time,
                success=True,
                value_before=value_before,
                value_after=value_after
            ))
        
        def put_worker(key: str, value: str, barrier: Barrier):
            barrier.wait()  # Synchronize start
            start_time = time.time()
            success = self.cache.put(key, value)
            end_time = time.time()
            
            self.record_pattern(AccessPattern(
                pattern='get_during_put',
                key=key,
                thread_id=threading.get_ident(),
                start_time=start_time,
                end_time=end_time,
                success=success
            ))
        
        # Run multiple iterations
        for _ in range(100):
            key = f"key_{random.randint(0, 100)}"
            value = f"value_{random.randint(0, 1000)}"
            barrier = Barrier(2)
            
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [
                    executor.submit(get_worker, key, barrier),
                    executor.submit(put_worker, key, value, barrier)
                ]
                for future in as_completed(futures):
                    future.result()
    
    def test_del_then_get(self):
        """Test DELETE followed by GET/PUT on the same key."""
        def del_then_get(key: str, barrier: Barrier):
            barrier.wait()  # Synchronize start
            start_time = time.time()
            self.cache.delete(key)
            time.sleep(0.001)  # Ensure overlap
            value_after = self.cache.get(key)
            end_time = time.time()
            
            self.record_pattern(AccessPattern(
                pattern='del_then_get',
                key=key,
                thread_id=threading.get_ident(),
                start_time=start_time,
                end_time=end_time,
                success=True,
                value_after=value_after
            ))
        
        def del_then_put(key: str, value: str, barrier: Barrier):
            barrier.wait()  # Synchronize start
            start_time = time.time()
            self.cache.delete(key)
            time.sleep(0.001)  # Ensure overlap
            success = self.cache.put(key, value)
            end_time = time.time()
            
            self.record_pattern(AccessPattern(
                pattern='del_then_get',
                key=key,
                thread_id=threading.get_ident(),
                start_time=start_time,
                end_time=end_time,
                success=success
            ))
        
        # Run multiple iterations
        for _ in range(100):
            key = f"key_{random.randint(0, 100)}"
            value = f"value_{random.randint(0, 1000)}"
            barrier = Barrier(2)
            
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [
                    executor.submit(del_then_get, key, barrier),
                    executor.submit(del_then_put, key, value, barrier)
                ]
                for future in as_completed(futures):
                    future.result()
    
    def test_ttl_concurrent_expiry(self):
        """Test concurrent operations during TTL expiry."""
        def get_worker(key: str, barrier: Barrier):
            barrier.wait()  # Synchronize start
            start_time = time.time()
            value_before = self.cache.get(key)
            time.sleep(0.2)  # Wait for TTL
            value_after = self.cache.get(key)
            end_time = time.time()
            
            self.record_pattern(AccessPattern(
                pattern='ttl_expiry',
                key=key,
                thread_id=threading.get_ident(),
                start_time=start_time,
                end_time=end_time,
                success=True,
                value_before=value_before,
                value_after=value_after
            ))
        
        def put_worker(key: str, value: str, barrier: Barrier):
            barrier.wait()  # Synchronize start
            start_time = time.time()
            success = self.cache.put(key, value, ttl=0.1)  # Short TTL
            end_time = time.time()
            
            self.record_pattern(AccessPattern(
                pattern='ttl_expiry',
                key=key,
                thread_id=threading.get_ident(),
                start_time=start_time,
                end_time=end_time,
                success=success
            ))
        
        # Run multiple iterations
        for _ in range(100):
            key = f"key_{random.randint(0, 100)}"
            value = f"value_{random.randint(0, 1000)}"
            barrier = Barrier(2)
            
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [
                    executor.submit(get_worker, key, barrier),
                    executor.submit(put_worker, key, value, barrier)
                ]
                for future in as_completed(futures):
                    future.result()
    
    def test_hammer_cache(self):
        """Hammer the cache with random operations from multiple threads."""
        def worker(thread_id: int):
            while not self.stop_event.is_set():
                try:
                    # Randomly choose operation
                    op = random.choice(['get', 'put', 'delete'])
                    key = f"key_{random.randint(0, 100)}"
                    value = f"value_{random.randint(0, 1000)}"
                    
                    start_time = time.time()
                    try:
                        if op == 'get':
                            result = self.cache.get(key)
                        elif op == 'put':
                            result = self.cache.put(key, value, ttl=random.uniform(0.1, 1.0))
                        else:  # delete
                            result = self.cache.delete(key)
                            
                        self.record_pattern(AccessPattern(
                            pattern='hammer',
                            key=key,
                            thread_id=thread_id,
                            start_time=start_time,
                            end_time=time.time(),
                            success=True
                        ))
                    except Exception as e:
                        logger.error(f"Operation failed: {op} {key} - {str(e)}")
                        self.record_pattern(AccessPattern(
                            pattern='hammer',
                            key=key,
                            thread_id=thread_id,
                            start_time=start_time,
                            end_time=time.time(),
                            success=False,
                            error=str(e)
                        ))
                        
                except Exception as e:
                    logger.error(f"Worker error: {str(e)}")
        
        # Start worker threads
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(worker, i) for i in range(20)]
            
            # Run for 10 seconds
            time.sleep(10)
            self.stop_event.set()
            
            # Wait for completion
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Thread error: {str(e)}")
        
        # Check metrics
        stats = self.cache.get_stats()
        logger.info("\nHammer Test Results:")
        logger.info(f"Total operations: {stats['hits'] + stats['misses'] + stats['puts'] + stats['deletes']}")
        logger.info(f"Hit rate: {stats['hit_rate']:.2%}")
        logger.info(f"Throughput: {stats['throughput']}")

if __name__ == '__main__':
    unittest.main() 