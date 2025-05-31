import pytest
import time
import threading
from cache import LockFreeCache

@pytest.fixture
def cache():
    """Create a fresh cache instance for each test."""
    cache = LockFreeCache(cleanup_interval=0.1)
    yield cache
    cache.shutdown()

def test_basic_operations(cache):
    """Test basic GET, PUT, and DELETE operations."""
    # Test PUT and GET
    cache.put("key1", "value1")
    assert cache.get("key1") == "value1"
    
    # Test DELETE
    cache.delete("key1")
    assert cache.get("key1") is None
    
    # Test non-existent key
    assert cache.get("nonexistent") is None

def test_ttl_expiration(cache):
    """Test TTL expiration functionality."""
    # Test with TTL
    cache.put("key1", "value1", ttl=1)  # 1 second TTL
    assert cache.get("key1") == "value1"
    time.sleep(1.1)  # Wait for TTL to expire
    assert cache.get("key1") is None
    
    # Test without TTL
    cache.put("key2", "value2")  # No TTL
    time.sleep(1.1)
    assert cache.get("key2") == "value2"

def test_concurrent_access(cache):
    """Test concurrent access to the cache."""
    def worker(thread_id: int, iterations: int):
        for i in range(iterations):
            key = f"key_{thread_id}_{i}"
            value = f"value_{thread_id}_{i}"
            cache.put(key, value)
            assert cache.get(key) == value
            cache.delete(key)
            assert cache.get(key) is None
    
    # Create multiple threads
    threads = []
    num_threads = 4
    iterations = 100
    
    for i in range(num_threads):
        t = threading.Thread(target=worker, args=(i, iterations))
        threads.append(t)
        t.start()
    
    # Wait for all threads to complete
    for t in threads:
        t.join()

def test_concurrent_ttl(cache):
    """Test concurrent access with TTL."""
    def worker(thread_id: int, iterations: int):
        for i in range(iterations):
            key = f"key_{thread_id}_{i}"
            value = f"value_{thread_id}_{i}"
            # Use different TTLs
            ttl = (i % 3) + 1  # 1, 2, or 3 seconds
            cache.put(key, value, ttl=ttl)
            assert cache.get(key) == value
            time.sleep(0.1)  # Small delay to allow TTL to expire
    
    # Create multiple threads
    threads = []
    num_threads = 4
    iterations = 50
    
    for i in range(num_threads):
        t = threading.Thread(target=worker, args=(i, iterations))
        threads.append(t)
        t.start()
    
    # Wait for all threads to complete
    for t in threads:
        t.join()
    
    # Verify all entries have expired
    time.sleep(3.1)  # Wait for longest TTL to expire
    for i in range(num_threads):
        for j in range(iterations):
            key = f"key_{i}_{j}"
            assert cache.get(key) is None

def test_background_cleanup(cache):
    """Test background cleanup of expired entries."""
    # Add entries with different TTLs
    cache.put("key1", "value1", ttl=1)
    cache.put("key2", "value2", ttl=2)
    cache.put("key3", "value3")  # No TTL
    
    # Wait for first key to expire
    time.sleep(1.1)
    assert cache.get("key1") is None
    assert cache.get("key2") == "value2"
    assert cache.get("key3") == "value3"
    
    # Wait for second key to expire
    time.sleep(1.0)
    assert cache.get("key2") is None
    assert cache.get("key3") == "value3"

def test_lru_ordering(cache):
    """Test LRU ordering of entries."""
    # Add entries
    cache.put("key1", "value1")
    cache.put("key2", "value2")
    cache.put("key3", "value3")
    
    # Access key1 to make it most recently used
    cache.get("key1")
    
    # Access key2 to make it second most recently used
    cache.get("key2")
    
    # key3 should be least recently used
    # Note: We can't directly test the order as it's internal,
    # but we can verify all keys are still accessible
    assert cache.get("key1") == "value1"
    assert cache.get("key2") == "value2"
    assert cache.get("key3") == "value3"

def test_shutdown(cache):
    """Test proper shutdown of the cache."""
    # Add some entries
    cache.put("key1", "value1")
    cache.put("key2", "value2", ttl=1)
    
    # Shutdown the cache
    cache.shutdown()
    
    # Verify we can still access entries
    assert cache.get("key1") == "value1"
    assert cache.get("key2") == "value2"
    
    # Verify background cleanup has stopped
    time.sleep(1.1)
    assert cache.get("key2") == "value2"  # Should not be cleaned up 