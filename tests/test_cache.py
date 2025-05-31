import time
from src.cache.cache import Cache
from src.cache.eviction import NoOpEviction, LRUEviction, LFUEviction, SizeBasedEviction

def test_memory_management():
    """Test memory management and limits."""
    cache = Cache(max_memory_mb=1)  # 1MB limit
    
    # Store a small value
    assert cache.put("key1", "small value")
    assert cache.get("key1") == "small value"
    
    # Store a large value that would exceed limit
    large_value = "x" * (2 * 1024 * 1024)  # 2MB string
    assert not cache.put("key2", large_value)
    assert cache.get("key2") is None
    
    # Verify memory stats
    stats = cache.get_stats()
    assert stats['max_memory_mb'] == 1
    assert stats['memory_used_mb'] < 1
    assert stats['memory_usage_percent'] < 100

def test_eviction():
    """Test LRU eviction when memory limit is reached."""
    cache = Cache(max_memory_mb=1)
    
    # Fill cache with values
    for i in range(10):
        value = "x" * (100 * 1024)  # 100KB each
        assert cache.put(f"key{i}", value)
    
    # Verify some entries were evicted
    stats = cache.get_stats()
    assert stats['memory_used_mb'] <= 1
    assert stats['memory_usage_percent'] <= 100
    
    # Most recently used should still be there
    assert cache.get("key9") is not None
    
    # Least recently used might be evicted
    assert cache.get("key0") is None

def test_size_limit():
    """Test size-based eviction."""
    cache = Cache(max_size=3)
    
    # Fill cache to limit
    assert cache.put("key1", "value1")
    assert cache.put("key2", "value2")
    assert cache.put("key3", "value3")
    
    # Try to add one more
    assert cache.put("key4", "value4")
    
    # Verify size limit
    stats = cache.get_stats()
    assert stats['size'] == 3
    assert stats['max_size'] == 3
    
    # Least recently used should be evicted
    assert cache.get("key1") is None
    assert cache.get("key4") is not None

def test_memory_and_size_limits():
    """Test interaction between memory and size limits."""
    cache = Cache(max_memory_mb=1, max_size=5)
    
    # Fill with small values (under size limit but over memory limit)
    for i in range(10):
        value = "x" * (200 * 1024)  # 200KB each
        if i < 5:
            assert cache.put(f"key{i}", value)
        else:
            assert not cache.put(f"key{i}", value)
    
    stats = cache.get_stats()
    assert stats['size'] <= 5
    assert stats['memory_used_mb'] <= 1

def test_memory_tracking():
    """Test accurate memory tracking."""
    cache = Cache(max_memory_mb=1)
    
    # Store different types of values
    assert cache.put("str", "hello" * 1000)
    assert cache.put("list", [1, 2, 3] * 1000)
    assert cache.put("dict", {"key": "value" * 1000})
    
    # Update value and verify memory is updated
    assert cache.put("str", "new value" * 1000)
    
    stats = cache.get_stats()
    assert stats['memory_used_mb'] <= 1
    assert stats['memory_usage_percent'] <= 100

def test_cleanup():
    """Test memory cleanup on deletion."""
    cache = Cache(max_memory_mb=1)
    
    # Store and delete values
    assert cache.put("key1", "x" * (500 * 1024))
    assert cache.put("key2", "x" * (500 * 1024))
    
    initial_stats = cache.get_stats()
    assert initial_stats['memory_used_mb'] > 0
    
    # Delete one entry
    assert cache.delete("key1")
    
    # Verify memory was freed
    stats = cache.get_stats()
    assert stats['memory_used_mb'] < initial_stats['memory_used_mb']
    
    # Delete all entries
    assert cache.delete("key2")
    stats = cache.get_stats()
    assert stats['memory_used_mb'] == 0
    assert stats['size'] == 0

def test_no_op_eviction():
    """Test NoOpEviction policy that never evicts."""
    cache = Cache(max_memory_mb=1, eviction_policy=NoOpEviction())
    
    # Fill cache beyond memory limit
    for i in range(10):
        value = "x" * (200 * 1024)  # 200KB each
        if i == 0:
            assert cache.put(f"key{i}", value)
        else:
            assert not cache.put(f"key{i}", value)
    
    # Verify no entries were evicted
    assert cache.get("key0") is not None
    assert len(cache._entries) == 1

def test_lru_eviction():
    """Test LRU eviction policy."""
    cache = Cache(max_memory_mb=1, eviction_policy=LRUEviction())
    
    # Fill cache with values
    for i in range(5):
        value = "x" * (200 * 1024)  # 200KB each
        assert cache.put(f"key{i}", value)
    
    # Access some keys to update LRU order
    cache.get("key0")
    cache.get("key2")
    
    # Add more values to trigger eviction
    for i in range(5, 10):
        value = "x" * (200 * 1024)
        assert cache.put(f"key{i}", value)
    
    # Least recently used (key1) should be evicted
    assert cache.get("key1") is None
    # Most recently used (key0, key2) should still be there
    assert cache.get("key0") is not None
    assert cache.get("key2") is not None

def test_lfu_eviction():
    """Test LFU eviction policy."""
    cache = Cache(max_memory_mb=1, eviction_policy=LFUEviction())
    
    # Fill cache with values
    for i in range(5):
        value = "x" * (200 * 1024)  # 200KB each
        assert cache.put(f"key{i}", value)
    
    # Access some keys multiple times
    for _ in range(3):
        cache.get("key0")
    for _ in range(2):
        cache.get("key1")
    cache.get("key2")
    
    # Add more values to trigger eviction
    for i in range(5, 10):
        value = "x" * (200 * 1024)
        assert cache.put(f"key{i}", value)
    
    # Least frequently used (key3, key4) should be evicted
    assert cache.get("key3") is None
    assert cache.get("key4") is None
    # Most frequently used (key0) should still be there
    assert cache.get("key0") is not None

def test_size_based_eviction():
    """Test size-based eviction policy."""
    cache = Cache(max_memory_mb=1, eviction_policy=SizeBasedEviction())
    
    # Add entries of different sizes
    assert cache.put("small", "x" * (100 * 1024))  # 100KB
    assert cache.put("medium", "x" * (300 * 1024))  # 300KB
    assert cache.put("large", "x" * (500 * 1024))  # 500KB
    
    # Add one more to trigger eviction
    assert cache.put("new", "x" * (400 * 1024))  # 400KB
    
    # Largest entry should be evicted
    assert cache.get("large") is None
    # Smaller entries should remain
    assert cache.get("small") is not None
    assert cache.get("medium") is not None
    assert cache.get("new") is not None

def test_eviction_policy_stats():
    """Test that eviction policy is included in stats."""
    policies = [
        NoOpEviction(),
        LRUEviction(),
        LFUEviction(),
        SizeBasedEviction()
    ]
    
    for policy in policies:
        cache = Cache(eviction_policy=policy)
        stats = cache.get_stats()
        assert stats['eviction_policy'] == policy.__class__.__name__

def test_eviction_policy_cleanup():
    """Test that eviction policies clean up properly on deletion."""
    cache = Cache(eviction_policy=LRUEviction())
    
    # Add and access some entries
    assert cache.put("key1", "value1")
    assert cache.put("key2", "value2")
    cache.get("key1")
    cache.get("key2")
    
    # Delete an entry
    assert cache.delete("key1")
    
    # Add more entries to trigger eviction
    assert cache.put("key3", "value3")
    assert cache.put("key4", "value4")
    
    # Deleted key should not be considered for eviction
    assert "key1" not in cache._eviction_policy._access_order

def test_mixed_eviction_policies():
    """Test interaction between different eviction policies."""
    # Create cache with size-based eviction
    cache = Cache(
        max_memory_mb=1,
        max_size=3,
        eviction_policy=SizeBasedEviction()
    )
    
    # Add entries of different sizes
    assert cache.put("tiny", "x" * (50 * 1024))  # 50KB
    assert cache.put("small", "x" * (100 * 1024))  # 100KB
    assert cache.put("medium", "x" * (300 * 1024))  # 300KB
    
    # Add one more to trigger eviction
    assert cache.put("new", "x" * (200 * 1024))  # 200KB
    
    # Verify eviction behavior
    stats = cache.get_stats()
    assert stats['size'] <= 3
    assert stats['memory_used_mb'] <= 1
    assert stats['eviction_policy'] == 'SizeBasedEviction' 