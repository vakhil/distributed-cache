from typing import Any, Dict, Optional, Tuple, TypeVar, Generic
import time
from collections import OrderedDict
import threading
import threading.local
from concurrent.futures import ThreadPoolExecutor
from .entry import CacheEntry
from .memory import MemoryManager
from .eviction import EvictionPolicy, LRUEviction, SizeBasedEviction
from .metrics import CacheMetrics

K = TypeVar('K')
V = TypeVar('V')

class Cache(Generic[K, V]):
    """
    A high-performance, lock-free in-memory cache with minimal contention.
    Uses atomic operations and thread-local data where possible.
    """
    
    def __init__(
        self,
        max_memory_mb: int = 200,
        max_size: Optional[int] = None,
        eviction_policy: Optional[EvictionPolicy[K, V]] = None
    ):
        """
        Initialize the cache.
        
        Args:
            max_memory_mb: Maximum memory usage in megabytes
            max_size: Maximum number of entries (None for unlimited)
            eviction_policy: Policy to use for eviction (defaults to LRU)
        """
        # Main storage - using dict which is thread-safe for single operations
        self._entries: Dict[K, CacheEntry] = {}
        
        # Thread-local storage for operation tracking
        self._thread_local = threading.local()
        
        # Memory management
        self._memory_manager = MemoryManager(max_memory_mb)
        self._max_size = max_size
        
        # Metrics
        self._metrics = CacheMetrics()
        
        # Eviction policy
        self._eviction_policy = eviction_policy or LRUEviction[K, V]()
        if isinstance(self._eviction_policy, SizeBasedEviction):
            self._size_policy = self._eviction_policy
        else:
            self._size_policy = None
            
        # Minimal lock only for eviction to prevent thundering herd
        self._eviction_lock = threading.Lock()
    
    def get(self, key: K) -> Optional[V]:
        """
        Get a value from the cache.
        Uses atomic dictionary operations for thread safety.
        
        Args:
            key: The key to look up
            
        Returns:
            The value if found and not expired, None otherwise
        """
        self._metrics.start_operation('get')
        try:
            # Atomic dictionary get
            entry = self._entries.get(key)
            
            if entry is None:
                self._metrics.record_miss()
                return None
                
            # Check expiry (atomic operation)
            if entry.is_expired():
                self._metrics.record_expiry()
                # Use atomic dict operation for removal
                self._entries.pop(key, None)
                entry.cleanup()
                self._metrics.record_miss()
                return None
            
            # Update eviction policy (thread-local)
            self._eviction_policy.on_get(key)
            self._metrics.record_hit()
            return entry.value
        finally:
            self._metrics.end_operation('get')
    
    def put(self, key: K, value: V, ttl: Optional[float] = None) -> bool:
        """
        Put a value into the cache.
        Uses atomic operations and minimal locking for eviction.
        
        Args:
            key: The key to store under
            value: The value to store
            ttl: Time to live in seconds (None for no expiration)
            
        Returns:
            True if the value was stored, False if we couldn't make space
        """
        self._metrics.start_operation('put')
        try:
            # Calculate size of new entry
            new_size = self._memory_manager._get_size(value)
            
            # Check if we need to evict (atomic check)
            if self._memory_manager.would_exceed_limit(new_size):
                # Only lock during eviction to prevent thundering herd
                with self._eviction_lock:
                    while not self._ensure_space(key, value):
                        if not self._evict_one():
                            return False
            
            # Create new entry
            entry = CacheEntry.create(
                value=value,
                ttl=ttl,
                memory_manager=self._memory_manager,
                key=key
            )
            
            # Atomic update of size tracking
            if self._size_policy:
                self._size_policy.update_size(key, entry.size_bytes)
            
            # Atomic removal of old entry if exists
            old_entry = self._entries.pop(key, None)
            if old_entry:
                old_entry.cleanup()
                self._eviction_policy.on_delete(key)
            
            # Atomic insertion of new entry
            self._entries[key] = entry
            self._eviction_policy.on_put(key)
            self._metrics.record_put()
            return True
        finally:
            self._metrics.end_operation('put')
    
    def delete(self, key: K) -> bool:
        """
        Delete a value from the cache.
        Uses atomic dictionary operations.
        
        Args:
            key: The key to delete
            
        Returns:
            True if the key was found and deleted
        """
        self._metrics.start_operation('delete')
        try:
            # Atomic removal
            entry = self._entries.pop(key, None)
            if entry:
                entry.cleanup()
                self._eviction_policy.on_delete(key)
                self._metrics.record_delete()
                return True
            return False
        finally:
            self._metrics.end_operation('delete')
    
    def _remove_entry(self, key : K) -> None:
        """Remove an entry using atomic operations."""
        entry = self._entries.pop(key, None)
        if entry:
            entry.cleanup()
            self._eviction_policy.on_delete(key)
    
    def _ensure_space(self, key: K, value: V) -> bool:
        """
        Ensure there is space for a new entry.
        Uses atomic operations for checks.
        """
        # Atomic size check
        new_size = self._memory_manager._get_size(value)
        
        # Atomic memory check
        if not self._memory_manager.would_exceed_limit(new_size):
            # Atomic size limit check
            if not self._max_size or len(self._entries) < self._max_size:
                return True
        
        return False
    
    def _evict_one(self) -> bool:
        """
        Evict one entry using the current eviction policy.
        Protected by eviction lock to prevent thundering herd.
        """
        key = self._eviction_policy.evict_one(self._entries)
        if key is None:
            return False
        self._remove_entry(key)
        return True
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary containing cache statistics
        """
        current_memory, max_memory = self._memory_manager.get_memory_usage()
        stats = {
            'size': len(self._entries),
            'max_size': self._max_size,
            'memory_used_mb': current_memory / (1024 * 1024),
            'max_memory_mb': max_memory / (1024 * 1024),
            'memory_usage_percent': (current_memory / max_memory) * 100 if max_memory > 0 else 0,
            'eviction_policy': self._eviction_policy.__class__.__name__
        }
        # Add metrics to stats
        stats.update(self._metrics.get_metrics())
        return stats
    
    def shutdown(self):
        """Shutdown the cache and its metrics reporting."""
        self._metrics.shutdown() 