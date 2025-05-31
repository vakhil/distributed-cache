from abc import ABC, abstractmethod
from collections import OrderedDict, defaultdict
from typing import Any, Dict, Optional, Protocol, TypeVar, Generic
import time

# Type variables for generic eviction policies
K = TypeVar('K')
V = TypeVar('V')

class CacheEntry(Protocol):
    """Protocol defining required attributes for cache entries."""
    value: Any
    expires_at: Optional[float]
    key: Optional[str]

class EvictionPolicy(Generic[K, V], ABC):
    """
    Abstract base class for cache eviction policies.
    Each policy must implement how to track and evict entries.
    """
    
    @abstractmethod
    def on_get(self, key: K) -> None:
        """Called when an entry is accessed via get()."""
        pass
    
    @abstractmethod
    def on_put(self, key: K) -> None:
        """Called when an entry is added or updated via put()."""
        pass
    
    @abstractmethod
    def on_delete(self, key: K) -> None:
        """Called when an entry is deleted."""
        pass
    
    @abstractmethod
    def evict_one(self, entries: Dict[K, CacheEntry]) -> Optional[K]:
        """
        Evict one entry according to the policy.
        
        Args:
            entries: Current cache entries
            
        Returns:
            Key of entry to evict, or None if no entry should be evicted
        """
        pass

class NoOpEviction(EvictionPolicy[K, V]):
    """
    No-op eviction policy that never evicts entries.
    Useful for testing or when eviction is not desired.
    """
    
    def on_get(self, key: K) -> None:
        pass
    
    def on_put(self, key: K) -> None:
        pass
    
    def on_delete(self, key: K) -> None:
        pass
    
    def evict_one(self, entries: Dict[K, CacheEntry]) -> Optional[K]:
        return None

class LRUEviction(EvictionPolicy[K, V]):
    """
    Least Recently Used (LRU) eviction policy.
    Uses OrderedDict for O(1) access and atomic updates.
    """
    
    def __init__(self):
        self._access_order: OrderedDict[K, float] = OrderedDict()
    
    def on_get(self, key: K) -> None:
        """Update access time and move to end of order."""
        self._access_order[key] = time.time()
        self._access_order.move_to_end(key)
    
    def on_put(self, key: K) -> None:
        """Add to end of order."""
        self._access_order[key] = time.time()
        self._access_order.move_to_end(key)
    
    def on_delete(self, key: K) -> None:
        """Remove from order."""
        self._access_order.pop(key, None)
    
    def evict_one(self, entries: Dict[K, CacheEntry]) -> Optional[K]:
        """Evict least recently used entry."""
        if not self._access_order:
            return None
        return next(iter(self._access_order))

class LFUEviction(EvictionPolicy[K, V]):
    """
    Least Frequently Used (LFU) eviction policy.
    Tracks access frequency and evicts least frequently used entries.
    """
    
    def __init__(self):
        self._access_count: Dict[K, int] = defaultdict(int)
        self._last_access: Dict[K, float] = {}
    
    def on_get(self, key: K) -> None:
        """Increment access count and update last access time."""
        self._access_count[key] += 1
        self._last_access[key] = time.time()
    
    def on_put(self, key: K) -> None:
        """Initialize access count and last access time."""
        self._access_count[key] = 1
        self._last_access[key] = time.time()
    
    def on_delete(self, key: K) -> None:
        """Remove access tracking."""
        self._access_count.pop(key, None)
        self._last_access.pop(key, None)
    
    def evict_one(self, entries: Dict[K, CacheEntry]) -> Optional[K]:
        """
        Evict least frequently used entry.
        In case of tie, evict the least recently accessed entry.
        """
        if not self._access_count:
            return None
        
        # Find entry with minimum access count
        min_count = min(self._access_count.values())
        candidates = [
            k for k, count in self._access_count.items()
            if count == min_count
        ]
        
        # If multiple candidates, choose least recently accessed
        if len(candidates) > 1:
            return min(candidates, key=lambda k: self._last_access.get(k, float('inf')))
        return candidates[0]

class SizeBasedEviction(EvictionPolicy[K, V]):
    """
    Size-based eviction policy.
    Evicts largest entries when space is needed.
    """
    
    def __init__(self):
        self._sizes: Dict[K, int] = {}
    
    def on_get(self, key: K) -> None:
        pass
    
    def on_put(self, key: K) -> None:
        """Update size tracking."""
        if key in self._sizes:
            self._sizes.pop(key)
    
    def on_delete(self, key: K) -> None:
        """Remove size tracking."""
        self._sizes.pop(key, None)
    
    def update_size(self, key: K, size: int) -> None:
        """Update the size of an entry."""
        self._sizes[key] = size
    
    def evict_one(self, entries: Dict[K, CacheEntry]) -> Optional[K]:
        """Evict largest entry."""
        if not self._sizes:
            return None
        return max(self._sizes.items(), key=lambda x: x[1])[0] 