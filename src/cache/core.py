from typing import Any, Optional
from .lru import LRUTracker
from .operations import OperationProcessor
from .cleanup import CleanupManager

class LockFreeCache:
    """
    A lock-free cache implementation that uses a queue-based approach
    for thread safety and atomic operations.
    """
    
    def __init__(self, cleanup_interval: float = 1.0):
        """
        Initialize the cache.
        
        Args:
            cleanup_interval: How often to run cleanup in seconds
        """
        # Initialize components
        self._store = LRUTracker()
        self._processor = OperationProcessor()
        self._cleanup = CleanupManager(self._store, cleanup_interval)
        
        # Connect processor to store
        self._processor._store = self._store
    
    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        Store a value in the cache.
        
        Args:
            key: The key to store the value under
            value: The value to store
            ttl: Time to live in seconds (None for no expiration)
        """
        self._processor.put(key, value, ttl)
    
    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve a value from the cache.
        
        Args:
            key: The key to look up
            
        Returns:
            The value if found and not expired, None otherwise
        """
        return self._processor.get(key)
    
    def delete(self, key: str) -> None:
        """
        Remove a key from the cache.
        
        Args:
            key: The key to remove
        """
        self._processor.delete(key)
    
    def get_size(self) -> int:
        """
        Get the number of entries in the cache.
        
        Returns:
            Number of non-expired entries
        """
        return self._store.get_size()
    
    def shutdown(self) -> None:
        """Stop all background threads and cleanup resources."""
        self._cleanup.shutdown()
        self._processor.shutdown() 