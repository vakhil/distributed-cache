import sys
from typing import Any, Dict, Tuple
import time

class MemoryManager:
    """
    Manages memory allocation and tracking for cache entries.
    Implements Redis-like zmalloc and zfree functions.
    """
    
    def __init__(self, max_memory_mb: int = 200):
        """
        Initialize the memory manager.
        
        Args:
            max_memory_mb: Maximum memory usage in megabytes
        """
        self._max_memory = max_memory_mb * 1024 * 1024  # Convert to bytes
        self._current_memory = 0
        self._entry_sizes: Dict[str, int] = {}  # Track size of each entry
    
    def zmalloc(self, key: str, value: Any) -> int:
        """
        Allocate memory for a value and track its size.
        Similar to Redis zmalloc.
        
        Args:
            key: The key for the value
            value: The value to measure
            
        Returns:
            Size of the value in bytes
        """
        size = self._get_size(value)
        self._entry_sizes[key] = size
        self._current_memory += size
        return size
    
    def zfree(self, key: str) -> int:
        """
        Free memory allocated for a value.
        Similar to Redis zfree.
        
        Args:
            key: The key to free memory for
            
        Returns:
            Amount of memory freed in bytes
        """
        size = self._entry_sizes.pop(key, 0)
        self._current_memory -= size
        return size
    
    def get_memory_usage(self) -> Tuple[int, int]:
        """
        Get current and maximum memory usage.
        
        Returns:
            Tuple of (current_memory, max_memory) in bytes
        """
        return self._current_memory, self._max_memory
    
    def would_exceed_limit(self, new_size: int) -> bool:
        """
        Check if adding a new entry would exceed memory limit.
        
        Args:
            new_size: Size of the new entry in bytes
            
        Returns:
            True if adding the entry would exceed the limit
        """
        return self._current_memory + new_size > self._max_memory
    
    def _get_size(self, obj: Any) -> int:
        """
        Calculate approximate memory size of an object.
        
        Args:
            obj: The object to measure
            
        Returns:
            Size in bytes
        """
        size = sys.getsizeof(obj)
        
        if isinstance(obj, (str, bytes)):
            return size
        
        if isinstance(obj, (list, tuple, set)):
            return size + sum(self._get_size(item) for item in obj)
        
        if isinstance(obj, dict):
            return size + sum(
                self._get_size(k) + self._get_size(v)
                for k, v in obj.items()
            )
        
        # For custom objects, estimate size of their attributes
        if hasattr(obj, '__dict__'):
            return size + self._get_size(obj.__dict__)
        
        return size 