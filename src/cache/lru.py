from collections import OrderedDict
from typing import Any, Optional
from .entry import CacheEntry

class LRUTracker:
    """
    Tracks least recently used entries using OrderedDict.
    The most recently accessed items are moved to the end.
    """
    
    def __init__(self):
        """Initialize an empty LRU tracker."""
        self._store: OrderedDict[str, CacheEntry] = OrderedDict()
    
    def put(self, key: str, entry: CacheEntry) -> None:
        """
        Add or update an entry, marking it as most recently used.
        
        Args:
            key: The key for the entry
            entry: The cache entry to store
        """
        self._store[key] = entry
        self._store.move_to_end(key)
    
    def get(self, key: str) -> Optional[CacheEntry]:
        """
        Get an entry and mark it as most recently used.
        
        Args:
            key: The key to look up
            
        Returns:
            The cache entry if found, None otherwise
        """
        entry = self._store.get(key)
        if entry is not None:
            entry.update_access()
            self._store.move_to_end(key)
        return entry
    
    def delete(self, key: str) -> None:
        """
        Remove an entry.
        
        Args:
            key: The key to remove
        """
        self._store.pop(key, None)
    
    def clear(self) -> None:
        """Remove all entries."""
        self._store.clear()
    
    def get_size(self) -> int:
        """
        Get the number of entries.
        
        Returns:
            Number of entries in the store
        """
        return len(self._store)
    
    def get_entries(self) -> list[tuple[str, CacheEntry]]:
        """
        Get all entries in order of least to most recently used.
        
        Returns:
            List of (key, entry) tuples
        """
        return list(self._store.items())
    
    def get_least_recently_used(self) -> Optional[tuple[str, CacheEntry]]:
        """
        Get the least recently used entry.
        
        Returns:
            Tuple of (key, entry) for the least recently used item,
            or None if the store is empty
        """
        if not self._store:
            return None
        key = next(iter(self._store))
        return key, self._store[key] 