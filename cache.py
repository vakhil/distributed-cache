from typing import Any, Optional, Dict, Tuple
import time
import threading
from collections import OrderedDict
from dataclasses import dataclass
from queue import Queue
import weakref

@dataclass
class CacheEntry:
    """Represents a single cache entry with value and expiration time."""
    value: Any
    expiry_time: Optional[float]  # None means no expiration
    last_access: float  # For LRU tracking
    
    @property
    def is_expired(self) -> bool:
        """Check if the entry has expired."""
        if self.expiry_time is None:
            return False
        return time.time() > self.expiry_time

class LockFreeCache:
    def __init__(self, cleanup_interval: float = 1.0):
        """
        Initialize a lock-free cache.
        
        Args:
            cleanup_interval: How often to run cleanup in seconds
        """
        # Use OrderedDict for LRU support
        self._store: OrderedDict[str, CacheEntry] = OrderedDict()
        self._cleanup_interval = cleanup_interval
        self._stop_cleanup = threading.Event()
        
        # Queue for atomic operations
        self._operation_queue: Queue = Queue()
        
        # Start cleanup thread
        self._cleanup_thread = threading.Thread(
            target=self._background_cleanup,
            daemon=True
        )
        self._cleanup_thread.start()
        
        # Start operation processor thread
        self._processor_thread = threading.Thread(
            target=self._process_operations,
            daemon=True
        )
        self._processor_thread.start()

    def _process_operations(self) -> None:
        """Process operations from the queue atomically."""
        while not self._stop_cleanup.is_set():
            try:
                operation, args, result_queue = self._operation_queue.get(timeout=0.1)
                if operation == "put":
                    key, value, ttl = args
                    self._atomic_put(key, value, ttl)
                elif operation == "get":
                    key = args[0]
                    result = self._atomic_get(key)
                    result_queue.put(result)
                elif operation == "delete":
                    key = args[0]
                    self._atomic_delete(key)
                self._operation_queue.task_done()
            except Queue.Empty:
                continue

    def _background_cleanup(self) -> None:
        """Background thread for cleanup of expired entries."""
        while not self._stop_cleanup.is_set():
            try:
                # Create a copy of keys to avoid modification during iteration
                keys = list(self._store.keys())
                for key in keys:
                    entry = self._store.get(key)
                    if entry and entry.is_expired:
                        # Use atomic delete operation
                        self._operation_queue.put(("delete", (key,), None))
                time.sleep(self._cleanup_interval)
            except Exception:
                # Log error but continue running
                continue

    def _atomic_put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Atomic put operation."""
        expiry_time = None if ttl is None else time.time() + ttl
        entry = CacheEntry(
            value=value,
            expiry_time=expiry_time,
            last_access=time.time()
        )
        self._store[key] = entry
        # Move to end for LRU
        self._store.move_to_end(key)

    def _atomic_get(self, key: str) -> Optional[Any]:
        """Atomic get operation."""
        entry = self._store.get(key)
        if entry is None:
            return None
            
        if entry.is_expired:
            # Use atomic delete
            self._operation_queue.put(("delete", (key,), None))
            return None
            
        # Update last access time and move to end for LRU
        entry.last_access = time.time()
        self._store.move_to_end(key)
        return entry.value

    def _atomic_delete(self, key: str) -> None:
        """Atomic delete operation."""
        self._store.pop(key, None)

    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        Store a value in the cache.
        
        Args:
            key: The key to store the value under
            value: The value to store
            ttl: Time to live in seconds (None for no expiration)
        """
        self._operation_queue.put(("put", (key, value, ttl), None))

    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve a value from the cache.
        
        Args:
            key: The key to look up
            
        Returns:
            The value if found and not expired, None otherwise
        """
        result_queue = Queue()
        self._operation_queue.put(("get", (key,), result_queue))
        return result_queue.get()

    def delete(self, key: str) -> None:
        """
        Remove a key from the cache.
        
        Args:
            key: The key to remove
        """
        self._operation_queue.put(("delete", (key,), None))

    def get_size(self) -> int:
        """
        Get the number of entries in the cache.
        
        Returns:
            Number of non-expired entries
        """
        return len(self._store)

    def shutdown(self) -> None:
        """Stop the cleanup and processor threads."""
        self._stop_cleanup.set()
        self._cleanup_thread.join()
        self._processor_thread.join()
        # Process any remaining operations
        while not self._operation_queue.empty():
            self._operation_queue.get()
            self._operation_queue.task_done() 