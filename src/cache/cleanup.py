import threading
import time
from typing import Optional
from .lru import LRUTracker

class CleanupManager:
    """
    Manages background cleanup of expired entries.
    Runs in a separate thread and doesn't block main operations.
    """
    
    def __init__(self, store: LRUTracker, cleanup_interval: float = 1.0):
        """
        Initialize the cleanup manager.
        
        Args:
            store: The LRU store to clean up
            cleanup_interval: How often to run cleanup in seconds
        """
        self._store = store
        self._cleanup_interval = cleanup_interval
        self._stop_event = threading.Event()
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            daemon=True
        )
        self._cleanup_thread.start()
    
    def _cleanup_loop(self) -> None:
        """Main cleanup loop."""
        while not self._stop_event.is_set():
            try:
                self._cleanup_expired()
                time.sleep(self._cleanup_interval)
            except Exception:
                # Log error but continue running
                continue
    
    def _cleanup_expired(self) -> None:
        """Remove expired entries from the store."""
        # Get a copy of entries to avoid modification during iteration
        entries = self._store.get_entries()
        for key, entry in entries:
            if entry.is_expired:
                self._store.delete(key)
    
    def shutdown(self) -> None:
        """Stop the cleanup manager."""
        self._stop_event.set()
        self._cleanup_thread.join() 