from queue import Queue, Empty
from typing import Any, Optional, Callable, Tuple
import threading
from dataclasses import dataclass
from .entry import CacheEntry

@dataclass
class Operation:
    """Represents a cache operation to be processed."""
    type: str  # 'put', 'get', or 'delete'
    key: str
    value: Any = None
    ttl: Optional[int] = None
    result_queue: Optional[Queue] = None

class OperationProcessor:
    """
    Processes cache operations through a queue to ensure atomicity.
    All operations are processed sequentially by a single thread.
    """
    
    def __init__(self):
        """Initialize the operation processor."""
        self._queue: Queue[Operation] = Queue()
        self._stop_event = threading.Event()
        self._processor_thread = threading.Thread(
            target=self._process_operations,
            daemon=True
        )
        self._processor_thread.start()
    
    def _process_operations(self) -> None:
        """Process operations from the queue."""
        while not self._stop_event.is_set():
            try:
                operation = self._queue.get(timeout=0.1)
                self._handle_operation(operation)
                self._queue.task_done()
            except Empty:
                continue
    
    def _handle_operation(self, operation: Operation) -> None:
        """
        Handle a single operation.
        
        Args:
            operation: The operation to process
        """
        if operation.type == 'put':
            self._handle_put(operation)
        elif operation.type == 'get':
            self._handle_get(operation)
        elif operation.type == 'delete':
            self._handle_delete(operation)
    
    def _handle_put(self, operation: Operation) -> None:
        """Handle a put operation."""
        if not hasattr(self, '_store'):
            return
        entry = CacheEntry.create(operation.value, operation.ttl)
        self._store.put(operation.key, entry)
    
    def _handle_get(self, operation: Operation) -> None:
        """Handle a get operation."""
        if not hasattr(self, '_store') or operation.result_queue is None:
            return
        entry = self._store.get(operation.key)
        if entry is None or entry.is_expired:
            if entry is not None and entry.is_expired:
                self._store.delete(operation.key)
            operation.result_queue.put(None)
        else:
            operation.result_queue.put(entry.value)
    
    def _handle_delete(self, operation: Operation) -> None:
        """Handle a delete operation."""
        if not hasattr(self, '_store'):
            return
        self._store.delete(operation.key)
    
    def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        Queue a put operation.
        
        Args:
            key: The key to store
            value: The value to store
            ttl: Time to live in seconds
        """
        operation = Operation(type='put', key=key, value=value, ttl=ttl)
        self._queue.put(operation)
    
    def get(self, key: str) -> Any:
        """
        Queue a get operation and wait for result.
        
        Args:
            key: The key to look up
            
        Returns:
            The value if found and not expired, None otherwise
        """
        result_queue = Queue()
        operation = Operation(type='get', key=key, result_queue=result_queue)
        self._queue.put(operation)
        return result_queue.get()
    
    def delete(self, key: str) -> None:
        """
        Queue a delete operation.
        
        Args:
            key: The key to remove
        """
        operation = Operation(type='delete', key=key)
        self._queue.put(operation)
    
    def shutdown(self) -> None:
        """Stop the operation processor."""
        self._stop_event.set()
        self._processor_thread.join()
        # Process any remaining operations
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
                self._queue.task_done()
            except Empty:
                break 