from threading import Thread, Event
from collections import Counter
from typing import Dict, Any
import time
import threading
from dataclasses import dataclass, field
from queue import Queue
import logging

logger = logging.getLogger(__name__)

@dataclass
class CacheMetrics:
    """Thread-safe metrics collection for cache operations."""
    # Atomic counters using threading.Lock
    _hit_count: int = 0
    _miss_count: int = 0
    _expiry_count: int = 0
    _put_count: int = 0
    _delete_count: int = 0
    
    # Operation timing using thread-local storage
    _timing_data: Dict[str, list] = field(default_factory=lambda: {
        'get': [],
        'put': [],
        'delete': []
    })
    
    # Thread-local storage for operation timing
    _thread_local = threading.local()
    
    # Background reporting
    _report_queue: Queue = field(default_factory=Queue)
    _stop_event: Event = field(default_factory=Event)
    _report_thread: Thread = None
    
    def __post_init__(self):
        """Start the background reporting thread."""
        self._report_thread = Thread(target=self._report_metrics, daemon=True)
        self._report_thread.start()
    
    def record_hit(self):
        """Record a cache hit."""
        self._hit_count += 1
    
    def record_miss(self):
        """Record a cache miss."""
        self._miss_count += 1
    
    def record_expiry(self):
        """Record a TTL expiry."""
        self._expiry_count += 1
    
    def record_put(self):
        """Record a put operation."""
        self._put_count += 1
    
    def record_delete(self):
        """Record a delete operation."""
        self._delete_count += 1
    
    def start_operation(self, op_type: str):
        """Start timing an operation."""
        if not hasattr(self._thread_local, 'start_times'):
            self._thread_local.start_times = {}
        self._thread_local.start_times[op_type] = time.perf_counter()
    
    def end_operation(self, op_type: str):
        """End timing an operation and record the duration."""
        if hasattr(self._thread_local, 'start_times') and op_type in self._thread_local.start_times:
            duration = time.perf_counter() - self._thread_local.start_times[op_type]
            self._timing_data[op_type].append(duration)
            # Keep only last 1000 measurements to prevent unbounded growth
            if len(self._timing_data[op_type]) > 1000:
                self._timing_data[op_type] = self._timing_data[op_type][-1000:]
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics snapshot."""
        # Calculate throughput (operations per second) for last minute
        now = time.time()
        throughput = {}
        for op_type in ['get', 'put', 'delete']:
            if self._timing_data[op_type]:
                # Calculate average time per operation
                avg_time = sum(self._timing_data[op_type]) / len(self._timing_data[op_type])
                # Convert to operations per second
                throughput[op_type] = 1.0 / avg_time if avg_time > 0 else 0
        
        return {
            'hits': self._hit_count,
            'misses': self._miss_count,
            'hit_rate': self._hit_count / (self._hit_count + self._miss_count) if (self._hit_count + self._miss_count) > 0 else 0,
            'expiries': self._expiry_count,
            'puts': self._put_count,
            'deletes': self._delete_count,
            'throughput': throughput,
            'memory_usage': self._get_memory_usage()
        }
    
    def _get_memory_usage(self) -> Dict[str, float]:
        """Get memory usage metrics."""
        # This would be implemented to get actual memory usage from the cache
        # For now returning placeholder
        return {
            'used_mb': 0.0,
            'max_mb': 0.0,
            'usage_percent': 0.0
        }
    
    def _report_metrics(self):
        """Background thread that reports metrics periodically."""
        while not self._stop_event.is_set():
            try:
                metrics = self.get_metrics()
                # Log metrics every 60 seconds
                logger.info("Cache Metrics: %s", metrics)
                # Here you could also send metrics to Prometheus or other monitoring systems
                time.sleep(60)
            except Exception as e:
                logger.error("Error reporting metrics: %s", e)
    
    def shutdown(self):
        """Shutdown the metrics reporting thread."""
        self._stop_event.set()
        if self._report_thread:
            self._report_thread.join(timeout=5.0) 