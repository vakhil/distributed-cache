import os
import json
import asyncio
import logging
import time
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class OperationType(Enum):
    PUT = "PUT"
    DELETE = "DELETE"
    TTL = "TTL"

@dataclass
class WALEntry:
    timestamp: float
    operation: OperationType
    key: str
    value: Optional[Any] = None
    ttl: Optional[float] = None
    sequence: int = 0

class WALConfig:
    def __init__(
        self,
        wal_dir: str = "wal",
        max_wal_size: int = 100 * 1024 * 1024,  # 100MB
        flush_interval: float = 1.0,  # seconds
        sync_on_write: bool = False,
        max_entries_per_file: int = 100000
    ):
        self.wal_dir = wal_dir
        self.max_wal_size = max_wal_size
        self.flush_interval = flush_interval
        self.sync_on_write = sync_on_write
        self.max_entries_per_file = max_entries_per_file

class WriteAheadLog:
    def __init__(self, node_id: str, config: Optional[WALConfig] = None):
        self.node_id = node_id
        self.config = config or WALConfig()
        self._current_file: Optional[str] = None
        self._current_sequence = 0
        self._buffer: List[WALEntry] = []
        self._buffer_lock = asyncio.Lock()
        self._flush_task: Optional[asyncio.Task] = None
        self._running = False
        self._last_flush = time.time()
        
        # Create WAL directory if it doesn't exist
        os.makedirs(self.config.wal_dir, exist_ok=True)
        
        # Find the latest sequence number
        self._current_sequence = self._find_latest_sequence()
        
    def _find_latest_sequence(self) -> int:
        """Find the latest sequence number from existing WAL files."""
        try:
            files = [f for f in os.listdir(self.config.wal_dir) 
                    if f.startswith(f"wal_{self.node_id}_") and f.endswith(".log")]
            if not files:
                return 0
            
            # Get the latest file
            latest_file = max(files, key=lambda x: int(x.split("_")[-1].split(".")[0]))
            with open(os.path.join(self.config.wal_dir, latest_file), 'r') as f:
                # Read the last line to get the sequence
                for line in f:
                    pass
                if line.strip():
                    entry = json.loads(line)
                    return entry.get('sequence', 0) + 1
        except Exception as e:
            logger.error(f"Error finding latest sequence: {e}")
        return 0

    def _get_next_wal_file(self) -> str:
        """Get the next WAL file name."""
        return os.path.join(
            self.config.wal_dir,
            f"wal_{self.node_id}_{self._current_sequence}.log"
        )

    async def start(self):
        """Start the WAL system."""
        if self._running:
            return
        
        self._running = True
        self._current_file = self._get_next_wal_file()
        self._flush_task = asyncio.create_task(self._flush_loop())
        logger.info(f"WAL started for node {self.node_id}")

    async def stop(self):
        """Stop the WAL system and flush any pending entries."""
        if not self._running:
            return
        
        self._running = False
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        
        # Flush any remaining entries
        async with self._buffer_lock:
            if self._buffer:
                await self._flush_buffer()
        
        logger.info(f"WAL stopped for node {self.node_id}")

    async def _flush_loop(self):
        """Background task to periodically flush the buffer."""
        while self._running:
            try:
                now = time.time()
                if now - self._last_flush >= self.config.flush_interval:
                    async with self._buffer_lock:
                        if self._buffer:
                            await self._flush_buffer()
                    self._last_flush = now
            except Exception as e:
                logger.error(f"Error in flush loop: {e}")
            await asyncio.sleep(0.1)

    async def _flush_buffer(self):
        """Flush the current buffer to disk."""
        if not self._buffer:
            return

        try:
            # Check if we need to rotate the file
            if (os.path.exists(self._current_file) and 
                os.path.getsize(self._current_file) >= self.config.max_wal_size):
                self._current_sequence += 1
                self._current_file = self._get_next_wal_file()

            # Write entries to file
            with open(self._current_file, 'a') as f:
                for entry in self._buffer:
                    f.write(json.dumps({
                        'timestamp': entry.timestamp,
                        'operation': entry.operation.value,
                        'key': entry.key,
                        'value': entry.value,
                        'ttl': entry.ttl,
                        'sequence': entry.sequence
                    }) + '\n')
                
                if self.config.sync_on_write:
                    f.flush()
                    os.fsync(f.fileno())

            # Clear buffer
            self._buffer.clear()
            
        except Exception as e:
            logger.error(f"Error flushing WAL buffer: {e}")
            raise

    async def log_operation(
        self,
        operation: OperationType,
        key: str,
        value: Optional[Any] = None,
        ttl: Optional[float] = None
    ) -> int:
        """Log a cache operation to the WAL."""
        async with self._buffer_lock:
            entry = WALEntry(
                timestamp=time.time(),
                operation=operation,
                key=key,
                value=value,
                ttl=ttl,
                sequence=self._current_sequence
            )
            self._buffer.append(entry)
            self._current_sequence += 1
            
            # If sync_on_write is True, flush immediately
            if self.config.sync_on_write:
                await self._flush_buffer()
            
            return entry.sequence

    async def replay(self, callback) -> Tuple[Dict[str, Any], Dict[str, float]]:
        """
        Replay the WAL to restore state.
        Returns a tuple of (cache_data, ttl_data).
        """
        cache_data: Dict[str, Any] = {}
        ttl_data: Dict[str, float] = {}
        
        try:
            # Get all WAL files in sequence order
            files = [f for f in os.listdir(self.config.wal_dir) 
                    if f.startswith(f"wal_{self.node_id}_") and f.endswith(".log")]
            files.sort(key=lambda x: int(x.split("_")[-1].split(".")[0]))
            
            for file in files:
                file_path = os.path.join(self.config.wal_dir, file)
                with open(file_path, 'r') as f:
                    for line in f:
                        if not line.strip():
                            continue
                            
                        entry = json.loads(line)
                        operation = OperationType(entry['operation'])
                        key = entry['key']
                        value = entry['value']
                        ttl = entry['ttl']
                        
                        # Apply the operation
                        if operation == OperationType.PUT:
                            cache_data[key] = value
                            if ttl is not None:
                                ttl_data[key] = ttl
                        elif operation == OperationType.DELETE:
                            cache_data.pop(key, None)
                            ttl_data.pop(key, None)
                        elif operation == OperationType.TTL:
                            if key in cache_data:
                                ttl_data[key] = ttl
                        
                        # Call the callback for each entry
                        if callback:
                            await callback(entry)
            
            logger.info(f"WAL replay completed for node {self.node_id}")
            return cache_data, ttl_data
            
        except Exception as e:
            logger.error(f"Error replaying WAL: {e}")
            raise

    async def compact(self):
        """Compact the WAL by creating a new file with only the latest state."""
        try:
            # Get current state through replay
            cache_data, ttl_data = await self.replay(None)
            
            # Create a new WAL file with the current state
            self._current_sequence += 1
            self._current_file = self._get_next_wal_file()
            
            # Write current state as PUT operations
            with open(self._current_file, 'w') as f:
                for key, value in cache_data.items():
                    entry = {
                        'timestamp': time.time(),
                        'operation': OperationType.PUT.value,
                        'key': key,
                        'value': value,
                        'ttl': ttl_data.get(key),
                        'sequence': self._current_sequence
                    }
                    f.write(json.dumps(entry) + '\n')
                    self._current_sequence += 1
            
            # Delete old WAL files
            old_files = [f for f in os.listdir(self.config.wal_dir) 
                        if f.startswith(f"wal_{self.node_id}_") and 
                        f != os.path.basename(self._current_file)]
            for file in old_files:
                os.remove(os.path.join(self.config.wal_dir, file))
                
            logger.info(f"WAL compacted for node {self.node_id}")
            
        except Exception as e:
            logger.error(f"Error compacting WAL: {e}")
            raise 