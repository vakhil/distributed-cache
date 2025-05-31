from dataclasses import dataclass, field
from typing import Any, Optional
import time
from .memory import MemoryManager

@dataclass
class CacheEntry:
    """
    Represents a single cache entry with value, expiration, and memory tracking.
    """
    value: Any
    expires_at: Optional[float] = None
    memory_manager: Optional[MemoryManager] = None
    key: Optional[str] = None
    size_bytes: int = field(default=0, init=False)
    
    def __post_init__(self):
        """Initialize memory tracking if manager is provided."""
        if self.memory_manager and self.key:
            self.size_bytes = self.memory_manager.zmalloc(self.key, self.value)
    
    def is_expired(self) -> bool:
        """Check if the entry has expired."""
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at
    
    def update_value(self, new_value: Any) -> None:
        """Update the value and recalculate memory usage."""
        if self.memory_manager and self.key:
            # Free old memory
            self.memory_manager.zfree(self.key)
            # Allocate new memory
            self.size_bytes = self.memory_manager.zmalloc(self.key, new_value)
        self.value = new_value
    
    def cleanup(self) -> None:
        """Clean up memory when entry is removed."""
        if self.memory_manager and self.key:
            self.memory_manager.zfree(self.key)
            self.size_bytes = 0
    
    @classmethod
    def create(cls, value: Any, ttl: Optional[int] = None) -> 'CacheEntry':
        """
        Create a new cache entry.
        
        Args:
            value: The value to store
            ttl: Time to live in seconds (None for no expiration)
            
        Returns:
            A new CacheEntry instance
        """
        expiry_time = None if ttl is None else time.time() + ttl
        return cls(value=value, expires_at=expiry_time) 