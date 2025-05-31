from cache import LockFreeCache
import threading
import time
import random

def worker(cache: LockFreeCache, thread_id: int, num_operations: int):
    """Worker function that performs random cache operations."""
    operations = ['get', 'put', 'delete']
    for i in range(num_operations):
        # Generate a random key and value
        key = f"key_{thread_id}_{i}"
        value = f"value_{thread_id}_{i}"
        
        # Choose a random operation
        operation = random.choice(operations)
        
        if operation == 'put':
            # Randomly decide whether to use TTL
            ttl = random.choice([None, 1, 2, 5])
            cache.put(key, value, ttl=ttl)
            print(f"Thread {thread_id}: PUT {key} -> {value} (TTL: {ttl})")
            
        elif operation == 'get':
            result = cache.get(key)
            print(f"Thread {thread_id}: GET {key} -> {result}")
            
        else:  # delete
            cache.delete(key)
            print(f"Thread {thread_id}: DELETE {key}")
        
        # Small random delay
        time.sleep(random.uniform(0.1, 0.3))

def main():
    # Create cache instance
    cache = LockFreeCache(cleanup_interval=0.5)
    print("Created lock-free cache")
    
    # Create and start worker threads
    threads = []
    num_threads = 4
    operations_per_thread = 10
    
    print(f"\nStarting {num_threads} threads with {operations_per_thread} operations each")
    print("Each thread will perform random GET, PUT, and DELETE operations")
    print("Some PUT operations will include random TTL values\n")
    
    for i in range(num_threads):
        t = threading.Thread(
            target=worker,
            args=(cache, i, operations_per_thread)
        )
        threads.append(t)
        t.start()
    
    # Wait for all threads to complete
    for t in threads:
        t.join()
    
    print("\nAll threads completed")
    print(f"Final cache size: {cache.get_size()} entries")
    
    # Demonstrate TTL expiration
    print("\nWaiting for TTL entries to expire...")
    time.sleep(6)  # Wait for longest TTL to expire
    print(f"Cache size after TTL expiration: {cache.get_size()} entries")
    
    # Cleanup
    cache.shutdown()
    print("\nCache shutdown complete")

if __name__ == "__main__":
    main() 