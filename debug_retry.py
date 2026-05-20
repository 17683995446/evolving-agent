"""
Debug the retry issue
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from execution.distributed_task_queue import (
    LocalTaskQueue,
    DistributedTask,
    TaskPriority
)

print("Debugging retry...")

queue = LocalTaskQueue()

task = DistributedTask(task_type="test", max_retries=1, priority=TaskPriority.NORMAL)
print(f"Created task with id: {task.task_id}")
print(f"Initial retry_count: {task.retry_count}")
print(f"Initial max_retries: {task.max_retries}")

queue.enqueue(task)

print(f"Queue pending after enqueue: {queue.get_pending_count()}")

# Dequeue
dequeued = queue.dequeue()
print(f"Dequeued: {dequeued.task_id if dequeued else None}")
print(f"Queue pending after dequeue: {queue.get_pending_count()}")
print(f"dequeued.retry_count: {dequeued.retry_count}")
print(f"dequeued.max_retries: {dequeued.max_retries}")

# Let's manually check the logic
print(f"\nManual logic: {dequeued.retry_count} < {dequeued.max_retries} = {dequeued.retry_count < dequeued.max_retries}")

# Fail it
print("\nCalling fail...")
result = queue.fail(dequeued.task_id, "test error")
print(f"Fail result: {result}")

print(f"Queue pending after fail: {queue.get_pending_count()}")

stats = queue.get_stats()
print(f"Stats: {stats}")

# Check if task is in queues
print("Checking queues...")
for prio, q in queue.queues.items():
    print(f"  {prio}: {len(q)} tasks")
    
print("\nTrying to dequeue again...")
retry_task = queue.dequeue()
print(f"Retry task: {retry_task}")
