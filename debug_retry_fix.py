
"""
快速调试失败重试的问题
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from execution.distributed_task_queue import LocalTaskQueue, DistributedTask


print("=== 开始调试 ===")
queue = LocalTaskQueue()

task = DistributedTask(task_type="test", max_retries=3)
print(f"初始任务: retry_count={task.retry_count}, max_retries={task.max_retries}")

queue.enqueue(task)
print(f"入队后 pending: {queue.get_pending_count()}")

# 多次操作直到失败
for i in range(1, 6):
    print(f"\n--- 第 {i} 次操作 ---")
    dequeued = queue.dequeue()
    if dequeued:
        print(f"取出后: retry_count={dequeued.retry_count}")
        result = queue.fail(dequeued.task_id, f"第{i}次失败")
        print(f"fail() 返回: {result}")
        stats = queue.get_stats()
        print(f"统计: pending={stats['pending']['total']}, failed={stats['failed']}")
    else:
        print("❌ 无法取出！")
        # 调试看看内部状态
        print("\n--- 调试内部状态 ---")
        print(f"queues['CRITICAL']: {len(queue.queues.get(task.priority, []))}")
        print(f"running: {len(queue.running)}")
        print(f"failed: {len(queue.failed)}, total_failed: {queue.total_failed}")
        break
