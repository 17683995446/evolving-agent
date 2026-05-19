#!/usr/bin/env python3
"""
诊断性测试 - 找出任务处理问题的根源
"""
import sys
import os
import time
import threading
import random
import uuid

sys.path.insert(0, '/workspace/evolving_agent')

from execution.distributed_task_queue import (
    DistributedTaskExecutor, TaskPriority, LocalTaskQueue, DistributedTask
)

def test_local_queue_basic():
    """测试本地队列的基本功能"""
    print("\n=== 测试 1: 本地队列基本功能 ===")
    queue = LocalTaskQueue()
    
    # 提交任务
    task = DistributedTask(
        task_type="test",
        description="test task",
        parameters={"x": 1},
        priority=TaskPriority.NORMAL
    )
    task_id = task.task_id
    
    print(f"提交任务: {task_id}")
    queue.enqueue(task)
    
    # 检查统计
    stats = queue.get_stats()
    print(f"队列统计 (提交后): {stats}")
    assert stats['pending']['total'] == 1, "任务应该在队列中"
    
    # 出队任务
    dequeued = queue.dequeue()
    print(f"出队任务: {dequeued}")
    assert dequeued is not None, "应该能取出任务"
    
    # 再次检查统计
    stats = queue.get_stats()
    print(f"队列统计 (出队后): {stats}")
    
    # 完成任务
    queue.complete(task_id, {"result": "ok"})
    stats = queue.get_stats()
    print(f"队列统计 (完成后): {stats}")
    assert stats['completed'] == 1, "应该有一个完成的任务"
    
    print("✅ 本地队列基本测试通过!")

def test_executor_basic():
    """测试执行器基本功能"""
    print("\n=== 测试 2: 执行器基本功能 ===")
    executor = DistributedTaskExecutor(max_workers=4)
    
    # 注册处理器
    completed_tasks = []
    lock = threading.Lock()
    
    def test_handler(params):
        task_id = params.get("task_id", "unknown")
        with lock:
            completed_tasks.append(task_id)
        return {"result": "ok"}
    
    executor.register_handler("test", test_handler)
    
    print("启动执行器...")
    executor.start(num_workers=2)
    time.sleep(0.1)
    
    # 提交任务
    print("提交 5 个任务...")
    task_ids = []
    for i in range(5):
        tid = executor.submit_task(
            "test", f"task-{i}", 
            {"task_id": i, "x": i},
            priority=TaskPriority.NORMAL
        )
        task_ids.append(tid)
    
    print(f"提交的任务: {task_ids}")
    
    # 等待处理
    print("等待任务处理...")
    start_time = time.time()
    timeout = 5
    while (len(completed_tasks) < 5 and 
           time.time() - start_time < timeout):
        time.sleep(0.1)
        stats = executor.get_statistics()
        print(f" - 已完成: {stats.get('completed', 0)}, "
              f"待处理: {stats.get('pending', {}).get('total', 0)}")
    
    stats = executor.get_statistics()
    print(f"最终统计: {stats}")
    print(f"已处理的任务: {len(completed_tasks)}")
    
    executor.stop()
    
    # 检查结果
    print(f"\n本地完成列表长度: {len(completed_tasks)}")
    if len(completed_tasks) < 5:
        print("❌ 执行器测试失败 - 任务没有被完全处理!")
        return False
    else:
        print("✅ 执行器基本测试通过!")
        return True

def test_priority_queue():
    """测试优先级队列"""
    print("\n=== 测试 3: 优先级队列 ===")
    queue = LocalTaskQueue()
    
    # 按不同优先级提交任务
    priorities = [TaskPriority.LOW, TaskPriority.NORMAL, 
                  TaskPriority.HIGH, TaskPriority.CRITICAL]
    task_labels = []
    
    for p in priorities:
        task = DistributedTask(
            task_type="test", description=f"{p.name}",
            priority=p
        )
        task_labels.append((task.task_id, p.name))
        queue.enqueue(task)
    
    # 出队并检查顺序
    print("出队顺序应该是: CRITICAL, HIGH, NORMAL, LOW")
    actual_order = []
    while True:
        task = queue.dequeue()
        if task is None:
            break
        actual_order.append(task.priority.name)
        queue.complete(task.task_id, None)
    
    expected_order = ["CRITICAL", "HIGH", "NORMAL", "LOW"]
    print(f"实际出队顺序: {actual_order}")
    print(f"预期出队顺序: {expected_order}")
    
    if actual_order == expected_order:
        print("✅ 优先级队列测试通过!")
        return True
    else:
        print("❌ 优先级队列测试失败!")
        return False

def main():
    print("=" * 60)
    print("持续进化 Agent - 诊断性测试")
    print("=" * 60)
    
    results = []
    
    try:
        # 测试1: 本地队列
        test_local_queue_basic()
        results.append(("本地队列", True))
        
        # 测试2: 执行器
        executor_ok = test_executor_basic()
        results.append(("执行器", executor_ok))
        
        # 测试3: 优先级
        priority_ok = test_priority_queue()
        results.append(("优先级队列", priority_ok))
        
        # 总结
        print("\n" + "=" * 60)
        print("测试总结")
        print("=" * 60)
        for name, ok in results:
            status = "✅ 通过" if ok else "❌ 失败"
            print(f"{name}: {status}")
        
        all_ok = all(ok for _, ok in results)
        return 0 if all_ok else 1
        
    except Exception as e:
        import traceback
        print(f"测试异常: {e}")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
