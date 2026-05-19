"""
验证测试：修复后的队列统计系统
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import time
import random
from execution.distributed_task_queue import DistributedTaskExecutor, TaskPriority

def test_basic_stats():
    """测试基本的统计功能"""
    print("=" * 80)
    print("测试 1: 队列统计功能验证")
    print("=" * 80)
    
    # 创建执行器
    executor = DistributedTaskExecutor(use_redis=False, max_workers=32)
    
    # 注册简单处理器
    def handler(params):
        x = params.get('x', 0)
        return x * x
    
    executor.register_handler('test', handler)
    executor.start(num_workers=32)
    
    print("开始提交任务...")
    num_tasks = 15000  # 超过 10,000 个，验证计数器是否正确
    start_time = time.time()
    
    for i in range(num_tasks):
        executor.submit_task('test', f"Test task {i}", {'x': random.randint(1, 100)}, 
                            priority=random.choice([TaskPriority.CRITICAL, TaskPriority.HIGH, TaskPriority.NORMAL, TaskPriority.LOW]))
        if i % 1000 == 0 and i > 0:
            print(f"已提交 {i} 个任务...")
    
    print(f"\n任务提交完成！共提交 {num_tasks} 个任务，等待处理...")
    
    # 监控处理进度
    last_check = time.time()
    while True:
        stats = executor.get_statistics()
        pending = stats['pending']['total']
        running = stats['running']
        completed = stats['completed']
        failed = stats['failed']
        
        print(f"\r等待中... 待处理: {pending:>6} | 运行中: {running:>3} | 已完成: {completed:>6} | 失败: {failed:>4}", end='', flush=True)
        
        if pending == 0 and running == 0:
            print()
            break
            
        if time.time() - last_check > 1.0:
            last_check = time.time()
        
        time.sleep(0.1)
    
    end_time = time.time()
    duration = end_time - start_time
    
    print("=" * 80)
    print("最终统计:")
    print(f"  总提交: {num_tasks}")
    print(f"  已完成: {stats['completed']}")
    print(f"  失败: {stats['failed']}")
    print(f"  总处理: {stats['completed'] + stats['failed']}")
    print(f"  耗时: {duration:.2f} 秒")
    print(f"  吞吐量: {num_tasks/duration:.2f} 任务/秒")
    print("=" * 80)
    
    # 验证
    success = stats['completed'] == num_tasks
    
    print(f"\n测试{'通过！' if success else '失败！'}")
    print(f"完成任务数验证：{'✓' if stats['completed'] == num_tasks else '✗'} (期望: {num_tasks}, 实际: {stats['completed']})")
    
    executor.stop()
    return success

if __name__ == "__main__":
    success = test_basic_stats()
    sys.exit(0 if success else 1)
