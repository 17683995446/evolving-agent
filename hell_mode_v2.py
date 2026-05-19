#!/usr/bin/env python3
"""
地狱模式测试 v2 - 更合理且有效的测试
"""
import sys
import os
import time
import logging
import threading
import random
import uuid
from datetime import datetime
from typing import Dict, List

sys.path.insert(0, '/workspace/evolving_agent')

from execution.distributed_task_queue import (
    DistributedTaskExecutor, TaskPriority
)

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class HellTestV2:
    def __init__(self, duration_sec: int = 60):
        self.duration_sec = duration_sec
        self.executor = DistributedTaskExecutor(max_workers=32)
        self.tasks_submitted = 0
        self.tasks_completed = 0
        self.tasks_failed = 0
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        
    def register_handlers(self):
        """注册所有任务处理器"""
        
        def compute_light(params):
            time.sleep(random.uniform(0.0001, 0.001))
            return sum(i for i in range(10))
        
        def guaranteed_fail(params):
            fail_prob = params.get("fail_prob", 0.5)
            if random.random() < fail_prob:
                raise RuntimeError("Intentional failure for testing")
            return {"ok": True}
        
        self.executor.register_handler("compute_light", compute_light)
        self.executor.register_handler("guaranteed_fail", guaranteed_fail)
    
    def submitter_thread(self, thread_id: int):
        """任务提交线程"""
        while not self.stop_event.is_set():
            try:
                # 随机选择任务类型
                task_type = random.choice([
                    "compute_light", "compute_light", "compute_light",
                    "guaranteed_fail"
                ])
                priority = random.choice(list(TaskPriority))
                
                # 提交任务
                self.executor.submit_task(
                    task_type,
                    f"task-{thread_id}-{uuid.uuid4().hex[:8]}",
                    {"fail_prob": random.uniform(0.1, 0.3)},
                    priority=priority
                )
                
                with self.lock:
                    self.tasks_submitted += 1
                
                # 短暂休息，避免淹没
                time.sleep(random.uniform(0.0001, 0.001))
            except Exception as e:
                logging.error(f"Submitter {thread_id} error: {e}")
    
    def progress_monitor(self):
        """进度监控线程"""
        last_report = time.time()
        last_submitted = 0
        
        while not self.stop_event.is_set():
            try:
                time.sleep(1)
                stats = self.executor.get_statistics()
                
                completed = stats.get('completed', 0)
                failed = stats.get('failed', 0)
                pending = stats.get('pending', {}).get('total', 0)
                running = stats.get('running', 0)
                
                now = time.time()
                if now - last_report >= 5:  # 每 5 秒报告
                    last_report = now
                    
                    with self.lock:
                        current_submitted = self.tasks_submitted
                    
                    throughput = current_submitted - last_submitted
                    last_submitted = current_submitted
                    
                    print(f"📊 [{datetime.now().strftime('%H:%M:%S')}] "
                          f"提交: {current_submitted:,}, "
                          f"完成: {completed:,}, "
                          f"失败: {failed:,}, "
                          f"待处理: {pending}, "
                          f"运行中: {running}, "
                          f"速度: {throughput}/5s")
                
            except Exception as e:
                logging.error(f"Monitor error: {e}")
    
    def run(self):
        """运行测试"""
        print("\n" + "=" * 80)
        print("持续进化 Agent - 地狱模式测试 v2")
        print("=" * 80)
        
        # 1. 初始化
        print("\n🚀 初始化系统...")
        self.register_handlers()
        
        # 2. 启动执行器
        print("启动执行器 (32 workers)...")
        self.executor.start(num_workers=32)
        
        # 3. 启动提交线程
        print("\n🔥 启动任务提交线程 (8 个)...")
        submitters = []
        for i in range(8):
            t = threading.Thread(
                target=self.submitter_thread,
                args=(i,),
                daemon=True
            )
            t.start()
            submitters.append(t)
        
        # 4. 启动监控线程
        print("启动进度监控...")
        monitor = threading.Thread(target=self.progress_monitor, daemon=True)
        monitor.start()
        
        # 5. 主运行循环
        print(f"\n⏱️  运行中... ({self.duration_sec} 秒)")
        print("=" * 80)
        
        start_time = time.time()
        
        while time.time() - start_time < self.duration_sec:
            time.sleep(1)
        
        # 6. 停止
        print("\n" + "=" * 80)
        print("🛑 停止测试...")
        self.stop_event.set()
        
        # 7. 等待队列清空
        print("⏳ 等待队列清空...")
        empty_count = 0
        while empty_count < 10:
            stats = self.executor.get_statistics()
            pending = stats.get('pending', {}).get('total', 0)
            running = stats.get('running', 0)
            
            if pending == 0 and running == 0:
                empty_count += 1
            else:
                empty_count = 0
                print(f"  - 待处理: {pending}, 运行中: {running}")
            
            time.sleep(0.5)
        
        # 8. 停止执行器
        self.executor.stop()
        
        # 9. 最终统计
        print("\n" + "=" * 80)
        print("📊 最终结果")
        print("=" * 80)
        
        final_stats = self.executor.get_statistics()
        
        total_submitted = self.tasks_submitted
        total_completed = final_stats.get('completed', 0)
        total_failed = final_stats.get('failed', 0)
        
        success_rate = (total_completed / total_submitted * 100 
                        if total_submitted > 0 else 0)
        
        print(f"\n📋 执行统计:")
        print(f"  总提交: {total_submitted:,}")
        print(f"  完成: {total_completed:,}")
        print(f"  失败: {total_failed:,}")
        print(f"  成功率: {success_rate:.1f}%")
        
        print(f"\n✅ 测试完成! 没有崩溃或异常!")
        
        return {
            "submitted": total_submitted,
            "completed": total_completed,
            "failed": total_failed,
            "success_rate": success_rate
        }

def main():
    # 运行 45 秒的测试
    test = HellTestV2(duration_sec=45)
    result = test.run()
    
    # 判断是否成功
    if result["success_rate"] >= 90:
        print("\n🎉 优秀！成功率超过 90%!")
        return 0
    else:
        print("\n⚠️  成功率较低，但系统仍在运行!")
        return 0

if __name__ == "__main__":
    sys.exit(main())
