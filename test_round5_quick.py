"""
第五轮迭代 - 简化版终极极端测试
"""
import sys
import os
import time
import threading
import logging
import random
import gc
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from execution.distributed_task_queue import (
    DistributedTaskExecutor, TaskPriority, DistributedTask, LocalTaskQueue
)

# 配置详细日志
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s'
)
logger = logging.getLogger("Round5Quick")


class Round5QuickTest:
    """第五轮迭代 - 简化版终极测试"""
    
    def __init__(self):
        self.executor = None
        self.total_submitted = 0
        self.start_time = None
        self.lock = threading.Lock()
        self.errors = []
        self.warnings = []
        self.running = True
        
    # 20+ 种处理器
    def handler_fast(self, params): return "fast"
    def handler_slow(self, params): time.sleep(0.005); return "slow"
    def handler_cpu(self, params): sum(i*i for i in range(500)); return "cpu"
    def handler_memory(self, params): [object() for _ in range(500)]; return "mem"
    def handler_fail(self, params): 
        if random.random() < 0.8: raise RuntimeError("failed")
        return "ok"
    def handler_random(self, params):
        if random.random() < 0.5: time.sleep(0.002)
        if random.random() < 0.3: raise ValueError("random err")
        return "random"
    
    def get_all_handlers(self):
        return [
            ('fast', self.handler_fast),
            ('slow', self.handler_slow),
            ('cpu', self.handler_cpu),
            ('memory', self.handler_memory),
            ('fail', self.handler_fail),
            ('random', self.handler_random),
        ]
    
    def generate_task(self, scenario_id):
        handlers = self.get_all_handlers()
        task_type = random.choice([name for name, _ in handlers])
        priority = random.choice(list(TaskPriority))
        max_retries = random.randint(0, 5)
        
        try:
            self.executor.submit_task(
                task_type, f"Quick-{scenario_id}", {}, 
                priority=priority, max_retries=max_retries
            )
            with self.lock:
                self.total_submitted += 1
        except Exception as e:
            with self.lock:
                self.errors.append(f"Submit: {e}")
    
    def monitor(self, duration):
        last_total = 0
        last_time = time.time()
        
        while self.running and (time.time() - self.start_time) < duration:
            try:
                stats = self.executor.get_statistics()
                pending = stats['pending']['total']
                running = stats['running']
                completed = stats['completed']
                failed = stats['failed']
                total = completed + failed
                
                now = time.time()
                delta_total = total - last_total
                delta_time = now - last_time
                throughput = delta_total / delta_time if delta_time > 0 else 0
                success_rate = (completed / total * 100) if total > 0 else 0
                
                elapsed = now - self.start_time
                timestamp = datetime.now().strftime("%H:%M:%S")
                
                logger.info(
                    f"[{timestamp}] ⏱️{elapsed:>6.1f}s | "
                    f"📥{self.total_submitted:>10,} | "
                    f"✅{completed:>10,} | "
                    f"❌{failed:>8,} | "
                    f"📋{pending:>8,} | "
                    f"⚙️{running:>4} | "
                    f"🚀{throughput:>6,.0f}/s | "
                    f"📈{success_rate:>5.1f}%"
                )
                
                last_total = total
                last_time = now
                time.sleep(1)
            except Exception as e:
                self.errors.append(f"Monitor: {e}")
                time.sleep(1)
    
    def run_test(self, duration_minutes=10):
        logger.info("=" * 100)
        logger.info("🔥🔥🔥 第五轮迭代 - 简化版终极测试 🔥🔥🔥")
        logger.info("=" * 100)
        
        self.start_time = time.time()
        
        try:
            # 初始化执行器
            self.executor = DistributedTaskExecutor(use_redis=False, max_workers=256)
            for name, handler in self.get_all_handlers():
                self.executor.register_handler(name, handler)
            self.executor.start(num_workers=256)
            logger.info("✅ 执行器启动完成 (256 workers)")
            
            # 监控线程
            monitor_thread = threading.Thread(
                target=self.monitor, args=(duration_minutes * 60,), daemon=True
            )
            monitor_thread.start()
            
            # 提交线程
            def submit_worker(worker_id):
                scenario_id = worker_id * 100000
                while self.running and (time.time() - self.start_time) < duration_minutes * 60:
                    self.generate_task(scenario_id)
                    scenario_id += 1
                    time.sleep(0.002)
            
            num_submitters = 128
            submit_threads = []
            for i in range(num_submitters):
                t = threading.Thread(target=submit_worker, args=(i,), daemon=True)
                submit_threads.append(t)
                t.start()
            logger.info(f"✅ {num_submitters} 个提交线程启动")
            
            # 等待
            monitor_thread.join()
            self.running = False
            for t in submit_threads:
                t.join(timeout=1)
            
            # 等待队列清空
            logger.info("\n⏳ 等待队列清空...")
            wait_count = 0
            while wait_count < 60:
                stats = self.executor.get_statistics()
                if stats['pending']['total'] == 0 and stats['running'] == 0:
                    break
                time.sleep(1)
                wait_count += 1
            
            # 最终统计
            stats = self.executor.get_statistics()
            completed = stats['completed']
            failed = stats['failed']
            total = completed + failed
            success_rate = (completed / total * 100) if total > 0 else 0
            elapsed = time.time() - self.start_time
            throughput = total / elapsed if elapsed > 0 else 0
            
            self.executor.stop()
            
            # 报告
            logger.info("\n" + "=" * 100)
            logger.info("📋 第五轮迭代 - 最终报告")
            logger.info("=" * 100)
            logger.info(f"  总提交:     {self.total_submitted:>15,}")
            logger.info(f"  完成:       {completed:>15,}")
            logger.info(f"  失败:       {failed:>15,}")
            logger.info(f"  总处理:     {total:>15,}")
            logger.info(f"  成功率:     {success_rate:>15.2f}%")
            logger.info(f"  总耗时:     {elapsed:>15.1f}s")
            logger.info(f"  平均吞吐:   {throughput:>15,.0f} 任务/秒")
            logger.info(f"\n⚠️  警告: {len(self.warnings)}")
            logger.info(f"❌ 错误: {len(self.errors)}")
            
            passed = success_rate >= 60 and len(self.errors) <= 100
            
            if passed:
                logger.info("\n🎉 第五轮迭代测试通过！")
            else:
                logger.warning("\n⚠️ 第五轮迭代测试部分不通过")
            
            return passed
            
        except Exception as e:
            logger.error(f"❌ 测试崩溃: {e}", exc_info=True)
            if self.executor:
                try:
                    self.executor.stop()
                except:
                    pass
            return False


if __name__ == "__main__":
    test = Round5QuickTest()
    result = test.run_test(duration_minutes=10)
    sys.exit(0 if result else 1)
