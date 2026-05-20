"""
极度混乱的压力测试 - 真实世界的最坏情况模拟
"""
import sys
import os
import time
import random
import threading
import logging
from datetime import datetime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from execution.distributed_task_queue import (
    DistributedTaskExecutor,
    TaskPriority,
    DistributedTask
)

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ChaosTest:
    def __init__(self):
        self.executor = None
        self.running = False
        self.total_submitted = 0
        self.start_time = None
        self.stats_lock = threading.Lock()
        self.errors = []
        self.crash_count = 0
        
        # 各种处理器
        self.handlers = {}
        self.handler_lock = threading.Lock()
    
    def handler_simple(self, params):
        """简单处理器 - 正常处理"""
        x = params.get('x', 0)
        return x * x
    
    def handler_sleep(self, params):
        """随机休眠模拟真实工作"""
        sleep_time = random.uniform(0.0001, 0.01)
        time.sleep(sleep_time)
        return f"slept {sleep_time:.4f}s"
    
    def handler_flaky(self, params):
        """经常失败的处理器"""
        if random.random() < 0.7:  # 70% 失败率
            raise ValueError("Intentional flaky failure")
        return "success"
    
    def handler_slow(self, params):
        """慢处理器"""
        time.sleep(random.uniform(0.05, 0.1))
        return "slow success"
    
    def handler_memory(self, params):
        """创建临时数据的处理器"""
        size = random.randint(100, 10000)
        data = "x" * size
        return len(data)
    
    def handler_deadlock_risk(self, params):
        """可能引发竞争的处理器"""
        lock = threading.Lock()
        with lock:
            return "safe"
    
    def init_handlers(self):
        """初始化所有处理器"""
        with self.handler_lock:
            self.handlers = {
                'simple': self.handler_simple,
                'sleep': self.handler_sleep,
                'flaky': self.handler_flaky,
                'slow': self.handler_slow,
                'memory': self.handler_memory,
                'deadlock': self.handler_deadlock_risk
            }
    
    def register_random_handler(self):
        """随机注册一个任务处理器"""
        with self.handler_lock:
            name = random.choice(list(self.handlers.keys()))
            self.executor.register_handler(name, self.handlers[name])
    
    def submit_chaos_tasks(self):
        """提交混乱任务"""
        task_types = ['simple', 'sleep', 'flaky', 'slow', 'memory', 'deadlock']
        priorities = [
            TaskPriority.CRITICAL,
            TaskPriority.HIGH,
            TaskPriority.NORMAL,
            TaskPriority.LOW
        ]
        
        while self.running:
            try:
                task_type = random.choice(task_types)
                priority = random.choice(priorities)
                
                # 随机参数
                params = {
                    'x': random.randint(1, 1000),
                    'timestamp': time.time(),
                    'random_data': [random.random() for _ in range(random.randint(0, 5))]
                }
                
                # 提交任务
                self.executor.submit_task(
                    task_type=task_type,
                    description=f"chaos-{random.randint(1, 1000000)}",
                    parameters=params,
                    priority=priority,
                    weight=random.uniform(0.1, 10.0)
                )
                
                with self.stats_lock:
                    self.total_submitted += 1
                
                # 疯狂的速度 - 随机延迟
                delay = random.uniform(0.00001, 0.001)
                time.sleep(delay)
                
            except Exception as e:
                logger.error(f"Submit error: {e}")
                with self.stats_lock:
                    self.errors.append(str(e))
    
    def monitor_progress(self):
        """监控进度"""
        last_check = time.time()
        last_submitted = 0
        
        while self.running:
            stats = self.executor.get_statistics()
            
            pending = stats['pending']['total']
            running = stats['running']
            completed = stats['completed']
            failed = stats['failed']
            
            with self.stats_lock:
                current_submitted = self.total_submitted
                errors = len(self.errors)
            
            delta = current_submitted - last_submitted
            last_submitted = current_submitted
            
            now = datetime.now().strftime("%H:%M:%S")
            logger.info(f"[{now}] "
                       f"📊 提交: {current_submitted:,}, "
                       f"完成: {completed:,}, "
                       f"失败: {failed:,}, "
                       f"待处理: {pending:,}, "
                       f"运行中: {running}, "
                       f"错误: {errors}")
            
            last_check = time.time()
            time.sleep(2)
    
    def chaos_operations(self):
        """混乱操作 - 模拟各种边缘情况"""
        operations = [
            'noop', 'noop', 'noop', 'noop',  # 大部分时间正常
            'register_random',
            'fast_submit_burst',
            'submit_same_task_many'
        ]
        
        while self.running:
            try:
                op = random.choice(operations)
                
                if op == 'register_random':
                    self.register_random_handler()
                    logger.info("📝 注册了随机处理器")
                
                elif op == 'fast_submit_burst':
                    # 快速提交一批
                    for _ in range(100):
                        task_type = random.choice(['simple', 'flaky', 'memory'])
                        self.executor.submit_task(
                            task_type=task_type,
                            parameters={'x': random.randint(1, 100)},
                            priority=random.choice([TaskPriority.HIGH, TaskPriority.NORMAL])
                        )
                        with self.stats_lock:
                            self.total_submitted += 1
                
                elif op == 'submit_same_task_many':
                    # 相同任务多次
                    for _ in range(50):
                        self.executor.submit_task(
                            task_type='simple',
                            parameters={'x': 42}
                        )
                        with self.stats_lock:
                            self.total_submitted += 1
                
                # 短暂休息
                time.sleep(random.uniform(0.01, 0.1))
                
            except Exception as e:
                logger.error(f"Chaos op error: {e}")
                with self.stats_lock:
                    self.errors.append(str(e))
    
    def run(self, duration_seconds=300):
        """运行测试"""
        logger.info("=" * 80)
        logger.info("🔥 极度混乱的压力测试 - 开始")
        logger.info("=" * 80)
        
        self.running = True
        self.start_time = time.time()
        
        try:
            # 1. 初始化执行器
            logger.info("🚀 初始化执行器 (64 workers)...")
            self.executor = DistributedTaskExecutor(
                use_redis=False,
                max_workers=64
            )
            
            # 2. 注册所有处理器
            self.init_handlers()
            for name, handler in self.handlers.items():
                self.executor.register_handler(name, handler)
            logger.info(f"✅ 注册了 {len(self.handlers)} 个处理器")
            
            # 3. 启动执行器
            self.executor.start(num_workers=64)
            logger.info("✅ 执行器启动")
            
            # 4. 启动监控线程
            monitor_thread = threading.Thread(target=self.monitor_progress, daemon=True)
            monitor_thread.start()
            
            # 5. 启动提交线程
            submit_threads = []
            for i in range(16):
                t = threading.Thread(target=self.submit_chaos_tasks, daemon=True, name=f"Submit-{i}")
                t.start()
                submit_threads.append(t)
            
            # 6. 启动混乱操作线程
            chaos_thread = threading.Thread(target=self.chaos_operations, daemon=True)
            chaos_thread.start()
            
            # 7. 主等待
            logger.info(f"⏱️  运行中... ({duration_seconds} 秒)")
            logger.info("=" * 80)
            
            time.sleep(duration_seconds)
            
            logger.info("=" * 80)
            logger.info("🛑 停止测试...")
            self.running = False
            
            # 等待所有线程结束
            for t in submit_threads:
                t.join(timeout=5)
            
            # 等待队列清空
            logger.info("⏳ 等待队列清空...")
            while True:
                stats = self.executor.get_statistics()
                pending = stats['pending']['total']
                running = stats['running']
                logger.info(f"  - 待处理: {pending}, 运行中: {running}")
                if pending == 0 and running == 0:
                    break
                time.sleep(1)
            
            # 获取最终统计
            stats = self.executor.get_statistics()
            
            # 停止执行器
            self.executor.stop()
            
            # 显示最终结果
            end_time = time.time()
            duration = end_time - self.start_time
            
            logger.info("=" * 80)
            logger.info("📊 最终结果")
            logger.info("=" * 80)
            
            with self.stats_lock:
                total = self.total_submitted
                error_count = len(self.errors)
            
            completed = stats['completed']
            failed = stats['failed']
            total_processed = completed + failed
            
            logger.info(f"📋 执行统计:")
            logger.info(f"  总提交: {total:,}")
            logger.info(f"  完成: {completed:,}")
            logger.info(f"  失败: {failed:,}")
            logger.info(f"  总计处理: {total_processed:,}")
            logger.info(f"  成功率: {(completed/total_processed*100):.1f}%")
            logger.info(f"  系统错误: {error_count}")
            logger.info(f"  总耗时: {duration:.1f}s")
            logger.info(f"  吞吐量: {total_processed/duration:.0f} 任务/s")
            logger.info("=" * 80)
            
            if error_count == 0 and failed / total_processed < 0.1:
                logger.info("✅ 测试完成! 没有崩溃或异常!")
                return True
            else:
                logger.warning(f"⚠️  测试中有些问题需要检查")
                return False
            
        except Exception as e:
            logger.error(f"❌ 测试崩溃: {e}", exc_info=True)
            self.running = False
            if self.executor:
                try:
                    self.executor.stop()
                except:
                    pass
            return False

if __name__ == "__main__":
    test = ChaosTest()
    success = test.run(duration_seconds=120)  # 先跑 2 分钟看看
    sys.exit(0 if success else 1)
