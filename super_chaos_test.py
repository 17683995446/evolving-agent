"""
超级混乱的超大规模测试 - 极限压力测试
模拟真实世界的最极端场景
"""
import sys
import os
import time
import random
import threading
import logging
import traceback
import gc
from datetime import datetime
from typing import Dict, Any
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from execution.distributed_task_queue import (
    DistributedTaskExecutor,
    TaskPriority,
    DistributedTask
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SuperChaosTest:
    def __init__(self):
        self.executor = None
        self.running = False
        self.total_submitted = 0
        self.start_time = None
        self.stats_lock = threading.Lock()
        self.errors = []
        self.warnings = []
        
        # 性能指标
        self.metrics = {
            'peak_throughput': 0,
            'current_throughput': 0,
            'memory_snapshots': []
        }
        
        # 处理器
        self.handlers = {}
    
    # ============ 各种处理器 ============
    
    def handler_cpu_intensive(self, params: Dict[str, Any]) -> int:
        """CPU密集型任务"""
        n = params.get('n', 10000)
        result = sum(i * i for i in range(n))
        return result
    
    def handler_io_intensive(self, params: Dict[str, Any]) -> str:
        """IO密集型任务"""
        time.sleep(random.uniform(0.001, 0.01))
        return f"io_done_{random.randint(1, 10000)}"
    
    def handler_memory_heavy(self, params: Dict[str, Any]) -> int:
        """内存密集型任务"""
        size = random.randint(1000, 50000)
        data = []
        for i in range(size):
            data.append({
                'id': i,
                'data': 'x' * random.randint(10, 100),
                'timestamp': time.time()
            })
        result = len(data)
        del data
        return result
    
    def handler_flaky(self, params: Dict[str, Any]) -> str:
        """不稳定的任务"""
        roll = random.random()
        if roll < 0.3:
            raise ValueError("Random failure 1")
        elif roll < 0.5:
            raise RuntimeError("Random failure 2")
        elif roll < 0.6:
            raise Exception("Random failure 3")
        return "success"
    
    def handler_timeout_risk(self, params: Dict[str, Any]) -> str:
        """有超时风险的任务"""
        duration = random.uniform(0.01, 0.05)
        if random.random() < 0.1:
            duration = 0.5  # 偶尔很慢
        time.sleep(duration)
        return f"completed_after_{duration:.3f}s"
    
    def handler_data_aggregation(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """数据聚合任务"""
        items = params.get('items', [])
        return {
            'count': len(items),
            'sum': sum(items),
            'avg': sum(items) / len(items) if items else 0,
            'max': max(items) if items else 0,
            'min': min(items) if items else 0
        }
    
    def handler_nested_tasks(self, params: Dict[str, Any]) -> str:
        """嵌套任务模拟"""
        depth = params.get('depth', 3)
        if depth > 0:
            return f"nested_level_{depth}"
        return "base"
    
    def handler_critical_path(self, params: Dict[str, Any]) -> bool:
        """关键路径任务"""
        result = params.get('value', 0) * 2
        if result < 0:
            raise ValueError("Critical path failure")
        return result > 0
    
    def init_handlers(self):
        """初始化所有处理器"""
        self.handlers = {
            'cpu': self.handler_cpu_intensive,
            'io': self.handler_io_intensive,
            'memory': self.handler_memory_heavy,
            'flaky': self.handler_flaky,
            'timeout': self.handler_timeout_risk,
            'aggregate': self.handler_data_aggregation,
            'nested': self.handler_nested_tasks,
            'critical': self.handler_critical_path
        }
    
    # ============ 混乱场景 ============
    
    def scenario_burst_submit(self, count: int):
        """批量提交场景"""
        task_types = list(self.handlers.keys())
        for _ in range(count):
            if not self.running:
                break
            try:
                task_type = random.choice(task_types)
                params = self._generate_params(task_type)
                
                self.executor.submit_task(
                    task_type=task_type,
                    description=f"burst-{random.randint(1, 100000)}",
                    parameters=params,
                    priority=random.choice([
                        TaskPriority.CRITICAL,
                        TaskPriority.HIGH,
                        TaskPriority.NORMAL,
                        TaskPriority.LOW
                    ])
                )
                
                with self.stats_lock:
                    self.total_submitted += 1
                    
            except Exception as e:
                with self.stats_lock:
                    self.errors.append(f"Burst submit error: {e}")
    
    def scenario_memory_pressure(self):
        """内存压力场景"""
        logger.info("🔥 触发内存压力...")
        for _ in range(5):
            if not self.running:
                break
            self.scenario_burst_submit(1000)
            gc.collect()
    
    def scenario_priority_storm(self):
        """优先级风暴场景"""
        logger.info("⚡ 触发优先级风暴...")
        priorities = [
            TaskPriority.CRITICAL,
            TaskPriority.HIGH,
            TaskPriority.NORMAL,
            TaskPriority.LOW
        ]
        
        for priority in priorities:
            for _ in range(500):
                if not self.running:
                    break
                try:
                    task_type = random.choice(list(self.handlers.keys()))
                    self.executor.submit_task(
                        task_type=task_type,
                        parameters=self._generate_params(task_type),
                        priority=priority
                    )
                    with self.stats_lock:
                        self.total_submitted += 1
                except Exception as e:
                    with self.stats_lock:
                        self.errors.append(f"Priority storm error: {e}")
    
    def scenario_handler_chaos(self):
        """处理器混乱场景 - 模拟未知任务类型"""
        logger.info("🎲 触发处理器混乱...")
        
        # 提交一些不存在处理器的任务
        for _ in range(100):
            if not self.running:
                break
            try:
                fake_type = f"unknown_{random.randint(1, 1000)}"
                self.executor.submit_task(
                    task_type=fake_type,
                    parameters={'x': random.randint(1, 100)},
                    priority=TaskPriority.NORMAL
                )
                with self.stats_lock:
                    self.total_submitted += 1
            except Exception as e:
                with self.stats_lock:
                    self.errors.append(f"Handler chaos: {e}")
    
    def _generate_params(self, task_type: str) -> Dict[str, Any]:
        """生成随机参数"""
        if task_type == 'cpu':
            return {'n': random.randint(1000, 50000)}
        elif task_type == 'aggregate':
            items = [random.randint(1, 100) for _ in range(random.randint(5, 50))]
            return {'items': items}
        elif task_type == 'nested':
            return {'depth': random.randint(1, 5)}
        elif task_type == 'critical':
            return {'value': random.randint(-100, 100)}
        else:
            return {'x': random.randint(1, 1000)}
    
    # ============ 主流程 ============
    
    def submit_chaos_tasks(self, thread_id: int):
        """混乱任务提交"""
        task_types = list(self.handlers.keys())
        
        while self.running:
            try:
                task_type = random.choice(task_types)
                params = self._generate_params(task_type)
                
                self.executor.submit_task(
                    task_type=task_type,
                    description=f"chaos-{thread_id}-{random.randint(1, 1000000)}",
                    parameters=params,
                    priority=random.choice([
                        TaskPriority.CRITICAL,
                        TaskPriority.HIGH,
                        TaskPriority.NORMAL,
                        TaskPriority.LOW
                    ])
                )
                
                with self.stats_lock:
                    self.total_submitted += 1
                
                # 随机延迟
                delay = random.uniform(0.00001, 0.0005)
                time.sleep(delay)
                
            except Exception as e:
                with self.stats_lock:
                    self.errors.append(f"Submit error: {e}")
                time.sleep(0.001)
    
    def chaos_operations(self):
        """混乱操作"""
        scenarios = [
            ('burst', self.scenario_burst_submit, 500),
            ('memory', self.scenario_memory_pressure, None),
            ('priority', self.scenario_priority_storm, None),
            ('handler', self.scenario_handler_chaos, None),
        ]
        
        while self.running:
            try:
                scenario_name, scenario_func, param = random.choice(scenarios)
                
                if param is not None:
                    scenario_func(param)
                else:
                    scenario_func()
                
                time.sleep(random.uniform(1, 5))
                
            except Exception as e:
                with self.stats_lock:
                    self.errors.append(f"Chaos op error: {e}")
    
    def monitor_progress(self):
        """监控进度"""
        last_submitted = 0
        last_check = time.time()
        
        while self.running:
            try:
                stats = self.executor.get_statistics()
                
                pending = stats['pending']['total']
                running = stats['running']
                completed = stats['completed']
                failed = stats['failed']
                
                with self.stats_lock:
                    current_submitted = self.total_submitted
                    error_count = len(self.errors)
                    warning_count = len(self.warnings)
                
                # 计算吞吐量
                now = time.time()
                delta_time = now - last_check
                delta_submitted = current_submitted - last_submitted
                throughput = delta_submitted / delta_time if delta_time > 0 else 0
                
                # 更新峰值吞吐量
                if throughput > self.metrics['peak_throughput']:
                    self.metrics['peak_throughput'] = throughput
                
                last_submitted = current_submitted
                last_check = now
                
                # 格式化输出
                timestamp = datetime.now().strftime("%H:%M:%S")
                success_rate = (completed / (completed + failed) * 100) if (completed + failed) > 0 else 0
                
                logger.info(
                    f"[{timestamp}] "
                    f"📊 提交: {current_submitted:>10,}, "
                    f"完成: {completed:>10,}, "
                    f"失败: {failed:>8,}, "
                    f"待: {pending:>8,}, "
                    f"运行: {running:>3}, "
                    f"速率: {throughput:>8,.0f}/s, "
                    f"成功率: {success_rate:>5.1f}%"
                )
                
                time.sleep(2)
                
            except Exception as e:
                with self.stats_lock:
                    self.errors.append(f"Monitor error: {e}")
                time.sleep(1)
    
    def run(self, duration_seconds=600):
        """运行测试"""
        logger.info("=" * 100)
        logger.info("🔥🔥🔥 超级混乱的超大规模测试 - 极限压力测试 🔥🔥🔥")
        logger.info("=" * 100)
        logger.info(f"⏱️  运行时间: {duration_seconds} 秒 ({duration_seconds/60:.1f} 分钟)")
        logger.info("=" * 100)
        
        self.running = True
        self.start_time = time.time()
        
        try:
            # 1. 初始化执行器
            logger.info("🚀 初始化执行器 (128 workers)...")
            self.executor = DistributedTaskExecutor(
                use_redis=False,
                max_workers=128
            )
            
            # 2. 注册处理器
            self.init_handlers()
            for name, handler in self.handlers.items():
                self.executor.register_handler(name, handler)
            logger.info(f"✅ 注册了 {len(self.handlers)} 个处理器")
            
            # 3. 启动执行器
            self.executor.start(num_workers=128)
            logger.info("✅ 执行器启动 (128 workers)")
            
            # 4. 启动监控线程
            monitor_thread = threading.Thread(target=self.monitor_progress, daemon=True)
            monitor_thread.start()
            
            # 5. 启动提交线程 (24个)
            submit_threads = []
            for i in range(24):
                t = threading.Thread(
                    target=self.submit_chaos_tasks,
                    daemon=True,
                    name=f"Submit-{i}",
                    args=(i,)
                )
                t.start()
                submit_threads.append(t)
                time.sleep(0.01)
            
            # 6. 启动混乱操作线程 (4个)
            chaos_threads = []
            for i in range(4):
                t = threading.Thread(
                    target=self.chaos_operations,
                    daemon=True,
                    name=f"Chaos-{i}"
                )
                t.start()
                chaos_threads.append(t)
                time.sleep(0.1)
            
            logger.info("=" * 100)
            logger.info(f"⏱️  开始压力测试 ({duration_seconds} 秒)...")
            logger.info("=" * 100)
            
            # 7. 主等待
            time.sleep(duration_seconds)
            
            # 8. 停止
            logger.info("=" * 100)
            logger.info("🛑 停止测试...")
            self.running = False
            
            # 等待所有提交线程
            for t in submit_threads:
                t.join(timeout=3)
            
            # 等待混乱操作线程
            for t in chaos_threads:
                t.join(timeout=3)
            
            # 等待队列清空
            logger.info("⏳ 等待队列清空...")
            max_wait = 60
            wait_count = 0
            while wait_count < max_wait:
                stats = self.executor.get_statistics()
                pending = stats['pending']['total']
                running = stats['running']
                
                if pending == 0 and running == 0:
                    break
                
                logger.info(f"  - 待处理: {pending:>10,}, 运行中: {running:>3}")
                
                if wait_count > 30 and pending < 100:
                    break
                
                time.sleep(1)
                wait_count += 1
            
            # 获取最终统计
            stats = self.executor.get_statistics()
            
            # 停止执行器
            self.executor.stop()
            
            # 显示最终结果
            end_time = time.time()
            duration = end_time - self.start_time
            
            with self.stats_lock:
                total = self.total_submitted
                error_count = len(self.errors)
            
            completed = stats['completed']
            failed = stats['failed']
            total_processed = completed + failed
            success_rate = (completed / total_processed * 100) if total_processed > 0 else 0
            avg_throughput = total_processed / duration if duration > 0 else 0
            
            logger.info("=" * 100)
            logger.info("📊 最终结果")
            logger.info("=" * 100)
            logger.info(f"📋 执行统计:")
            logger.info(f"  总提交:     {total:>15,}")
            logger.info(f"  完成:       {completed:>15,}")
            logger.info(f"  失败:       {failed:>15,} ({failed/total_processed*100:.2f}%)")
            logger.info(f"  总处理:     {total_processed:>15,}")
            logger.info(f"  成功率:     {success_rate:>15.2f}%")
            logger.info(f"  系统错误:   {error_count:>15,}")
            logger.info(f"  总耗时:     {duration:>15.1f}s ({duration/60:.1f} 分钟)")
            logger.info(f"  平均吞吐:   {avg_throughput:>15,.0f} 任务/s")
            logger.info(f"  峰值吞吐:   {self.metrics['peak_throughput']:>15,.0f} 任务/s")
            logger.info("=" * 100)
            
            # 验证
            if error_count == 0 and success_rate > 85:
                logger.info("🎉 测试完成! 系统表现优秀!")
                return True
            elif error_count < 10 and success_rate > 80:
                logger.info("✅ 测试完成! 系统运行正常!")
                return True
            else:
                logger.warning(f"⚠️  测试中有些问题需要检查")
                if error_count > 0:
                    logger.warning(f"   系统错误: {error_count}")
                if success_rate < 80:
                    logger.warning(f"   成功率过低: {success_rate:.1f}%")
                return False
            
        except KeyboardInterrupt:
            logger.info("⛔ 测试被中断")
            self.running = False
            if self.executor:
                self.executor.stop()
            return False
            
        except Exception as e:
            logger.error(f"❌ 测试崩溃: {e}", exc_info=True)
            logger.error(traceback.format_exc())
            self.running = False
            if self.executor:
                try:
                    self.executor.stop()
                except:
                    pass
            return False

if __name__ == "__main__":
    test = SuperChaosTest()
    success = test.run(duration_seconds=600)  # 10分钟超大规模测试
    sys.exit(0 if success else 1)
