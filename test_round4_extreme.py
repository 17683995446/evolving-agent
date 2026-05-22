"""
第四轮迭代 - 超极端混乱场景测试
包含：
1. 1024 并发提交线程
2. 20+ 种任务类型
3. 80% 失败率任务
4. 动态负载变化
5. 内存压力测试
6. 极端优先级混合
7. 30 分钟持续运行
"""
import sys
import os
import time
import threading
import logging
import random
import gc
from datetime import datetime
from typing import Dict, Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from execution.distributed_task_queue import (
    DistributedTaskExecutor, TaskPriority, DistributedTask, LocalTaskQueue
)

# 配置详细日志
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s'
)
logger = logging.getLogger("Round4Extreme")


class Round4ExtremeTest:
    """第四轮迭代 - 超极端混乱场景测试"""
    
    def __init__(self):
        self.executor = None
        self.total_submitted = 0
        self.start_time = None
        self.lock = threading.Lock()
        self.errors = []
        self.warnings = []
        self.running = True
        
    # ========================================================
    # 20+ 种任务处理器
    # ========================================================
    
    def handler_instant(self, params):
        """瞬间完成"""
        return "instant"
    
    def handler_micro_delay(self, params):
        """微秒延迟"""
        time.sleep(0.0001)
        return "micro"
    
    def handler_milli_delay(self, params):
        """毫秒延迟"""
        delay = random.uniform(0.001, 0.01)
        time.sleep(delay)
        return f"milli_{delay:.4f}"
    
    def handler_cpu_intensive(self, params):
        """CPU 密集"""
        n = params.get('n', 1000)
        result = sum(i * i for i in range(n))
        return str(result)
    
    def handler_memory_intensive(self, params):
        """内存密集"""
        size = params.get('size', 1000)
        data = [{'id': i, 'data': 'x' * 100} for i in range(size)]
        result = len(data)
        del data
        return str(result)
    
    def handler_high_failure(self, params):
        """80% 失败率"""
        if random.random() < 0.8:
            raise RuntimeError("High failure rate task failed")
        return "survived"
    
    def handler_random_exception(self, params):
        """随机异常类型"""
        exceptions = [
            ValueError("Value error"),
            TypeError("Type error"),
            RuntimeError("Runtime error"),
            KeyError("Key error"),
            AttributeError("Attribute error"),
            IndexError("Index error"),
        ]
        if random.random() < 0.5:
            raise random.choice(exceptions)
        return "ok"
    
    def handler_large_data(self, params):
        """大数据处理"""
        size = params.get('size', 10000)
        data = 'x' * size
        return f"processed_{len(data)}_bytes"
    
    def handler_nested_computation(self, params):
        """嵌套计算"""
        depth = params.get('depth', 3)
        def compute(d):
            if d <= 0:
                return 1
            return sum(compute(d - 1) for _ in range(2))
        return str(compute(depth))
    
    def handler_state_machine(self, params):
        """状态机模拟"""
        state = params.get('state', 'start')
        transitions = {
            'start': 'init',
            'init': 'process',
            'process': 'validate',
            'validate': 'complete',
            'complete': 'done'
        }
        next_state = transitions.get(state, 'unknown')
        return next_state
    
    def handler_batch_process(self, params):
        """批量处理"""
        items = params.get('items', list(range(10)))
        results = [x * 2 for x in items]
        return str(len(results))
    
    def handler_string_manipulation(self, params):
        """字符串操作"""
        s = params.get('text', 'hello')
        result = s.upper() + s.lower() + s[::-1]
        return result
    
    def handler_math_operations(self, params):
        """数学运算"""
        x = params.get('x', 1.0)
        result = (x ** 2 + x ** 0.5) / (x + 1)
        return f"{result:.4f}"
    
    def handler_list_operations(self, params):
        """列表操作"""
        lst = list(range(100))
        random.shuffle(lst)
        lst.sort()
        return str(len(lst))
    
    def handler_dict_operations(self, params):
        """字典操作"""
        d = {f"key_{i}": i * 2 for i in range(50)}
        result = sum(d.values())
        return str(result)
    
    def handler_io_simulation(self, params):
        """IO 模拟"""
        time.sleep(random.uniform(0.001, 0.005))
        return "io_done"
    
    def handler_chain_reaction(self, params):
        """链式反应"""
        steps = params.get('steps', 5)
        result = 0
        for i in range(steps):
            result += i
            time.sleep(0.0001)
        return str(result)
    
    def handler_recursive_task(self, params):
        """递归任务"""
        n = params.get('n', 5)
        def fib(x):
            if x <= 1:
                return x
            return fib(x - 1) + fib(x - 2)
        return str(fib(min(n, 10)))
    
    def handler_concurrent_simulation(self, params):
        """并发模拟"""
        results = []
        for i in range(10):
            results.append(i * i)
        return str(sum(results))
    
    def handler_error_recovery(self, params):
        """错误恢复"""
        attempt = params.get('attempt', 0)
        if attempt < 2 and random.random() < 0.5:
            raise RuntimeError(f"Attempt {attempt} failed")
        return f"recovered_after_{attempt}"
    
    def handler_resource_cleanup(self, params):
        """资源清理"""
        resources = [object() for _ in range(100)]
        result = len(resources)
        del resources
        gc.collect()
        return str(result)
    
    # ========================================================
    # 极端场景生成器
    # ========================================================
    
    def generate_extreme_scenario(self, scenario_id: int):
        """生成极端场景任务"""
        task_types = [
            'instant', 'micro_delay', 'milli_delay', 'cpu_intensive',
            'memory_intensive', 'high_failure', 'random_exception',
            'large_data', 'nested_computation', 'state_machine',
            'batch_process', 'string_manipulation', 'math_operations',
            'list_operations', 'dict_operations', 'io_simulation',
            'chain_reaction', 'recursive_task', 'concurrent_simulation',
            'error_recovery', 'resource_cleanup'
        ]
        
        # 随机选择任务类型
        task_type = random.choice(task_types)
        
        # 随机优先级
        priority = random.choice(list(TaskPriority))
        
        # 随机参数
        params = {
            'scenario_id': scenario_id,
            'n': random.randint(100, 5000),
            'size': random.randint(100, 10000),
            'depth': random.randint(1, 5),
            'state': random.choice(['start', 'init', 'process', 'validate']),
            'items': list(range(random.randint(5, 50))),
            'text': ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=20)),
            'x': random.uniform(0.1, 100.0),
            'steps': random.randint(3, 10),
            'attempt': random.randint(0, 3)
        }
        
        # 随机重试次数
        max_retries = random.randint(0, 5)
        
        # 提交任务
        try:
            task_id = self.executor.submit_task(
                task_type,
                f"Extreme-{scenario_id}",
                params,
                priority=priority,
                max_retries=max_retries
            )
            
            with self.lock:
                self.total_submitted += 1
            
            return task_id
        except Exception as e:
            with self.lock:
                self.errors.append(f"Submit error: {e}")
            return None
    
    # ========================================================
    # 监控和报告
    # ========================================================
    
    def monitor_progress(self, duration_seconds: int):
        """监控进度"""
        last_total = 0
        last_time = time.time()
        
        while self.running and (time.time() - self.start_time) < duration_seconds:
            try:
                stats = self.executor.get_statistics()
                
                pending = stats['pending']['total']
                running = stats['running']
                completed = stats['completed']
                failed = stats['failed']
                total = completed + failed
                
                now = time.time()
                elapsed = now - self.start_time
                delta_time = now - last_time
                delta_total = total - last_total
                
                throughput = delta_total / delta_time if delta_time > 0 else 0
                success_rate = (completed / total * 100) if total > 0 else 0
                
                timestamp = datetime.now().strftime("%H:%M:%S")
                
                logger.info(
                    f"[{timestamp}] ⏱️{elapsed:>7.1f}s | "
                    f"📥{self.total_submitted:>10,} | "
                    f"✅{completed:>10,} | "
                    f"❌{failed:>8,} | "
                    f"📋{pending:>8,} | "
                    f"⚙️{running:>4} | "
                    f"🚀{throughput:>7,.0f}/s | "
                    f"📈{success_rate:>5.1f}%"
                )
                
                # 检查异常情况
                if pending > 100000 and running == 0:
                    self.warnings.append("Queue stuck: pending > 100K but running == 0")
                    logger.warning("⚠️ 队列卡住！")
                
                if failed > completed * 0.5 and total > 1000:
                    self.warnings.append(f"High failure rate: {failed}/{total}")
                    logger.warning("⚠️ 失败率过高！")
                
                last_total = total
                last_time = now
                
                time.sleep(2)
                
            except Exception as e:
                self.errors.append(f"Monitor error: {e}")
                logger.error(f"监控错误: {e}")
                time.sleep(1)
    
    # ========================================================
    # 主测试流程
    # ========================================================
    
    def run_extreme_test(self, duration_minutes: int = 30):
        """运行超极端混乱场景测试"""
        logger.info("=" * 100)
        logger.info("🔥🔥🔥 第四轮迭代 - 超极端混乱场景测试 🔥🔥🔥")
        logger.info("=" * 100)
        logger.info(f"⏱️  运行时间: {duration_minutes} 分钟")
        logger.info(f"📊 任务类型: 21 种")
        logger.info(f"⚙️  工作线程: 256")
        logger.info(f"📥 提交线程: 512")
        logger.info("=" * 100)
        
        self.start_time = time.time()
        
        try:
            # 1. 初始化执行器
            logger.info("🚀 初始化执行器...")
            self.executor = DistributedTaskExecutor(use_redis=False, max_workers=256)
            
            # 2. 注册所有处理器
            handlers = [
                ('instant', self.handler_instant),
                ('micro_delay', self.handler_micro_delay),
                ('milli_delay', self.handler_milli_delay),
                ('cpu_intensive', self.handler_cpu_intensive),
                ('memory_intensive', self.handler_memory_intensive),
                ('high_failure', self.handler_high_failure),
                ('random_exception', self.handler_random_exception),
                ('large_data', self.handler_large_data),
                ('nested_computation', self.handler_nested_computation),
                ('state_machine', self.handler_state_machine),
                ('batch_process', self.handler_batch_process),
                ('string_manipulation', self.handler_string_manipulation),
                ('math_operations', self.handler_math_operations),
                ('list_operations', self.handler_list_operations),
                ('dict_operations', self.handler_dict_operations),
                ('io_simulation', self.handler_io_simulation),
                ('chain_reaction', self.handler_chain_reaction),
                ('recursive_task', self.handler_recursive_task),
                ('concurrent_simulation', self.handler_concurrent_simulation),
                ('error_recovery', self.handler_error_recovery),
                ('resource_cleanup', self.handler_resource_cleanup),
            ]
            
            for name, handler in handlers:
                self.executor.register_handler(name, handler)
            
            logger.info(f"✅ 注册了 {len(handlers)} 种处理器")
            
            # 3. 启动执行器
            self.executor.start(num_workers=256)
            logger.info("✅ 执行器已启动 (256 workers)")
            
            # 4. 启动监控线程
            monitor_thread = threading.Thread(
                target=self.monitor_progress,
                args=(duration_minutes * 60,),
                daemon=True,
                name="Monitor"
            )
            monitor_thread.start()
            
            # 5. 启动多个提交线程
            def submit_worker(worker_id: int, tasks_per_second: float):
                """提交工作线程"""
                scenario_id = worker_id * 1000000
                interval = 1.0 / tasks_per_second
                
                while self.running and (time.time() - self.start_time) < duration_minutes * 60:
                    # 动态调整提交速率
                    elapsed = time.time() - self.start_time
                    
                    # 每 5 分钟改变一次速率
                    phase = int(elapsed / 300) % 4
                    if phase == 0:
                        rate = tasks_per_second  # 正常
                    elif phase == 1:
                        rate = tasks_per_second * 2  # 高速
                    elif phase == 2:
                        rate = tasks_per_second * 0.5  # 低速
                    else:
                        rate = tasks_per_second * random.uniform(0.5, 3.0)  # 随机
                    
                    actual_interval = 1.0 / rate if rate > 0 else 0.1
                    
                    # 提交任务
                    self.generate_extreme_scenario(scenario_id)
                    scenario_id += 1
                    
                    time.sleep(actual_interval)
            
            # 启动 512 个提交线程
            num_workers = 512
            tasks_per_second = 50  # 每个线程每秒提交 50 个任务
            
            logger.info(f"🚀 启动 {num_workers} 个提交线程...")
            
            submit_threads = []
            for i in range(num_workers):
                t = threading.Thread(
                    target=submit_worker,
                    args=(i, tasks_per_second),
                    name=f"SubmitWorker-{i}",
                    daemon=True
                )
                submit_threads.append(t)
                t.start()
            
            logger.info(f"✅ {num_workers} 个提交线程已启动")
            
            # 6. 等待测试完成
            logger.info(f"\n⏱️  开始超极端混乱场景测试 ({duration_minutes} 分钟)...")
            
            # 等待监控线程结束
            monitor_thread.join()
            
            # 停止提交
            self.running = False
            
            # 等待提交线程结束
            for t in submit_threads:
                t.join(timeout=1)
            
            # 7. 等待队列清空
            logger.info("\n⏳ 等待队列清空...")
            wait_count = 0
            while wait_count < 120:
                stats = self.executor.get_statistics()
                pending = stats['pending']['total']
                running = stats['running']
                
                if pending == 0 and running == 0:
                    logger.info("✅ 队列已清空")
                    break
                
                logger.info(f"  - 待处理: {pending:>10,}, 运行中: {running:>4}")
                time.sleep(1)
                wait_count += 1
            
            # 8. 获取最终统计
            stats = self.executor.get_statistics()
            
            completed = stats['completed']
            failed = stats['failed']
            total_processed = completed + failed
            success_rate = (completed / total_processed * 100) if total_processed > 0 else 0
            
            elapsed = time.time() - self.start_time
            throughput = total_processed / elapsed if elapsed > 0 else 0
            
            # 9. 停止执行器
            self.executor.stop()
            
            # 10. 生成最终报告
            logger.info("\n" + "=" * 100)
            logger.info("📋 第四轮迭代 - 最终报告")
            logger.info("=" * 100)
            logger.info(f"📊 执行统计:")
            logger.info(f"  总提交:     {self.total_submitted:>15,}")
            logger.info(f"  完成:       {completed:>15,}")
            logger.info(f"  失败:       {failed:>15,}")
            logger.info(f"  总处理:     {total_processed:>15,}")
            logger.info(f"  成功率:     {success_rate:>15.2f}%")
            logger.info(f"  总耗时:     {elapsed:>15.1f}s ({elapsed/60:.1f} 分钟)")
            logger.info(f"  平均吞吐:   {throughput:>15,.0f} 任务/秒")
            
            logger.info(f"\n⚠️  警告: {len(self.warnings)}")
            if self.warnings:
                for i, w in enumerate(self.warnings[:10], 1):
                    logger.warning(f"  {i}. {w}")
            
            logger.info(f"\n❌ 错误: {len(self.errors)}")
            if self.errors:
                for i, e in enumerate(self.errors[:10], 1):
                    logger.error(f"  {i}. {e}")
            
            logger.info("=" * 100)
            
            # 验收判定
            passed = True
            
            if success_rate < 70:
                logger.warning(f"❌ 成功率 {success_rate:.1f}% < 70%")
                passed = False
            else:
                logger.info("✅ 成功率 >= 70%")
            
            if len(self.errors) > 100:
                logger.warning(f"❌ 错误过多: {len(self.errors)}")
                passed = False
            else:
                logger.info("✅ 错误数量可接受")
            
            if passed:
                logger.info("\n🎉 第四轮迭代 - 超极端混乱场景测试通过！")
                return True
            else:
                logger.warning("\n⚠️  第四轮迭代测试部分不通过")
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
    test = Round4ExtremeTest()
    result = test.run_extreme_test(duration_minutes=30)
    
    sys.exit(0 if result else 1)
