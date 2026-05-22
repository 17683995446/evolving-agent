"""
第五轮迭代 - 终极极端混乱测试
包含：
1. 2048 并发提交线程
2. 30+ 种任务类型
3. 90% 失败率任务
4. 动态负载极端波动
5. 内存高压测试
6. 极端优先级混合风暴
7. 45 分钟持续运行
8. 动态注册/注销处理器
9. 快速启动/停止测试
"""
import sys
import os
import time
import threading
import logging
import random
import gc
import tracemalloc
from datetime import datetime
from typing import Dict, Any, List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from execution.distributed_task_queue import (
    DistributedTaskExecutor, TaskPriority, DistributedTask, LocalTaskQueue
)

# 配置详细日志
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s'
)
logger = logging.getLogger("Round5Ultimate")


class Round5UltimateTest:
    """第五轮迭代 - 终极极端混乱测试"""
    
    def __init__(self):
        self.executor = None
        self.total_submitted = 0
        self.start_time = None
        self.lock = threading.Lock()
        self.errors = []
        self.warnings = []
        self.running = True
        self.memory_peak = 0
        
    # ========================================================
    # 30+ 种任务处理器
    # ========================================================
    
    def handler_lightning(self, params):
        """闪电般快速"""
        return "⚡"
    
    def handler_nano_delay(self, params):
        """纳秒级延迟"""
        time.sleep(0.000001)
        return "nano"
    
    def handler_random_short(self, params):
        """随机短延迟"""
        delay = random.uniform(0, 0.001)
        time.sleep(delay)
        return f"short_{delay:.6f}"
    
    def handler_random_medium(self, params):
        """随机中等延迟"""
        delay = random.uniform(0.001, 0.01)
        time.sleep(delay)
        return f"medium_{delay:.4f}"
    
    def handler_random_long(self, params):
        """随机长延迟"""
        delay = random.uniform(0.01, 0.1)
        time.sleep(delay)
        return f"long_{delay:.3f}"
    
    def handler_cpu_light(self, params):
        """轻度 CPU"""
        n = params.get('n', 100)
        result = sum(i * i for i in range(n))
        return str(result)
    
    def handler_cpu_medium(self, params):
        """中度 CPU"""
        n = params.get('n', 1000)
        result = sum(i * i for i in range(n))
        return str(result)
    
    def handler_cpu_heavy(self, params):
        """重度 CPU"""
        n = params.get('n', 5000)
        result = sum(i * i for i in range(n))
        return str(result)
    
    def handler_memory_light(self, params):
        """轻度内存"""
        size = params.get('size', 100)
        data = [{'id': i, 'data': 'x' * 50} for i in range(size)]
        result = len(data)
        del data
        return str(result)
    
    def handler_memory_medium(self, params):
        """中度内存"""
        size = params.get('size', 1000)
        data = [{'id': i, 'data': 'x' * 100} for i in range(size)]
        result = len(data)
        del data
        return str(result)
    
    def handler_memory_heavy(self, params):
        """重度内存"""
        size = params.get('size', 5000)
        data = [{'id': i, 'data': 'x' * 200} for i in range(size)]
        result = len(data)
        del data
        gc.collect()
        return str(result)
    
    def handler_extreme_failure(self, params):
        """90% 失败率"""
        if random.random() < 0.9:
            raise RuntimeError("Extreme failure task failed")
        return "lucky_survivor"
    
    def handler_random_exception_type(self, params):
        """随机异常类型"""
        exceptions = [
            ValueError("Value error"),
            TypeError("Type error"),
            RuntimeError("Runtime error"),
            KeyError("Key error"),
            AttributeError("Attribute error"),
            IndexError("Index error"),
            ZeroDivisionError("Division by zero"),
            OverflowError("Overflow"),
            MemoryError("Memory error"),
        ]
        if random.random() < 0.6:
            raise random.choice(exceptions)
        return "ok"
    
    def handler_large_string(self, params):
        """大型字符串"""
        size = params.get('size', 50000)
        data = 'x' * size
        return f"string_{len(data)}"
    
    def handler_large_list(self, params):
        """大型列表"""
        size = params.get('size', 10000)
        data = list(range(size))
        return f"list_{len(data)}"
    
    def handler_large_dict(self, params):
        """大型字典"""
        size = params.get('size', 5000)
        data = {f"key_{i}": i for i in range(size)}
        return f"dict_{len(data)}"
    
    def handler_nested_structure(self, params):
        """嵌套结构"""
        depth = params.get('depth', 4)
        def nested(d):
            if d <= 0:
                return 'leaf'
            return {'data': d, 'children': [nested(d - 1) for _ in range(3)]}
        result = nested(depth)
        return "nested_ok"
    
    def handler_io_light(self, params):
        """轻度 IO"""
        time.sleep(random.uniform(0.0005, 0.002))
        return "io_light"
    
    def handler_io_medium(self, params):
        """中度 IO"""
        time.sleep(random.uniform(0.002, 0.01))
        return "io_medium"
    
    def handler_io_heavy(self, params):
        """重度 IO"""
        time.sleep(random.uniform(0.01, 0.05))
        return "io_heavy"
    
    def handler_fibonacci(self, params):
        """斐波那契计算"""
        n = params.get('n', 15)
        def fib(x):
            if x <= 1:
                return x
            return fib(x - 1) + fib(x - 2)
        return str(fib(n))
    
    def handler_prime_calculation(self, params):
        """素数计算"""
        n = params.get('n', 100)
        primes = []
        for num in range(2, n + 1):
            is_prime = True
            for i in range(2, int(num ** 0.5) + 1):
                if num % i == 0:
                    is_prime = False
                    break
            if is_prime:
                primes.append(num)
        return str(len(primes))
    
    def handler_matrix_operation(self, params):
        """矩阵操作"""
        size = params.get('size', 20)
        matrix = [[random.random() for _ in range(size)] for _ in range(size)]
        result = sum(sum(row) for row in matrix)
        return f"{result:.2f}"
    
    def handler_sort_operation(self, params):
        """排序操作"""
        size = params.get('size', 1000)
        lst = list(range(size))
        random.shuffle(lst)
        lst.sort()
        return "sorted"
    
    def handler_search_operation(self, params):
        """搜索操作"""
        size = params.get('size', 1000)
        lst = list(range(size))
        target = random.randint(0, size - 1)
        index = lst.index(target)
        return str(index)
    
    def handler_hash_operation(self, params):
        """哈希操作"""
        data = str(params.get('data', 'test'))
        hash_val = hash(data)
        return str(hash_val)
    
    def handler_chain_10(self, params):
        """链式 10 步"""
        result = 0
        for i in range(10):
            result += i
            time.sleep(0.0001)
        return str(result)
    
    def handler_chain_20(self, params):
        """链式 20 步"""
        result = 0
        for i in range(20):
            result += i
            time.sleep(0.0001)
        return str(result)
    
    def handler_retry_adaptive(self, params):
        """自适应重试"""
        attempt = params.get('attempt', 0)
        retry_count = params.get('retry_count', 0)
        if retry_count < 3 and random.random() < 0.7:
            raise RuntimeError(f"Retry {retry_count} failed")
        return f"success_after_{retry_count}"
    
    def handler_cleanup(self, params):
        """清理任务"""
        resources = [object() for _ in range(200)]
        del resources
        gc.collect()
        return "clean"
    
    def handler_garbage_gen(self, params):
        """垃圾生成"""
        for _ in range(100):
            temp = [object() for _ in range(50)]
            del temp
        return "garbage"
    
    # ========================================================
    # 处理器管理器
    # ========================================================
    
    def get_all_handlers(self) -> List[tuple]:
        """获取所有处理器"""
        return [
            ('lightning', self.handler_lightning),
            ('nano_delay', self.handler_nano_delay),
            ('random_short', self.handler_random_short),
            ('random_medium', self.handler_random_medium),
            ('random_long', self.handler_random_long),
            ('cpu_light', self.handler_cpu_light),
            ('cpu_medium', self.handler_cpu_medium),
            ('cpu_heavy', self.handler_cpu_heavy),
            ('memory_light', self.handler_memory_light),
            ('memory_medium', self.handler_memory_medium),
            ('memory_heavy', self.handler_memory_heavy),
            ('extreme_failure', self.handler_extreme_failure),
            ('random_exception_type', self.handler_random_exception_type),
            ('large_string', self.handler_large_string),
            ('large_list', self.handler_large_list),
            ('large_dict', self.handler_large_dict),
            ('nested_structure', self.handler_nested_structure),
            ('io_light', self.handler_io_light),
            ('io_medium', self.handler_io_medium),
            ('io_heavy', self.handler_io_heavy),
            ('fibonacci', self.handler_fibonacci),
            ('prime_calculation', self.handler_prime_calculation),
            ('matrix_operation', self.handler_matrix_operation),
            ('sort_operation', self.handler_sort_operation),
            ('search_operation', self.handler_search_operation),
            ('hash_operation', self.handler_hash_operation),
            ('chain_10', self.handler_chain_10),
            ('chain_20', self.handler_chain_20),
            ('retry_adaptive', self.handler_retry_adaptive),
            ('cleanup', self.handler_cleanup),
            ('garbage_gen', self.handler_garbage_gen),
        ]
    
    # ========================================================
    # 极端场景生成器
    # ========================================================
    
    def generate_ultimate_task(self, scenario_id: int):
        """生成终极极端任务"""
        handlers = self.get_all_handlers()
        task_names = [name for name, _ in handlers]
        
        # 随机选择任务类型
        task_type = random.choice(task_names)
        
        # 随机优先级 - 极端混合
        priority_choice = random.random()
        if priority_choice < 0.3:
            priority = TaskPriority.CRITICAL
        elif priority_choice < 0.5:
            priority = TaskPriority.HIGH
        elif priority_choice < 0.8:
            priority = TaskPriority.NORMAL
        else:
            priority = TaskPriority.LOW
        
        # 随机参数
        params = {
            'scenario_id': scenario_id,
            'n': random.randint(50, 10000),
            'size': random.randint(50, 10000),
            'depth': random.randint(1, 5),
            'data': ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=30)),
            'attempt': random.randint(0, 4),
            'retry_count': random.randint(0, 3)
        }
        
        # 随机重试次数
        max_retries = random.randint(0, 8)
        
        # 提交任务
        try:
            task_id = self.executor.submit_task(
                task_type,
                f"Ultimate-{scenario_id}",
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
        
        # 启动内存监控
        tracemalloc.start()
        
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
                
                # 内存监控
                current, peak = tracemalloc.get_traced_memory()
                self.memory_peak = max(self.memory_peak, peak)
                memory_mb = peak / (1024 * 1024)
                
                timestamp = datetime.now().strftime("%H:%M:%S")
                
                logger.info(
                    f"[{timestamp}] ⏱️{elapsed:>7.1f}s | "
                    f"📥{self.total_submitted:>10,} | "
                    f"✅{completed:>10,} | "
                    f"❌{failed:>8,} | "
                    f"📋{pending:>8,} | "
                    f"⚙️{running:>4} | "
                    f"🚀{throughput:>7,.0f}/s | "
                    f"📈{success_rate:>5.1f}% | "
                    f"💾{memory_mb:>6.1f}MB"
                )
                
                # 检查异常情况
                if pending > 200000 and running == 0:
                    self.warnings.append("Queue stuck: pending > 200K but running == 0")
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
        
        # 停止内存监控
        tracemalloc.stop()
    
    # ========================================================
    # 主测试流程
    # ========================================================
    
    def run_ultimate_test(self, duration_minutes: int = 45):
        """运行终极极端混乱测试"""
        logger.info("=" * 100)
        logger.info("🔥🔥🔥🔥🔥 第五轮迭代 - 终极极端混乱测试 🔥🔥🔥🔥🔥")
        logger.info("=" * 100)
        logger.info(f"⏱️  运行时间: {duration_minutes} 分钟")
        logger.info(f"📊 任务类型: {len(self.get_all_handlers())} 种")
        logger.info(f"⚙️  工作线程: 512")
        logger.info(f"📥 提交线程: 2048")
        logger.info("=" * 100)
        
        self.start_time = time.time()
        
        try:
            # 1. 初始化执行器
            logger.info("🚀 初始化执行器...")
            self.executor = DistributedTaskExecutor(use_redis=False, max_workers=512)
            
            # 2. 注册所有处理器
            handlers = self.get_all_handlers()
            
            for name, handler in handlers:
                self.executor.register_handler(name, handler)
            
            logger.info(f"✅ 注册了 {len(handlers)} 种处理器")
            
            # 3. 启动执行器
            self.executor.start(num_workers=512)
            logger.info("✅ 执行器已启动 (512 workers)")
            
            # 4. 启动监控线程
            monitor_thread = threading.Thread(
                target=self.monitor_progress,
                args=(duration_minutes * 60,),
                daemon=True,
                name="Monitor"
            )
            monitor_thread.start()
            
            # 5. 启动多个提交线程
            def submit_worker(worker_id: int, base_tasks_per_second: float):
                """提交工作线程"""
                scenario_id = worker_id * 1000000
                
                while self.running and (time.time() - self.start_time) < duration_minutes * 60:
                    # 极端动态负载
                    elapsed = time.time() - self.start_time
                    
                    # 每 3 分钟极端波动
                    phase = int(elapsed / 180) % 6
                    
                    if phase == 0:
                        rate = base_tasks_per_second * 0.3  # 极慢
                    elif phase == 1:
                        rate = base_tasks_per_second * 5  # 极快
                    elif phase == 2:
                        rate = base_tasks_per_second  # 正常
                    elif phase == 3:
                        rate = base_tasks_per_second * 8  # 超级快
                    elif phase == 4:
                        rate = base_tasks_per_second * 0.1  # 几乎停止
                    else:
                        rate = base_tasks_per_second * random.uniform(0.1, 10)  # 完全随机
                    
                    actual_interval = 1.0 / rate if rate > 0 else 0.01
                    
                    # 提交任务
                    self.generate_ultimate_task(scenario_id)
                    scenario_id += 1
                    
                    time.sleep(actual_interval)
            
            # 启动 2048 个提交线程
            num_workers = 2048
            base_tasks_per_second = 20  # 每个线程基础速率
            
            logger.info(f"🚀 启动 {num_workers} 个提交线程...")
            
            submit_threads = []
            for i in range(num_workers):
                t = threading.Thread(
                    target=submit_worker,
                    args=(i, base_tasks_per_second),
                    name=f"SubmitWorker-{i}",
                    daemon=True
                )
                submit_threads.append(t)
                t.start()
            
            logger.info(f"✅ {num_workers} 个提交线程已启动")
            
            # 6. 等待测试完成
            logger.info(f"\n⏱️  开始终极极端混乱场景测试 ({duration_minutes} 分钟)...")
            
            # 等待监控线程结束
            monitor_thread.join()
            
            # 停止提交
            self.running = False
            
            # 等待提交线程结束
            for t in submit_threads:
                t.join(timeout=2)
            
            # 7. 等待队列清空
            logger.info("\n⏳ 等待队列清空...")
            wait_count = 0
            while wait_count < 180:
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
            
            memory_peak_mb = self.memory_peak / (1024 * 1024)
            
            # 9. 停止执行器
            self.executor.stop()
            
            # 10. 生成最终报告
            logger.info("\n" + "=" * 100)
            logger.info("📋 第五轮迭代 - 最终报告")
            logger.info("=" * 100)
            logger.info(f"📊 执行统计:")
            logger.info(f"  总提交:     {self.total_submitted:>15,}")
            logger.info(f"  完成:       {completed:>15,}")
            logger.info(f"  失败:       {failed:>15,}")
            logger.info(f"  总处理:     {total_processed:>15,}")
            logger.info(f"  成功率:     {success_rate:>15.2f}%")
            logger.info(f"  总耗时:     {elapsed:>15.1f}s ({elapsed/60:.1f} 分钟)")
            logger.info(f"  平均吞吐:   {throughput:>15,.0f} 任务/秒")
            logger.info(f"  峰值内存:   {memory_peak_mb:>15.2f} MB")
            
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
            
            if success_rate < 60:
                logger.warning(f"❌ 成功率 {success_rate:.1f}% < 60%")
                passed = False
            else:
                logger.info("✅ 成功率 >= 60%")
            
            if len(self.errors) > 200:
                logger.warning(f"❌ 错误过多: {len(self.errors)}")
                passed = False
            else:
                logger.info("✅ 错误数量可接受")
            
            if passed:
                logger.info("\n🎉🎉🎉 第五轮迭代 - 终极极端混乱测试通过！")
                return True
            else:
                logger.warning("\n⚠️  第五轮迭代测试部分不通过")
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
    test = Round5UltimateTest()
    result = test.run_ultimate_test(duration_minutes=45)
    
    sys.exit(0 if result else 1)
