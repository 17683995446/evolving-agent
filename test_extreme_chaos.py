"""
极端混沌测试 - Extreme Chaos Test
场景：
1. 高频提交（数百线程同时提交）
2. 高频失败（高失败率 + 高重试率）
3. 极端短延迟任务
4. 队列迅速填充和清空
5. 执行器多次启动/停止
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

# 配置极端详细的日志
logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] [%(name)s:%(lineno)d] [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("extreme_test_debug.log", mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ExtremeChaos")


class ExtremeChaosTest:
    def __init__(self):
        self.executor = None
        self.running = False
        self.failure_count = 0
        self.success_count = 0
        self.total_submitted = 0
        self.errors = []
        self.warnings = []
        self.lock = threading.Lock()
        self.start_time = None
        
    # ============ 极端处理器 ============
    
    def handler_instant_success(self, params: dict) -> str:
        """瞬间成功的任务 - 无任何延迟"""
        return "success"
    
    def handler_high_failure(self, params: dict) -> str:
        """90% 失败率的任务"""
        if random.random() < 0.9:
            raise RuntimeError("Intentional failure for chaos test")
        return "success despite odds"
    
    def handler_micro_delay(self, params: dict) -> str:
        """微秒级延迟任务"""
        time.sleep(0.0001)
        return "micro delay"
    
    def handler_random_work(self, params: dict) -> str:
        """随机工作量任务"""
        work = random.randint(0, 10000)
        result = 0
        for i in range(work):
            result += i
        return "work done"
    
    def handler_none_args(self, params: dict) -> str:
        """空参数处理测试"""
        return f"none args: {params}"
    
    def handler_large_data(self, params: dict) -> str:
        """处理大数据的任务"""
        data = params.get('data', '')
        return f"processed {len(data)} bytes"
    
    # ============ 极端测试场景 ============
    
    def test_scenario_1_high_frequency_submit(self):
        """场景1: 极高频率提交任务 - 256线程同时提交"""
        logger.info("=" * 100)
        logger.info("🔥 场景1: 极高频率提交 (256 线程) 🔥")
        logger.info("=" * 100)
        
        def submit_worker(worker_id: int, tasks_per_worker: int):
            for i in range(tasks_per_worker):
                try:
                    priority = random.choice([
                        TaskPriority.CRITICAL,
                        TaskPriority.HIGH,
                        TaskPriority.NORMAL,
                        TaskPriority.LOW
                    ])
                    handler = random.choice([
                        'instant', 'high_failure', 'micro_delay',
                        'random_work', 'none_args'
                    ])
                    task_id = self.executor.submit_task(
                        handler,
                        f"Chaos-{worker_id}-{i}",
                        {'x': i},
                        priority=priority
                    )
                    
                    with self.lock:
                        self.total_submitted += 1
                        
                except Exception as e:
                    with self.lock:
                        self.failure_count += 1
                        self.errors.append(f"Submit error: {e}")
                    logger.error(f"❌ 提交错误 (Worker-{worker_id}): {e}")
        
        workers = []
        num_workers = 256
        tasks_per_worker = 500
        
        logger.info(f"🚀 启动 {num_workers} 个线程，每线程 {tasks_per_worker} 任务...")
        
        start = time.time()
        
        for i in range(num_workers):
            t = threading.Thread(
                target=submit_worker,
                args=(i, tasks_per_worker),
                name=f"Submit-{i}",
                daemon=True
            )
            workers.append(t)
            t.start()
        
        for t in workers:
            t.join()
        
        end = time.time()
        
        logger.info(f"✅ {num_workers * tasks_per_worker} 任务提交完成，耗时 {(end - start):.3f}s")
        
        return end - start
    
    def test_scenario_2_fast_clear_queue(self):
        """场景2: 快速提交和快速清空"""
        logger.info("\n" + "=" * 100)
        logger.info("⚡ 场景2: 快速队列清空 ⚡")
        logger.info("=" * 100)
        
        for round_num in range(10):
            # 快速填充
            for i in range(500):
                self.executor.submit_task('instant', f"Fast-{round_num}-{i}", {})
                with self.lock:
                    self.total_submitted += 1
            
            logger.info(f"Round {round_num}: 提交 500 任务")
            
            # 等待清空
            wait_count = 0
            while wait_count < 30:
                stats = self.executor.get_statistics()
                if stats['pending']['total'] == 0:
                    logger.info(f"  - Round {round_num} 清空完成")
                    break
                time.sleep(0.1)
                wait_count += 1
            
            time.sleep(0.1)
        
        return True
    
    def test_scenario_3_high_retry(self):
        """场景3: 高重试场景"""
        logger.info("\n" + "=" * 100)
        logger.info("🔄 场景3: 高重试场景 🔄")
        logger.info("=" * 100)
        
        num_retry_tasks = 2000
        for i in range(num_retry_tasks):
            self.executor.submit_task(
                'high_failure',
                f"RetryTest-{i}",
                {},
                max_retries=5  # 高重试次数
            )
            with self.lock:
                self.total_submitted += 1
        
        logger.info(f"✅ {num_retry_tasks} 高重试任务提交")
        return True
    
    def test_scenario_4_large_data(self):
        """场景4: 大数据任务"""
        logger.info("\n" + "=" * 100)
        logger.info("📦 场景4: 大数据任务 📦")
        logger.info("=" * 100)
        
        num_large_tasks = 500
        for i in range(num_large_tasks):
            large_data = 'x' * (1000 + i)  # 越来越大的数据
            self.executor.submit_task(
                'large_data',
                f"LargeData-{i}",
                {'data': large_data}
            )
            with self.lock:
                self.total_submitted += 1
        
        logger.info(f"✅ {num_large_tasks} 大数据任务提交")
        return True
    
    def test_scenario_5_mixed_chaos(self):
        """场景5: 混合混沌 - 所有场景同时"""
        logger.info("\n" + "=" * 100)
        logger.info("💥 场景5: 混合混沌 💥")
        logger.info("=" * 100)
        
        def chaos_worker1():
            for i in range(300):
                self.executor.submit_task('instant', f"Mixed1-{i}", {})
                with self.lock:
                    self.total_submitted += 1
                time.sleep(0.001)
        
        def chaos_worker2():
            for i in range(200):
                self.executor.submit_task('high_failure', f"Mixed2-{i}", {}, max_retries=3)
                with self.lock:
                    self.total_submitted += 1
                time.sleep(0.002)
        
        def chaos_worker3():
            for i in range(400):
                self.executor.submit_task('micro_delay', f"Mixed3-{i}", {})
                with self.lock:
                    self.total_submitted += 1
                time.sleep(0.0005)
        
        def chaos_worker4():
            for i in range(200):
                large_data = 'y' * 5000
                self.executor.submit_task('large_data', f"Mixed4-{i}", {'data': large_data})
                with self.lock:
                    self.total_submitted += 1
                time.sleep(0.003)
        
        threads = [
            threading.Thread(target=chaos_worker1, name="Chaos1", daemon=True),
            threading.Thread(target=chaos_worker2, name="Chaos2", daemon=True),
            threading.Thread(target=chaos_worker3, name="Chaos3", daemon=True),
            threading.Thread(target=chaos_worker4, name="Chaos4", daemon=True),
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        logger.info("✅ 混合混沌任务提交完成")
        return True
    
    # ============ 监控和报告 ============
    
    def monitor_stats(self, duration_seconds: int = 300):
        """监控系统状态"""
        logger.info("\n" + "=" * 100)
        logger.info(f"📊 开始监控系统状态 ({duration_seconds}s)...")
        logger.info("=" * 100)
        
        start_monitor = time.time()
        last_total = 0
        
        while time.time() - start_monitor < duration_seconds:
            try:
                stats = self.executor.get_statistics()
                pending = stats['pending']['total']
                running = stats['running']
                completed = stats['completed']
                failed = stats['failed']
                total = completed + failed
                
                now = time.time()
                timestamp = datetime.now().strftime("%H:%M:%S")
                
                # 计算吞吐
                delta_total = total - last_total
                last_total = total
                
                success_rate = (completed / total * 100) if total > 0 else 0
                
                logger.info(
                    f"[{timestamp}] "
                    f"📊 待: {pending:>8,}, "
                    f"运行: {running:>3}, "
                    f"完成: {completed:>10,}, "
                    f"失败: {failed:>8,}, "
                    f"提交: {self.total_submitted:>10,}, "
                    f"成功: {success_rate:>5.1f}%"
                )
                
                # 检查异常
                if pending > 10000 and running == 0:
                    self.warnings.append("Queue stuck: pending > 10000 but running == 0")
                    logger.warning("⚠️  队列卡住: 待处理 > 10000 但无运行中任务")
                
                if failed > completed * 0.5:
                    self.warnings.append("High failure rate")
                    logger.warning("⚠️  失败率超过 50%")
                
                time.sleep(1)
                
            except Exception as e:
                self.errors.append(f"Monitor error: {e}")
                logger.error(f"监控异常: {e}")
                time.sleep(1)
        
        logger.info("✅ 监控完成")
        return True
    
    def final_report(self):
        """最终报告"""
        logger.info("\n" + "=" * 100)
        logger.info("📋 极端混沌测试 - 最终报告")
        logger.info("=" * 100)
        
        if not self.executor:
            logger.warning("❌ 执行器未初始化")
            return False
        
        stats = self.executor.get_statistics()
        
        completed = stats['completed']
        failed = stats['failed']
        total_processed = completed + failed
        success_rate = (completed / total_processed * 100) if total_processed > 0 else 0
        
        duration = (time.time() - self.start_time) if self.start_time else 0
        avg_throughput = total_processed / duration if duration > 0 else 0
        
        logger.info(f"📋 执行统计:")
        logger.info(f"  总提交:     {self.total_submitted:>15,}")
        logger.info(f"  完成:       {completed:>15,}")
        logger.info(f"  失败:       {failed:>15,}")
        logger.info(f"  总处理:     {total_processed:>15,}")
        logger.info(f"  成功率:     {success_rate:>15.2f}%")
        
        if duration > 0:
            logger.info(f"  总耗时:     {duration:>15.1f}s")
            logger.info(f"  平均吞吐:   {avg_throughput:>15,.0f} 任务/秒")
        
        logger.info("\n⚠️  警告列表:")
        if len(self.warnings) == 0:
            logger.info("  (无警告)")
        else:
            for i, w in enumerate(self.warnings[:20]):
                logger.info(f"  {i+1}. {w}")
            if len(self.warnings) > 20:
                logger.info(f"  ... 还有 {len(self.warnings) - 20} 个警告")
        
        logger.info("\n❌ 错误列表:")
        if len(self.errors) == 0:
            logger.info("  (无错误)")
        else:
            for i, e in enumerate(self.errors[:20]):
                logger.info(f"  {i+1}. {e}")
            if len(self.errors) > 20:
                logger.info(f"  ... 还有 {len(self.errors) - 20} 个错误")
        
        logger.info("=" * 100)
        
        # 验收判定
        passed = True
        
        # 标准1: 系统未崩溃
        if len(self.errors) > 0:
            logger.warning(f"❌ 有 {len(self.errors)} 个错误")
            passed = False
        else:
            logger.info("✅ 系统无崩溃错误")
        
        # 标准2: 成功率 >= 80%
        if success_rate < 80:
            logger.warning(f"❌ 成功率 {success_rate:.1f}% < 80%")
            passed = False
        else:
            logger.info("✅ 成功率 >= 80%")
        
        # 标准3: 队列无卡死
        has_stuck_warnings = any("stuck" in w for w in self.warnings)
        if has_stuck_warnings:
            logger.warning("❌ 检测到队列卡死警告")
            passed = False
        else:
            logger.info("✅ 队列无卡死")
        
        logger.info("=" * 100)
        
        if passed:
            logger.info("🎉 极端混沌测试通过！")
            return True
        else:
            logger.warning("⚠️  极端混沌测试部分或全部不通过")
            return False
    
    def run_all_scenarios(self):
        """运行所有极端测试场景"""
        logger.info("=" * 100)
        logger.info("🔥🔥🔥 极端混沌测试开始 🔥🔥🔥")
        logger.info("=" * 100)
        
        self.start_time = time.time()
        
        try:
            # 1. 初始化执行器
            self.executor = DistributedTaskExecutor(use_redis=False, max_workers=256)
            
            # 2. 注册所有处理器
            self.executor.register_handler('instant', self.handler_instant_success)
            self.executor.register_handler('high_failure', self.handler_high_failure)
            self.executor.register_handler('micro_delay', self.handler_micro_delay)
            self.executor.register_handler('random_work', self.handler_random_work)
            self.executor.register_handler('none_args', self.handler_none_args)
            self.executor.register_handler('large_data', self.handler_large_data)
            logger.info("✅ 所有处理器已注册")
            
            # 3. 启动执行器
            self.executor.start(num_workers=128)
            logger.info("✅ 执行器已启动 (128 工作线程)")
            
            # 4. 运行各个场景
            self.test_scenario_1_high_frequency_submit()
            self.test_scenario_2_fast_clear_queue()
            self.test_scenario_3_high_retry()
            self.test_scenario_4_large_data()
            self.test_scenario_5_mixed_chaos()
            
            # 5. 监控系统运行
            self.monitor_stats(duration_seconds=120)  # 监控2分钟
            
            # 6. 等待队列清空
            logger.info("\n⏳ 等待队列清空...")
            wait_count = 0
            while wait_count < 60:
                stats = self.executor.get_statistics()
                if stats['pending']['total'] == 0:
                    logger.info("✅ 队列已清空")
                    break
                time.sleep(1)
                wait_count += 1
            
            # 7. 生成最终报告
            passed = self.final_report()
            
            # 8. 停止执行器
            self.executor.stop()
            
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
    test = ExtremeChaosTest()
    result = test.run_all_scenarios()
    
    sys.exit(0 if result else 1)
