"""
超极限测试 - Ultra Extreme Test
500万+任务，512并发线程，30分钟持续运行
"""
import sys
import os
import time
import threading
import logging
import random
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from execution.distributed_task_queue import (
    DistributedTaskExecutor, TaskPriority, DistributedTask, LocalTaskQueue
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s'
)
logger = logging.getLogger("UltraExtreme")

class UltraExtremeTest:
    def __init__(self):
        self.executor = None
        self.total_submitted = 0
        self.start_time = None
        self.lock = threading.Lock()
        self.errors = []
        self.warnings = []
        
    def handler_fast_success(self, params):
        """瞬间成功"""
        return "success"
    
    def handler_random_delay(self, params):
        """随机延迟"""
        delay = random.uniform(0, 0.01)
        time.sleep(delay)
        return f"done in {delay:.4f}s"
    
    def handler_cpu_work(self, params):
        """CPU密集"""
        result = sum(i * i for i in range(1000))
        return str(result)
    
    def handler_random_fail(self, params):
        """随机失败"""
        if random.random() < 0.5:
            raise RuntimeError("Random failure")
        return "success"
    
    def run_ultra_extreme_test(self, duration_minutes=30):
        """运行超极限测试"""
        logger.info("=" * 100)
        logger.info("🔥🔥🔥 超极限测试开始 - 500万+任务，30分钟 🔥🔥🔥")
        logger.info("=" * 100)
        
        self.start_time = time.time()
        
        try:
            # 初始化执行器
            logger.info("🚀 初始化执行器 (512 workers)...")
            self.executor = DistributedTaskExecutor(use_redis=False, max_workers=512)
            
            # 注册处理器
            handlers = [
                ('fast_success', self.handler_fast_success),
                ('random_delay', self.handler_random_delay),
                ('cpu_work', self.handler_cpu_work),
                ('random_fail', self.handler_random_fail)
            ]
            
            for name, handler in handlers:
                self.executor.register_handler(name, handler)
            
            # 启动执行器
            self.executor.start(num_workers=256)
            logger.info("✅ 执行器已启动 (256 workers)")
            
            # 多线程提交任务
            def submit_worker(worker_id, num_tasks):
                for i in range(num_tasks):
                    try:
                        task_type = random.choice(['fast_success', 'random_delay', 'cpu_work', 'random_fail'])
                        priority = random.choice(list(TaskPriority))
                        
                        self.executor.submit_task(
                            task_type,
                            f"Ultra-{worker_id}-{i}",
                            {'task': i},
                            priority=priority
                        )
                        
                        with self.lock:
                            self.total_submitted += 1
                            
                        if i % 10000 == 0:
                            logger.info(f"Worker-{worker_id}: 提交了 {i} 个任务")
                            
                    except Exception as e:
                        with self.lock:
                            self.errors.append(f"Submit error: {e}")
            
            # 启动512个提交线程，每个线程提交10000个任务
            num_workers = 512
            tasks_per_worker = 10000
            total_tasks = num_workers * tasks_per_worker
            
            logger.info(f"🚀 启动 {num_workers} 个提交线程，每线程 {tasks_per_worker} 任务...")
            logger.info(f"📊 总计将提交 {total_tasks:,} 个任务")
            
            threads = []
            for i in range(num_workers):
                t = threading.Thread(
                    target=submit_worker,
                    args=(i, tasks_per_worker),
                    name=f"SubmitWorker-{i}",
                    daemon=True
                )
                threads.append(t)
                t.start()
                
                # 每启动32个线程，等待一下
                if (i + 1) % 32 == 0:
                    logger.info(f"已启动 {i + 1}/{num_workers} 个提交线程")
            
            # 等待所有提交线程完成
            for t in threads:
                t.join()
            
            logger.info(f"✅ 所有 {total_tasks:,} 任务提交完成")
            
            # 监控处理进度
            logger.info("📊 开始监控处理进度...")
            last_total = 0
            stall_count = 0
            
            while True:
                stats = self.executor.get_statistics()
                pending = stats['pending']['total']
                running = stats['running']
                completed = stats['completed']
                failed = stats['failed']
                total = completed + failed
                
                timestamp = datetime.now().strftime("%H:%M:%S")
                elapsed = time.time() - self.start_time
                
                # 计算速率
                delta_total = total - last_total
                last_total = total
                rate = delta_total if delta_total > 0 else 0
                
                # 成功率
                success_rate = (completed / total * 100) if total > 0 else 0
                
                # 队列状态
                queue_status = "正常" if pending < 50000 else "⚠️积压"
                stall_status = "" if stall_count < 5 else "⚠️停滞"
                
                logger.info(
                    f"[{timestamp}] ⏱️{elapsed:>8.1f}s | "
                    f"📥提交: {self.total_submitted:>10,} | "
                    f"✅完成: {completed:>10,} | "
                    f"❌失败: {failed:>8,} | "
                    f"📋待处理: {pending:>8,} | "
                    f"⚙️运行: {running:>4} | "
                    f"🚀速率: {rate:>8,}/s | "
                    f"📈成功率: {success_rate:>5.2f}% | "
                    f"{queue_status} {stall_status}"
                )
                
                # 检查是否完成
                if pending == 0 and running == 0:
                    logger.info("✅ 队列已清空，任务处理完成")
                    break
                
                # 检查是否卡住
                if total == last_total:
                    stall_count += 1
                    if stall_count > 30:
                        logger.warning(f"⚠️ 系统可能卡住，{stall_count}次无进展")
                else:
                    stall_count = 0
                
                # 检查时间
                if elapsed > duration_minutes * 60:
                    logger.warning(f"⏰ 达到最大运行时间 {duration_minutes} 分钟，停止监控")
                    break
                
                time.sleep(2)
            
            # 获取最终统计
            stats = self.executor.get_statistics()
            
            completed = stats['completed']
            failed = stats['failed']
            total_processed = completed + failed
            success_rate = (completed / total_processed * 100) if total_processed > 0 else 0
            
            elapsed = time.time() - self.start_time
            throughput = total_processed / elapsed if elapsed > 0 else 0
            
            # 停止执行器
            self.executor.stop()
            
            # 生成报告
            logger.info("\n" + "=" * 100)
            logger.info("📋 超极限测试 - 最终报告")
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
            logger.info(f"❌ 错误: {len(self.errors)}")
            
            logger.info("=" * 100)
            
            # 验收
            passed = True
            
            if success_rate < 80:
                logger.warning(f"❌ 成功率 {success_rate:.1f}% < 80%")
                passed = False
            else:
                logger.info("✅ 成功率 >= 80%")
            
            if len(self.errors) > 100:
                logger.warning(f"❌ 错误过多: {len(self.errors)}")
                passed = False
            else:
                logger.info("✅ 错误数量可接受")
            
            if passed:
                logger.info("\n🎉 超极限测试通过！")
                return True
            else:
                logger.warning("\n⚠️  超极限测试部分不通过")
                return False
                
        except Exception as e:
            logger.error(f"❌ 测试崩溃: {e}", exc_info=True)
            if self.executor:
                try:
                    self.executor.stop()
                except:
                    pass
            return False

if __name__ == "__main__":
    test = UltraExtremeTest()
    result = test.run_ultra_extreme_test(duration_minutes=30)
    sys.exit(0 if result else 1)
