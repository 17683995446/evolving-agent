"""
第 1 轮：详细日志深度测试 - 找出真实运行时 bug
"""
import sys
import os
import time
import threading
import logging
import random
import traceback
from typing import Dict, Any

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 配置详细日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Iteration1")

def test_1_race_condition():
    """测试问题 1: 并发条件下的竞态"""
    logger.info("=" * 100)
    logger.info("测试 1: 并发条件下的竞态")
    logger.info("=" * 100)
    
    from execution.distributed_task_queue import LocalTaskQueue, DistributedTask
    
    queue = LocalTaskQueue()
    
    error_count = 0
    processed_count = 0
    lock = threading.Lock()
    
    def submit_worker(n):
        nonlocal error_count
        for i in range(n):
            try:
                task = DistributedTask(task_type=f"submit_{i}")
                queue.enqueue(task)
            except Exception as e:
                with lock:
                    error_count += 1
                logger.error(f"提交错误: {e}")
    
    def process_worker(n):
        nonlocal processed_count, error_count
        local_processed = 0
        for i in range(n):
            try:
                task = queue.dequeue()
                if task:
                    queue.complete(task.task_id, f"done_{task.task_id}")
                    local_processed += 1
            except Exception as e:
                with lock:
                    error_count += 1
                logger.error(f"处理错误: {e}")
        
        with lock:
            processed_count += local_processed
    
    num_tasks_per_worker = 1000
    num_submit_threads = 10
    num_process_threads = 10
    
    # 启动提交线程
    submit_threads = []
    for i in range(num_submit_threads):
        t = threading.Thread(target=submit_worker, args=(num_tasks_per_worker,), name=f"Submit-{i}")
        submit_threads.append(t)
        t.start()
    
    # 启动处理线程
    process_threads = []
    for i in range(num_process_threads):
        t = threading.Thread(target=process_worker, args=(num_tasks_per_worker,), name=f"Process-{i}")
        process_threads.append(t)
        t.start()
    
    # 等待所有提交线程完成
    for t in submit_threads:
        t.join()
    
    # 等待足够时间让处理线程完成
    logger.info("⏳ 等待任务处理...")
    time.sleep(2)
    
    # 结束处理线程（因为队列可能空了）
    for t in process_threads:
        t.join(timeout=1)
    
    total_submitted = num_submit_threads * num_tasks_per_worker
    
    stats = queue.get_stats()
    
    logger.info(f"📊 结果:")
    logger.info(f"  总提交: {total_submitted}")
    logger.info(f"  统计完成: {stats['completed']}")
    logger.info(f"  统计失败: {stats['failed']}")
    logger.info(f"  实际处理计数: {processed_count}")
    logger.info(f"  错误数: {error_count}")
    logger.info(f"  剩余待处理: {stats['pending']['total']}")
    
    # 找问题
    total_in_stats = stats['completed'] + stats['failed'] + stats['pending']['total']
    if total_in_stats < total_submitted * 0.9:
        logger.warning(f"⚠️ 发现问题！任务丢失了: 总任务数 {total_submitted}，但状态里只有 {total_in_stats}")
    
    return error_count == 0

def test_2_failed_retry_bug():
    """测试问题 2: 失败重试流程"""
    logger.info("=" * 100)
    logger.info("测试 2: 失败重试流程")
    logger.info("=" * 100)
    
    from execution.distributed_task_queue import LocalTaskQueue, DistributedTask
    
    queue = LocalTaskQueue()
    
    task = DistributedTask(task_type="retry_test", max_retries=3)
    
    logger.info(f"初始任务: id={task.task_id}, retry_count={task.retry_count}, max_retries={task.max_retries}")
    logger.info("说明: max_retries=3表示最多重试3次，共4次尝试")
    
    queue.enqueue(task)
    
    # 第1次取出和失败 - 应该重试
    dequeued_1 = queue.dequeue()
    logger.info(f"第1次尝试: retry_count={dequeued_1.retry_count}")
    result_1 = queue.fail(dequeued_1.task_id, "第1次失败")
    stats_1 = queue.get_stats()
    logger.info(f"  结果: pending={stats_1['pending']['total']}, failed={stats_1['failed']}")
    
    # 第2次取出和失败 - 应该重试
    dequeued_2 = queue.dequeue()
    if dequeued_2:
        logger.info(f"第2次尝试: retry_count={dequeued_2.retry_count}")
        result_2 = queue.fail(dequeued_2.task_id, "第2次失败")
        stats_2 = queue.get_stats()
        logger.info(f"  结果: pending={stats_2['pending']['total']}, failed={stats_2['failed']}")
    else:
        logger.warning("❌ 第2次无法取出！重试流程有问题！")
        return False
    
    # 第3次取出和失败 - 应该重试
    dequeued_3 = queue.dequeue()
    if dequeued_3:
        logger.info(f"第3次尝试: retry_count={dequeued_3.retry_count}")
        result_3 = queue.fail(dequeued_3.task_id, "第3次失败")
        stats_3 = queue.get_stats()
        logger.info(f"  结果: pending={stats_3['pending']['total']}, failed={stats_3['failed']}")
    else:
        logger.warning("❌ 第3次无法取出！重试流程有问题！")
        return False
    
    # 第4次取出和失败 - 这次应该不再重试
    dequeued_4 = queue.dequeue()
    if dequeued_4:
        logger.info(f"第4次尝试: retry_count={dequeued_4.retry_count}")
        result_4 = queue.fail(dequeued_4.task_id, "第4次失败")
        stats_4 = queue.get_stats()
        logger.info(f"  结果: pending={stats_4['pending']['total']}, failed={stats_4['failed']}")
        
        # 第5次应该没有任务了
        dequeued_5 = queue.dequeue()
        if dequeued_5:
            logger.warning("❌ 问题！第5次还能取出！")
            return False
        else:
            if stats_4['failed'] == 1:
                logger.info("✅ 最终 failed 计数 1，正确！")
                return True
            else:
                logger.warning(f"❌ failed 计数 {stats_4['failed']}，期望 1！")
                return False
    else:
        logger.warning("❌ 第 2 次无法取出！重试流程有问题！")
        return False

def test_3_executor_full_flow():
    """测试问题 3: 完整执行器流程"""
    logger.info("=" * 100)
    logger.info("测试 3: 完整执行器流程")
    logger.info("=" * 100)
    
    from execution.distributed_task_queue import DistributedTaskExecutor, TaskPriority
    
    results_collected = []
    errors_collected = []
    lock = threading.Lock()
    
    executor = DistributedTaskExecutor(use_redis=False, max_workers=8)
    
    def handler_success(params):
        return params.get("value", 0) * 2
    
    def handler_flaky(params):
        if random.random() < 0.2:
            raise RuntimeError("随机性失败!")
        return "ok"
    
    def handler_slow(params):
        time.sleep(0.005)
        return "slow"
    
    executor.register_handler("success", handler_success)
    executor.register_handler("flaky", handler_flaky)
    executor.register_handler("slow", handler_slow)
    
    executor.start(num_workers=8)
    
    num_tasks = 1000
    
    logger.info(f"📋 开始提交 {num_tasks} 任务...")
    
    for i in range(num_tasks):
        if i % 3 == 0:
            executor.submit_task("success", f"成功-{i}", {"value": i}, priority=TaskPriority.HIGH)
        elif i % 3 == 1:
            executor.submit_task("flaky", f"不稳定-{i}", {}, priority=TaskPriority.NORMAL)
        else:
            executor.submit_task("slow", f"慢速-{i}", {}, priority=TaskPriority.LOW)
    
    logger.info(f"✅ 所有 {num_tasks} 任务已提交")
    
    # 监控进度
    for i in range(10):
        stats = executor.get_statistics()
        logger.info(f"  [{i+1}] 待处理: {stats['pending']['total']:4}, 运行中: {stats['running']:2}, "
                   f"完成: {stats['completed']:4}, 失败: {stats['failed']:3}")
        time.sleep(0.5)
    
    # 最终统计
    stats = executor.get_statistics()
    total_processed = stats['completed'] + stats['failed']
    
    executor.stop()
    
    logger.info("=" * 100)
    logger.info("📊 最终结果:")
    logger.info(f"  总提交: {num_tasks}")
    logger.info(f"  完成: {stats['completed']}")
    logger.info(f"  失败: {stats['failed']}")
    logger.info(f"  总处理: {total_processed}")
    logger.info(f"  丢失: {num_tasks - total_processed}")
    
    if num_tasks - total_processed > 10:
        logger.warning(f"❌ 有严重问题！丢失了 {num_tasks - total_processed} 个任务！")
        return False
    
    return True

def test_4_massive_chaos():
    """测试问题 4: 超级混乱场景"""
    logger.info("=" * 100)
    logger.info("测试 4: 超级混乱场景")
    logger.info("=" * 100)
    
    from execution.distributed_task_queue import DistributedTaskExecutor, TaskPriority
    
    executor = DistributedTaskExecutor(use_redis=False, max_workers=16)
    
    # 大量不同的处理器
    def handler_a(p): return "A"
    def handler_b(p): return "B"
    def handler_c(p): 
        time.sleep(random.random() * 0.02)
        return "C"
    def handler_d(p):
        if random.random() < 0.1:
            raise Exception("D 随机错误")
        return "D"
    
    executor.register_handler("A", handler_a)
    executor.register_handler("B", handler_b)
    executor.register_handler("C", handler_c)
    executor.register_handler("D", handler_d)
    
    executor.start(num_workers=16)
    
    num_tasks = 5000
    
    def chaos_submitter():
        """混乱的提交线程"""
        types = ["A", "B", "C", "D", "Unknown"]
        priorities = list(TaskPriority)
        for i in range(num_tasks // 4):
            task_type = random.choice(types)
            priority = random.choice(priorities)
            executor.submit_task(task_type, f"混乱-{i}", {}, priority)
    
    # 启动多个混乱提交线程
    submit_threads = []
    for i in range(4):
        t = threading.Thread(target=chaos_submitter, name=f"Chaos-{i}")
        t.start()
        submit_threads.append(t)
    
    # 等待提交
    for t in submit_threads:
        t.join()
    
    logger.info(f"📋 所有 {num_tasks} 混乱任务已提交")
    
    # 长时间监控
    max_wait = 10
    start_time = time.time()
    
    last_total = 0
    stalled_count = 0
    
    while time.time() - start_time < max_wait:
        stats = executor.get_statistics()
        total = stats['completed'] + stats['failed']
        
        # 检查是否卡住
        if total == last_total:
            stalled_count += 1
        else:
            stalled_count = 0
        
        if stalled_count > 5:
            logger.warning(f"⚠️  系统好像卡住了！{stalled_count} 次无进展！")
        
        logger.info(f"  [等待] 待处理: {stats['pending']['total']:4}, "
                   f"运行中: {stats['running']:2}, 完成: {stats['completed']:4}, "
                   f"失败: {stats['failed']:3}, 总处理: {total}")
        
        last_total = total
        time.sleep(0.5)
    
    # 最终状态
    stats = executor.get_statistics()
    total_processed = stats['completed'] + stats['failed']
    
    executor.stop()
    
    logger.info("=" * 100)
    logger.info(f"📊 混乱场景结果:")
    logger.info(f"  总提交: {num_tasks}")
    logger.info(f"  完成: {stats['completed']}")
    logger.info(f"  失败: {stats['failed']}")
    logger.info(f"  总处理: {total_processed}")
    logger.info(f"  完成率: {total_processed/num_tasks*100:.1f}%")
    
    return total_processed > num_tasks * 0.8  # 至少 80% 完成率

def run_all_iteration_1_tests():
    """运行第 1 轮所有测试"""
    logger.info("=" * 100)
    logger.info("🚀 第 1 轮：详细日志深度测试 - 开始")
    logger.info("=" * 100)
    
    results = {}
    
    # 测试 1: 竞态条件
    results["竞态条件"] = test_1_race_condition()
    
    # 测试 2: 失败重试
    results["失败重试"] = test_2_failed_retry_bug()
    
    # 测试 3: 完整流程
    results["完整流程"] = test_3_executor_full_flow()
    
    # 测试 4: 混乱场景
    results["混乱场景"] = test_4_massive_chaos()
    
    logger.info("\n" + "=" * 100)
    logger.info("📋 第 1 轮测试结果汇总")
    logger.info("=" * 100)
    
    all_passed = True
    for name, passed in results.items():
        status = "✅" if passed else "❌"
        logger.info(f"{status} {name}")
        if not passed:
            all_passed = False
    
    logger.info("=" * 100)
    if all_passed:
        logger.info("🎉 第 1 轮所有测试通过！")
    else:
        logger.warning("⚠️  第 1 轮有测试失败，需要修复问题！")
    
    return all_passed

if __name__ == "__main__":
    success = run_all_iteration_1_tests()
    sys.exit(0 if success else 1)
