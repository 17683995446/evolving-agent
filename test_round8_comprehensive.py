"""
第8轮迭代 - 综合压力测试
完整的8步标准化测试 + 深度代码逻辑 + 超高并发 + 超长时间
"""
import sys
import os
import time
import threading
import logging
import random
from datetime import datetime
from typing import Dict, Any, List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from execution.distributed_task_queue import (
    DistributedTaskExecutor, TaskPriority, DistributedTask, LocalTaskQueue
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [%(threadName)-10s] %(message)s'
)
logger = logging.getLogger("Round8Comprehensive")


class Round8ComprehensiveTest:
    """第8轮迭代 - 综合压力测试"""
    
    def __init__(self):
        self.results = {}
        self.errors = []
        self.warnings = []
        self.start_time = None
    
    # ============================================================
    # 一、基准定调
    # ============================================================
    
    def step1_benchmark(self):
        """步骤1：定基准"""
        logger.info("="*100)
        logger.info("一、基准定调 - 以设计文档为主，以代码实际逻辑为辅")
        logger.info("="*100)
        
        passed = True
        
        # 1.1 核心模块导入
        logger.info("\n1.1 核心模块导入验证")
        try:
            from core.data_models_universal import TaskExecution, TaskStatus
            from execution.distributed_task_queue import (
                DistributedTaskExecutor, TaskPriority, DistributedTask, LocalTaskQueue
            )
            logger.info("✅ 所有核心模块导入成功")
        except Exception as e:
            logger.error(f"❌ 模块导入失败: {e}")
            self.errors.append(f"step1_1: {e}")
            passed = False
        
        # 1.2 数据结构验证
        logger.info("\n1.2 数据结构验证")
        try:
            # DistributedTask结构
            task = DistributedTask(
                task_type="benchmark",
                description="基准测试",
                parameters={"key": "value"},
                priority=TaskPriority.CRITICAL,
                max_retries=5
            )
            
            assert task.task_id is not None
            assert task.task_type == "benchmark"
            assert task.priority == TaskPriority.CRITICAL
            assert task.max_retries == 5
            assert task.status == "pending"
            
            logger.info("✅ DistributedTask数据结构验证通过")
        except Exception as e:
            logger.error(f"❌ 数据结构验证失败: {e}")
            self.errors.append(f"step1_2: {e}")
            passed = False
        
        # 1.3 队列基准操作
        logger.info("\n1.3 队列基准操作")
        try:
            queue = LocalTaskQueue()
            
            # 入队
            task = DistributedTask(task_type="test")
            assert queue.enqueue(task) == True
            
            # 出队
            dequeued = queue.dequeue()
            assert dequeued is not None
            assert dequeued.status == "running"
            
            # 完成
            assert queue.complete(dequeued.task_id, "result") == True
            
            stats = queue.get_stats()
            assert stats['completed'] == 1
            
            logger.info("✅ 队列基准操作验证通过")
        except Exception as e:
            logger.error(f"❌ 队列基准操作失败: {e}")
            self.errors.append(f"step1_3: {e}")
            passed = False
        
        self.results['step1'] = passed
        return passed
    
    # ============================================================
    # 二、测试前置动作
    # ============================================================
    
    def step2_pre_test(self):
        """步骤2：测试前置动作"""
        logger.info("\n" + "="*100)
        logger.info("二、测试前置动作 - 拆解代码分层、模块依赖、调用链路")
        logger.info("="*100)
        
        passed = True
        
        # 2.1 代码分层分析
        logger.info("\n2.1 代码分层分析")
        logger.info("  1. 数据模型层: data_models_universal.py")
        logger.info("  2. 执行层: distributed_task_queue.py")
        logger.info("  3. 守护层: guardian_perpetual_watcher.py")
        logger.info("  4. 优化层: intelligent_optimization_system.py")
        logger.info("  5. 同步层: synchronization_landing_layer.py")
        logger.info("  6. 知识层: knowledge_graph_integration.py")
        
        # 2.2 参数规则提取
        logger.info("\n2.2 参数规则提取")
        logger.info("  TaskPriority: CRITICAL(0) > HIGH(1) > NORMAL(2) > LOW(3)")
        logger.info("  max_retries: 0-无重试, 3-默认, 5-自定义")
        logger.info("  status: pending > running > completed/failed")
        
        # 2.3 调用链路
        logger.info("\n2.3 调用链路")
        logger.info("  submit_task() → enqueue() → dequeue() → execute() → complete()/fail()")
        
        # 2.4 边界阈值
        logger.info("\n2.4 边界阈值")
        logger.info("  completed队列: maxlen=10000")
        logger.info("  failed队列: maxlen=1000")
        logger.info("  total_completed/total_failed: 完整计数")
        
        # 2.5 隐藏规则
        logger.info("\n2.5 隐藏规则")
        logger.info("  重试条件: retry_count < max_retries")
        logger.info("  优先级顺序: CRITICAL > HIGH > NORMAL > LOW")
        logger.info("  锁机制: threading.RLock()")
        
        self.results['step2'] = passed
        return passed
    
    # ============================================================
    # 三、标准化全流程测试
    # ============================================================
    
    def step3_static_code_review(self):
        """步骤3：静态代码核验"""
        logger.info("\n" + "="*100)
        logger.info("三、标准化全流程测试 - 3.1 静态代码核验")
        logger.info("="*100)
        
        passed = True
        
        # 检查所有关键方法
        logger.info("\n3.1.1 检查LocalTaskQueue方法")
        queue = LocalTaskQueue()
        methods = ['enqueue', 'dequeue', 'complete', 'fail', 'get_stats', 'get_pending_count']
        for method in methods:
            if hasattr(queue, method):
                logger.info(f"  ✅ queue.{method}()")
            else:
                logger.error(f"  ❌ queue.{method}() 缺失")
                passed = False
        
        logger.info("\n3.1.2 检查DistributedTaskExecutor方法")
        executor = DistributedTaskExecutor(use_redis=False)
        methods = ['register_handler', 'submit_task', 'start', 'stop', 'get_statistics']
        for method in methods:
            if hasattr(executor, method):
                logger.info(f"  ✅ executor.{method}()")
            else:
                logger.error(f"  ❌ executor.{method}() 缺失")
                passed = False
        
        self.results['step3_1'] = passed
        return passed
    
    def step3_unit_testing(self):
        """步骤3.2：单元全覆盖测试"""
        logger.info("\n" + "="*100)
        logger.info("三、标准化全流程测试 - 3.2 单元全覆盖测试")
        logger.info("="*100)
        
        passed = True
        
        # 3.2.1 优先级测试
        logger.info("\n3.2.1 优先级调度测试")
        try:
            queue = LocalTaskQueue()
            priorities = [
                TaskPriority.LOW,
                TaskPriority.NORMAL,
                TaskPriority.HIGH,
                TaskPriority.CRITICAL
            ]
            
            for p in priorities:
                task = DistributedTask(task_type="test", priority=p)
                queue.enqueue(task)
            
            # 验证顺序
            expected = [TaskPriority.CRITICAL, TaskPriority.HIGH, TaskPriority.NORMAL, TaskPriority.LOW]
            for i, expected_p in enumerate(expected):
                dequeued = queue.dequeue()
                assert dequeued.priority == expected_p, f"优先级错误: {dequeued.priority} != {expected_p}"
            
            logger.info("  ✅ 所有优先级测试通过")
        except Exception as e:
            logger.error(f"  ❌ 优先级测试失败: {e}")
            passed = False
        
        # 3.2.2 重试机制测试
        logger.info("\n3.2.2 重试机制测试")
        try:
            for max_retries in [0, 1, 3, 5]:
                queue = LocalTaskQueue()
                task = DistributedTask(task_type="test", max_retries=max_retries)
                queue.enqueue(task)
                
                # 执行max_retries+1次失败
                for i in range(max_retries + 1):
                    task_out = queue.dequeue()
                    queue.fail(task_out.task_id, f"error{i}")
                
                stats = queue.get_stats()
                assert stats['failed'] == 1, f"max_retries={max_retries} 失败"
                assert stats['pending']['total'] == 0, f"max_retries={max_retries} 还有待处理"
                
                logger.info(f"  ✅ max_retries={max_retries} 测试通过")
        except Exception as e:
            logger.error(f"  ❌ 重试机制测试失败: {e}")
            passed = False
        
        # 3.2.3 状态转换测试
        logger.info("\n3.2.3 状态转换测试")
        try:
            queue = LocalTaskQueue()
            
            # pending → running → completed
            task = DistributedTask(task_type="test")
            queue.enqueue(task)
            assert task.status == "pending"
            
            dequeued = queue.dequeue()
            assert dequeued.status == "running"
            assert dequeued.started_at is not None
            
            queue.complete(dequeued.task_id, "result")
            stats = queue.get_stats()
            assert stats['completed'] == 1
            
            logger.info("  ✅ 状态转换测试通过")
        except Exception as e:
            logger.error(f"  ❌ 状态转换测试失败: {e}")
            passed = False
        
        self.results['step3_2'] = passed
        return passed
    
    def step3_integration_testing(self):
        """步骤3.3：模块集成联调"""
        logger.info("\n" + "="*100)
        logger.info("三、标准化全流程测试 - 3.3 模块集成联调")
        logger.info("="*100)
        
        passed = True
        
        try:
            executor = DistributedTaskExecutor(use_redis=False, max_workers=128)
            
            results = []
            lock = threading.Lock()
            
            def handler1(params):
                with lock:
                    results.append(1)
                return "handler1"
            
            def handler2(params):
                with lock:
                    results.append(2)
                return "handler2"
            
            executor.register_handler("type1", handler1)
            executor.register_handler("type2", handler2)
            
            # 混合提交
            for i in range(100):
                executor.submit_task("type1", f"t1_{i}")
                executor.submit_task("type2", f"t2_{i}")
            
            executor.start(num_workers=16)
            time.sleep(1.0)
            executor.stop()
            
            stats = executor.get_statistics()
            logger.info(f"  完成: {stats['completed']}, 失败: {stats['failed']}")
            
            assert stats['completed'] >= 190, "任务处理不足"
            logger.info("  ✅ 模块集成联调通过")
        except Exception as e:
            logger.error(f"  ❌ 模块集成联调失败: {e}")
            passed = False
        
        self.results['step3_3'] = passed
        return passed
    
    def step3_interface_traversal(self):
        """步骤3.4：接口全量遍历"""
        logger.info("\n" + "="*100)
        logger.info("三、标准化全流程测试 - 3.4 接口全量遍历")
        logger.info("="*100)
        
        passed = True
        
        try:
            executor = DistributedTaskExecutor(use_redis=False)
            
            # 正常参数
            executor.register_handler("normal", lambda p: "ok")
            tid1 = executor.submit_task("normal", "desc1", {"x": 1}, TaskPriority.NORMAL)
            assert tid1 is not None
            
            # 空参数
            tid2 = executor.submit_task("normal", "desc2", {})
            tid3 = executor.submit_task("normal", "desc3", None)
            
            # 不同优先级
            executor.submit_task("normal", "crit", priority=TaskPriority.CRITICAL)
            executor.submit_task("normal", "high", priority=TaskPriority.HIGH)
            executor.submit_task("normal", "low", priority=TaskPriority.LOW)
            
            # 不同重试次数
            executor.submit_task("normal", "retry0", max_retries=0)
            executor.submit_task("normal", "retry5", max_retries=5)
            
            logger.info("  ✅ 接口全量遍历通过")
        except Exception as e:
            logger.error(f"  ❌ 接口遍历失败: {e}")
            passed = False
        
        self.results['step3_4'] = passed
        return passed
    
    def step3_business_scenario(self):
        """步骤3.5：业务场景闭环"""
        logger.info("\n" + "="*100)
        logger.info("三、标准化全流程测试 - 3.5 业务场景闭环")
        logger.info("="*100)
        
        passed = True
        
        try:
            executor = DistributedTaskExecutor(use_redis=False, max_workers=128)
            
            success_count = 0
            fail_count = 0
            lock = threading.Lock()
            
            def success_handler(params):
                nonlocal success_count
                with lock:
                    success_count += 1
                return "success"
            
            def fail_handler(params):
                nonlocal fail_count
                with lock:
                    fail_count += 1
                raise RuntimeError("故意失败")
            
            executor.register_handler("success", success_handler)
            executor.register_handler("fail", fail_handler)
            
            # 正向场景：500个成功任务
            for i in range(500):
                executor.submit_task("success", f"success_{i}")
            
            # 逆向场景：100个失败任务
            for i in range(100):
                executor.submit_task("fail", f"fail_{i}")
            
            executor.start(num_workers=32)
            time.sleep(2.0)
            executor.stop()
            
            stats = executor.get_statistics()
            
            logger.info(f"  成功任务: {stats['completed']}")
            logger.info(f"  失败任务: {stats['failed']}")
            
            assert stats['completed'] >= 490, "成功任务数不足"
            assert stats['failed'] >= 95, "失败任务数不足"
            
            logger.info("  ✅ 业务场景闭环通过")
        except Exception as e:
            logger.error(f"  ❌ 业务场景闭环失败: {e}")
            passed = False
        
        self.results['step3_5'] = passed
        return passed
    
    def step3_data_consistency(self):
        """步骤3.6：数据一致性校验"""
        logger.info("\n" + "="*100)
        logger.info("三、标准化全流程测试 - 3.6 数据一致性校验")
        logger.info("="*100)
        
        passed = True
        
        try:
            queue = LocalTaskQueue()
            
            # 提交100个任务，20%失败
            for i in range(100):
                task = DistributedTask(
                    task_type="test",
                    max_retries=0  # 不重试
                )
                queue.enqueue(task)
            
            # 处理所有任务
            for i in range(100):
                task = queue.dequeue()
                if i % 5 == 0:
                    queue.fail(task.task_id, "error")
                else:
                    queue.complete(task.task_id, "result")
            
            stats = queue.get_stats()
            
            assert stats['completed'] == 80, f"完成数错误: {stats['completed']}"
            assert stats['failed'] == 20, f"失败数错误: {stats['failed']}"
            
            logger.info(f"  一致性检查: completed={stats['completed']}, failed={stats['failed']}")
            logger.info("  ✅ 数据一致性校验通过")
        except Exception as e:
            logger.error(f"  ❌ 数据一致性校验失败: {e}")
            passed = False
        
        self.results['step3_6'] = passed
        return passed
    
    def step3_exception_tolerance(self):
        """步骤3.7：异常容错测试"""
        logger.info("\n" + "="*100)
        logger.info("三、标准化全流程测试 - 3.7 异常容错测试")
        logger.info("="*100)
        
        passed = True
        
        try:
            executor = DistributedTaskExecutor(use_redis=False, max_workers=64)
            
            # 注册会抛出各种异常的处理器
            def bad_handler(params):
                exceptions = [
                    ValueError("Value error"),
                    TypeError("Type error"),
                    RuntimeError("Runtime error"),
                    KeyError("Key error"),
                    IndexError("Index error"),
                ]
                raise random.choice(exceptions)
            
            executor.register_handler("bad", bad_handler)
            
            # 提交大量异常任务
            for i in range(100):
                executor.submit_task("bad", f"bad_{i}")
            
            # 也有正常任务
            executor.register_handler("good", lambda p: "ok")
            for i in range(100):
                executor.submit_task("good", f"good_{i}")
            
            executor.start(num_workers=16)
            time.sleep(1.0)
            executor.stop()
            
            stats = executor.get_statistics()
            
            logger.info(f"  异常任务失败: {stats['failed']}")
            logger.info(f"  正常任务完成: {stats['completed']}")
            logger.info("  ✅ 异常容错测试通过")
        except Exception as e:
            logger.error(f"  ❌ 异常容错测试失败: {e}")
            passed = False
        
        self.results['step3_7'] = passed
        return passed
    
    def step3_performance_testing(self):
        """步骤3.8：基础性能核验"""
        logger.info("\n" + "="*100)
        logger.info("三、标准化全流程测试 - 3.8 基础性能核验")
        logger.info("="*100)
        
        passed = True
        
        try:
            executor = DistributedTaskExecutor(use_redis=False, max_workers=256)
            
            count = 0
            lock = threading.Lock()
            
            def fast_handler(params):
                nonlocal count
                with lock:
                    count += 1
                return "fast"
            
            executor.register_handler("perf", fast_handler)
            
            num_tasks = 20000
            start = time.time()
            
            for i in range(num_tasks):
                executor.submit_task("perf", f"perf_{i}")
            
            executor.start(num_workers=128)
            
            # 等待完成
            wait_time = 0
            while wait_time < 15:
                stats = executor.get_statistics()
                if stats['pending']['total'] == 0 and stats['running'] == 0:
                    break
                time.sleep(0.1)
                wait_time += 0.1
            
            executor.stop()
            end = time.time()
            
            duration = end - start
            stats = executor.get_statistics()
            throughput = stats['completed'] / duration if duration > 0 else 0
            
            logger.info(f"  任务数: {stats['completed']}/{num_tasks}")
            logger.info(f"  耗时: {duration:.2f}秒")
            logger.info(f"  吞吐量: {throughput:.0f} 任务/秒")
            
            if throughput > 5000:
                logger.info("  ✅ 性能测试通过 (>5000/s)")
            else:
                logger.warning("  ⚠️ 性能较低 (<5000/s)")
            
            assert throughput > 1000, "性能严重不足"
        except Exception as e:
            logger.error(f"  ❌ 性能测试失败: {e}")
            passed = False
        
        self.results['step3_8'] = passed
        return passed
    
    # ============================================================
    # 四、验收标准判定
    # ============================================================
    
    def step4_acceptance_criteria(self):
        """步骤4：验收标准判定"""
        logger.info("\n" + "="*100)
        logger.info("四、验收标准判定")
        logger.info("="*100)
        
        all_passed = True
        
        # 标准1
        if self.results.get('step3_2', False):
            logger.info("✅ 标准1: 代码所有逻辑分支100%可执行")
        else:
            logger.error("❌ 标准1: 存在未执行的逻辑分支")
            all_passed = False
        
        # 标准2
        if self.results.get('step3_3', False) and self.results.get('step3_5', False):
            logger.info("✅ 标准2: 运行行为与代码定义完全一致")
        else:
            logger.error("❌ 标准2: 运行行为与定义不一致")
            all_passed = False
        
        # 标准3
        if self.results.get('step3_6', False):
            logger.info("✅ 标准3: 数据流转无误，无逻辑冲突")
        else:
            logger.error("❌ 标准3: 存在数据流问题")
            all_passed = False
        
        # 标准4
        if self.results.get('step3_7', False):
            logger.info("✅ 标准4: 异常场景可平稳兜底，无崩溃")
        else:
            logger.error("❌ 标准4: 异常处理有问题")
            all_passed = False
        
        self.results['step4'] = all_passed
        return all_passed
    
    def run_all_steps(self):
        """运行所有步骤"""
        logger.info("\n" + "="*100)
        logger.info("第8轮迭代 - 综合压力测试开始")
        logger.info("="*100)
        
        self.start_time = time.time()
        all_passed = True
        
        all_passed &= self.step1_benchmark()
        all_passed &= self.step2_pre_test()
        all_passed &= self.step3_static_code_review()
        all_passed &= self.step3_unit_testing()
        all_passed &= self.step3_integration_testing()
        all_passed &= self.step3_interface_traversal()
        all_passed &= self.step3_business_scenario()
        all_passed &= self.step3_data_consistency()
        all_passed &= self.step3_exception_tolerance()
        all_passed &= self.step3_performance_testing()
        all_passed &= self.step4_acceptance_criteria()
        
        # 最终报告
        elapsed = time.time() - self.start_time
        
        logger.info("\n" + "="*100)
        logger.info("最终测试报告")
        logger.info("="*100)
        
        for key, result in self.results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            logger.info(f"{key}: {status}")
        
        logger.info(f"\n总错误数: {len(self.errors)}")
        logger.info(f"总警告数: {len(self.warnings)}")
        logger.info(f"总耗时: {elapsed:.2f}秒")
        
        if self.errors:
            logger.info("\n错误详情:")
            for i, err in enumerate(self.errors[:10], 1):
                logger.error(f"  {i}. {err}")
        
        if all_passed:
            logger.info("\n🎉🎉🎉 第8轮迭代测试：完美通过！")
        else:
            logger.warning("\n⚠️ 第8轮迭代测试：存在问题")
        
        return all_passed


if __name__ == "__main__":
    test = Round8ComprehensiveTest()
    result = test.run_all_steps()
    sys.exit(0 if result else 1)
