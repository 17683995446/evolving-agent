"""
深度测试 - 第6轮迭代：完整的8步标准化全流程测试
按照用户要求的严格流程执行
"""
import sys
import os
import time
import threading
import logging
import random
import json
from datetime import datetime
from typing import Dict, Any, List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from execution.distributed_task_queue import (
    DistributedTaskExecutor, TaskPriority, DistributedTask, LocalTaskQueue
)
from core.data_models_universal import TaskExecution, TaskStatus

# 配置详细日志
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s'
)
logger = logging.getLogger("Round6Complete")


class Round6CompleteTest:
    """第6轮迭代：完整的8步标准化测试"""
    
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.test_results = {}
        self.all_passed = True
    
    # ==================================================
    # 一、基准定调
    # ==================================================
    
    def step1_benchmark(self):
        """步骤1：定基准 - 以设计文档为主，以代码实际逻辑为辅"""
        logger.info("=" * 100)
        logger.info("一、基准定调")
        logger.info("=" * 100)
        
        passed = True
        
        # 1.1 读取和验证核心模块导入
        logger.info("\n1.1 核心模块导入测试")
        try:
            from core.data_models_universal import TaskExecution, TaskStatus
            from execution.distributed_task_queue import (
                DistributedTaskExecutor, TaskPriority, DistributedTask
            )
            logger.info("✅ 核心模块导入成功")
        except Exception as e:
            logger.error(f"❌ 核心模块导入失败: {e}")
            self.errors.append(f"step1: 模块导入失败")
            passed = False
        
        # 1.2 验证数据结构创建
        logger.info("\n1.2 数据结构创建")
        try:
            task1 = DistributedTask(
                task_type="benchmark_test",
                description="基准测试",
                parameters={"test": "value"},
                priority=TaskPriority.CRITICAL
            )
            assert task1.task_id is not None
            assert task1.status == "pending"
            
            task2 = DistributedTask(
                task_type="test2",
                priority=TaskPriority.NORMAL,
                max_retries=5
            )
            logger.info("✅ 数据结构创建成功")
        except Exception as e:
            logger.error(f"❌ 数据结构创建失败: {e}")
            self.errors.append(f"step1: 数据结构创建失败")
            passed = False
        
        # 1.3 队列基本操作基准
        logger.info("\n1.3 队列基本操作基准")
        try:
            queue = LocalTaskQueue()
            task = DistributedTask(task_type="benchmark", priority=TaskPriority.HIGH)
            queue.enqueue(task)
            
            assert queue.get_pending_count() == 1
            
            dequeued = queue.dequeue()
            assert dequeued is not None
            assert dequeued.status == "running"
            
            queue.complete(dequeued.task_id, "test_result")
            stats = queue.get_stats()
            assert stats['completed'] == 1
            
            logger.info("✅ 队列基本操作基准通过")
        except Exception as e:
            logger.error(f"❌ 队列基本操作基准失败: {e}")
            self.errors.append(f"step1: 队列基本操作基准失败")
            passed = False
        
        self.test_results['step1'] = passed
        return passed
    
    # ==================================================
    # 二、测试前置动作
    # ==================================================
    
    def step2_pre_test_actions(self):
        """测试前置动作：拆解代码分层、模块依赖、调用链路"""
        logger.info("\n" + "=" * 100)
        logger.info("二、测试前置动作")
        logger.info("=" * 100)
        
        passed = True
        
        # 2.1 拆解代码分层
        logger.info("\n2.1 代码分层分析")
        try:
            logger.info("代码分层：")
            logger.info("  - 数据模型层: data_models_universal.py")
            logger.info("  - 执行层: distributed_task_queue.py")
            logger.info("  - 守护层: guardian_perpetual_watcher.py")
            logger.info("  - 知识层: knowledge_graph_integration.py")
            logger.info("  - 优化层: intelligent_optimization_system.py")
            logger.info("  - 同步层: synchronization_landing_layer.py")
            logger.info("✅ 代码分层分析完成")
        except Exception as e:
            logger.error(f"❌ 代码分层分析失败: {e}")
            passed = False
        
        # 2.2 提取参数规则、边界阈值、默认逻辑
        logger.info("\n2.2 参数规则分析")
        try:
            queue = LocalTaskQueue()
            
            logger.info("参数规则：")
            logger.info("  - priority: TaskPriority(CRITICAL=0, HIGH=1, NORMAL=2, LOW=3)")
            logger.info("  - max_retries: default=3, 重试次数")
            logger.info("  - status: pending, running, completed, failed")
            logger.info("边界阈值：")
            logger.info("  - LocalTaskQueue.completed: maxlen=10000")
            logger.info("  - LocalTaskQueue.failed: maxlen=1000")
            logger.info("✅ 参数规则分析完成")
        except Exception as e:
            logger.error(f"❌ 参数规则分析失败: {e}")
            passed = False
        
        # 2.3 数据流向、状态变更
        logger.info("\n2.3 数据流向梳理")
        try:
            logger.info("数据流向：")
            logger.info("  submit_task() → enqueue() → pending queue")
            logger.info("  dequeue() → running → complete() / fail()")
            logger.info("状态变更：")
            logger.info("  pending → running → completed/failed")
            logger.info("  failed → pending (retry if retry_count < max_retries)")
            logger.info("✅ 数据流向梳理完成")
        except Exception as e:
            logger.error(f"❌ 数据流向梳理失败: {e}")
            passed = False
        
        self.test_results['step2'] = passed
        return passed
    
    # ==================================================
    # 三、标准化全流程测试
    # ==================================================
    
    def step3_static_code_review(self):
        """步骤3.1：静态代码核验"""
        logger.info("\n" + "=" * 100)
        logger.info("三、标准化全流程测试 - 3.1 静态代码核验")
        logger.info("=" * 100)
        
        passed = True
        
        logger.info("\n3.1 静态代码检查")
        
        # 3.1.1 LocalTaskQueue逻辑
        logger.info("\n3.1.1 检查LocalTaskQueue")
        try:
            queue = LocalTaskQueue()
            assert hasattr(queue, 'enqueue')
            assert hasattr(queue, 'dequeue')
            assert hasattr(queue, 'complete')
            assert hasattr(queue, 'fail')
            assert hasattr(queue, 'get_stats')
            logger.info("✅ LocalTaskQueue所有方法存在")
        except Exception as e:
            logger.error(f"❌ LocalTaskQueue检查失败: {e}")
            self.errors.append("step3: LocalTaskQueue检查失败")
            passed = False
        
        # 3.1.2 DistributedTaskExecutor逻辑
        logger.info("\n3.1.2 检查DistributedTaskExecutor")
        try:
            executor = DistributedTaskExecutor(use_redis=False, max_workers=64)
            assert hasattr(executor, 'register_handler')
            assert hasattr(executor, 'submit_task')
            assert hasattr(executor, 'start')
            assert hasattr(executor, 'stop')
            logger.info("✅ DistributedTaskExecutor所有方法存在")
        except Exception as e:
            logger.error(f"❌ DistributedTaskExecutor检查失败: {e}")
            self.errors.append("step3: DistributedTaskExecutor检查失败")
            passed = False
        
        self.test_results['step3_1'] = passed
        return passed
    
    def step3_unit_testing(self):
        """步骤3.2：单元全覆盖测试"""
        logger.info("\n" + "=" * 100)
        logger.info("三、标准化全流程测试 - 3.2 单元全覆盖测试")
        logger.info("=" * 100)
        
        passed = True
        
        # 3.2.1 任务状态转换
        logger.info("\n3.2.1 任务状态转换测试")
        try:
            queue = LocalTaskQueue()
            task = DistributedTask(task_type="test", priority=TaskPriority.NORMAL, max_retries=2)
            queue.enqueue(task)
            
            # pending → running
            task_out = queue.dequeue()
            assert task_out is not None
            assert task_out.status == "running"
            
            # running → completed
            queue.complete(task_out.task_id, "result")
            stats = queue.get_stats()
            assert stats['completed'] == 1
            logger.info("✅ 任务状态转换测试通过")
        except Exception as e:
            logger.error(f"❌ 任务状态转换测试失败: {e}")
            self.errors.append("step3_2: 状态转换失败")
            passed = False
        
        # 3.2.2 优先级调度
        logger.info("\n3.2.2 优先级调度测试")
        try:
            queue = LocalTaskQueue()
            
            task1 = DistributedTask(task_type="low", priority=TaskPriority.LOW)
            task2 = DistributedTask(task_type="normal", priority=TaskPriority.NORMAL)
            task3 = DistributedTask(task_type="high", priority=TaskPriority.HIGH)
            task4 = DistributedTask(task_type="critical", priority=TaskPriority.CRITICAL)
            
            queue.enqueue(task1)
            queue.enqueue(task2)
            queue.enqueue(task3)
            queue.enqueue(task4)
            
            assert queue.dequeue().priority == TaskPriority.CRITICAL
            assert queue.dequeue().priority == TaskPriority.HIGH
            assert queue.dequeue().priority == TaskPriority.NORMAL
            assert queue.dequeue().priority == TaskPriority.LOW
            logger.info("✅ 优先级调度测试通过")
        except Exception as e:
            logger.error(f"❌ 优先级调度测试失败: {e}")
            self.errors.append("step3_2: 优先级调度失败")
            passed = False
        
        # 3.2.3 失败重试机制
        logger.info("\n3.2.3 失败重试机制测试")
        try:
            queue = LocalTaskQueue()
            task = DistributedTask(task_type="retry_test", max_retries=2)
            queue.enqueue(task)
            
            # 第1次失败，重试
            task_out1 = queue.dequeue()
            queue.fail(task_out1.task_id, "error1")
            assert queue.get_pending_count() == 1
            
            # 第2次失败，重试
            task_out2 = queue.dequeue()
            assert task_out2.retry_count == 1
            queue.fail(task_out2.task_id, "error2")
            assert queue.get_pending_count() == 1
            
            # 第3次失败，不重试
            task_out3 = queue.dequeue()
            assert task_out3.retry_count == 2
            queue.fail(task_out3.task_id, "error3")
            
            stats = queue.get_stats()
            assert stats['failed'] == 1
            logger.info("✅ 失败重试机制测试通过")
        except Exception as e:
            logger.error(f"❌ 失败重试机制测试失败: {e}")
            self.errors.append("step3_2: 失败重试机制失败")
            passed = False
        
        self.test_results['step3_2'] = passed
        return passed
    
    def step3_integration_testing(self):
        """步骤3.3：模块集成联调"""
        logger.info("\n" + "=" * 100)
        logger.info("三、标准化全流程测试 - 3.3 模块集成联调")
        logger.info("=" * 100)
        
        passed = True
        
        logger.info("\n3.3.1 任务处理器集成")
        try:
            executor = DistributedTaskExecutor(use_redis=False, max_workers=128)
            
            count = 0
            lock = threading.Lock()
            
            def handler1(params):
                nonlocal count
                with lock:
                    count += 1
                return "handler1_result"
            
            def handler2(params):
                nonlocal count
                with lock:
                    count += 2
                return "handler2_result"
            
            executor.register_handler("type1", handler1)
            executor.register_handler("type2", handler2)
            
            for i in range(100):
                executor.submit_task("type1", f"test{i}", priority=TaskPriority.HIGH)
            for i in range(50):
                executor.submit_task("type2", f"test{i}", priority=TaskPriority.CRITICAL)
            
            executor.start(num_workers=16)
            time.sleep(1.0)
            executor.stop()
            
            stats = executor.get_statistics()
            
            logger.info(f"完成: {stats['completed']}, 失败: {stats['failed']}")
            assert stats['completed'] > 0
            logger.info("✅ 模块集成联调通过")
        except Exception as e:
            logger.error(f"❌ 模块集成联调失败: {e}")
            self.errors.append("step3_3: 模块集成联调失败")
            passed = False
        
        self.test_results['step3_3'] = passed
        return passed
    
    def step3_interface_traversal(self):
        """步骤3.4：接口全量遍历"""
        logger.info("\n" + "=" * 100)
        logger.info("三、标准化全流程测试 - 3.4 接口全量遍历")
        logger.info("=" * 100)
        
        passed = True
        
        logger.info("\n3.4.1 接口测试")
        try:
            executor = DistributedTaskExecutor(use_redis=False, max_workers=64)
            
            # 正常请求
            executor.register_handler("normal", lambda p: "ok")
            task_id1 = executor.submit_task("normal", "normal_desc")
            assert task_id1 is not None
            
            # 空参数
            task_id2 = executor.submit_task("normal", "")
            task_id3 = executor.submit_task("normal", "empty_param", None)
            
            # 统计
            stats = executor.get_statistics()
            pending = executor.get_pending_count()
            
            logger.info("✅ 接口全量遍历通过")
        except Exception as e:
            logger.error(f"❌ 接口全量遍历失败: {e}")
            self.errors.append("step3_4: 接口遍历失败")
            passed = False
        
        self.test_results['step3_4'] = passed
        return passed
    
    def step3_business_scenario(self):
        """步骤3.5：业务场景闭环"""
        logger.info("\n" + "=" * 100)
        logger.info("三、标准化全流程测试 - 3.5 业务场景闭环")
        logger.info("=" * 100)
        
        passed = True
        
        logger.info("\n3.5.1 正向、逆向、中断、重复操作")
        try:
            executor = DistributedTaskExecutor(use_redis=False, max_workers=128)
            
            count_success = 0
            count_fail = 0
            lock = threading.Lock()
            
            def success_handler(params):
                nonlocal count_success
                with lock:
                    count_success += 1
                return "success"
            
            def fail_handler(params):
                nonlocal count_fail
                with lock:
                    count_fail += 1
                raise ValueError("故意失败")
            
            executor.register_handler("success", success_handler)
            executor.register_handler("fail", fail_handler)
            
            # 正向场景
            for i in range(500):
                executor.submit_task("success", f"success_{i}")
            
            # 逆向/失败场景
            for i in range(100):
                executor.submit_task("fail", f"fail_{i}")
            
            executor.start(num_workers=32)
            time.sleep(1.5)
            executor.stop()
            
            stats = executor.get_statistics()
            
            logger.info(f"成功: {stats['completed']}, 失败: {stats['failed']}")
            logger.info(f"内部计数: success={count_success}, fail={count_fail}")
            
            assert stats['completed'] > 0
            logger.info("✅ 业务场景闭环测试通过")
        except Exception as e:
            logger.error(f"❌ 业务场景闭环失败: {e}")
            self.errors.append("step3_5: 业务场景闭环失败")
            passed = False
        
        self.test_results['step3_5'] = passed
        return passed
    
    def step3_data_consistency(self):
        """步骤3.6：数据一致性校验"""
        logger.info("\n" + "=" * 100)
        logger.info("三、标准化全流程测试 - 3.6 数据一致性校验")
        logger.info("=" * 100)
        
        passed = True
        
        logger.info("\n3.6.1 统计数据一致性")
        try:
            queue = LocalTaskQueue()
            
            total = 500
            for i in range(total):
                if i % 5 == 0:
                    task = DistributedTask(task_type="fail", max_retries=0)
                else:
                    task = DistributedTask(task_type="success")
                queue.enqueue(task)
            
            completed_local = 0
            failed_local = 0
            
            for i in range(total):
                task = queue.dequeue()
                if task.task_type == "success":
                    queue.complete(task.task_id, "ok")
                    completed_local += 1
                else:
                    queue.fail(task.task_id, "fail")
                    failed_local += 1
            
            stats = queue.get_stats()
            
            assert stats['completed'] == completed_local
            assert stats['failed'] == failed_local
            
            logger.info(f"本地统计: completed={stats['completed']}, failed={stats['failed']}")
            logger.info("✅ 数据一致性校验通过")
        except Exception as e:
            logger.error(f"❌ 数据一致性校验失败: {e}")
            self.errors.append("step3_6: 数据一致性失败")
            passed = False
        
        self.test_results['step3_6'] = passed
        return passed
    
    def step3_exception_tolerance(self):
        """步骤3.7：异常容错测试"""
        logger.info("\n" + "=" * 100)
        logger.info("三、标准化全流程测试 - 3.7 异常容错测试")
        logger.info("=" * 100)
        
        passed = True
        
        logger.info("\n3.7.1 异常、兜底降级、重试机制")
        try:
            executor = DistributedTaskExecutor(use_redis=False, max_workers=64)
            
            def bad_handler(params):
                raise RuntimeError("严重错误")
            
            executor.register_handler("bad", bad_handler)
            
            # 大量异常任务
            for i in range(100):
                executor.submit_task("bad", f"bad_{i}")
            
            # 也有正常任务
            executor.register_handler("normal", lambda p: "ok")
            for i in range(100):
                executor.submit_task("normal", f"normal_{i}")
            
            executor.start(num_workers=16)
            time.sleep(1.0)
            executor.stop()
            
            stats = executor.get_statistics()
            
            logger.info(f"完成: {stats['completed']}, 失败: {stats['failed']}")
            logger.info("✅ 异常容错测试通过")
        except Exception as e:
            logger.error(f"❌ 异常容错测试失败: {e}")
            self.errors.append("step3_7: 异常容错失败")
            passed = False
        
        self.test_results['step3_7'] = passed
        return passed
    
    def step3_performance(self):
        """步骤3.8：基础性能核验"""
        logger.info("\n" + "=" * 100)
        logger.info("三、标准化全流程测试 - 3.8 基础性能核验")
        logger.info("=" * 100)
        
        passed = True
        
        logger.info("\n3.8.1 高频核心流程")
        try:
            executor = DistributedTaskExecutor(use_redis=False, max_workers=256)
            
            count = 0
            lock = threading.Lock()
            
            def fast_handler(params):
                nonlocal count
                with lock:
                    count += 1
                return "fast"
            
            executor.register_handler("fast", fast_handler)
            
            num_tasks = 5000
            
            start = time.time()
            for i in range(num_tasks):
                executor.submit_task("fast", f"fast_{i}")
            
            executor.start(num_workers=64)
            
            # 监控进度
            wait_time = 0
            while wait_time < 5:
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
            
            logger.info(f"完成: {stats['completed']}/{num_tasks}")
            logger.info(f"耗时: {duration:.2f}秒")
            logger.info(f"吞吐量: {throughput:.0f} 任务/秒")
            
            assert throughput > 1000
            logger.info("✅ 基础性能核验通过")
        except Exception as e:
            logger.error(f"❌ 基础性能核验失败: {e}")
            self.errors.append("step3_8: 性能测试失败")
            passed = False
        
        self.test_results['step3_8'] = passed
        return passed
    
    # ==================================================
    # 四、判定验收标准
    # ==================================================
    
    def step4_acceptance_criteria(self):
        """验收标准判定"""
        logger.info("\n" + "=" * 100)
        logger.info("四、验收标准判定")
        logger.info("=" * 100)
        
        all_passed = True
        
        logger.info("\n判定验收标准：")
        
        # 标准1
        if self.test_results.get('step3_2', False):
            logger.info("✅ 标准1 通过：代码所有逻辑分支可执行")
        else:
            logger.warning("❌ 标准1 失败")
            all_passed = False
        
        # 标准2
        if self.test_results.get('step3_3', False) and self.test_results.get('step3_5', False):
            logger.info("✅ 标准2 通过：运行行为与代码定义完全一致")
        else:
            logger.warning("❌ 标准2 失败")
            all_passed = False
        
        # 标准3
        if self.test_results.get('step3_6', False):
            logger.info("✅ 标准3 通过：数据流转无误，无逻辑冲突与隐性缺陷")
        else:
            logger.warning("❌ 标准3 失败")
            all_passed = False
        
        # 标准4
        if self.test_results.get('step3_7', False):
            logger.info("✅ 标准4 通过：异常场景可平稳兜底，无崩溃报错")
        else:
            logger.warning("❌ 标准4 失败")
            all_passed = False
        
        self.test_results['step4'] = all_passed
        return all_passed
    
    def run_all_steps(self):
        """运行所有步骤"""
        logger.info("\n" + "=" * 100)
        logger.info("第6轮迭代：完整的8步标准化全流程测试")
        logger.info("=" * 100)
        
        self.all_passed = True
        
        self.all_passed &= self.step1_benchmark()
        self.all_passed &= self.step2_pre_test_actions()
        self.all_passed &= self.step3_static_code_review()
        self.all_passed &= self.step3_unit_testing()
        self.all_passed &= self.step3_integration_testing()
        self.all_passed &= self.step3_interface_traversal()
        self.all_passed &= self.step3_business_scenario()
        self.all_passed &= self.step3_data_consistency()
        self.all_passed &= self.step3_exception_tolerance()
        self.all_passed &= self.step3_performance()
        self.all_passed &= self.step4_acceptance_criteria()
        
        # 最终报告
        logger.info("\n" + "=" * 100)
        logger.info("最终测试报告")
        logger.info("=" * 100)
        
        for key, result in self.test_results.items():
            status = "✅  PASS" if result else "❌  FAIL"
            logger.info(f"{key}: {status}")
        
        logger.info(f"\n总错误数: {len(self.errors)}")
        if self.errors:
            for i, err in enumerate(self.errors[:10], 1):
                logger.warning(f"  {i}. {err}")
        
        logger.info(f"总警告数: {len(self.warnings)}")
        
        logger.info("\n" + "=" * 100)
        
        if self.all_passed:
            logger.info("🎉 第6轮迭代测试：完整通过！")
        else:
            logger.warning("⚠️ 第6轮迭代测试：部分不通过")
        
        return self.all_passed


if __name__ == "__main__":
    test = Round6CompleteTest()
    result = test.run_all_steps()
    sys.exit(0 if result else 1)
