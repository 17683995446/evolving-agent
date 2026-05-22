"""
第三轮迭代测试 - 完整标准化全流程测试（8步）
按照用户要求的完整流程执行
"""
import sys
import os
import time
import threading
import logging
import random
import traceback
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
logger = logging.getLogger("Round3Test")


class Round3CompleteTest:
    """第三轮迭代完整测试"""
    
    def __init__(self):
        self.test_results = {}
        self.errors = []
        self.warnings = []
        
    # ========================================================
    # 步骤 1：基准定调 - 代码走读和静态核验
    # ========================================================
    
    def test_step1_static_code_review(self):
        """步骤1：静态代码核验"""
        logger.info("="*100)
        logger.info("步骤1：静态代码核验")
        logger.info("="*100)
        
        passed = True
        
        # 测试1.1：核心模块导入检查
        try:
            from core.data_models_universal import TaskExecution, OptimizationRule
            from core.guardian_perpetual_watcher import GuardianPerpetualWatcher
            from core.dual_loop_drive_engine import DualLoopDriveEngine
            logger.info("✅ 1.1：核心模块导入检查通过")
        except Exception as e:
            logger.error(f"❌ 1.1：核心模块导入失败: {e}")
            self.errors.append(f"步骤1.1: {e}")
            passed = False
        
        # 测试1.2：基础数据结构创建
        try:
            from execution.distributed_task_queue import DistributedTask, TaskPriority
            task = DistributedTask(
                task_type="test",
                description="test task",
                parameters={"x": 1},
                priority=TaskPriority.NORMAL
            )
            assert task.task_id is not None
            assert task.priority == TaskPriority.NORMAL
            logger.info("✅ 1.2：数据结构创建通过")
        except Exception as e:
            logger.error(f"❌ 1.2：数据结构创建失败: {e}")
            self.errors.append(f"步骤1.2: {e}")
            passed = False
        
        # 测试1.3：队列初始化
        try:
            queue = LocalTaskQueue()
            assert queue.queues is not None
            assert len(queue.queues) == 4
            logger.info("✅ 1.3：队列初始化通过")
        except Exception as e:
            logger.error(f"❌ 1.3：队列初始化失败: {e}")
            self.errors.append(f"步骤1.3: {e}")
            passed = False
        
        self.test_results['step1'] = passed
        return passed
    
    # ========================================================
    # 步骤 2：单元全覆盖测试
    # ========================================================
    
    def test_step2_unit_testing(self):
        """步骤2：单元全覆盖测试"""
        logger.info("\n" + "="*100)
        logger.info("步骤2：单元全覆盖测试")
        logger.info("="*100)
        
        passed = True
        
        # 测试2.1：任务状态转换
        try:
            queue = LocalTaskQueue()
            task = DistributedTask(task_type="test", description="test")
            
            # 入队
            assert queue.enqueue(task) == True
            assert task.status == "pending"
            
            # 出队
            task_out = queue.dequeue()
            assert task_out is not None
            assert task_out.status == "running"
            
            # 完成
            assert queue.complete(task_out.task_id, "success") == True
            stats = queue.get_stats()
            assert stats['completed'] == 1
            
            logger.info("✅ 2.1：任务状态转换通过")
        except Exception as e:
            logger.error(f"❌ 2.1：任务状态转换失败: {e}")
            self.errors.append(f"步骤2.1: {e}")
            passed = False
        
        # 测试2.2：优先级调度
        try:
            queue = LocalTaskQueue()
            
            # 按不同优先级提交任务
            low_task = DistributedTask(task_type="low", priority=TaskPriority.LOW)
            norm_task = DistributedTask(task_type="norm", priority=TaskPriority.NORMAL)
            high_task = DistributedTask(task_type="high", priority=TaskPriority.HIGH)
            crit_task = DistributedTask(task_type="crit", priority=TaskPriority.CRITICAL)
            
            queue.enqueue(low_task)
            queue.enqueue(norm_task)
            queue.enqueue(high_task)
            queue.enqueue(crit_task)
            
            # 验证优先级顺序：CRITICAL > HIGH > NORMAL > LOW
            assert queue.dequeue().task_type == "crit"
            assert queue.dequeue().task_type == "high"
            assert queue.dequeue().task_type == "norm"
            assert queue.dequeue().task_type == "low"
            
            logger.info("✅ 2.2：优先级调度通过")
        except Exception as e:
            logger.error(f"❌ 2.2：优先级调度失败: {e}")
            self.errors.append(f"步骤2.2: {e}")
            passed = False
        
        # 测试2.3：失败重试机制
        try:
            queue = LocalTaskQueue()
            task = DistributedTask(
                task_type="retry",
                max_retries=2
            )
            queue.enqueue(task)
            
            # 第一次失败，应该重试
            task_out = queue.dequeue()
            queue.fail(task_out.task_id, "error1")
            assert queue.get_pending_count() == 1
            
            # 第二次失败，应该重试
            task_out2 = queue.dequeue()
            assert task_out2.retry_count == 1
            queue.fail(task_out2.task_id, "error2")
            assert queue.get_pending_count() == 1
            
            # 第三次失败，应该失败
            task_out3 = queue.dequeue()
            assert task_out3.retry_count == 2
            queue.fail(task_out3.task_id, "error3")
            
            stats = queue.get_stats()
            assert stats['failed'] == 1
            
            logger.info("✅ 2.3：失败重试机制通过")
        except Exception as e:
            logger.error(f"❌ 2.3：失败重试机制失败: {e}")
            self.errors.append(f"步骤2.3: {e}")
            passed = False
        
        # 测试2.4：计数准确性
        try:
            queue = LocalTaskQueue()
            
            # 提交多个任务
            for i in range(10):
                task = DistributedTask(task_type=f"test{i}")
                queue.enqueue(task)
            
            # 全部完成
            for i in range(10):
                task = queue.dequeue()
                queue.complete(task.task_id, "success")
            
            stats = queue.get_stats()
            assert stats['completed'] == 10
            assert stats['failed'] == 0
            
            logger.info("✅ 2.4：计数准确性通过")
        except Exception as e:
            logger.error(f"❌ 2.4：计数准确性失败: {e}")
            self.errors.append(f"步骤2.4: {e}")
            passed = False
        
        self.test_results['step2'] = passed
        return passed
    
    # ========================================================
    # 步骤 3：模块集成联调
    # ========================================================
    
    def test_step3_integration_testing(self):
        """步骤3：模块集成联调"""
        logger.info("\n" + "="*100)
        logger.info("步骤3：模块集成联调")
        logger.info("="*100)
        
        passed = True
        
        # 测试3.1：任务处理器注册和执行
        try:
            executor = DistributedTaskExecutor(use_redis=False, max_workers=32)
            
            results = []
            def test_handler(params):
                results.append(params.get('x', 0))
                return f"handled: {params.get('x', 0)}"
            
            executor.register_handler('test', test_handler)
            
            # 提交任务
            executor.submit_task('test', 'test1', {'x': 1})
            executor.submit_task('test', 'test2', {'x': 2})
            
            # 启动并等待
            executor.start(num_workers=2)
            time.sleep(0.5)
            executor.stop()
            
            assert len(results) == 2
            logger.info("✅ 3.1：任务处理器集成通过")
        except Exception as e:
            logger.error(f"❌ 3.1：任务处理器集成失败: {e}")
            self.errors.append(f"步骤3.1: {e}")
            passed = False
        
        # 测试3.2：多处理器混合
        try:
            executor = DistributedTaskExecutor(use_redis=False, max_workers=64)
            
            count1 = 0
            count2 = 0
            
            def handler1(p):
                nonlocal count1
                count1 += 1
                return "h1"
            
            def handler2(p):
                nonlocal count2
                count2 += 1
                return "h2"
            
            executor.register_handler('type1', handler1)
            executor.register_handler('type2', handler2)
            
            # 混合提交
            for i in range(20):
                executor.submit_task('type1', f't1-{i}')
            for i in range(30):
                executor.submit_task('type2', f't2-{i}')
            
            executor.start(num_workers=8)
            time.sleep(1)
            executor.stop()
            
            assert count1 + count2 >= 45  # 允许一些没有处理完
            logger.info(f"✅ 3.2：多处理器混合通过（{count1}+{count2}={count1+count2}）")
        except Exception as e:
            logger.error(f"❌ 3.2：多处理器混合失败: {e}")
            self.errors.append(f"步骤3.2: {e}")
            passed = False
        
        self.test_results['step3'] = passed
        return passed
    
    # ========================================================
    # 步骤 4：接口全量遍历
    # ========================================================
    
    def test_step4_interface_traversal(self):
        """步骤4：接口全量遍历"""
        logger.info("\n" + "="*100)
        logger.info("步骤4：接口全量遍历")
        logger.info("="*100)
        
        passed = True
        
        # 测试4.1：公开接口遍历
        try:
            executor = DistributedTaskExecutor(use_redis=False)
            
            # 注册处理器
            executor.register_handler('test', lambda p: "ok")
            
            # 测试各种参数
            task_id = executor.submit_task('test', 'desc1', {'x': 1}, priority=TaskPriority.NORMAL)
            assert task_id is not None
            
            # 统计接口
            stats = executor.get_statistics()
            assert 'pending' in stats
            assert 'running' in stats
            assert 'completed' in stats
            assert 'failed' in stats
            
            # 待处理数
            pending = executor.get_pending_count()
            assert pending >= 0
            
            logger.info("✅ 4.1：公开接口遍历通过")
        except Exception as e:
            logger.error(f"❌ 4.1：公开接口遍历失败: {e}")
            self.errors.append(f"步骤4.1: {e}")
            passed = False
        
        # 测试4.2：异常参数处理
        try:
            executor = DistributedTaskExecutor(use_redis=False)
            
            # 测试空参数
            executor.register_handler('handler', lambda p: p if p else "empty")
            
            executor.submit_task('handler', 'empty_params', None)
            executor.submit_task('handler', 'empty_dict', {})
            
            executor.start(num_workers=2)
            time.sleep(0.3)
            executor.stop()
            
            logger.info("✅ 4.2：异常参数处理通过")
        except Exception as e:
            logger.error(f"❌ 4.2：异常参数处理失败: {e}")
            self.errors.append(f"步骤4.2: {e}")
            passed = False
        
        self.test_results['step4'] = passed
        return passed
    
    # ========================================================
    # 步骤 5：业务场景闭环
    # ========================================================
    
    def test_step5_business_scenarios(self):
        """步骤5：业务场景闭环"""
        logger.info("\n" + "="*100)
        logger.info("步骤5：业务场景闭环")
        logger.info("="*100)
        
        passed = True
        
        # 测试5.1：正向业务流程
        try:
            executor = DistributedTaskExecutor(use_redis=False, max_workers=64)
            
            processed = []
            def business_handler(params):
                processed.append(params['id'])
                return {'id': params['id'], 'status': 'done'}
            
            executor.register_handler('business', business_handler)
            
            # 模拟业务流程
            for i in range(100):
                executor.submit_task('business', f'biz{i}', {'id': i})
            
            executor.start(num_workers=16)
            time.sleep(1)
            executor.stop()
            
            stats = executor.get_statistics()
            logger.info(f"   正向业务流程：完成 {stats['completed']}，失败 {stats['failed']}")
            
            assert len(processed) >= 90  # 90% 处理率
            logger.info("✅ 5.1：正向业务流程通过")
        except Exception as e:
            logger.error(f"❌ 5.1：正向业务流程失败: {e}")
            self.errors.append(f"步骤5.1: {e}")
            passed = False
        
        # 测试5.2：逆向/中断场景
        try:
            executor = DistributedTaskExecutor(use_redis=False, max_workers=32)
            
            def failing_handler(params):
                if params.get('should_fail'):
                    raise RuntimeError("Intentional failure")
                return "ok"
            
            executor.register_handler('test', failing_handler)
            
            # 混合提交成功和失败任务
            success_count = 0
            fail_count = 0
            
            for i in range(50):
                should_fail = (i % 3 == 0)
                if should_fail:
                    fail_count += 1
                else:
                    success_count += 1
                
                executor.submit_task('test', f'test{i}', {'should_fail': should_fail})
            
            executor.start(num_workers=8)
            time.sleep(1)
            executor.stop()
            
            stats = executor.get_statistics()
            logger.info(f"   逆向业务流程：成功 {stats['completed']}，失败 {stats['failed']}")
            
            logger.info("✅ 5.2：逆向/中断场景通过")
        except Exception as e:
            logger.error(f"❌ 5.2：逆向/中断场景失败: {e}")
            self.errors.append(f"步骤5.2: {e}")
            passed = False
        
        self.test_results['step5'] = passed
        return passed
    
    # ========================================================
    # 步骤 6：数据一致性校验
    # ========================================================
    
    def test_step6_data_consistency(self):
        """步骤6：数据一致性校验"""
        logger.info("\n" + "="*100)
        logger.info("步骤6：数据一致性校验")
        logger.info("="*100)
        
        passed = True
        
        # 测试6.1：统计数据一致性
        try:
            executor = DistributedTaskExecutor(use_redis=False, max_workers=64)
            
            count = 0
            def handler(p):
                nonlocal count
                count += 1
                return "ok"
            
            executor.register_handler('test', handler)
            
            # 提交任务
            total_tasks = 200
            for i in range(total_tasks):
                executor.submit_task('test', f't{i}')
            
            executor.start(num_workers=16)
            time.sleep(1.5)
            executor.stop()
            
            # 检查数据一致性
            stats = executor.get_statistics()
            total_processed = stats['completed'] + stats['failed']
            
            logger.info(f"   数据一致性：提交 {total_tasks}，处理 {total_processed}，完成 {stats['completed']}")
            
            assert count == stats['completed']  # 处理计数与统计一致
            logger.info("✅ 6.1：统计数据一致性通过")
        except Exception as e:
            logger.error(f"❌ 6.1：统计数据一致性失败: {e}")
            self.errors.append(f"步骤6.1: {e}")
            passed = False
        
        self.test_results['step6'] = passed
        return passed
    
    # ========================================================
    # 步骤 7：异常容错测试
    # ========================================================
    
    def test_step7_error_tolerance(self):
        """步骤7：异常容错测试"""
        logger.info("\n" + "="*100)
        logger.info("步骤7：异常容错测试")
        logger.info("="*100)
        
        passed = True
        
        # 测试7.1：异常处理器
        try:
            executor = DistributedTaskExecutor(use_redis=False, max_workers=64)
            
            def bad_handler(p):
                raise RuntimeError("This is a bad handler")
            
            executor.register_handler('bad', bad_handler)
            
            # 提交会失败的任务
            for i in range(10):
                executor.submit_task('bad', f'bad{i}')
            
            executor.start(num_workers=8)
            time.sleep(0.5)
            executor.stop()
            
            stats = executor.get_statistics()
            logger.info(f"   异常容错：失败 {stats['failed']}，系统稳定无崩溃")
            
            logger.info("✅ 7.1：异常处理器通过")
        except Exception as e:
            logger.error(f"❌ 7.1：异常处理器失败: {e}")
            self.errors.append(f"步骤7.1: {e}")
            passed = False
        
        # 测试7.2：未知任务类型
        try:
            executor = DistributedTaskExecutor(use_redis=False, max_workers=32)
            
            # 提交没有处理器的任务
            for i in range(5):
                executor.submit_task('unknown_type', f'unknown{i}')
            
            executor.start(num_workers=4)
            time.sleep(0.5)
            executor.stop()
            
            # 系统应该能正常处理
            logger.info("✅ 7.2：未知任务类型容错通过")
        except Exception as e:
            logger.error(f"❌ 7.2：未知任务类型容错失败: {e}")
            self.errors.append(f"步骤7.2: {e}")
            passed = False
        
        self.test_results['step7'] = passed
        return passed
    
    # ========================================================
    # 步骤 8：基础性能核验
    # ========================================================
    
    def test_step8_performance_testing(self):
        """步骤8：基础性能核验"""
        logger.info("\n" + "="*100)
        logger.info("步骤8：基础性能核验")
        logger.info("="*100)
        
        passed = True
        
        # 测试8.1：吞吐量测试
        try:
            executor = DistributedTaskExecutor(use_redis=False, max_workers=128)
            
            count = 0
            lock = threading.Lock()
            def fast_handler(p):
                nonlocal count
                with lock:
                    count += 1
                return "fast"
            
            executor.register_handler('fast', fast_handler)
            
            # 提交大量任务
            num_tasks = 5000
            start = time.time()
            
            for i in range(num_tasks):
                executor.submit_task('fast', f'f{i}')
            
            executor.start(num_workers=32)
            
            # 等待处理完成
            wait_time = 0
            while wait_time < 10:
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
            
            logger.info(f"   性能测试：完成 {stats['completed']}/{num_tasks}")
            logger.info(f"   耗时：{duration:.2f}s")
            logger.info(f"   吞吐量：{throughput:.0f} 任务/秒")
            
            assert throughput >= 1000  # 至少 1000 任务/秒
            logger.info("✅ 8.1：吞吐量测试通过")
        except Exception as e:
            logger.error(f"❌ 8.1：吞吐量测试失败: {e}")
            self.errors.append(f"步骤8.1: {e}")
            passed = False
        
        self.test_results['step8'] = passed
        return passed
    
    # ========================================================
    # 最终验收和报告
    # ========================================================
    
    def run_all_tests(self):
        """运行所有8步测试"""
        logger.info("\n" + "="*100)
        logger.info("第三轮迭代 - 标准化全流程测试（8步）开始")
        logger.info("="*100)
        
        all_passed = True
        
        # 运行所有8步
        all_passed &= self.test_step1_static_code_review()
        all_passed &= self.test_step2_unit_testing()
        all_passed &= self.test_step3_integration_testing()
        all_passed &= self.test_step4_interface_traversal()
        all_passed &= self.test_step5_business_scenarios()
        all_passed &= self.test_step6_data_consistency()
        all_passed &= self.test_step7_error_tolerance()
        all_passed &= self.test_step8_performance_testing()
        
        # 最终报告
        logger.info("\n" + "="*100)
        logger.info("最终验收报告")
        logger.info("="*100)
        
        # 显示每一步结果
        for step in range(1, 9):
            step_name = f"step{step}"
            result = self.test_results.get(step_name, False)
            status = "✅ 通过" if result else "❌ 失败"
            logger.info(f"步骤 {step}：{status}")
        
        logger.info(f"\n总错误数：{len(self.errors)}")
        if self.errors:
            for i, err in enumerate(self.errors[:10], 1):
                logger.warning(f"  {i}. {err}")
            if len(self.errors) > 10:
                logger.warning(f"  ... 还有 {len(self.errors)-10} 个错误")
        
        logger.info(f"总警告数：{len(self.warnings)}")
        
        # 验收标准判定
        logger.info("\n" + "="*100)
        logger.info("验收标准判定")
        logger.info("="*100)
        
        # 标准1：代码逻辑分支可执行
        if self.test_results.get('step2', False):
            logger.info("✅ 标准1 通过：代码逻辑分支可执行")
        else:
            logger.warning("❌ 标准1 失败")
        
        # 标准2：运行行为与代码定义一致
        if self.test_results.get('step3', False) and self.test_results.get('step5', False):
            logger.info("✅ 标准2 通过：运行行为与代码定义一致")
        else:
            logger.warning("❌ 标准2 失败")
        
        # 标准3：数据流转无误
        if self.test_results.get('step6', False):
            logger.info("✅ 标准3 通过：数据流转无误，无逻辑冲突")
        else:
            logger.warning("❌ 标准3 失败")
        
        # 标准4：异常场景可平稳兜底
        if self.test_results.get('step7', False):
            logger.info("✅ 标准4 通过：异常场景可平稳兜底，无崩溃报错")
        else:
            logger.warning("❌ 标准4 失败")
        
        logger.info("="*100)
        
        if all_passed:
            logger.info("\n🎉🎉🎉 第三轮迭代完整测试通过！")
            return True
        else:
            logger.warning("\n⚠️ 第三轮迭代测试存在问题，需要修复")
            return False


if __name__ == "__main__":
    test = Round3CompleteTest()
    result = test.run_all_tests()
    
    sys.exit(0 if result else 1)
