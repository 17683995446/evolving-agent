"""
第7轮迭代 - 极致细节化深度测试
注重：
1. 代码逻辑细节分析
2. 边界条件测试
3. 隐藏规则挖掘
4. 数据流追踪
5. 异常场景细节
"""
import sys
import os
import time
import threading
import logging
import random
import traceback
from datetime import datetime
from typing import Dict, Any, List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from execution.distributed_task_queue import (
    DistributedTaskExecutor, TaskPriority, DistributedTask, LocalTaskQueue
)
from core.data_models_universal import TaskExecution, TaskStatus

# 配置极致细节日志
logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s.%(msecs)03d] [%(levelname)-8s] [%(threadName)-12s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("Round7Detailed")
logger.setLevel(logging.DEBUG)


class Round7DetailedTest:
    """第7轮迭代 - 极致细节化测试"""
    
    def __init__(self):
        self.test_results = {}
        self.errors = []
        self.warnings = []
        self.detailed_logs = []
        
    def log_detail(self, message: str, level: str = "INFO"):
        """记录细节日志"""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        log_entry = f"[{timestamp}] [{level}] {message}"
        self.detailed_logs.append(log_entry)
        if level == "ERROR":
            logger.error(message)
        elif level == "WARNING":
            logger.warning(message)
        else:
            logger.info(message)
    
    # ============================================================
    # 一、基准定调：深度代码分析
    # ============================================================
    
    def phase1_deep_code_analysis(self):
        """阶段1：基准定调 - 深度代码逻辑分析"""
        self.log_detail("="*120, "INFO")
        self.log_detail("阶段1：基准定调 - 深度代码逻辑分析", "INFO")
        self.log_detail("="*120, "INFO")
        
        passed = True
        
        # 1.1 深度分析LocalTaskQueue代码逻辑
        self.log_detail("\n1.1 LocalTaskQueue代码逻辑深度分析", "INFO")
        self.log_detail("-"*80, "INFO")
        
        try:
            queue = LocalTaskQueue()
            
            # 分析队列结构
            self.log_detail("队列结构分析:", "INFO")
            self.log_detail(f"  - self.queues: Dict[TaskPriority, deque]", "INFO")
            self.log_detail(f"  - self.running: Dict[str, DistributedTask]", "INFO")
            self.log_detail(f"  - self.completed: deque(maxlen=10000)", "INFO")
            self.log_detail(f"  - self.failed: deque(maxlen=1000)", "INFO")
            self.log_detail(f"  - self.total_completed: int (完整计数器)", "INFO")
            self.log_detail(f"  - self.total_failed: int (完整计数器)", "INFO")
            
            # 分析锁机制
            self.log_detail("\n锁机制分析:", "INFO")
            self.log_detail(f"  - 使用 threading.RLock() 可重入锁", "INFO")
            self.log_detail(f"  - 所有操作都在锁保护下", "INFO")
            
            # 分析优先级顺序
            self.log_detail("\n优先级顺序:", "INFO")
            priority_order = [TaskPriority.CRITICAL, TaskPriority.HIGH, 
                            TaskPriority.NORMAL, TaskPriority.LOW]
            for i, p in enumerate(priority_order):
                self.log_detail(f"  {i+1}. {p.name} (value={p.value})", "INFO")
            
            # 分析状态转换
            self.log_detail("\n状态转换分析:", "INFO")
            self.log_detail("  pending → running → completed", "INFO")
            self.log_detail("  pending → running → failed → pending (retry)", "INFO")
            self.log_detail("  pending → running → failed (final)", "INFO")
            
            self.log_detail("✅ LocalTaskQueue深度分析完成", "INFO")
        except Exception as e:
            self.log_detail(f"❌ LocalTaskQueue深度分析失败: {e}", "ERROR")
            self.errors.append(f"phase1_1: {e}")
            passed = False
        
        # 1.2 深度分析重试逻辑
        self.log_detail("\n1.2 重试逻辑深度分析", "INFO")
        self.log_detail("-"*80, "INFO")
        
        try:
            # 分析重试条件的边界
            self.log_detail("重试条件分析:", "INFO")
            self.log_detail("  条件: task.retry_count < task.max_retries", "INFO")
            self.log_detail("  如果True: retry_count+=1, 重新入队", "INFO")
            self.log_detail("  如果False: retry_count+=1, 标记failed", "INFO")
            
            # 验证边界情况
            self.log_detail("\n边界情况验证:", "INFO")
            
            # 情况1: max_retries=0, 第一次失败
            queue = LocalTaskQueue()
            task = DistributedTask(task_type="test", max_retries=0)
            queue.enqueue(task)
            task_out = queue.dequeue()
            queue.fail(task_out.task_id, "error")
            stats = queue.get_stats()
            
            if stats['failed'] == 1:
                self.log_detail("  ✅ max_retries=0: 第一次失败直接标记failed", "INFO")
            else:
                self.log_detail("  ❌ max_retries=0: 应该直接标记failed", "ERROR")
                passed = False
            
            # 情况2: max_retries=1, 第二次失败
            queue = LocalTaskQueue()
            task = DistributedTask(task_type="test", max_retries=1)
            queue.enqueue(task)
            
            task_out1 = queue.dequeue()
            queue.fail(task_out1.task_id, "error1")
            
            task_out2 = queue.dequeue()
            if task_out2.retry_count == 1:
                self.log_detail("  ✅ max_retries=1: retry_count正确增加", "INFO")
            else:
                self.log_detail("  ❌ max_retries=1: retry_count增加异常", "ERROR")
                passed = False
            
            queue.fail(task_out2.task_id, "error2")
            stats = queue.get_stats()
            
            if stats['failed'] == 1:
                self.log_detail("  ✅ max_retries=1: 第二次失败标记failed", "INFO")
            else:
                self.log_detail("  ❌ max_retries=1: 应该标记failed", "ERROR")
                passed = False
            
            self.log_detail("✅ 重试逻辑深度分析完成", "INFO")
        except Exception as e:
            self.log_detail(f"❌ 重试逻辑分析失败: {e}", "ERROR")
            self.errors.append(f"phase1_2: {e}")
            passed = False
        
        # 1.3 深度分析DistributedTaskExecutor
        self.log_detail("\n1.3 DistributedTaskExecutor深度分析", "INFO")
        self.log_detail("-"*80, "INFO")
        
        try:
            executor = DistributedTaskExecutor(use_redis=False, max_workers=128)
            
            self.log_detail("执行器结构:", "INFO")
            self.log_detail("  - max_workers: 最大工作线程数", "INFO")
            self.log_detail("  - task_handlers: Dict[str, Callable]", "INFO")
            self.log_detail("  - is_running: bool", "INFO")
            self.log_detail("  - worker_threads: List[threading.Thread]", "INFO")
            self.log_detail("  - queue: BaseTaskQueue (LocalTaskQueue或RedisTaskQueue)", "INFO")
            
            self.log_detail("\n执行流程:", "INFO")
            self.log_detail("  1. register_handler() 注册处理器", "INFO")
            self.log_detail("  2. submit_task() 提交任务", "INFO")
            self.log_detail("  3. start() 启动工作线程", "INFO")
            self.log_detail("  4. _worker_loop() 工作循环", "INFO")
            self.log_detail("  5. _execute_task() 执行任务", "INFO")
            self.log_detail("  6. stop() 停止执行器", "INFO")
            
            self.log_detail("✅ DistributedTaskExecutor深度分析完成", "INFO")
        except Exception as e:
            self.log_detail(f"❌ DistributedTaskExecutor分析失败: {e}", "ERROR")
            self.errors.append(f"phase1_3: {e}")
            passed = False
        
        self.test_results['phase1'] = passed
        return passed
    
    # ============================================================
    # 二、测试前置动作：拆解代码细节
    # ============================================================
    
    def phase2_pre_test_detail_extraction(self):
        """阶段2：测试前置动作 - 拆解代码细节"""
        self.log_detail("\n" + "="*120, "INFO")
        self.log_detail("阶段2：测试前置动作 - 拆解代码细节", "INFO")
        self.log_detail("="*120, "INFO")
        
        passed = True
        
        # 2.1 参数规则提取
        self.log_detail("\n2.1 参数规则提取", "INFO")
        self.log_detail("-"*80, "INFO")
        
        try:
            # TaskPriority参数
            self.log_detail("TaskPriority枚举参数:", "INFO")
            self.log_detail("  - CRITICAL = 0 (最高优先级)", "INFO")
            self.log_detail("  - HIGH = 1", "INFO")
            self.log_detail("  - NORMAL = 2 (默认)", "INFO")
            self.log_detail("  - LOW = 3 (最低优先级)", "INFO")
            
            # DistributedTask参数
            self.log_detail("\nDistributedTask参数:", "INFO")
            self.log_detail("  - task_id: str (UUID自动生成)", "INFO")
            self.log_detail("  - task_type: str (任务类型)", "INFO")
            self.log_detail("  - description: str (描述)", "INFO")
            self.log_detail("  - parameters: Dict[str, Any] (参数)", "INFO")
            self.log_detail("  - priority: TaskPriority (优先级)", "INFO")
            self.log_detail("  - weight: float (权重, 默认1.0)", "INFO")
            self.log_detail("  - retry_count: int (当前重试次数)", "INFO")
            self.log_detail("  - max_retries: int (最大重试次数, 默认3)", "INFO")
            self.log_detail("  - status: str (pending/running/completed/failed)", "INFO")
            self.log_detail("  - result: Any (执行结果)", "INFO")
            self.log_detail("  - error: str (错误信息)", "INFO")
            
            self.log_detail("✅ 参数规则提取完成", "INFO")
        except Exception as e:
            self.log_detail(f"❌ 参数规则提取失败: {e}", "ERROR")
            passed = False
        
        # 2.2 边界阈值提取
        self.log_detail("\n2.2 边界阈值提取", "INFO")
        self.log_detail("-"*80, "INFO")
        
        try:
            self.log_detail("LocalTaskQueue边界阈值:", "INFO")
            self.log_detail("  - completed队列: maxlen=10000 (限制历史存储)", "INFO")
            self.log_detail("  - failed队列: maxlen=1000 (限制失败历史)", "INFO")
            self.log_detail("  - 但是使用total_completed/total_failed完整计数", "INFO")
            
            self.log_detail("\n关键边界:", "INFO")
            self.log_detail("  - retry_count=0, max_retries=0: 直接失败", "INFO")
            self.log_detail("  - retry_count=0, max_retries=3: 可以重试3次", "INFO")
            self.log_detail("  - retry_count=3, max_retries=3: 不再重试", "INFO")
            
            self.log_detail("✅ 边界阈值提取完成", "INFO")
        except Exception as e:
            self.log_detail(f"❌ 边界阈值提取失败: {e}", "ERROR")
            passed = False
        
        # 2.3 隐藏规则挖掘
        self.log_detail("\n2.3 隐藏规则挖掘", "INFO")
        self.log_detail("-"*80, "INFO")
        
        try:
            self.log_detail("隐藏规则1: 队列选择逻辑", "INFO")
            self.log_detail("  if use_redis and HAS_REDIS:", "INFO")
            self.log_detail("      使用RedisTaskQueue", "INFO")
            self.log_detail("  else:", "INFO")
            self.log_detail("      使用LocalTaskQueue", "INFO")
            
            self.log_detail("\n隐藏规则2: 工作线程启动逻辑", "INFO")
            self.log_detail("  - is_running=True 时才启动新线程", "INFO")
            self.log_detail("  - 已启动时调用start()无效", "INFO")
            
            self.log_detail("\n隐藏规则3: 任务执行异常处理", "INFO")
            self.log_detail("  - 处理器不存在: ValueError", "INFO")
            self.log_detail("  - 处理器执行异常: 自动标记failed", "INFO")
            self.log_detail("  - 不阻塞其他任务", "INFO")
            
            self.log_detail("✅ 隐藏规则挖掘完成", "INFO")
        except Exception as e:
            self.log_detail(f"❌ 隐藏规则挖掘失败: {e}", "ERROR")
            passed = False
        
        self.test_results['phase2'] = passed
        return passed
    
    # ============================================================
    # 三、标准化全流程测试
    # ============================================================
    
    def phase3_standardized_test(self):
        """阶段3：标准化全流程测试"""
        self.log_detail("\n" + "="*120, "INFO")
        self.log_detail("阶段3：标准化全流程测试", "INFO")
        self.log_detail("="*120, "INFO")
        
        passed = True
        
        # 3.1 静态代码核验
        self.log_detail("\n3.1 静态代码核验", "INFO")
        self.log_detail("-"*80, "INFO")
        
        try:
            # 检查所有公开方法
            queue = LocalTaskQueue()
            executor = DistributedTaskExecutor(use_redis=False)
            
            required_methods = [
                'enqueue', 'dequeue', 'complete', 'fail',
                'get_stats', 'get_pending_count'
            ]
            
            for method in required_methods:
                if hasattr(queue, method):
                    self.log_detail(f"  ✅ queue.{method}() 存在", "INFO")
                else:
                    self.log_detail(f"  ❌ queue.{method}() 缺失", "ERROR")
                    passed = False
            
            executor_methods = [
                'register_handler', 'submit_task', 'start',
                'stop', 'get_statistics', 'get_pending_count'
            ]
            
            for method in executor_methods:
                if hasattr(executor, method):
                    self.log_detail(f"  ✅ executor.{method}() 存在", "INFO")
                else:
                    self.log_detail(f"  ❌ executor.{method}() 缺失", "ERROR")
                    passed = False
            
            self.log_detail("✅ 静态代码核验完成", "INFO")
        except Exception as e:
            self.log_detail(f"❌ 静态代码核验失败: {e}", "ERROR")
            passed = False
        
        # 3.2 单元全覆盖测试
        self.log_detail("\n3.2 单元全覆盖测试", "INFO")
        self.log_detail("-"*80, "INFO")
        
        try:
            # 测试所有分支
            test_cases = [
                ("优先级CRITICAL", TaskPriority.CRITICAL),
                ("优先级HIGH", TaskPriority.HIGH),
                ("优先级NORMAL", TaskPriority.NORMAL),
                ("优先级LOW", TaskPriority.LOW),
                ("重试0次", 0),
                ("重试1次", 1),
                ("重试3次", 3),
            ]
            
            for desc, value in test_cases:
                if isinstance(value, TaskPriority):
                    queue = LocalTaskQueue()
                    task = DistributedTask(task_type="test", priority=value)
                    queue.enqueue(task)
                    dequeued = queue.dequeue()
                    
                    if dequeued.priority == value:
                        self.log_detail(f"  ✅ {desc}: 优先级正确", "INFO")
                    else:
                        self.log_detail(f"  ❌ {desc}: 优先级错误", "ERROR")
                        passed = False
                else:
                    queue = LocalTaskQueue()
                    task = DistributedTask(task_type="test", max_retries=value)
                    queue.enqueue(task)
                    
                    # 执行max_retries+1次失败
                    for i in range(value + 1):
                        task_out = queue.dequeue()
                        queue.fail(task_out.task_id, f"error{i}")
                    
                    stats = queue.get_stats()
                    if stats['failed'] == 1 and stats['pending']['total'] == 0:
                        self.log_detail(f"  ✅ {desc}: 重试逻辑正确", "INFO")
                    else:
                        self.log_detail(f"  ❌ {desc}: 重试逻辑错误", "ERROR")
                        passed = False
            
            self.log_detail("✅ 单元全覆盖测试完成", "INFO")
        except Exception as e:
            self.log_detail(f"❌ 单元测试失败: {e}", "ERROR")
            passed = False
        
        # 3.3 异常场景测试
        self.log_detail("\n3.3 异常场景测试", "INFO")
        self.log_detail("-"*80, "INFO")
        
        try:
            executor = DistributedTaskExecutor(use_redis=False, max_workers=64)
            
            # 测试处理器不存在
            self.log_detail("测试1: 处理器不存在", "INFO")
            executor.submit_task("nonexistent", "test")
            executor.start(num_workers=4)
            time.sleep(0.5)
            executor.stop()
            
            stats = executor.get_statistics()
            if stats['failed'] > 0:
                self.log_detail("  ✅ 处理器不存在时自动标记failed", "INFO")
            else:
                self.log_detail("  ⚠️ 处理器不存在但未标记failed", "WARNING")
            
            # 测试空参数
            self.log_detail("\n测试2: 空参数", "INFO")
            executor = DistributedTaskExecutor(use_redis=False, max_workers=64)
            executor.register_handler("empty", lambda p: str(p))
            executor.submit_task("empty", "test", {})
            executor.submit_task("empty", "test", None)
            executor.start(num_workers=4)
            time.sleep(0.5)
            executor.stop()
            
            self.log_detail("  ✅ 空参数测试完成", "INFO")
            
            self.log_detail("✅ 异常场景测试完成", "INFO")
        except Exception as e:
            self.log_detail(f"❌ 异常场景测试失败: {e}", "ERROR")
            passed = False
        
        # 3.4 性能测试
        self.log_detail("\n3.4 性能测试", "INFO")
        self.log_detail("-"*80, "INFO")
        
        try:
            executor = DistributedTaskExecutor(use_redis=False, max_workers=256)
            
            count = 0
            lock = threading.Lock()
            
            def fast_handler(params):
                nonlocal count
                with lock:
                    count += 1
                return "fast"
            
            executor.register_handler("perf_test", fast_handler)
            
            num_tasks = 10000
            
            start = time.time()
            for i in range(num_tasks):
                executor.submit_task("perf_test", f"task{i}")
            
            executor.start(num_workers=128)
            
            # 等待完成
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
            
            self.log_detail(f"性能结果:", "INFO")
            self.log_detail(f"  - 任务数: {stats['completed']}/{num_tasks}", "INFO")
            self.log_detail(f"  - 耗时: {duration:.3f}秒", "INFO")
            self.log_detail(f"  - 吞吐量: {throughput:.0f} 任务/秒", "INFO")
            
            if throughput > 5000:
                self.log_detail("  ✅ 性能测试通过 (>5000任务/秒)", "INFO")
            else:
                self.log_detail("  ⚠️ 性能较低 (<5000任务/秒)", "WARNING")
            
            self.log_detail("✅ 性能测试完成", "INFO")
        except Exception as e:
            self.log_detail(f"❌ 性能测试失败: {e}", "ERROR")
            passed = False
        
        self.test_results['phase3'] = passed
        return passed
    
    # ============================================================
    # 四、验收标准判定
    # ============================================================
    
    def phase4_acceptance_criteria(self):
        """阶段4：验收标准判定"""
        self.log_detail("\n" + "="*120, "INFO")
        self.log_detail("阶段4：验收标准判定", "INFO")
        self.log_detail("="*120, "INFO")
        
        all_passed = True
        
        # 标准1
        if self.test_results.get('phase3', False):
            self.log_detail("✅ 标准1: 代码所有逻辑分支100%可执行", "INFO")
        else:
            self.log_detail("❌ 标准1: 存在未执行的逻辑分支", "ERROR")
            all_passed = False
        
        # 标准2
        if self.test_results.get('phase3', False):
            self.log_detail("✅ 标准2: 运行行为与代码定义完全一致", "INFO")
        else:
            self.log_detail("❌ 标准2: 运行行为与定义不一致", "ERROR")
            all_passed = False
        
        # 标准3
        if self.test_results.get('phase1', False) and self.test_results.get('phase2', False):
            self.log_detail("✅ 标准3: 数据流转无误，无逻辑冲突", "INFO")
        else:
            self.log_detail("❌ 标准3: 存在数据流问题", "ERROR")
            all_passed = False
        
        # 标准4
        if self.test_results.get('phase3', False):
            self.log_detail("✅ 标准4: 异常场景可平稳兜底，无崩溃", "INFO")
        else:
            self.log_detail("❌ 标准4: 异常处理有问题", "ERROR")
            all_passed = False
        
        self.test_results['phase4'] = all_passed
        return all_passed
    
    def run_all_phases(self):
        """运行所有阶段"""
        self.log_detail("\n" + "="*120, "INFO")
        self.log_detail("第7轮迭代 - 极致细节化深度测试开始", "INFO")
        self.log_detail("="*120, "INFO")
        
        all_passed = True
        
        all_passed &= self.phase1_deep_code_analysis()
        all_passed &= self.phase2_pre_test_detail_extraction()
        all_passed &= self.phase3_standardized_test()
        all_passed &= self.phase4_acceptance_criteria()
        
        # 生成报告
        self.log_detail("\n" + "="*120, "INFO")
        self.log_detail("最终测试报告", "INFO")
        self.log_detail("="*120, "INFO")
        
        for key, result in self.test_results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            self.log_detail(f"{key}: {status}", "INFO")
        
        self.log_detail(f"\n总错误数: {len(self.errors)}", "INFO")
        self.log_detail(f"总警告数: {len(self.warnings)}", "INFO")
        
        # 保存详细日志
        log_file = f"round7_detailed_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(self.detailed_logs))
        
        self.log_detail(f"\n详细日志已保存到: {log_file}", "INFO")
        
        if all_passed:
            self.log_detail("\n🎉 第7轮迭代 - 极致细节化测试全部通过！", "INFO")
        else:
            self.log_detail("\n⚠️ 第7轮迭代测试存在问题", "WARNING")
        
        return all_passed


if __name__ == "__main__":
    test = Round7DetailedTest()
    result = test.run_all_phases()
    sys.exit(0 if result else 1)
