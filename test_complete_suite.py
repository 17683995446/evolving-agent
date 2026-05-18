#!/usr/bin/env python3
"""
完整测试套件 - 单元测试、集成测试、性能基准测试
"""
import sys
import os
import time
import logging
import json
from typing import Dict, List, Any
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

sys.path.insert(0, '/workspace/evolving_agent')


class TestResult:
    """测试结果"""
    def __init__(self, test_name: str, passed: bool, duration: float,
                 error: str = None, details: Dict[str, Any] = None):
        self.test_name = test_name
        self.passed = passed
        self.duration = duration
        self.error = error
        self.details = details or {}
        self.timestamp = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        return {
            'test_name': self.test_name,
            'passed': self.passed,
            'duration': self.duration,
            'error': self.error,
            'details': self.details,
            'timestamp': self.timestamp.isoformat()
        }


class TestSuite:
    """测试套件"""
    
    def __init__(self, name: str):
        self.name = name
        self.tests: List[callable] = []
        self.results: List[TestResult] = []
        self.logger = logging.getLogger(name)

    def add_test(self, test_func: callable):
        """添加测试"""
        self.tests.append(test_func)

    def run(self) -> Dict[str, Any]:
        """运行所有测试"""
        self.logger.info(f"Running test suite: {self.name}")
        
        for test in self.tests:
            start_time = time.time()
            try:
                self.logger.info(f"Running test: {test.__name__}")
                result = test()
                duration = time.time() - start_time
                self.results.append(TestResult(
                    test_name=test.__name__,
                    passed=True,
                    duration=duration,
                    details={'result': result} if result else {}
                ))
                self.logger.info(f"✓ PASSED: {test.__name__} ({duration:.3f}s)")
            except Exception as e:
                duration = time.time() - start_time
                import traceback
                error = traceback.format_exc()
                self.results.append(TestResult(
                    test_name=test.__name__,
                    passed=False,
                    duration=duration,
                    error=error
                ))
                self.logger.error(f"✗ FAILED: {test.__name__}")
                self.logger.error(error)
        
        return self._generate_summary()

    def _generate_summary(self) -> Dict[str, Any]:
        """生成测试摘要"""
        total = len(self.results)
        passed = len([r for r in self.results if r.passed])
        failed = len([r for r in self.results if not r.passed])
        avg_duration = sum(r.duration for r in self.results) / total if total > 0 else 0
        
        return {
            'suite_name': self.name,
            'total_tests': total,
            'passed': passed,
            'failed': failed,
            'success_rate': (passed / total * 100) if total > 0 else 0,
            'average_duration': avg_duration,
            'results': [r.to_dict() for r in self.results],
            'timestamp': datetime.now().isoformat()
        }


class UnitTests:
    """单元测试"""
    
    @staticmethod
    def test_data_models():
        """测试数据模型"""
        from core.data_models_universal import TaskExecution, TaskStatus
        
        task = TaskExecution(
            task_id="test-123",
            task_type="compute",
            description="Test task",
            status=TaskStatus.PENDING
        )
        
        assert task.task_id == "test-123"
        assert task.task_type == "compute"
        assert task.status == TaskStatus.PENDING
        return True

    @staticmethod
    def test_distributed_task():
        """测试分布式任务"""
        from execution.distributed_task_queue import DistributedTask, TaskPriority
        
        task = DistributedTask(
            task_type="test",
            description="Test distributed task",
            priority=TaskPriority.HIGH
        )
        
        task_dict = task.to_dict()
        assert task_dict['task_type'] == "test"
        assert task_dict['priority'] == TaskPriority.HIGH.value
        
        restored = DistributedTask.from_dict(task_dict)
        assert restored.task_type == "test"
        assert restored.priority == TaskPriority.HIGH
        return True

    @staticmethod
    def test_local_queue():
        """测试本地队列"""
        from execution.distributed_task_queue import LocalTaskQueue, DistributedTask, TaskPriority
        
        queue = LocalTaskQueue()
        task = DistributedTask(task_type="test", description="Queue test")
        
        # 入队
        assert queue.enqueue(task) == True
        assert queue.get_pending_count() == 1
        
        # 出队
        dequeued = queue.dequeue()
        assert dequeued is not None
        assert dequeued.task_type == "test"
        assert queue.get_pending_count() == 0
        
        # 完成任务
        queue.complete(dequeued.task_id, {"result": "success"})
        stats = queue.get_stats()
        assert stats['completed'] == 1
        return True

    @staticmethod
    def test_metrics_collector():
        """测试指标收集器"""
        from core.advanced_monitoring import MetricsCollector, AlertLevel
        
        collector = MetricsCollector()
        
        # 测试计数器
        collector.counter("test_counter", 1)
        collector.counter("test_counter", 1)
        metric = collector.get_metric("test_counter")
        assert metric.value == 2
        
        # 测试仪表
        collector.gauge("test_gauge", 42.5)
        metric = collector.get_metric("test_gauge")
        assert metric.value == 42.5
        
        # 测试告警
        alert_id = collector.create_alert(AlertLevel.WARNING, "Test warning")
        active_alerts = collector.get_active_alerts()
        assert len(active_alerts) == 1
        
        collector.resolve_alert(alert_id)
        active_alerts = collector.get_active_alerts()
        assert len(active_alerts) == 0
        
        return True


class IntegrationTests:
    """集成测试"""
    
    @staticmethod
    def test_task_execution():
        """测试任务执行"""
        from execution.distributed_task_queue import DistributedTaskExecutor
        
        executor = DistributedTaskExecutor(max_workers=4)
        
        # 注册处理器
        def test_handler(params):
            return {"sum": sum(params.get('numbers', []))}
        
        executor.register_handler("compute", test_handler)
        executor.start(num_workers=2)
        
        # 提交任务
        task_id = executor.submit_task(
            "compute",
            "Integration test",
            {"numbers": [1, 2, 3, 4, 5]}
        )
        
        # 等待完成
        time.sleep(1)
        
        stats = executor.get_statistics()
        executor.stop()
        
        return True

    @staticmethod
    def test_distributed_queue_workflow():
        """测试分布式队列工作流"""
        from execution.distributed_task_queue import LocalTaskQueue, DistributedTask, TaskPriority
        
        queue = LocalTaskQueue()
        results = []
        
        # 提交多个不同优先级的任务
        for i in range(10):
            priority = TaskPriority(i % 4)
            task = DistributedTask(
                task_type=f"priority_test",
                description=f"Task with {priority.name}",
                priority=priority
            )
            queue.enqueue(task)
        
        # 处理任务（应该按优先级顺序）
        processed_priorities = []
        while True:
            task = queue.dequeue()
            if not task:
                break
            processed_priorities.append(task.priority)
            queue.complete(task.task_id, {"done": True})
        
        # 验证统计
        stats = queue.get_stats()
        assert stats['completed'] == 10
        assert stats['pending']['total'] == 0
        
        return True

    @staticmethod
    def test_dual_loop_engine():
        """测试双循环引擎"""
        from core.dual_loop_drive_engine import DualLoopDriveEngine
        
        engine = DualLoopDriveEngine({'max_workers': 4})
        engine.initialize()
        
        # 验证组件初始化
        assert engine.task_engine is not None
        assert engine.optimization_layer is not None
        assert engine.guardian is not None
        
        return True


class PerformanceBenchmark:
    """性能基准测试"""
    
    @staticmethod
    def benchmark_task_throughput(num_tasks: int = 1000):
        """基准测试：任务吞吐量"""
        from execution.distributed_task_queue import DistributedTaskExecutor
        
        executor = DistributedTaskExecutor(max_workers=32)
        
        results = []
        def handler(params):
            return params
        
        executor.register_handler("benchmark", handler)
        executor.start(num_workers=8)
        
        start_time = time.time()
        
        # 提交任务
        for i in range(num_tasks):
            executor.submit_task("benchmark", f"Benchmark {i}", {"index": i})
        
        # 等待完成
        while executor.get_pending_count() > 0:
            time.sleep(0.1)
        
        duration = time.time() - start_time
        throughput = num_tasks / duration if duration > 0 else 0
        
        executor.stop()
        
        return {
            'num_tasks': num_tasks,
            'duration': duration,
            'throughput': throughput,
            'tasks_per_second': round(throughput, 2)
        }

    @staticmethod
    def benchmark_queue_operations(num_operations: int = 10000):
        """基准测试：队列操作"""
        from execution.distributed_task_queue import LocalTaskQueue, DistributedTask
        
        queue = LocalTaskQueue()
        
        # 入队基准
        start = time.time()
        for i in range(num_operations):
            task = DistributedTask(task_type="test")
            queue.enqueue(task)
        enqueue_time = time.time() - start
        
        # 出队基准
        start = time.time()
        for i in range(num_operations):
            queue.dequeue()
        dequeue_time = time.time() - start
        
        return {
            'num_operations': num_operations,
            'enqueue_time': enqueue_time,
            'dequeue_time': dequeue_time,
            'enqueue_ops_per_sec': round(num_operations / enqueue_time, 2) if enqueue_time > 0 else 0,
            'dequeue_ops_per_sec': round(num_operations / dequeue_time, 2) if dequeue_time > 0 else 0
        }


def run_all_tests():
    """运行所有测试"""
    print("=" * 80)
    print("持续进化 Agent - 完整测试套件")
    print("=" * 80)
    
    all_summaries = []
    
    # 单元测试
    print("\n--- 单元测试 ---")
    unit_suite = TestSuite("Unit Tests")
    unit_suite.add_test(UnitTests.test_data_models)
    unit_suite.add_test(UnitTests.test_distributed_task)
    unit_suite.add_test(UnitTests.test_local_queue)
    unit_suite.add_test(UnitTests.test_metrics_collector)
    unit_summary = unit_suite.run()
    all_summaries.append(unit_summary)
    
    # 集成测试
    print("\n--- 集成测试 ---")
    integration_suite = TestSuite("Integration Tests")
    integration_suite.add_test(IntegrationTests.test_task_execution)
    integration_suite.add_test(IntegrationTests.test_distributed_queue_workflow)
    integration_suite.add_test(IntegrationTests.test_dual_loop_engine)
    integration_summary = integration_suite.run()
    all_summaries.append(integration_summary)
    
    # 性能基准测试
    print("\n--- 性能基准测试 ---")
    print("Running task throughput benchmark (1000 tasks)...")
    throughput_result = PerformanceBenchmark.benchmark_task_throughput(1000)
    print(f"  Throughput: {throughput_result['tasks_per_second']} tasks/sec")
    
    print("Running queue operations benchmark (10000 ops)...")
    queue_result = PerformanceBenchmark.benchmark_queue_operations(10000)
    print(f"  Enqueue: {queue_result['enqueue_ops_per_sec']} ops/sec")
    print(f"  Dequeue: {queue_result['dequeue_ops_per_sec']} ops/sec")
    
    # 生成报告
    print("\n" + "=" * 80)
    print("测试报告摘要")
    print("=" * 80)
    
    total_passed = sum(s['passed'] for s in all_summaries)
    total_tests = sum(s['total_tests'] for s in all_summaries)
    
    print(f"\n总测试数: {total_tests}")
    print(f"通过: {total_passed}")
    print(f"失败: {total_tests - total_passed}")
    print(f"成功率: {(total_passed / total_tests * 100):.1f}%" if total_tests > 0 else "N/A")
    
    # 保存报告
    report = {
        'timestamp': datetime.now().isoformat(),
        'suites': all_summaries,
        'benchmarks': {
            'throughput': throughput_result,
            'queue_operations': queue_result
        }
    }
    
    report_path = "/workspace/evolving_agent/test_report.json"
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"\n完整报告已保存到: {report_path}")
    
    return total_passed == total_tests


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
