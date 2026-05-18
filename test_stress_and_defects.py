#!/usr/bin/env python3
"""
持续进化系统压力测试与缺陷发现框架
Stress Test & Defect Discovery Framework
"""

import sys
import time
import logging
import threading
import random
import traceback
from datetime import datetime
from typing import Dict, List, Any, Callable, Optional
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import json

sys.path.insert(0, '/workspace/evolving_agent')

from core.dual_loop_drive_engine import DualLoopDriveEngine
from core.data_models_universal import ExecutionLevel
from knowledge.knowledge_graph_integration import KnowledgeGraphIntegration
from execution.task_execution_engine import TaskExecutionEngine

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/workspace/evolving_agent/test_debug.log'),
        logging.StreamHandler()
    ]
)


class DefectTracker:
    """缺陷追踪器"""
    
    def __init__(self):
        self.defects: List[Dict[str, Any]] = []
        self.lock = threading.RLock()
        
    def record_defect(self, defect_type: str, description: str, 
                     context: Dict[str, Any] = None, severity: str = "medium"):
        with self.lock:
            self.defects.append({
                'defect_id': len(self.defects) + 1,
                'type': defect_type,
                'description': description,
                'severity': severity,
                'context': context or {},
                'timestamp': datetime.now(),
                'resolved': False
            })
            logging.error(f"DEFECT RECORDED: [{severity}] {defect_type} - {description}")
    
    def get_defects(self, severity: str = None) -> List[Dict[str, Any]]:
        with self.lock:
            if severity:
                return [d for d in self.defects if d['severity'] == severity]
            return list(self.defects)
    
    def resolve_defect(self, defect_id: int):
        with self.lock:
            for d in self.defects:
                if d['defect_id'] == defect_id:
                    d['resolved'] = True
                    d['resolved_at'] = datetime.now()


class PerformanceMonitor:
    """性能监控器"""
    
    def __init__(self, sample_interval: float = 1.0):
        self.sample_interval = sample_interval
        self.samples: deque = deque(maxlen=1000)
        self.lock = threading.RLock()
        self.is_monitoring = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.engine: Optional[DualLoopDriveEngine] = None
        
    def start_monitoring(self, engine: DualLoopDriveEngine):
        self.engine = engine
        self.is_monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        
    def stop_monitoring(self):
        self.is_monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
            
    def _monitor_loop(self):
        while self.is_monitoring:
            try:
                if self.engine:
                    status = self.engine.get_system_status()
                    self._record_sample(status)
            except Exception as e:
                pass
            time.sleep(self.sample_interval)
            
    def _record_sample(self, status: Dict[str, Any]):
        with self.lock:
            self.samples.append({
                'timestamp': datetime.now(),
                'task_stats': status.get('task_engine_stats', {}),
                'optimization_stats': status.get('optimization_stats', {}),
                'guardian_status': status.get('guardian_status', {})
            })
    
    def get_performance_summary(self) -> Dict[str, Any]:
        with self.lock:
            if not self.samples:
                return {}
                
            pending_counts = [s['task_stats']['queue_stats']['pending'] 
                             for s in self.samples if 'task_stats' in s]
            running_counts = [s['task_stats']['queue_stats']['running'] 
                             for s in self.samples if 'task_stats' in s]
            
            return {
                'sample_count': len(self.samples),
                'avg_pending': sum(pending_counts) / len(pending_counts) if pending_counts else 0,
                'max_pending': max(pending_counts) if pending_counts else 0,
                'avg_running': sum(running_counts) / len(running_counts) if running_counts else 0,
                'max_running': max(running_counts) if running_counts else 0
            }


class StressTestCase:
    """压力测试用例基类"""
    
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
    def setup(self, engine: DualLoopDriveEngine, defect_tracker: DefectTracker):
        raise NotImplementedError
        
    def execute(self, engine: DualLoopDriveEngine, defect_tracker: DefectTracker):
        raise NotImplementedError
        
    def teardown(self, engine: DualLoopDriveEngine, defect_tracker: DefectTracker):
        pass
        
    def run(self, engine: DualLoopDriveEngine, defect_tracker: DefectTracker):
        logging.info(f"Running test case: {self.name}")
        self.start_time = datetime.now()
        
        try:
            self.setup(engine, defect_tracker)
            self.execute(engine, defect_tracker)
        except Exception as e:
            defect_tracker.record_defect(
                'test_case_failure',
                f"Test {self.name} failed with exception",
                {'error': str(e), 'traceback': traceback.format_exc()},
                severity='critical'
            )
        finally:
            self.teardown(engine, defect_tracker)
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            logging.info(f"Test {self.name} completed in {duration:.2f}s")


class HighThroughputTaskTest(StressTestCase):
    """高吞吐量任务测试"""
    
    def __init__(self):
        super().__init__(
            'HighThroughputTask',
            'Test system under high task throughput'
        )
        
    def setup(self, engine: DualLoopDriveEngine, defect_tracker: DefectTracker):
        engine.task_engine.register_handler('computation', self._computation_task)
        
    def _computation_task(self, params: Dict[str, Any]) -> Any:
        n = params.get('n', 1000)
        result = sum(i * i for i in range(n))
        return result
        
    def execute(self, engine: DualLoopDriveEngine, defect_tracker: DefectTracker):
        for i in range(500):
            engine.add_task(
                'computation',
                f'High-throughput task {i}',
                {'n': random.randint(100, 5000)}
            )
            time.sleep(0.01)
            
        time.sleep(30)
        
        status = engine.get_system_status()
        pending = status['task_engine_stats']['queue_stats']['pending']
        
        if pending > 100:
            defect_tracker.record_defect(
                'task_queue_backlog',
                f'Task queue backlog detected: {pending} pending',
                {'pending_tasks': pending},
                severity='high'
            )


class ConcurrentKnowledgeQueryTest(StressTestCase):
    """并发知识查询测试"""
    
    def __init__(self):
        super().__init__(
            'ConcurrentKnowledgeQuery',
            'Test concurrent knowledge graph queries'
        )
        
    def setup(self, engine: DualLoopDriveEngine, defect_tracker: DefectTracker):
        for i in range(50):
            engine.integrate_experience(
                'test_experience',
                f'Knowledge entry {i} for testing concurrent queries'
            )
            
    def _query_worker(self, engine: DualLoopDriveEngine, query_num: int, 
                    defect_tracker: DefectTracker):
        try:
            for i in range(100):
                query = f"test {query_num} query {i}"
                results = engine.query_knowledge(query, max_results=3)
                time.sleep(random.random() * 0.05)
        except Exception as e:
            defect_tracker.record_defect(
                'concurrent_knowledge_query_error',
                f'Concurrent query failed',
                {'error': str(e), 'query_num': query_num},
                severity='high'
            )
            
    def execute(self, engine: DualLoopDriveEngine, defect_tracker: DefectTracker):
        threads = []
        for i in range(10):
            t = threading.Thread(
                target=self._query_worker,
                args=(engine, i, defect_tracker)
            )
            threads.append(t)
            t.start()
            
        for t in threads:
            t.join()


class RapidOptimizationTest(StressTestCase):
    """快速优化规则应用测试"""
    
    def __init__(self):
        super().__init__(
            'RapidOptimization',
            'Test rapid optimization rule applications'
        )
        
    def setup(self, engine: DualLoopDriveEngine, defect_tracker: DefectTracker):
        from core.data_models_universal import OptimizationRule, generate_rule_id
        
    def execute(self, engine: DualLoopDriveEngine, defect_tracker: DefectTracker):
        from core.data_models_universal import OptimizationRule, generate_rule_id
        
        for i in range(100):
            rule = OptimizationRule(
                rule_id=generate_rule_id(),
                name=f'optimization_{i}',
                description=f'Test optimization rule {i}',
                target_component='task_execution',
                optimization_type='performance',
                parameters={'test_param': i}
            )
            engine.rule_emitter.landing_layer.queue_rule_application(rule, priority=random.randint(1, 10))
            
        time.sleep(20)
        
        sync_stats = engine.synchronization_layer.get_synchronization_status()
        if sync_stats['recent_failures'] > 10:
            defect_tracker.record_defect(
                'optimization_rule_application_failure',
                f'Too many optimization failures: {sync_stats["recent_failures"]}',
                sync_stats,
                severity='medium'
            )


class LongRunningStabilityTest(StressTestCase):
    """长时间稳定性测试"""
    
    def __init__(self, duration_minutes: int = 2):
        super().__init__(
            'LongRunningStability',
            f'Test system stability over {duration_minutes} minutes'
        )
        self.duration_minutes = duration_minutes
        
    def setup(self, engine: DualLoopDriveEngine, defect_tracker: DefectTracker):
        engine.task_engine.register_handler('continuous_task', self._continuous_task)
        
    def _continuous_task(self, params: Dict[str, Any]) -> Any:
        time.sleep(random.random() * 0.5)
        return {'result': 'done', 'iterations': params.get('iterations', 0)}
        
    def execute(self, engine: DualLoopDriveEngine, defect_tracker: DefectTracker):
        end_time = time.time() + (self.duration_minutes * 60)
        task_counter = 0
        
        while time.time() < end_time:
            engine.add_task(
                'continuous_task',
                f'Stability test task {task_counter}',
                {'iterations': task_counter}
            )
            task_counter += 1
            time.sleep(0.5)
            
            if task_counter % 20 == 0:
                engine.integrate_experience(
                    'stability_test',
                    f'Experience from stability test iteration {task_counter}'
                )
                
        logging.info(f"Completed {task_counter} stability test tasks")


class EdgeCaseBehaviorTest(StressTestCase):
    """边界行为测试"""
    
    def __init__(self):
        super().__init__(
            'EdgeCaseBehavior',
            'Test system behavior under edge cases'
        )
        
    def setup(self, engine: DualLoopDriveEngine, defect_tracker: DefectTracker):
        engine.task_engine.register_handler('edge_case_task', self._edge_case_task)
        
    def _edge_case_task(self, params: Dict[str, Any]) -> Any:
        if params.get('should_fail', False):
            raise ValueError(f"Intentional failure for testing: {params.get('error_msg', 'Test error')}")
        if params.get('should_sleep', False):
            time.sleep(params.get('sleep_duration', 2.0))
        return {'result': 'success', 'params': params}
        
    def execute(self, engine: DualLoopDriveEngine, defect_tracker: DefectTracker):
        for i in range(100):
            should_fail = (i % 5 == 0)
            engine.add_task(
                'edge_case_task',
                f'Edge case test task {i}',
                {
                    'should_fail': should_fail,
                    'error_msg': f'Edge case error {i}',
                    'should_sleep': (i % 7 == 0),
                    'sleep_duration': random.random() * 3.0
                }
            )
            time.sleep(0.02)
            
        time.sleep(30)
        
        failed_count = len([t for t in engine.task_engine.task_queue.failed if t.status == 'failed'])
        if failed_count < 15:
            defect_tracker.record_defect(
                'task_failure_tracking_issue',
                f'Expected ~20 failed tasks, only {failed_count} tracked',
                {'expected': 20, 'actual': failed_count},
                severity='medium'
            )


class StressTestFramework:
    """压力测试框架"""
    
    def __init__(self):
        self.engine = None
        self.defect_tracker = DefectTracker()
        self.performance_monitor = PerformanceMonitor()
        self.test_cases: List[StressTestCase] = []
        
    def setup(self):
        logging.info("Setting up stress test framework...")
        self.engine = DualLoopDriveEngine({
            'max_workers': 8,
            'batch_size': 20,
            'optimization_interval': 10.0,
            'knowledge_db_path': 'test_knowledge_graph.db'
        })
        self.engine.initialize()
        
        self.test_cases = [
            HighThroughputTaskTest(),
            ConcurrentKnowledgeQueryTest(),
            RapidOptimizationTest(),
            LongRunningStabilityTest(duration_minutes=2),
            EdgeCaseBehaviorTest()
        ]
        
    def run_all_tests(self):
        logging.info("Starting comprehensive stress test suite...")
        
        self.engine.start()
        self.performance_monitor.start_monitoring(self.engine)
        
        try:
            for test_case in self.test_cases:
                test_case.run(self.engine, self.defect_tracker)
                
            logging.info("All test cases completed!")
            
        finally:
            self.performance_monitor.stop_monitoring()
            self.engine.stop()
            
    def generate_report(self) -> str:
        report = []
        report.append("=" * 80)
        report.append("PERSISTENT EVOLUTION AGENT - STRESS TEST REPORT")
        report.append("=" * 80)
        report.append(f"Generated at: {datetime.now()}")
        report.append("")
        
        performance = self.performance_monitor.get_performance_summary()
        report.append("PERFORMANCE SUMMARY:")
        if performance:
            for key, value in performance.items():
                report.append(f"  {key}: {value}")
        else:
            report.append("  No performance data available")
        report.append("")
        
        defects = self.defect_tracker.get_defects()
        report.append(f"DEFECTS FOUND: {len(defects)}")
        
        if defects:
            severity_groups: Dict[str, List] = {}
            for defect in defects:
                sev = defect['severity']
                if sev not in severity_groups:
                    severity_groups[sev] = []
                severity_groups[sev].append(defect)
                
            for sev in ['critical', 'high', 'medium', 'low']:
                if sev in severity_groups:
                    report.append(f"\n{sev.upper()} SEVERITY ({len(severity_groups[sev])}):")
                    for d in severity_groups[sev]:
                        report.append(f"  [{d['defect_id']}] {d['type']}: {d['description']}")
        else:
            report.append("  No defects found!")
            
        report.append("\n" + "=" * 80)
        
        report_str = "\n".join(report)
        
        with open('/workspace/evolving_agent/test_report.txt', 'w') as f:
            f.write(report_str)
            
        with open('/workspace/evolving_agent/defects.json', 'w') as f:
            json.dump(self.defect_tracker.get_defects(), f, default=str, indent=2)
            
        return report_str


def main():
    framework = StressTestFramework()
    framework.setup()
    
    try:
        framework.run_all_tests()
        report = framework.generate_report()
        print("\n" + report)
        
    except KeyboardInterrupt:
        logging.info("Test interrupted by user")
        if framework.engine:
            framework.engine.stop()
    except Exception as e:
        logging.error(f"Fatal test framework error: {e}")
        traceback.print_exc()
        if framework.engine:
            framework.engine.stop()
        raise


if __name__ == '__main__':
    main()
