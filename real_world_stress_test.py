#!/usr/bin/env python3
"""
真实场景长期压力测试框架
Real-World Long-Term Stress Testing Framework
"""

import sys
import time
import logging
import threading
import random
import gc
import traceback
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable
from collections import deque, defaultdict
import json
import os
import psutil

sys.path.insert(0, '/workspace/evolving_agent')

from core.dual_loop_drive_engine import DualLoopDriveEngine, PerpetualEvolutionController
from core.data_models_universal import ExecutionLevel, generate_task_id

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/workspace/evolving_agent/stress_test.log'),
        logging.StreamHandler()
    ]
)


class BugCollector:
    """Bug收集和追踪器"""
    def __init__(self):
        self.bugs: List[Dict[str, Any]] = []
        self.bug_count: int = 0
        self.lock = threading.RLock()

    def record_bug(self, bug_type: str, description: str, exception: Optional[Exception] = None,
                   context: Optional[Dict[str, Any]] = None, severity: str = "medium"):
        with self.lock:
            self.bug_count += 1
            bug = {
                'bug_id': self.bug_count,
                'timestamp': datetime.now().isoformat(),
                'type': bug_type,
                'description': description,
                'severity': severity,
                'context': context or {},
                'stack_trace': traceback.format_exc() if exception else None,
                'resolved': False
            }
            self.bugs.append(bug)
            logging.critical(f"\n!!! BUG RECORDED #{self.bug_count} [{severity}]\n{bug_type}: {description}\n")

    def get_bugs(self) -> List[Dict[str, Any]]:
        with self.lock:
            return list(self.bugs)

    def save_bugs(self, filename: str = '/workspace/evolving_agent/bugs.json'):
        with self.lock:
            with open(filename, 'w') as f:
                json.dump(self.bugs, f, indent=2, default=str)
        logging.info(f"Saved {len(self.bugs)} bugs to {filename}")


class PerformanceMonitor:
    """系统性能监控器"""
    def __init__(self, sample_interval: float = 1.0):
        self.sample_interval = sample_interval
        self.metrics_history: deque = deque(maxlen=1000)
        self.is_running = False
        self.thread: Optional[threading.Thread] = None
        self.lock = threading.RLock()

    def start(self):
        self.is_running = True
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.is_running = False
        if self.thread:
            self.thread.join(timeout=5)

    def _monitor_loop(self):
        while self.is_running:
            try:
                self._sample_metrics()
            except Exception:
                pass
            time.sleep(self.sample_interval)

    def _sample_metrics(self):
        try:
            process = psutil.Process(os.getpid())
            with self.lock:
                self.metrics_history.append({
                    'timestamp': datetime.now().isoformat(),
                    'cpu_percent': process.cpu_percent(),
                    'memory_mb': process.memory_info().rss / 1024 / 1024,
                    'num_threads': process.num_threads(),
                    'num_fds': len(process.open_files()) if hasattr(process, 'open_files') else 0,
                })
        except Exception:
            pass

    def get_stats(self) -> Dict[str, Any]:
        with self.lock:
            if not self.metrics_history:
                return {}
            mem_usage = [m['memory_mb'] for m in self.metrics_history]
            cpu_usage = [m['cpu_percent'] for m in self.metrics_history]
            return {
                'samples': len(self.metrics_history),
                'max_memory_mb': max(mem_usage) if mem_usage else 0,
                'avg_memory_mb': sum(mem_usage) / len(mem_usage) if mem_usage else 0,
                'max_cpu_percent': max(cpu_usage) if cpu_usage else 0,
                'avg_cpu_percent': sum(cpu_usage) / len(cpu_usage) if cpu_usage else 0,
            }


class StressTaskGenerator:
    """压力测试任务生成器"""
    def __init__(self):
        self.task_types = ['compute', 'io_bound', 'memory_hungry', 'random', 'knowledge_query', 'project_integration']
        self.data_sizes = [100, 1000, 5000, 10000, 50000]
        self.work_durations = [0.01, 0.05, 0.1, 0.2, 0.5]

    def generate_task_params(self, task_type: Optional[str] = None) -> Dict[str, Any]:
        if task_type is None:
            task_type = random.choice(self.task_types)
        params = {
            'task_type': task_type,
            'n': random.choice(self.data_sizes),
            'duration': random.choice(self.work_durations),
            'memory_alloc': random.randint(10000, 1000000),
            'query': random.choice(['test', 'demo', 'project', 'evolution', 'system']),
            'content': f"Generated content at {datetime.now().isoformat()}",
            'project_id': str(uuid.uuid4()),
        }
        return params


class StressWorkload:
    """压力测试工作负载"""
    def __init__(self, engine: DualLoopDriveEngine, bug_collector: BugCollector):
        self.engine = engine
        self.bug_collector = bug_collector
        self.task_gen = StressTaskGenerator()
        self.tasks_submitted = 0
        self.lock = threading.RLock()

    def register_task_handlers(self):
        try:
            self.engine.task_engine.register_handler('compute', self._compute_task)
            self.engine.task_engine.register_handler('io_bound', self._io_bound_task)
            self.engine.task_engine.register_handler('memory_hungry', self._memory_hungry_task)
            self.engine.task_engine.register_handler('random', self._random_task)
            self.engine.task_engine.register_handler('knowledge_query', self._knowledge_query_task)
            self.engine.task_engine.register_handler('project_integration', self._project_integration_task)
            logging.info("All stress test handlers registered")
        except Exception as e:
            self.bug_collector.record_bug(
                'handler_registration_failure',
                f'Failed to register handlers: {e}',
                exception=e,
                severity='critical'
            )

    def _compute_task(self, params: Dict[str, Any]) -> Any:
        try:
            n = params.get('n', 1000)
            result = sum(i * i for i in range(n))
            time.sleep(params.get('duration', 0.01))
            return {'result': result, 'type': 'compute'}
        except Exception as e:
            self.bug_collector.record_bug('compute_task_error', f'Compute task failed', exception=e)
            raise

    def _io_bound_task(self, params: Dict[str, Any]) -> Any:
        try:
            temp_file = f'temp_io_{uuid.uuid4().hex}.txt'
            with open(temp_file, 'w') as f:
                f.write('x' * params.get('n', 1000))
            with open(temp_file, 'r') as f:
                content = f.read()
            try:
                os.remove(temp_file)
            except Exception:
                pass
            time.sleep(params.get('duration', 0.05))
            return {'result': 'io_done', 'bytes_read': len(content)}
        except Exception as e:
            self.bug_collector.record_bug('io_task_error', f'IO task failed', exception=e)
            raise

    def _memory_hungry_task(self, params: Dict[str, Any]) -> Any:
        try:
            allocation_size = params.get('memory_alloc', 100000)
            big_list = list(range(allocation_size))
            time.sleep(params.get('duration', 0.1))
            result = len(big_list)
            del big_list
            gc.collect()
            return {'result': 'memory_test', 'allocated': result}
        except Exception as e:
            self.bug_collector.record_bug('memory_task_error', f'Memory task failed', exception=e)
            raise

    def _random_task(self, params: Dict[str, Any]) -> Any:
        try:
            n = params.get('n', 100)
            random_list = [random.random() for _ in range(n)]
            time.sleep(params.get('duration', 0.05))
            return {'result': sum(random_list), 'type': 'random'}
        except Exception as e:
            self.bug_collector.record_bug('random_task_error', f'Random task failed', exception=e)
            raise

    def _knowledge_query_task(self, params: Dict[str, Any]) -> Any:
        try:
            query = params.get('query', 'test')
            results = self.engine.query_knowledge(query, max_results=5)
            return {'results_count': len(results), 'query': query}
        except Exception as e:
            self.bug_collector.record_bug('knowledge_query_task_error', f'Knowledge query failed', exception=e)
            raise

    def _project_integration_task(self, params: Dict[str, Any]) -> Any:
        try:
            project_id = params.get('project_id', 'unknown')
            project_name = f"Project_{project_id[:8]}"
            description = f"Generated project at {datetime.now().isoformat()}"
            technologies = ["Python", "AI", random.choice(["GPU", "ML", "DL", "Cloud", "Edge"])]
            outcomes = "Successful integration"
            node_id = self.engine.integrate_project(project_name, description, technologies, outcomes)
            return {'project_node_id': node_id, 'project_name': project_name}
        except Exception as e:
            self.bug_collector.record_bug('project_integration_task_error', f'Project integration failed', exception=e)
            raise

    def submit_random_tasks(self, count: int, delay: float = 0.005):
        with self.lock:
            for i in range(count):
                try:
                    params = self.task_gen.generate_task_params()
                    task_type = params['task_type']
                    self.engine.add_task(task_type, f"Stress task #{self.tasks_submitted + 1}", params)
                    self.tasks_submitted += 1
                    time.sleep(delay)
                except Exception as e:
                    self.bug_collector.record_bug(
                        'task_submission_error',
                        f'Failed to submit task #{self.tasks_submitted}',
                        exception=e,
                        severity='high'
                    )


class LongTermStressTest:
    """长期压力测试"""
    def __init__(self, duration_minutes: int = 5, task_count: int = 500):
        self.duration_minutes = duration_minutes
        self.target_task_count = task_count
        self.engine = None
        self.bug_collector = BugCollector()
        self.perf_monitor = PerformanceMonitor(sample_interval=0.5)
        self.stats = defaultdict(int)

    def setup(self):
        try:
            logging.info(f"[SETUP] Initializing stress test engine with duration={self.duration_minutes} minutes...")
            self.engine = DualLoopDriveEngine({
                'max_workers': 12,
                'optimization_interval': 30.0,
                'knowledge_db_path': '/workspace/evolving_agent/stress_test_knowledge.db'
            })
            self.engine.initialize()
            
            self.workload = StressWorkload(self.engine, self.bug_collector)
            self.workload.register_task_handlers()
            
            self.evolution_controller = PerpetualEvolutionController(self.engine)
            logging.info("[SETUP] Stress test setup complete")
        except Exception as e:
            self.bug_collector.record_bug(
                'setup_failure',
                f'Stress test setup failed',
                exception=e,
                severity='critical'
            )
            raise

    def run(self):
        start_time = time.time()
        end_time = start_time + (self.duration_minutes * 60)
        iteration = 0
        
        logging.info(f"[START] Starting stress test at {datetime.now()}")
        logging.info(f"[PLAN] Will run for {self.duration_minutes} minutes or until {self.target_task_count} tasks submitted")
        
        self.engine.start()
        self.evolution_controller.start_evolution()
        self.perf_monitor.start()
        
        try:
            while time.time() < end_time and self.workload.tasks_submitted < self.target_task_count:
                iteration += 1
                
                tasks_per_iteration = random.randint(5, 30)
                self.workload.submit_random_tasks(tasks_per_iteration, delay=0.005)
                
                if iteration % 20 == 0:
                    self._report_progress(iteration)
                
                gc.collect()
                time.sleep(1.0)
                
            logging.info(f"[END] Stopping stress test...")
            
        except KeyboardInterrupt:
            logging.info("[STOP] Stress test interrupted by user")
        except Exception as e:
            self.bug_collector.record_bug('main_loop_failure', f'Main test loop failed', exception=e, severity='critical')
        finally:
            self._cleanup()

    def _report_progress(self, iteration: int):
        try:
            stats = self.engine.task_engine.get_statistics()
            qstats = stats['queue_stats']
            perf_stats = self.perf_monitor.get_stats()
            
            print(f"\n{'='*60}")
            print(f"PROGRESS REPORT - Iteration {iteration}")
            print(f"{'='*60}")
            print(f"  Submitted Tasks: {self.workload.tasks_submitted}/{self.target_task_count}")
            print(f"  Task Queue: P={qstats['pending']} R={qstats['running']} C={qstats['completed']} F={qstats['failed']}")
            print(f"  Memory: {perf_stats.get('avg_memory_mb', 0):.1f} MB avg, {perf_stats.get('max_memory_mb', 0):.1f} MB max")
            print(f"  CPU: {perf_stats.get('avg_cpu_percent', 0):.1f}% avg, {perf_stats.get('max_cpu_percent', 0):.1f}% max")
            print(f"  Bugs Found: {self.bug_collector.bug_count}")
            print(f"{'='*60}\n")
            
            knowledge_stats = self.engine.knowledge_graph.get_statistics()
            logging.info(f"Knowledge Graph: {knowledge_stats.get('total_nodes', 0)} nodes")
            
        except Exception as e:
            self.bug_collector.record_bug('progress_report_error', f'Failed to report progress', exception=e)

    def _cleanup(self):
        logging.info("[CLEANUP] Shutting down systems...")
        try:
            self.perf_monitor.stop()
            self.engine.stop()
        except Exception:
            pass
        
        self._final_report()

    def _final_report(self):
        final_stats = self.engine.task_engine.get_statistics()
        perf_stats = self.perf_monitor.get_stats()
        
        print("\n" + "="*80)
        print("FINAL STRESS TEST REPORT")
        print("="*80)
        
        print("\n[EXECUTION SUMMARY]")
        print(f"  Total Tasks Submitted: {self.workload.tasks_submitted}")
        print(f"  Completed: {final_stats['queue_stats']['completed']}")
        print(f"  Failed: {final_stats['queue_stats']['failed']}")
        print(f"  Failure Rate: {(final_stats['queue_stats']['failed'] / max(self.workload.tasks_submitted, 1)):.2%}")
        
        print("\n[PERFORMANCE SUMMARY]")
        print(f"  Avg Memory: {perf_stats.get('avg_memory_mb', 0):.1f} MB")
        print(f"  Peak Memory: {perf_stats.get('max_memory_mb', 0):.1f} MB")
        print(f"  Avg CPU: {perf_stats.get('avg_cpu_percent', 0):.1f}%")
        print(f"  Peak CPU: {perf_stats.get('max_cpu_percent', 0):.1f}%")
        
        print("\n[BUG SUMMARY]")
        print(f"  Total Bugs Found: {self.bug_collector.bug_count}")
        
        bugs_by_severity = defaultdict(int)
        for bug in self.bug_collector.get_bugs():
            bugs_by_severity[bug['severity']] += 1
            
        for severity in ['critical', 'high', 'medium', 'low']:
            count = bugs_by_severity.get(severity, 0)
            if count > 0:
                print(f"    {severity.upper()}: {count}")
                
        print("\n" + "="*80)
        
        self.bug_collector.save_bugs()
        
        return final_stats


def main():
    print("\n" + "="*80)
    print("REAL-WORLD STRESS TESTING FRAMEWORK")
    print("="*80 + "\n")
    
    try:
        psutil.Process(os.getpid())
    except ImportError:
        print("Note: psutil not available, performance monitoring limited")
        
    try:
        test = LongTermStressTest(duration_minutes=3, task_count=500)
        test.setup()
        test.run()
    except Exception as e:
        logging.critical(f"Fatal error in stress test: {e}")
        traceback.print_exc()
        return 1
        
    return 0


if __name__ == '__main__':
    sys.exit(main())
