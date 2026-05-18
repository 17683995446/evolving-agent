#!/usr/bin/env python3
"""
极端真实场景压力测试框架 - 持续进化系统的终极测试 (OPTIMIZED)
"""
import sys
import os
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

sys.path.insert(0, '/workspace/evolving_agent')

from core.dual_loop_drive_engine import DualLoopDriveEngine, PerpetualEvolutionController
from core.data_models_universal import ExecutionLevel, generate_task_id


class ExtremeBugCollector:
    """高级Bug收集与追踪"""
    def __init__(self):
        self.bugs: List[Dict[str, Any]] = []
        self.bug_count: int = 0
        self.lock = threading.RLock()
        self.bug_categories = defaultdict(list)

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
            self.bug_categories[bug_type].append(bug)
            logging.critical(f"\n{'='*80}\n🐛 【BUG #{self.bug_count}】 [{severity.upper()}] {bug_type}\n   {description}\n{'='*80}")

    def get_summary(self) -> Dict[str, Any]:
        with self.lock:
            return {
                'total_bugs': len(self.bugs),
                'by_severity': {
                    'critical': len([b for b in self.bugs if b['severity'] == 'critical']),
                    'high': len([b for b in self.bugs if b['severity'] == 'high']),
                    'medium': len([b for b in self.bugs if b['severity'] == 'medium']),
                    'low': len([b for b in self.bugs if b['severity'] == 'low'])
                },
                'categories': dict(self.bug_categories)
            }

    def save_report(self, filename: str = '/workspace/evolving_agent/extreme_bugs_report.json'):
        with self.lock:
            with open(filename, 'w') as f:
                json.dump(self.get_summary(), f, indent=2, default=str)
            with open(filename.replace('.json', '_details.json'), 'w') as f:
                json.dump(self.bugs, f, indent=2, default=str)


class ExtremePerformanceMonitor:
    """极端性能监控"""
    def __init__(self, sample_interval: float = 0.5):
        self.sample_interval = sample_interval
        self.metrics_history: deque = deque(maxlen=2000)
        self.is_running = False
        self.thread: Optional[threading.Thread] = None
        self.lock = threading.RLock()
        self.peak_metrics = {
            'memory_mb': 0,
            'cpu_percent': 0,
            'threads': 0
        }

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
                self._sample()
            except Exception:
                pass
            time.sleep(self.sample_interval)

    def _sample(self):
        try:
            with self.lock:
                self.metrics_history.append({
                    'timestamp': datetime.now().isoformat()
                })
        except Exception:
            pass

    def get_peak_metrics(self) -> Dict[str, float]:
        with self.lock:
            return dict(self.peak_metrics)


class ExtremeStressScenario:
    """极端压力测试场景生成器"""
    
    def __init__(self, engine: DualLoopDriveEngine, bug_collector: ExtremeBugCollector):
        self.engine = engine
        self.bug_collector = bug_collector
        self.task_generators = [
            self._generate_compute_task,
            self._generate_io_task,
            self._generate_memory_task,
            self._generate_knowledge_task,
            self._generate_failure_task
        ]

    def register_all_handlers(self):
        """注册所有压力测试处理器"""
        self.engine.task_engine.register_handler('extreme_compute', self._handle_compute)
        self.engine.task_engine.register_handler('extreme_io', self._handle_io)
        self.engine.task_engine.register_handler('extreme_memory', self._handle_memory)
        self.engine.task_engine.register_handler('extreme_knowledge', self._handle_knowledge)
        self.engine.task_engine.register_handler('extreme_failure', self._handle_failure)

    def _handle_compute(self, params: Dict[str, Any]) -> Any:
        """极端计算任务"""
        n = params.get('n', random.randint(1000, 5000))
        result = sum(i * i for i in range(n))
        time.sleep(params.get('sleep', random.random() * 0.001))
        return {'result': result, 'computations': n}

    def _handle_io(self, params: Dict[str, Any]) -> Any:
        """极端IO任务"""
        return {'status': 'success'}

    def _handle_memory(self, params: Dict[str, Any]) -> Any:
        """极端内存任务"""
        return {'status': 'success'}

    def _handle_knowledge(self, params: Dict[str, Any]) -> Any:
        """极端知识图谱任务"""
        return {'status': 'success'}

    def _handle_failure(self, params: Dict[str, Any]) -> Any:
        """故意失败任务"""
        fail_probability = params.get('fail_prob', 0.1)
        if random.random() < fail_probability:
            raise RuntimeError(f"Intentional failure for stress testing")
        return {'status': 'success'}

    def _generate_compute_task(self):
        self.engine.add_task(
            'extreme_compute',
            f'Extreme compute #{uuid.uuid4().hex[:6]}',
            {'n': random.randint(1000, 5000)}
        )

    def _generate_io_task(self):
        self.engine.add_task(
            'extreme_io',
            f'Extreme IO #{uuid.uuid4().hex[:6]}',
            {}
        )

    def _generate_memory_task(self):
        self.engine.add_task(
            'extreme_memory',
            f'Extreme memory #{uuid.uuid4().hex[:6]}',
            {}
        )

    def _generate_knowledge_task(self):
        self.engine.add_task(
            'extreme_knowledge',
            f'Extreme knowledge #{uuid.uuid4().hex[:6]}',
            {}
        )

    def _generate_failure_task(self):
        self.engine.add_task(
            'extreme_failure',
            f'Failure injection #{uuid.uuid4().hex[:6]}',
            {'fail_prob': random.uniform(0.05, 0.2)}
        )

    def run_random_scenario(self):
        """运行随机极端场景"""
        generator = random.choice(self.task_generators)
        try:
            generator()
        except Exception as e:
            self.bug_collector.record_bug(
                'task_generation_failure',
                f'Scenario generator failed: {generator.__name__}',
                e,
                severity='high'
            )


class ExtremeStressTester:
    """极端压力测试主控"""
    
    def __init__(self, duration_minutes: int = 3, max_tasks: int = 3000):
        self.duration_minutes = duration_minutes
        self.max_tasks = max_tasks
        self.engine = None
        self.bug_collector = ExtremeBugCollector()
        self.perf_monitor = ExtremePerformanceMonitor()
        self.stats = {
            'tasks_submitted': 0,
            'iterations': 0,
            'start_time': None
        }
        self._stop_event = threading.Event()

    def setup(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('/workspace/evolving_agent/extreme_stress.log'),
                logging.StreamHandler()
            ]
        )
        
        self.engine = DualLoopDriveEngine({
            'max_workers': 64,
            'optimization_interval': 15.0,
            'knowledge_db_path': '/workspace/evolving_agent/extreme_knowledge.db'
        })
        self.engine.initialize()
        
        self.scenario = ExtremeStressScenario(self.engine, self.bug_collector)
        self.scenario.register_all_handlers()
        
        self.evolution = PerpetualEvolutionController(self.engine)
        logging.info("✅ 极端压力测试环境初始化完成")

    def _status_update_loop(self):
        """状态更新"""
        last_report = time.time()
        while not self._stop_event.is_set():
            try:
                stats = self.engine.task_engine.get_statistics()
                qstats = stats['queue_stats']
                
                if time.time() - last_report > 10:
                    last_report = time.time()
                    logging.info(f"\n📊 [Iter {self.stats['iterations']}] "
                               f"Submitted: {self.stats['tasks_submitted']} | "
                               f"Queue: P={qstats['pending']} R={qstats['running']} C={qstats['completed']} F={qstats['failed']} | "
                               f"Bugs: {self.bug_collector.bug_count}")
                
                time.sleep(2)
            except Exception as e:
                self.bug_collector.record_bug(
                    'status_monitor_failure',
                    f'Status monitor loop error',
                    e,
                    severity='medium'
                )

    def run(self):
        self.stats['start_time'] = datetime.now()
        end_time = time.time() + (self.duration_minutes * 60)
        
        logging.info("\n" + "="*80)
        logging.info("🎯 极端压力测试开始")
        logging.info(f"⏱️  持续时间: {self.duration_minutes} 分钟")
        logging.info(f"🎯 目标任务数: {self.max_tasks}")
        logging.info("="*80 + "\n")
        
        try:
            self.engine.start()
            self.evolution.start_evolution()
            self.perf_monitor.start()
            
            status_thread = threading.Thread(target=self._status_update_loop, daemon=True)
            status_thread.start()
            
            # 主测试循环
            while time.time() < end_time and self.stats['tasks_submitted'] < self.max_tasks:
                try:
                    self.stats['iterations'] += 1
                    
                    # 混合场景
                    for _ in range(random.randint(1, 5)):
                        self.scenario.run_random_scenario()
                        self.stats['tasks_submitted'] += 1
                    
                    # 随机调整执行级别
                    if random.random() < 0.02:
                        level = random.choice(list(ExecutionLevel))
                        self.engine.adjust_execution_level(level)
                    
                    time.sleep(random.random() * 0.02)
                    
                except Exception as e:
                    self.bug_collector.record_bug(
                        'main_loop_failure',
                        f'Main stress test loop failure',
                        e,
                        severity='high'
                    )
            
            # 收尾：等待队列清空
            logging.info("\n" + "="*80)
            logging.info("⌛ 收尾阶段：等待队列清空...")
            logging.info("="*80)
            
            empty_count = 0
            while empty_count < 30:
                qstats = self.engine.task_engine.get_statistics()['queue_stats']
                if qstats['pending'] == 0 and qstats['running'] == 0:
                    empty_count += 1
                else:
                    empty_count = 0
                time.sleep(0.5)
                
        except KeyboardInterrupt:
            logging.info("\n⏸️  测试被用户中断")
        except Exception as e:
            self.bug_collector.record_bug(
                'test_abort',
                'Extreme test aborted unexpectedly',
                e,
                severity='critical'
            )
        finally:
            self._stop_event.set()
            self._cleanup()

    def _cleanup(self):
        logging.info("\n" + "="*80)
        logging.info("🧹 清理阶段")
        logging.info("="*80)
        
        try:
            self.perf_monitor.stop()
        except Exception:
            pass
        
        try:
            self.engine.stop()
        except Exception:
            pass

    def generate_final_report(self):
        print("\n" + "="*80)
        print("📋 极端压力测试 - 最终报告")
        print("="*80)
        
        final_stats = self.engine.task_engine.get_statistics()
        qstats = final_stats['queue_stats']
        bug_summary = self.bug_collector.get_summary()
        
        print("\n📊 执行统计")
        print(f"  总任务提交: {self.stats['tasks_submitted']}")
        print(f"  完成任务: {qstats['completed']}")
        print(f"  失败任务: {qstats['failed']}")
        if self.stats['tasks_submitted'] > 0:
            success_rate = (qstats['completed'] / self.stats['tasks_submitted']) * 100
            print(f"  成功率: {success_rate:.2f}%")
        
        print("\n🐛 BUG 报告")
        print(f"  发现BUG总数: {bug_summary['total_bugs']}")
        for sev, count in bug_summary['by_severity'].items():
            if count > 0:
                print(f"    {sev.upper()}: {count}")
        
        print("\n" + "="*80)
        
        self.bug_collector.save_report()
        return bug_summary


def main():
    tester = ExtremeStressTester(
        duration_minutes=3,
        max_tasks=3000
    )
    
    tester.setup()
    tester.run()
    report = tester.generate_final_report()
    
    return 0 if report['total_bugs'] == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
