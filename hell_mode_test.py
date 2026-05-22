#!/usr/bin/env python3
"""
超极端真实场景压力测试 - 地狱模式
包含各种边缘情况、并发问题、资源耗尽等真实挑战
"""
import sys
import os
import time
import logging
import threading
import random
import gc
import uuid
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable
from collections import deque, defaultdict
import json

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False
    print("⚠️  psutil not available - resource monitoring limited")

sys.path.insert(0, '/workspace/evolving_agent')

from core.dual_loop_drive_engine import DualLoopDriveEngine
from execution.distributed_task_queue import (
    DistributedTaskExecutor,
    DistributedTask,
    TaskPriority,
    LocalTaskQueue
)
from core.advanced_monitoring import (
    SystemMonitor,
    MetricsCollector,
    AlertLevel
)
from collaboration.multi_agent_system import MultiAgentSystem
from plugins.plugin_system import PluginManager, ExampleTaskPlugin
from evolution.deep_evolution import DeepEvolutionEngine


class HellBugCollector:
    """地狱模式Bug收集器"""
    def __init__(self):
        self.bugs: List[Dict[str, Any]] = []
        self.bug_count: int = 0
        self.lock = threading.RLock()
        self.bug_categories = defaultdict(list)

    def record_bug(self, bug_type: str, description: str, 
                   exception: Optional[Exception] = None,
                   context: Optional[Dict[str, Any]] = None,
                   severity: str = "medium"):
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
            print(f"\n{'='*80}")
            print(f"🐛 【BUG #{self.bug_count}】【{severity.upper()}】{bug_type}")
            print(f"   {description}")
            print(f"{'='*80}")

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
                'categories': {k: len(v) for k, v in self.bug_categories.items()}
            }

    def save_report(self, filename: str = '/workspace/evolving_agent/hell_bugs_report.json'):
        with self.lock:
            with open(filename, 'w') as f:
                json.dump(self.get_summary(), f, indent=2, default=str)
            with open(filename.replace('.json', '_details.json'), 'w') as f:
                json.dump(self.bugs, f, indent=2, default=str)


class HellScenarioGenerator:
    """地狱场景生成器"""
    def __init__(self, executor: DistributedTaskExecutor, 
                 bug_collector: HellBugCollector,
                 monitor: SystemMonitor):
        self.executor = executor
        self.bug_collector = bug_collector
        self.monitor = monitor
        self.stop_flag = False
        self.tasks_submitted = 0
        self.lock = threading.RLock()

    def scenario_1_high_concurrency_flood(self):
        """场景1：极高并发洪水"""
        print("\n🔥 场景1：极高并发洪水")
        threads = []
        
        def flood_worker():
            while not self.stop_flag:
                try:
                    for _ in range(random.randint(1, 5)):
                        task_type = random.choice([
                            'compute_light', 'sleep_short', 
                            'failure_test'
                        ])
                        self.executor.submit_task(
                            task_type,
                            f"Flood-{uuid.uuid4().hex[:8]}",
                            {'data': random.random()},
                            priority=random.choice(list(TaskPriority))
                        )
                        with self.lock:
                            self.tasks_submitted += 1
                except Exception as e:
                    self.bug_collector.record_bug(
                        'concurrent_submit_failure',
                        f"并发提交失败: {e}",
                        e,
                        severity='high'
                    )
                time.sleep(random.random() * 0.001)
        
        for i in range(32):  # 32个并发提交线程
            t = threading.Thread(target=flood_worker, daemon=True, name=f"Flood-{i}")
            t.start()
            threads.append(t)
        
        return threads

    def scenario_2_failure_injection(self):
        """场景2：故障注入"""
        print("\n💥 场景2：故障注入")
        
        def failure_worker():
            while not self.stop_flag:
                try:
                    for _ in range(random.randint(1, 5)):
                        self.executor.submit_task(
                            'guaranteed_failure',
                            f"Failure-{uuid.uuid4().hex[:8]}",
                            {'failure_prob': random.uniform(0.3, 0.8)},
                            priority=TaskPriority.CRITICAL
                        )
                        with self.lock:
                            self.tasks_submitted += 1
                except Exception as e:
                    self.bug_collector.record_bug(
                        'failure_injection_error',
                        f"故障注入出错: {e}",
                        e,
                        severity='medium'
                    )
                time.sleep(random.random() * 0.1)
        
        t = threading.Thread(target=failure_worker, daemon=True)
        t.start()
        return [t]

    def scenario_3_resource_stress(self):
        """场景3：资源压力"""
        print("\n💾 场景3：资源压力")
        
        def resource_worker():
            while not self.stop_flag:
                try:
                    self.executor.submit_task(
                        'memory_hungry',
                        f"Resource-{uuid.uuid4().hex[:8]}",
                        {'alloc_size': random.randint(100, 10000)},
                        priority=TaskPriority.LOW
                    )
                    with self.lock:
                        self.tasks_submitted += 1
                except Exception as e:
                    self.bug_collector.record_bug(
                        'resource_stress_failure',
                        f"资源压力测试出错: {e}",
                        e,
                        severity='medium'
                    )
                time.sleep(random.random() * 0.2)
        
        t = threading.Thread(target=resource_worker, daemon=True)
        t.start()
        return [t]

    def scenario_4_rapid_create_destroy(self):
        """场景4：快速创建销毁"""
        print("\n⚡ 场景4：快速创建销毁")
        
        def rapid_worker():
            while not self.stop_flag:
                try:
                    # 快速任务 - 几乎立即完成
                    self.executor.submit_task(
                        'instant_task',
                        f"Rapid-{uuid.uuid4().hex[:8]}",
                        {},
                        priority=TaskPriority.HIGH
                    )
                    with self.lock:
                        self.tasks_submitted += 1
                except Exception as e:
                    self.bug_collector.record_bug(
                        'rapid_failure',
                        f"快速创建销毁出错: {e}",
                        e,
                        severity='high'
                    )
                time.sleep(random.random() * 0.001)
        
        threads = []
        for i in range(16):
            t = threading.Thread(target=rapid_worker, daemon=True, name=f"Rapid-{i}")
            t.start()
            threads.append(t)
        return threads

    def scenario_5_unpredictable_load(self):
        """场景5：不可预测负载"""
        print("\n🌊 场景5：不可预测负载")
        
        def unpredictable_worker():
            while not self.stop_flag:
                try:
                    # 随机爆发
                    if random.random() < 0.3:
                        for _ in range(random.randint(50, 200)):
                            self.executor.submit_task(
                                'bursty_task',
                                f"Burst-{uuid.uuid4().hex[:8]}",
                                {},
                                priority=random.choice(list(TaskPriority))
                            )
                            with self.lock:
                                self.tasks_submitted += 1
                    else:
                        # 正常速度
                        self.executor.submit_task(
                            'normal_task',
                            f"Normal-{uuid.uuid4().hex[:8]}",
                            {'data': random.random()},
                            priority=TaskPriority.NORMAL
                        )
                        with self.lock:
                            self.tasks_submitted += 1
                except Exception as e:
                    self.bug_collector.record_bug(
                        'unpredictable_load_failure',
                        f"不可预测负载出错: {e}",
                        e,
                        severity='medium'
                    )
                time.sleep(random.random() * 0.5)
        
        t = threading.Thread(target=unpredictable_worker, daemon=True)
        t.start()
        return [t]

    def stop_all(self):
        self.stop_flag = True


class HellTestMonitor:
    """地狱模式监控器"""
    def __init__(self, bug_collector: HellBugCollector):
        self.bug_collector = bug_collector
        self.stop_flag = False
        self.peak_memory = 0
        self.peak_threads = 0

    def monitor_loop(self):
        if HAS_PSUTIL:
            process = psutil.Process()
        while not self.stop_flag:
            try:
                if HAS_PSUTIL:
                    memory_mb = process.memory_info().rss / (1024 * 1024)
                    num_threads = process.num_threads()
                    self.peak_memory = max(self.peak_memory, memory_mb)
                    self.peak_threads = max(self.peak_threads, num_threads)
                    
                    # 检查内存泄漏警告
                    if memory_mb > 2000:  # 2GB
                        self.bug_collector.record_bug(
                            'high_memory_usage',
                            f"内存使用过高: {memory_mb:.0f}MB",
                            severity='warning'
                        )
                    
                time.sleep(0.5)
            except Exception as e:
                print(f"监控错误: {e}")

    def start(self):
        t = threading.Thread(target=self.monitor_loop, daemon=True)
        t.start()
        return t

    def stop(self):
        self.stop_flag = True


def register_hell_handlers(executor: DistributedTaskExecutor, 
                          bug_collector: HellBugCollector):
    """注册地狱模式任务处理器"""
    
    def compute_light(params):
        result = sum(i for i in range(100))
        time.sleep(0.001)
        return result

    def compute_heavy(params):
        result = sum(i * i for i in range(random.randint(1000, 5000)))
        time.sleep(0.01)
        return result

    def sleep_short(params):
        time.sleep(random.random() * 0.05)
        return 'slept'

    def knowledge_query(params):
        return {'result': 'knowledge'}

    def guaranteed_failure(params):
        fail_prob = params.get('failure_prob', 0.5)
        if random.random() < fail_prob:
            raise RuntimeError(f"故意失败！概率: {fail_prob}")
        return 'success'

    def memory_hungry(params):
        alloc_size = params.get('alloc_size', 1000)
        temp_data = [random.random() for _ in range(alloc_size)]
        result = sum(temp_data)
        del temp_data
        return result

    def instant_task(params):
        return 'done'

    def bursty_task(params):
        time.sleep(0.002)
        return 'burst'

    def normal_task(params):
        time.sleep(random.random() * 0.02)
        return 'normal'

    executor.register_handler('compute_light', compute_light)
    executor.register_handler('compute_heavy', compute_heavy)
    executor.register_handler('sleep_short', sleep_short)
    executor.register_handler('knowledge_query', knowledge_query)
    executor.register_handler('guaranteed_failure', guaranteed_failure)
    executor.register_handler('memory_hungry', memory_hungry)
    executor.register_handler('instant_task', instant_task)
    executor.register_handler('bursty_task', bursty_task)
    executor.register_handler('normal_task', normal_task)


def run_hell_test():
    """运行地狱模式测试"""
    print("\n" + "="*80)
    print("持续进化 Agent - 地狱模式压力测试")
    print("="*80)
    
    bug_collector = HellBugCollector()
    monitor = SystemMonitor()
    monitor.start()
    
    print("\n🚀 初始化分布式任务执行器...")
    executor = DistributedTaskExecutor(max_workers=128)
    register_hell_handlers(executor, bug_collector)
    executor.start(num_workers=32)
    
    print("🧪 初始化场景生成器...")
    scenario_generator = HellScenarioGenerator(executor, bug_collector, monitor)
    
    print("📊 初始化系统监控...")
    hell_monitor = HellTestMonitor(bug_collector)
    monitor_thread = hell_monitor.start()
    
    # 启动所有场景
    all_threads = []
    all_threads.extend(scenario_generator.scenario_1_high_concurrency_flood())
    all_threads.extend(scenario_generator.scenario_2_failure_injection())
    all_threads.extend(scenario_generator.scenario_3_resource_stress())
    all_threads.extend(scenario_generator.scenario_4_rapid_create_destroy())
    all_threads.extend(scenario_generator.scenario_5_unpredictable_load())
    
    # 运行时间
    duration_minutes = 2
    end_time = time.time() + duration_minutes * 60
    print(f"\n⏱️  地狱模式运行中... ({duration_minutes} 分钟)")
    print("="*80)
    
    # 定期报告
    last_report = time.time()
    while time.time() < end_time:
        try:
            if time.time() - last_report > 10:
                last_report = time.time()
                stats = executor.get_statistics()
                queue_stats = stats.get('queue_stats', {})
                
                print(f"\n📊 【状态报告】 任务提交: {scenario_generator.tasks_submitted} | "
                      f"待处理: {queue_stats.get('pending', {}).get('total', 0)} | "
                      f"完成: {queue_stats.get('completed', 0)} | "
                      f"失败: {queue_stats.get('failed', 0)} | "
                      f"BUG: {bug_collector.bug_count} | "
                      f"内存: {hell_monitor.peak_memory:.0f}MB")
                
        except Exception as e:
            bug_collector.record_bug('report_error', f"报告错误: {e}", e, severity='low')
        
        time.sleep(1)
    
    # 停止所有场景
    print("\n" + "="*80)
    print("🛑 停止所有场景...")
    scenario_generator.stop_all()
    hell_monitor.stop()
    
    # 等待队列清空
    print("⏳ 等待队列清空...")
    empty_count = 0
    while empty_count < 30:
        try:
            stats = executor.get_statistics()
            queue_stats = stats.get('queue_stats', {})
            pending = queue_stats.get('pending', {}).get('total', 0)
            running = queue_stats.get('running', 0)
            
            if pending == 0 and running == 0:
                empty_count += 1
            else:
                empty_count = 0
                print(f"   待处理: {pending}, 运行中: {running}")
            
        except Exception as e:
            bug_collector.record_bug('wait_error', f"等待错误: {e}", e, severity='low')
        
        time.sleep(0.5)
    
    # 停止系统
    print("\n🧹 停止系统...")
    executor.stop()
    monitor.stop()
    
    # 最终报告
    print("\n" + "="*80)
    print("🔥 地狱模式 - 最终报告")
    print("="*80)
    
    stats = executor.get_statistics()
    queue_stats = stats.get('queue_stats', {})
    bug_summary = bug_collector.get_summary()
    
    print(f"\n📋 执行统计:")
    print(f"  总任务提交: {scenario_generator.tasks_submitted}")
    print(f"  完成任务: {queue_stats.get('completed', 0)}")
    print(f"  失败任务: {queue_stats.get('failed', 0)}")
    print(f"  成功率: {((queue_stats.get('completed', 0) / scenario_generator.tasks_submitted * 100) if scenario_generator.tasks_submitted > 0 else 0):.1f}%")
    
    print(f"\n💻 资源使用:")
    if HAS_PSUTIL:
        print(f"  峰值内存: {hell_monitor.peak_memory:.0f}MB")
        print(f"  峰值线程: {hell_monitor.peak_threads}")
    else:
        print(f"  (psutil not available - no resource metrics)")
    
    print(f"\n🐛 BUG 报告:")
    print(f"  总 BUG: {bug_summary['total_bugs']}")
    print(f"  按严重程度: {bug_summary['by_severity']}")
    print(f"  按类别: {bug_summary['categories']}")
    
    # 保存报告
    bug_collector.save_report()
    
    print("\n" + "="*80)
    
    return bug_summary


if __name__ == "__main__":
    try:
        results = run_hell_test()
        sys.exit(0 if results['total_bugs'] == 0 else 1)
    except KeyboardInterrupt:
        print("\n⏸️  测试被用户中断")
        sys.exit(130)
