#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
第9轮迭代测试：超长时间大规模混乱场景压力测试
测试目标：发现系统在真实运行时的潜在bug

已修复问题：
1. DistributedTask使用parameters而不是data
2. Handler签名改为接受parameters字典
"""

import sys
import os
import time
import threading
import random
import traceback
from datetime import datetime
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from execution.distributed_task_queue import (
    DistributedTaskExecutor,
    DistributedTask,
    TaskPriority
)
from core.data_models_universal import TaskStatus

class Round9ChaosTest:
    def __init__(self):
        self.error_count = 0
        self.warning_count = 0
        self.test_results = {}
        self.start_time = time.time()
        self.lock = threading.Lock()
        self.logger = self._setup_logger()

    def _setup_logger(self):
        import logging
        logging.basicConfig(
            level=logging.INFO,
            format='[%(asctime)s] [%(levelname)s] [%(threadName)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        return logging.getLogger('Round9')

    def log(self, level, msg):
        if level == 'info':
            self.logger.info(msg)
        elif level == 'error':
            self.logger.error(msg)
        elif level == 'warning':
            self.logger.warning(msg)
        elif level == 'critical':
            self.logger.critical(msg)

    def test_scenario_1_burst_flood(self):
        """场景1：突发洪泛测试 - 短时间内大量任务涌入"""
        self.log('info', '\n' + '='*80)
        self.log('info', '场景1：突发洪泛测试')
        self.log('info', '='*80)

        executor = DistributedTaskExecutor(max_workers=256)
        results = {'success': 0, 'fail': 0, 'error': []}
        results_lock = threading.Lock()

        def burst_handler(parameters):
            try:
                if random.random() < 0.1:
                    raise ValueError("Simulated burst error")
                return f"Burst-{parameters.get('batch')}-{parameters.get('index')}"
            except Exception as e:
                with results_lock:
                    results['error'].append(str(e))
                raise

        executor.register_handler('burst', burst_handler)
        executor.start()

        self.log('info', '开始突发洪泛测试：10000个任务分批涌入')

        batch_count = 10
        batch_size = 1000

        for batch in range(batch_count):
            for i in range(batch_size):
                task = DistributedTask(
                    task_id=f'burst-{batch}-{i}',
                    task_type='burst',
                    parameters={'batch': batch, 'index': i, 'timestamp': time.time()}
                )
                try:
                    executor.queue.enqueue(task)
                    with results_lock:
                        results['success'] += 1
                except Exception as e:
                    with results_lock:
                        results['fail'] += 1
                        results['error'].append(f"Submit-{str(e)}")

            self.log('info', f'  已提交第{batch+1}批任务')

        self.log('info', '等待所有任务完成...')
        time.sleep(8)

        stats = executor.get_statistics()
        executor.stop()

        self.log('info', f'  突发洪泛测试结果:')
        self.log('info', f'    成功提交: {results["success"]}')
        self.log('info', f'    提交失败: {results["fail"]}')
        self.log('info', f'    总完成任务: {stats.get("total_completed", 0)}')
        self.log('info', f'    总失败任务: {stats.get("total_failed", 0)}')
        self.log('info', f'    错误数: {len(results["error"])}')

        if stats.get("total_completed", 0) < 5000:
            self.error_count += 1
            self.log('error', f'  ❌ 突发洪泛测试失败：完成任务数过少')
            return False

        self.log('info', '  ✅ 突发洪泛测试通过')
        return True

    def test_scenario_2_priority_mixing(self):
        """场景2：优先级混乱测试 - 大量不同优先级任务混合"""
        self.log('info', '\n' + '='*80)
        self.log('info', '场景2：优先级混乱测试')
        self.log('info', '='*80)

        executor = DistributedTaskExecutor(max_workers=128)
        priority_results = {priority: {'success': 0, 'completed': 0, 'failed': 0}
                          for priority in ['CRITICAL', 'HIGH', 'NORMAL', 'LOW']}
        results_lock = threading.Lock()

        def priority_handler(parameters):
            try:
                priority = parameters.get('priority', 'NORMAL')
                execution_time = random.uniform(0.001, 0.01)
                time.sleep(execution_time)

                if random.random() < 0.05:
                    raise ValueError(f"Priority-{priority}-Error")

                with results_lock:
                    priority_results[priority]['completed'] += 1
                return f"Priority-{priority}-OK"
            except Exception as e:
                with results_lock:
                    priority_results[priority]['failed'] += 1
                raise

        executor.register_handler('priority', priority_handler)
        executor.start()

        self.log('info', '提交混合优先级任务（每种优先级2000个）')

        tasks_per_priority = 2000
        for priority_name in ['CRITICAL', 'HIGH', 'NORMAL', 'LOW']:
            priority = TaskPriority[priority_name]
            for i in range(tasks_per_priority):
                task = DistributedTask(
                    task_id=f'{priority_name}-{i}',
                    task_type='priority',
                    priority=priority,
                    parameters={'priority': priority_name, 'index': i}
                )
                try:
                    executor.queue.enqueue(task)
                    with results_lock:
                        priority_results[priority_name]['success'] += 1
                except Exception as e:
                    self.log('error', f'  提交失败: {str(e)}')

        self.log('info', '等待任务完成...')
        time.sleep(12)

        stats = executor.get_statistics()
        executor.stop()

        self.log('info', f'  优先级测试结果:')
        all_passed = True
        for priority_name in ['CRITICAL', 'HIGH', 'NORMAL', 'LOW']:
            stats_p = priority_results[priority_name]
            total = stats_p['completed'] + stats_p['failed']
            success_rate = stats_p['completed'] / total if total > 0 else 0
            self.log('info', f'    {priority_name}: 提交{stats_p["success"]}, '
                           f'完成{stats_p["completed"]}, 失败{stats_p["failed"]}, '
                           f'成功率:{success_rate:.2%}')

            if success_rate < 0.8:
                all_passed = False
                self.error_count += 1

        if all_passed:
            self.log('info', '  ✅ 优先级混乱测试通过')
            return True
        else:
            self.log('error', '  ❌ 优先级混乱测试失败：某些优先级成功率过低')
            return False

    def test_scenario_3_retry_storm(self):
        """场景3：重试风暴测试 - 大量任务同时重试"""
        self.log('info', '\n' + '='*80)
        self.log('info', '场景3：重试风暴测试')
        self.log('info', '='*80)

        executor = DistributedTaskExecutor(max_workers=64)
        retry_stats = {'attempts': 0, 'success': 0, 'exhausted': 0}
        retry_lock = threading.Lock()

        def retry_handler(parameters):
            with retry_lock:
                retry_stats['attempts'] += 1

            attempt = parameters.get('attempt', 0)
            if attempt < 3:
                if random.random() < 0.7:
                    raise ValueError(f"Retry attempt {attempt} failed")

            with retry_lock:
                retry_stats['success'] += 1
            return f"Retry-{attempt}-OK"

        executor.register_handler('retry', retry_handler)
        executor.start()

        self.log('info', '提交500个需要重试的任务')

        retry_tasks = []
        for i in range(500):
            task = DistributedTask(
                task_id=f'retry-{i}',
                task_type='retry',
                max_retries=3,
                parameters={'attempt': 0, 'index': i}
            )
            retry_tasks.append(task)
            executor.queue.enqueue(task)

        self.log('info', '等待重试完成...')
        time.sleep(15)

        stats = executor.get_statistics()
        executor.stop()

        self.log('info', f'  重试风暴测试结果:')
        self.log('info', f'    总执行次数: {retry_stats["attempts"]}')
        self.log('info', f'    最终成功: {retry_stats["success"]}')
        self.log('info', f'    总完成任务: {stats.get("total_completed", 0)}')
        self.log('info', f'    总失败任务: {stats.get("total_failed", 0)}')

        if retry_stats["attempts"] < 500:
            self.warning_count += 1
            self.log('warning', f'  ⚠️  重试风暴测试：执行次数少于预期')

        if retry_stats["success"] < 400:
            self.error_count += 1
            self.log('error', '  ❌ 重试风暴测试失败：成功率过低')
            return False

        self.log('info', '  ✅ 重试风暴测试通过')
        return True

    def test_scenario_4_rapid_start_stop(self):
        """场景4：快速启停测试 - 反复快速启停executor"""
        self.log('info', '\n' + '='*80)
        self.log('info', '场景4：快速启停测试')
        self.log('info', '='*80)

        cycle_count = 20
        cycle_results = []

        def quick_handler(parameters):
            time.sleep(0.001)
            return f"Quick-{parameters.get('index')}"

        for cycle in range(cycle_count):
            executor = DistributedTaskExecutor(max_workers=32)
            executor.register_handler('quick', quick_handler)

            try:
                executor.start()
                for i in range(100):
                    task = DistributedTask(
                        task_id=f'cycle-{cycle}-{i}',
                        task_type='quick',
                        parameters={'cycle': cycle, 'index': i}
                    )
                    executor.queue.enqueue(task)

                time.sleep(1.5)
                executor.stop()

                stats = executor.get_statistics()
                cycle_results.append({
                    'cycle': cycle,
                    'completed': stats.get('total_completed', 0),
                    'failed': stats.get('total_failed', 0)
                })

                self.log('info', f'  循环{cycle+1}/{cycle_count}: '
                               f'完成{stats.get("total_completed", 0)}, '
                               f'失败{stats.get("total_failed", 0)}')

            except Exception as e:
                self.error_count += 1
                self.log('error', f'  ❌ 循环{cycle+1}失败: {str(e)}')
                cycle_results.append({
                    'cycle': cycle,
                    'error': str(e)
                })

        failed_cycles = [r for r in cycle_results if 'error' in r]
        if failed_cycles:
            self.log('error', f'  ❌ 快速启停测试失败：{len(failed_cycles)}个循环出错')
            return False

        self.log('info', '  ✅ 快速启停测试通过')
        return True

    def test_scenario_5_queue_boundary(self):
        """场景5：队列边界测试 - 测试队列临界状态"""
        self.log('info', '\n' + '='*80)
        self.log('info', '场景5：队列边界测试')
        self.log('info', '='*80)

        executor = DistributedTaskExecutor(max_workers=32)
        boundary_results = {'high': 0, 'low': 0}
        boundary_lock = threading.Lock()

        def boundary_handler(parameters):
            with boundary_lock:
                boundary_results[parameters['level']] += 1
            time.sleep(0.01)
            return f"Boundary-{parameters.get('index')}"

        executor.register_handler('boundary', boundary_handler)
        executor.start()

        self.log('info', '测试队列接近满的状态')
        high_tasks = 5000
        for i in range(high_tasks):
            task = DistributedTask(
                task_id=f'high-{i}',
                task_type='boundary',
                parameters={'level': 'high', 'index': i}
            )
            executor.queue.enqueue(task)

        self.log('info', '等待队列消耗到低水位')
        time.sleep(8)

        self.log('info', '测试队列接近空的状态')
        low_tasks = 10
        for i in range(low_tasks):
            task = DistributedTask(
                task_id=f'low-{i}',
                task_type='boundary',
                parameters={'level': 'low', 'index': i}
            )
            executor.queue.enqueue(task)

        time.sleep(2)

        stats = executor.get_statistics()
        executor.stop()

        self.log('info', f'  队列边界测试结果:')
        self.log('info', f'    高水位任务处理: {boundary_results["high"]}/{high_tasks}')
        self.log('info', f'    低水位任务处理: {boundary_results["low"]}/{low_tasks}')
        self.log('info', f'    总完成: {stats.get("total_completed", 0)}')

        if boundary_results["high"] < high_tasks * 0.8:
            self.error_count += 1
            self.log('error', '  ❌ 队列边界测试失败：高水位处理率过低')
            return False

        self.log('info', '  ✅ 队列边界测试通过')
        return True

    def test_scenario_6_memory_stress(self):
        """场景6：内存压力测试 - 大量任务快速创建销毁"""
        self.log('info', '\n' + '='*80)
        self.log('info', '场景6：内存压力测试')
        self.log('info', '='*80)

        executor = DistributedTaskExecutor(max_workers=128)
        memory_tasks = []
        completion_count = [0]
        completion_lock = threading.Lock()

        def memory_handler(parameters):
            with completion_lock:
                completion_count[0] += 1
            return f"Memory-{parameters.get('index')}"

        executor.register_handler('memory', memory_handler)
        executor.start()

        self.log('info', '创建30000个任务进行内存压力测试')

        for i in range(30000):
            task = DistributedTask(
                task_id=f'memory-{i}',
                task_type='memory',
                parameters={'index': i}
            )
            memory_tasks.append(task)
            executor.queue.enqueue(task)

            if i % 5000 == 0 and i > 0:
                self.log('info', f'  已提交 {i} 个任务')
                time.sleep(0.5)

        self.log('info', '等待任务完成...')
        time.sleep(10)

        stats = executor.get_statistics()
        executor.stop()

        del memory_tasks

        self.log('info', f'  内存压力测试结果:')
        self.log('info', f'    提交任务: 30000')
        self.log('info', f'    完成任务: {completion_count[0]}')
        self.log('info', f'    总完成统计: {stats.get("total_completed", 0)}')

        if completion_count[0] < 28000:
            self.error_count += 1
            self.log('error', '  ❌ 内存压力测试失败：完成率过低')
            return False

        self.log('info', '  ✅ 内存压力测试通过')
        return True

    def test_scenario_7_concurrent_handlers(self):
        """场景7：多处理器并发测试 - 大量不同handler同时工作"""
        self.log('info', '\n' + '='*80)
        self.log('info', '场景7：多处理器并发测试')
        self.log('info', '='*80)

        executor = DistributedTaskExecutor(max_workers=256)
        handler_stats = {f'handler_{i}': {'count': 0} for i in range(20)}
        stats_lock = threading.Lock()

        for i in range(20):
            handler_name = f'handler_{i}'

            def make_handler(name):
                def handler(parameters):
                    with stats_lock:
                        handler_stats[name]['count'] += 1
                    time.sleep(0.001)
                    return f"{name}-{parameters.get('index')}"
                return handler

            executor.register_handler(handler_name, make_handler(handler_name))

        executor.start()

        self.log('info', '提交10000个任务到20个不同的handler')

        for i in range(10000):
            handler_idx = i % 20
            task = DistributedTask(
                task_id=f'concurrent-{i}',
                task_type=f'handler_{handler_idx}',
                parameters={'index': i}
            )
            executor.queue.enqueue(task)

        self.log('info', '等待并发处理完成...')
        time.sleep(10)

        stats = executor.get_statistics()
        executor.stop()

        self.log('info', f'  多处理器并发测试结果:')
        total_handled = sum(h['count'] for h in handler_stats.values())
        self.log('info', f'    总处理: {total_handled}')
        self.log('info', f'    总完成统计: {stats.get("total_completed", 0)}')

        active_handlers = sum(1 for h in handler_stats.values() if h['count'] > 0)
        self.log('info', f'    活跃handler数: {active_handlers}/20')

        if active_handlers < 15:
            self.warning_count += 1
            self.log('warning', f'  ⚠️  多处理器并发测试：部分handler未处理任务')

        if total_handled < 9000:
            self.error_count += 1
            self.log('error', '  ❌ 多处理器并发测试失败：总处理量过低')
            return False

        self.log('info', '  ✅ 多处理器并发测试通过')
        return True

    def test_scenario_8_exception_propagation(self):
        """场景8：异常传播测试 - 异常在handler中的传播"""
        self.log('info', '\n' + '='*80)
        self.log('info', '场景8：异常传播测试')
        self.log('info', '='*80)

        executor = DistributedTaskExecutor(max_workers=32)
        exception_results = {'caught': 0, 'reraised': 0}
        exception_lock = threading.Lock()

        def exception_handler(parameters):
            exception_type = parameters.get('type', 'normal')

            try:
                if exception_type == 'value':
                    raise ValueError("ValueError in handler")
                elif exception_type == 'key':
                    raise KeyError("KeyError in handler")
                elif exception_type == 'type':
                    raise TypeError("TypeError in handler")
                elif exception_type == 'runtime':
                    raise RuntimeError("RuntimeError in handler")
                elif exception_type == 'multi':
                    if random.random() < 0.5:
                        raise ValueError("Multi-error 1")
                    else:
                        raise RuntimeError("Multi-error 2")
                else:
                    return f"Normal-{parameters.get('index')}"

            except ValueError:
                with exception_lock:
                    exception_results['caught'] += 1
                raise
            except Exception:
                with exception_lock:
                    exception_results['reraised'] += 1
                raise

        executor.register_handler('exception', exception_handler)
        executor.start()

        self.log('info', '提交各种异常类型的任务')

        exception_types = ['value', 'key', 'type', 'runtime', 'multi', 'normal']
        tasks_per_type = 100

        for exc_type in exception_types:
            for i in range(tasks_per_type):
                task = DistributedTask(
                    task_id=f'exc-{exc_type}-{i}',
                    task_type='exception',
                    max_retries=0,
                    parameters={'type': exc_type, 'index': i}
                )
                executor.queue.enqueue(task)

        self.log('info', '等待异常处理...')
        time.sleep(8)

        stats = executor.get_statistics()
        executor.stop()

        self.log('info', f'  异常传播测试结果:')
        self.log('info', f'    捕获异常: {exception_results["caught"]}')
        self.log('info', f'    重抛异常: {exception_results["reraised"]}')
        self.log('info', f'    总失败: {stats.get("total_failed", 0)}')

        total_exceptions = exception_results['caught'] + exception_results['reraised']
        expected_failures = tasks_per_type * (len(exception_types) - 1)

        if total_exceptions < expected_failures * 0.8:
            self.warning_count += 1
            self.log('warning', f'  ⚠️  异常传播测试：异常捕获率低于预期')

        self.log('info', '  ✅ 异常传播测试完成')
        return True

    def test_scenario_9_lock_contention(self):
        """场景9：锁竞争测试 - 高并发下的锁竞争"""
        self.log('info', '\n' + '='*80)
        self.log('info', '场景9：锁竞争测试')
        self.log('info', '='*80)

        executor = DistributedTaskExecutor(max_workers=512)
        contention_stats = {'acquired': 0, 'contended': 0, 'timeout': 0}
        contention_lock = threading.Lock()

        def contention_handler(parameters):
            acquired = contention_lock.acquire(blocking=True, timeout=0.1)
            if acquired:
                try:
                    contention_stats['acquired'] += 1
                    time.sleep(0.001)
                finally:
                    contention_lock.release()
            else:
                contention_stats['timeout'] += 1
            return f"Contention-{parameters.get('index')}"

        executor.register_handler('contention', contention_handler)
        executor.start()

        self.log('info', '提交10000个任务进行锁竞争测试')

        for i in range(10000):
            task = DistributedTask(
                task_id=f'contention-{i}',
                task_type='contention',
                parameters={'index': i}
            )
            executor.queue.enqueue(task)

        self.log('info', '等待锁竞争测试完成...')
        time.sleep(8)

        stats = executor.get_statistics()
        executor.stop()

        self.log('info', f'  锁竞争测试结果:')
        self.log('info', f'    成功获取锁: {contention_stats["acquired"]}')
        self.log('info', f'    锁超时: {contention_stats["timeout"]}')
        self.log('info', f'    总完成: {stats.get("total_completed", 0)}')

        if contention_stats['timeout'] > contention_stats['acquired']:
            self.warning_count += 1
            self.log('warning', '  ⚠️  锁竞争测试：超时次数过多')

        self.log('info', '  ✅ 锁竞争测试通过')
        return True

    def test_scenario_10_chaos_monkey(self):
        """场景10：混沌猴子测试 - 随机故障注入"""
        self.log('info', '\n' + '='*80)
        self.log('info', '场景10：混沌猴子测试')
        self.log('info', '='*80)

        executor = DistributedTaskExecutor(max_workers=128)
        chaos_results = {'success': 0, 'fail': 0, 'retry': 0, 'timeout': 0}
        chaos_lock = threading.Lock()

        def chaos_handler(parameters):
            chaos_type = random.choice(['success', 'fail', 'retry', 'timeout', 'error'])

            if chaos_type == 'success':
                with chaos_lock:
                    chaos_results['success'] += 1
                return f"Chaos-Success-{parameters.get('index')}"

            elif chaos_type == 'fail':
                with chaos_lock:
                    chaos_results['fail'] += 1
                raise ValueError("Chaos failure")

            elif chaos_type == 'retry':
                with chaos_lock:
                    chaos_results['retry'] += 1
                raise ConnectionError("Chaos retry")

            elif chaos_type == 'timeout':
                with chaos_lock:
                    chaos_results['timeout'] += 1
                time.sleep(0.5)
                return f"Chaos-Timeout-{parameters.get('index')}"

            else:
                with chaos_lock:
                    chaos_results['error'] += 1
                raise RuntimeError("Chaos error")

        executor.register_handler('chaos', chaos_handler)
        executor.start()

        self.log('info', '混沌猴子测试：5000个随机故障任务')

        for i in range(5000):
            task = DistributedTask(
                task_id=f'chaos-{i}',
                task_type='chaos',
                max_retries=2,
                parameters={'index': i}
            )
            executor.queue.enqueue(task)

        self.log('info', '等待混沌测试完成...')
        time.sleep(20)

        stats = executor.get_statistics()
        executor.stop()

        self.log('info', f'  混沌猴子测试结果:')
        self.log('info', f'    成功任务: {chaos_results["success"]}')
        self.log('info', f'    失败任务: {chaos_results["fail"]}')
        self.log('info', f'    重试任务: {chaos_results["retry"]}')
        self.log('info', f'    超时任务: {chaos_results["timeout"]}')
        self.log('info', f'    总完成: {stats.get("total_completed", 0)}')
        self.log('info', f'    总失败: {stats.get("total_failed", 0)}')

        total_processed = sum(chaos_results.values())
        if stats.get('total_completed', 0) + stats.get('total_failed', 0) < 4000:
            self.error_count += 1
            self.log('error', '  ❌ 混沌猴子测试失败：任务处理率过低')
            return False

        self.log('info', '  ✅ 混沌猴子测试通过')
        return True

    def run_all_tests(self):
        """运行所有混沌测试场景"""
        self.log('info', '\n' + '='*80)
        self.log('info', '第9轮迭代 - 超长时间大规模混乱场景压力测试')
        self.log('info', '='*80)
        self.log('info', '测试开始时间: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        self.log('info', '='*80)

        test_start = time.time()

        scenarios = [
            ('突发洪泛测试', self.test_scenario_1_burst_flood),
            ('优先级混乱测试', self.test_scenario_2_priority_mixing),
            ('重试风暴测试', self.test_scenario_3_retry_storm),
            ('快速启停测试', self.test_scenario_4_rapid_start_stop),
            ('队列边界测试', self.test_scenario_5_queue_boundary),
            ('内存压力测试', self.test_scenario_6_memory_stress),
            ('多处理器并发测试', self.test_scenario_7_concurrent_handlers),
            ('异常传播测试', self.test_scenario_8_exception_propagation),
            ('锁竞争测试', self.test_scenario_9_lock_contention),
            ('混沌猴子测试', self.test_scenario_10_chaos_monkey),
        ]

        results = {}
        for name, test_func in scenarios:
            try:
                result = test_func()
                results[name] = result
                self.test_results[name] = result
            except Exception as e:
                self.log('error', f'{name} 执行异常: {str(e)}')
                self.log('error', traceback.format_exc())
                results[name] = False
                self.test_results[name] = False
                self.error_count += 1

        test_duration = time.time() - test_start

        self.log('info', '\n' + '='*80)
        self.log('info', '第9轮测试结果汇总')
        self.log('info', '='*80)

        passed = sum(1 for r in results.values() if r)
        failed = len(results) - passed

        for name, result in results.items():
            status = '✅ PASS' if result else '❌ FAIL'
            self.log('info', f'{name}: {status}')

        self.log('info', '\n' + '='*80)
        self.log('info', '测试统计')
        self.log('info', '='*80)
        self.log('info', f'总测试数: {len(results)}')
        self.log('info', f'通过数: {passed}')
        self.log('info', f'失败数: {failed}')
        self.log('info', f'错误数: {self.error_count}')
        self.log('info', f'警告数: {self.warning_count}')
        self.log('info', f'总耗时: {test_duration:.2f}秒')

        self.log('info', '\n测试结束时间: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        if failed == 0 and self.error_count == 0:
            self.log('info', '\n🎉🎉🎉 第9轮混沌测试：完美通过！')
            return True
        else:
            self.log('error', f'\n❌❌❌ 第9轮混沌测试：{failed}个场景失败，{self.error_count}个错误')
            return False

if __name__ == '__main__':
    test = Round9ChaosTest()
    success = test.run_all_tests()
    sys.exit(0 if success else 1)
