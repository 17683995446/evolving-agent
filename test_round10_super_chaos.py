#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
第10轮迭代测试：超级混乱场景压力测试
测试目标：发现系统在极端混乱、长时间运行的真实运行时bug
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


class Round10SuperChaosTest:
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
        return logging.getLogger('Round10')

    def log(self, level, msg):
        if level == 'info':
            self.logger.info(msg)
        elif level == 'error':
            self.logger.error(msg)
        elif level == 'warning':
            self.logger.warning(msg)
        elif level == 'critical':
            self.logger.critical(msg)

    def test_scenario_1_long_running_sustained(self):
        """场景1：超长持续运行 - 100,000任务处理"""
        self.log('info', '\n' + '=' * 80)
        self.log('info', '场景1：超长持续运行 - 100,000任务')
        self.log('info', '=' * 80)

        executor = DistributedTaskExecutor(max_workers=256)
        completed_count = [0]
        failed_count = [0]
        results_lock = threading.Lock()

        def sustained_handler(parameters):
            try:
                time.sleep(random.uniform(0.0001, 0.005))  # 随机延迟
                if random.random() < 0.02:  # 2%失败率
                    raise ValueError("Random failure in sustained test")
                with results_lock:
                    completed_count[0] += 1
                return f"success"
            except Exception as e:
                with results_lock:
                    failed_count[0] += 1
                raise

        executor.register_handler('sustained', sustained_handler)
        executor.start()

        self.log('info', '开始入队100,000任务')
        for i in range(100000):
            task = DistributedTask(
                task_id=f"sustained-{i}",
                task_type='sustained',
                priority=random.choice([TaskPriority.CRITICAL, TaskPriority.HIGH, TaskPriority.NORMAL, TaskPriority.LOW]),
                parameters={'index': i}
            )
            executor.queue.enqueue(task)
            if i % 10000 == 0 and i > 0:
                self.log('info', f'已提交 {i} 任务')

        self.log('info', '等待所有任务完成...')
        wait_start = time.time()
        while True:
            time.sleep(2)
            total = completed_count[0] + failed_count[0]
            stats = executor.get_statistics()
            pending = stats['pending']['total']
            running = stats['running']
            
            self.log('info', f'进度: 完成={completed_count[0]} 失败={failed_count[0]} 待处理={pending} 运行中={running}')
            
            if pending == 0 and running == 0:
                break
            if time.time() - wait_start > 300:  # 5分钟超时
                self.log('warning', '5分钟超时，停止测试')
                break

        total_time = time.time() - self.start_time
        executor.stop()

        total_processed = completed_count[0] + failed_count[0]
        self.log('info', f'超长持续运行结果:')
        self.log('info', f'总提交: 100,000')
        self.log('info', f'总处理: {total_processed}')
        self.log('info', f'成功: {completed_count[0]}')
        self.log('info', f'失败: {failed_count[0]}')
        self.log('info', f'统计完成: {executor.get_statistics()["completed"]}')
        self.log('info', f'统计失败: {executor.get_statistics()["failed"]}')

        success = completed_count[0] > 90000
        if not success:
            self.error_count += 1
            self.log('error', '❌ 超长持续运行测试失败')
        else:
            self.log('info', '✅ 超长持续运行测试通过')
        return success

    def test_scenario_2_concurrent_handler_storm(self):
        """场景2：并发处理器风暴 - 100个不同handler同时工作"""
        self.log('info', '\n' + '=' * 80)
        self.log('info', '场景2：并发处理器风暴 - 100个handler')
        self.log('info', '=' * 80)

        executor = DistributedTaskExecutor(max_workers=512)
        handler_counts = {}
        results_lock = threading.Lock()

        for i in range(100):
            handler_name = f"handler_{i:03d}"
            
            def make_handler(name):
                def handler(parameters):
                    with results_lock:
                        if name not in handler_counts:
                            handler_counts[name] = 0
                        handler_counts[name] += 1
                    time.sleep(random.uniform(0.0001, 0.001))
                    if random.random() < 0.05:
                        raise ValueError(f"Random failure in {name}")
                    return f"{name}-done"
                return handler

            executor.register_handler(handler_name, make_handler(handler_name))

        executor.start()

        self.log('info', '提交50,000任务到100个handler')
        for i in range(50000):
            handler_idx = random.randint(0, 99)
            task = DistributedTask(
                task_id=f"storm-{i}",
                task_type=f"handler_{handler_idx:03d}",
                priority=random.choice([TaskPriority.CRITICAL, TaskPriority.HIGH, TaskPriority.NORMAL, TaskPriority.LOW]),
                parameters={'index': i}
            )
            executor.queue.enqueue(task)
            if i % 5000 == 0 and i > 0:
                self.log('info', f'已提交 {i} 任务')

        self.log('info', '等待并发处理完成...')
        time.sleep(30)

        executor.stop()

        active_handlers = sum(1 for count in handler_counts.values() if count > 0)
        total_processed = sum(handler_counts.values())
        self.log('info', f'并发处理器风暴结果:')
        self.log('info', f'活跃handler数: {active_handlers}/100')
        self.log('info', f'总处理任务数: {total_processed}')

        success = active_handlers >= 90 and total_processed >= 40000
        if not success:
            self.error_count += 1
            self.log('error', '❌ 并发处理器风暴测试失败')
        else:
            self.log('info', '✅ 并发处理器风暴测试通过')
        return success

    def test_scenario_3_priority_flood(self):
        """场景3：优先级洪泛 - 高优先级任务洪水式涌入"""
        self.log('info', '\n' + '=' * 80)
        self.log('info', '场景3：优先级洪泛 - 10,000高优先级任务')
        self.log('info', '=' * 80)

        executor = DistributedTaskExecutor(max_workers=128)
        priority_stats = {
            TaskPriority.CRITICAL: [0, 0],
            TaskPriority.HIGH: [0, 0],
            TaskPriority.NORMAL: [0, 0],
            TaskPriority.LOW: [0, 0]
        }
        stats_lock = threading.Lock()

        def priority_handler(parameters):
            priority = TaskPriority(parameters.get('priority', 2))
            with stats_lock:
                priority_stats[priority][0] += 1
            time.sleep(random.uniform(0.0001, 0.002))
            return "done"

        executor.register_handler('priority', priority_handler)
        executor.start()

        self.log('info', '混合优先级任务洪泛式提交')
        
        def submit_low_tasks():
            for i in range(1000):
                task = DistributedTask(
                    task_id=f"low-{i}",
                    task_type='priority',
                    priority=TaskPriority.LOW,
                    parameters={'index': i, 'priority': 3}
                )
                executor.queue.enqueue(task)
            self.log('info', '低优先级任务提交完成')

        def submit_normal_tasks():
            for i in range(1000):
                task = DistributedTask(
                    task_id=f"normal-{i}",
                    task_type='priority',
                    priority=TaskPriority.NORMAL,
                    parameters={'index': i, 'priority': 2}
                )
                executor.queue.enqueue(task)
            self.log('info', '正常优先级任务提交完成')

        def submit_high_tasks():
            for i in range(1000):
                task = DistributedTask(
                    task_id=f"high-{i}",
                    task_type='priority',
                    priority=TaskPriority.HIGH,
                    parameters={'index': i, 'priority': 1}
                )
                executor.queue.enqueue(task)
            self.log('info', '高优先级任务提交完成')

        def submit_critical_tasks():
            for i in range(10000):
                task = DistributedTask(
                    task_id=f"critical-{i}",
                    task_type='priority',
                    priority=TaskPriority.CRITICAL,
                    parameters={'index': i, 'priority': 0}
                )
                executor.queue.enqueue(task)
                if i % 2000 == 0:
                    self.log('info', f'CRITICAL {i} 任务已提交')
            self.log('info', 'CRITICAL优先级任务提交完成')

        threads = []
        threads.append(threading.Thread(target=submit_low_tasks))
        threads.append(threading.Thread(target=submit_normal_tasks))
        threads.append(threading.Thread(target=submit_high_tasks))
        threads.append(threading.Thread(target=submit_critical_tasks))

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        self.log('info', '等待处理完成...')
        time.sleep(30)

        executor.stop()

        total_processed = sum(s[0] for s in priority_stats.values())
        self.log('info', f'优先级洪泛结果:')
        self.log('info', f'CRITICAL处理: {priority_stats[TaskPriority.CRITICAL][0]}/10,000')
        self.log('info', f'HIGH处理: {priority_stats[TaskPriority.HIGH][0]}/1,000')
        self.log('info', f'NORMAL处理: {priority_stats[TaskPriority.NORMAL][0]}/1,000')
        self.log('info', f'LOW处理: {priority_stats[TaskPriority.LOW][0]}/1,000')
        self.log('info', f'总处理: {total_processed}')

        success = total_processed >= 10000
        if not success:
            self.error_count += 1
            self.log('error', '❌ 优先级洪泛测试失败')
        else:
            self.log('info', '✅ 优先级洪泛测试通过')
        return success

    def test_scenario_4_mixed_failure_retry(self):
        """场景4：混合失败重试 - 各种失败模式"""
        self.log('info', '\n' + '=' * 80)
        self.log('info', '场景4：混合失败重试 - 20,000任务混合失败')
        self.log('info', '=' * 80)

        executor = DistributedTaskExecutor(max_workers=128)
        failure_stats = {
            'total_submitted': 0,
            'processed_attempted': 0,
            'eventually_succeeded': 0,
            'eventually_failed': 0
        }
        stats_lock = threading.Lock()

        def mixed_failure_handler(parameters):
            with stats_lock:
                failure_stats['processed_attempted'] += 1
            fail_type = parameters.get('fail_type', 'none')
            
            if fail_type == 'always_fail':
                raise ValueError("Always fail")
            elif fail_type == 'fail_then_succeed':
                if random.random() < 0.5:
                    raise ValueError("Fail then succeed")
            elif fail_type == 'random':
                if random.random() < 0.3:
                    raise ValueError("Random failure")
            elif fail_type == 'timeout':
                time.sleep(0.1)
                raise TimeoutError("Timeout")

            with stats_lock:
                failure_stats['eventually_succeeded'] += 1
            return "eventually-success"

        executor.register_handler('mixed', mixed_failure_handler)
        executor.start()

        self.log('info', '混合失败重试测试')
        
        fail_types = ['none', 'always_fail', 'fail_then_succeed', 'random', 'timeout']
        for i in range(20000):
            fail_type = random.choice(fail_types)
            task = DistributedTask(
                task_id=f"mixed-{i}",
                task_type='mixed',
                priority=random.choice([TaskPriority.CRITICAL, TaskPriority.HIGH, TaskPriority.NORMAL, TaskPriority.LOW]),
                max_retries=random.randint(0, 5),
                parameters={'index': i, 'fail_type': fail_type}
            )
            executor.queue.enqueue(task)
            failure_stats['total_submitted'] += 1
            if i % 2000 == 0 and i > 0:
                self.log('info', f'已提交 {i} 任务')

        self.log('info', '等待处理完成...')
        time.sleep(45)

        executor.stop()

        self.log('info', f'混合失败重试结果:')
        self.log('info', f'总提交: {failure_stats["total_submitted"]}')
        self.log('info', f'总尝试: {failure_stats["processed_attempted"]}')
        self.log('info', f'最终成功: {failure_stats["eventually_succeeded"]}')

        success = failure_stats['processed_attempted'] > 20000
        if not success:
            self.error_count += 1
            self.log('error', '❌ 混合失败重试测试失败')
        else:
            self.log('info', '✅ 混合失败重试测试通过')
        return success

    def test_scenario_5_hanging_tasks(self):
        """场景5：悬挂任务 - 长时间运行任务"""
        self.log('info', '\n' + '=' * 80)
        self.log('info', '场景5：悬挂任务 - 500个长时间任务')
        self.log('info', '=' * 80)

        executor = DistributedTaskExecutor(max_workers=256)
        hanging_stats = {'started': 0, 'completed': 0, 'running_too_long': 0}
        stats_lock = threading.Lock()

        def hanging_handler(parameters):
            with stats_lock:
                hanging_stats['started'] += 1
            duration = parameters.get('duration', 0.001)
            time.sleep(duration)
            with stats_lock:
                hanging_stats['completed'] += 1
                if duration > 0.05:
                    hanging_stats['running_too_long'] += 1
            return f"hanging-done"

        executor.register_handler('hanging', hanging_handler)
        executor.start()

        self.log('info', '混合悬挂任务')
        for i in range(500):
            duration = random.choice([0.001, 0.01, 0.05, 0.1, 0.2, 0.5])
            task = DistributedTask(
                task_id=f"hanging-{i}",
                task_type='hanging',
                priority=TaskPriority.NORMAL,
                parameters={'index': i, 'duration': duration}
            )
            executor.queue.enqueue(task)

        self.log('info', '等待处理完成...')
        time.sleep(60)

        executor.stop()

        self.log('info', f'悬挂任务结果:')
        self.log('info', f'已启动: {hanging_stats["started"]}')
        self.log('info', f'已完成: {hanging_stats["completed"]}')
        self.log('info', f'长时间任务: {hanging_stats["running_too_long"]}')

        success = hanging_stats['completed'] >= 450
        if not success:
            self.error_count += 1
            self.log('error', '❌ 悬挂任务测试失败')
        else:
            self.log('info', '✅ 悬挂任务测试通过')
        return success

    def test_scenario_6_concurrent_enqueue_dequeue(self):
        """场景6：并发入队出队 - 10个线程同时操作"""
        self.log('info', '\n' + '=' * 80)
        self.log('info', '场景6：并发入队出队 - 多线程操作')
        self.log('info', '=' * 80)

        executor = DistributedTaskExecutor(max_workers=256)
        processed_count = [0]
        lock = threading.Lock()

        def concurrent_handler(parameters):
            with lock:
                processed_count[0] += 1
            return "done"

        executor.register_handler('concurrent', concurrent_handler)
        executor.start()

        self.log('info', '多线程并发入队出队测试')
        enqueued = [0]
        enqueue_lock = threading.Lock()
        
        def enqueue_thread():
            for _ in range(10000):
                task = DistributedTask(
                    task_type='concurrent',
                    parameters={'thread': threading.current_thread().name}
                )
                executor.queue.enqueue(task)
                with enqueue_lock:
                    enqueued[0] += 1

        threads = []
        for i in range(10):
            t = threading.Thread(target=enqueue_thread, name=f"enqueue-{i}")
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        self.log('info', f'已入队: {enqueued[0]}')
        self.log('info', '等待处理完成...')
        time.sleep(30)

        executor.stop()

        self.log('info', f'并发入队出队结果:')
        self.log('info', f'入队: {enqueued[0]}')
        self.log('info', f'处理: {processed_count[0]}')

        success = processed_count[0] >= 90000
        if not success:
            self.error_count += 1
            self.log('error', '❌ 并发入队出队测试失败')
        else:
            self.log('info', '✅ 并发入队出队测试通过')
        return success

    def test_scenario_7_large_parameters(self):
        """场景7：大参数任务 - 参数包含大量数据"""
        self.log('info', '\n' + '=' * 80)
        self.log('info', '场景7：大参数任务 - 大数据负载')
        self.log('info', '=' * 80)

        executor = DistributedTaskExecutor(max_workers=128)
        large_count = [0]
        param_sizes = []

        def large_param_handler(parameters):
            large_count[0] += 1
            large_data = parameters.get('data', '')
            param_sizes.append(len(str(large_data)))
            return f"large-done"

        executor.register_handler('large', large_param_handler)
        executor.start()

        self.log('info', '大参数任务提交')
        
        def generate_large_data(size_kb):
            return 'x' * (size_kb * 1024)

        for i in range(5000):
            data_size = random.randint(1, 100)  # 1KB到100KB
            task = DistributedTask(
                task_id=f"large-{i}",
                task_type='large',
                priority=TaskPriority.NORMAL,
                parameters={
                    'index': i,
                    'data_size': data_size,
                    'data': generate_large_data(data_size)
                }
            )
            executor.queue.enqueue(task)
            if i % 500 == 0 and i > 0:
                self.log('info', f'已提交 {i} 任务')

        self.log('info', '等待处理完成...')
        time.sleep(45)

        executor.stop()

        avg_size = sum(param_sizes) / len(param_sizes) if param_sizes else 0
        self.log('info', f'大参数任务结果:')
        self.log('info', f'处理: {large_count[0]}')
        self.log('info', f'平均参数大小: {avg_size:.2f} KB')

        success = large_count[0] >= 4000
        if not success:
            self.error_count += 1
            self.log('error', '❌ 大参数任务测试失败')
        else:
            self.log('info', '✅ 大参数任务测试通过')
        return success

    def test_scenario_8_rapid_submit_start_stop(self):
        """场景8：快速启动停止 - 50次循环"""
        self.log('info', '\n' + '=' * 80)
        self.log('info', '场景8：快速启动停止 - 50次循环')
        self.log('info', '=' * 80)

        total_processed = 0
        errors_occurred = 0

        for cycle in range(50):
            try:
                executor = DistributedTaskExecutor(max_workers=64)
                
                def quick_handler(parameters):
                    return "quick-done"

                executor.register_handler('quick', quick_handler)
                executor.start()

                for i in range(500):
                    task = DistributedTask(
                        task_type='quick',
                        parameters={'cycle': cycle, 'index': i}
                    )
                    executor.queue.enqueue(task)

                time.sleep(1.5)
                executor.stop()
                
                stats = executor.get_statistics()
                total_processed += stats['completed']
                
                if cycle % 10 == 0:
                    self.log('info', f'循环 {cycle}: 完成 {stats["completed"]}')

            except Exception as e:
                errors_occurred += 1
                self.log('error', f'循环 {cycle} 失败: {str(e)}')

        self.log('info', f'快速启动停止结果:')
        self.log('info', f'总循环: 50')
        self.log('info', f'总完成: {total_processed}')
        self.log('info', f'错误数: {errors_occurred}')

        success = errors_occurred <= 5
        if not success:
            self.error_count += 1
            self.log('error', '❌ 快速启动停止测试失败')
        else:
            self.log('info', '✅ 快速启动停止测试通过')
        return success

    def test_scenario_9_all_together(self):
        """场景9：全部一起 - 综合超级混乱"""
        self.log('info', '\n' + '=' * 80)
        self.log('info', '场景9：全部一起 - 超级混乱')
        self.log('info', '=' * 80)

        executor = DistributedTaskExecutor(max_workers=512)
        all_counts = {}
        all_lock = threading.Lock()

        def make_handler(name):
            def handler(parameters):
                with all_lock:
                    if name not in all_counts:
                        all_counts[name] = 0
                    all_counts[name] += 1
                if random.random() < 0.03:
                    raise ValueError(f"Super chaos failure")
                time.sleep(random.uniform(0.0001, 0.01))
                return "chaos-done"
            return handler

        for i in range(50):
            executor.register_handler(f"chaos-{i}", make_handler(f"chaos-{i}"))

        executor.start()

        self.log('info', '超级混乱场景 - 所有场景混合')
        total_tasks = 50000
        for i in range(total_tasks):
            handler_name = f"chaos-{random.randint(0, 49)}"
            task = DistributedTask(
                task_id=f"chaos-{i}",
                task_type=handler_name,
                priority=random.choice([TaskPriority.CRITICAL, TaskPriority.HIGH, TaskPriority.NORMAL, TaskPriority.LOW]),
                max_retries=random.randint(0, 3),
                parameters={
                    'index': i,
                    'data': 'x' * random.randint(100, 10000)
                }
            )
            executor.queue.enqueue(task)
            if i % 5000 == 0 and i > 0:
                self.log('info', f'已提交 {i} 任务')

        self.log('info', '等待超级混乱处理完成...')
        time.sleep(90)

        executor.stop()

        total_processed = sum(all_counts.values())
        self.log('info', f'超级混乱结果:')
        self.log('info', f'总提交: {total_tasks}')
        self.log('info', f'总处理: {total_processed}')
        self.log('info', f'活跃handler数: {len(all_counts)}')

        success = total_processed >= 40000
        if not success:
            self.error_count += 1
            self.log('error', '❌ 超级混乱测试失败')
        else:
            self.log('info', '✅ 超级混乱测试通过')
        return success

    def run_all_tests(self):
        """运行所有超级混乱测试场景"""
        self.log('info', '\n' + '=' * 80)
        self.log('info', '第10轮迭代 - 超级混乱场景压力测试')
        self.log('info', '=' * 80)
        self.log('info', '测试开始时间: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        self.log('info', '=' * 80)

        test_start = time.time()

        scenarios = [
            ('超长持续运行', self.test_scenario_1_long_running_sustained),
            ('并发处理器风暴', self.test_scenario_2_concurrent_handler_storm),
            ('优先级洪泛', self.test_scenario_3_priority_flood),
            ('混合失败重试', self.test_scenario_4_mixed_failure_retry),
            ('悬挂任务', self.test_scenario_5_hanging_tasks),
            ('并发入队出队', self.test_scenario_6_concurrent_enqueue_dequeue),
            ('大参数任务', self.test_scenario_7_large_parameters),
            ('快速启动停止', self.test_scenario_8_rapid_submit_start_stop),
            ('超级混乱场景', self.test_scenario_9_all_together)
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

        self.log('info', '\n' + '=' * 80)
        self.log('info', '第10轮测试结果汇总')
        self.log('info', '=' * 80)

        passed = sum(1 for r in results.values() if r)
        failed = len(results) - passed

        for name, result in results.items():
            status = '✅ PASS' if result else '❌ FAIL'
            self.log('info', f'{name}: {status}')

        self.log('info', '\n' + '=' * 80)
        self.log('info', '测试统计')
        self.log('info', '=' * 80)
        self.log('info', f'总测试数: {len(results)}')
        self.log('info', f'通过数: {passed}')
        self.log('info', f'失败数: {failed}')
        self.log('info', f'错误数: {self.error_count}')
        self.log('info', f'警告数: {self.warning_count}')
        self.log('info', f'总耗时: {test_duration:.2f}秒')

        self.log('info', '\n测试结束时间: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        if failed == 0 and self.error_count == 0:
            self.log('info', '\n🎉🎉🎉 第10轮超级混乱测试：完美通过！')
        else:
            self.log('error', f'\n❌❌❌ 第10轮超级混乱测试：{failed}个场景失败，{self.error_count}个错误')

        return failed == 0


if __name__ == '__main__':
    test = Round10SuperChaosTest()
    success = test.run_all_tests()
    sys.exit(0 if success else 1)
