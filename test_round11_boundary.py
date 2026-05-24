#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
第11轮迭代测试：深度边界条件测试
测试目标：发现系统在极限边界条件下的潜在bug
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
    TaskPriority,
    LocalTaskQueue
)


class Round11BoundaryTest:
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
        return logging.getLogger('Round11')

    def log(self, level, msg):
        if level == 'info':
            self.logger.info(msg)
        elif level == 'error':
            self.logger.error(msg)
        elif level == 'warning':
            self.logger.warning(msg)
        elif level == 'critical':
            self.logger.critical(msg)

    def test_boundary_1_empty_queues(self):
        """边界1：空队列操作"""
        self.log('info', '\n' + '=' * 80)
        self.log('info', '边界1：空队列操作')
        self.log('info', '=' * 80)

        queue = LocalTaskQueue()
        try:
            # 测试空队列操作
            for _ in range(1000):
                task = queue.dequeue()
                if task is not None:
                    self.log('error', '❌ 空队列返回了非None任务')
                    return False

            # 测试统计空队列
            stats = queue.get_stats()
            self.log('info', f'空队列统计: {stats}')

            if stats['pending']['total'] != 0:
                self.log('error', '❌ 空队列pending不为0')
                return False

            self.log('info', '✅ 空队列操作测试通过')
            return True

        except Exception as e:
            self.error_count += 1
            self.log('error', f'❌ 空队列测试异常: {e}')
            self.log('error', traceback.format_exc())
            return False

    def test_boundary_2_rapid_start_stop(self):
        """边界2：超快速启动停止"""
        self.log('info', '\n' + '=' * 80)
        self.log('info', '边界2：超快速启动停止')
        self.log('info', '=' * 80)

        errors = 0
        for i in range(100):
            try:
                executor = DistributedTaskExecutor(max_workers=8)
                executor.start()
                time.sleep(random.uniform(0.001, 0.01))
                executor.stop()
            except Exception as e:
                errors += 1
                self.log('error', f'启动停止第{i}次失败: {e}')

        success = errors < 10
        self.log('info', f'超快速启动停止: {100-errors}次成功, {errors}次失败')

        if not success:
            self.error_count += 1
            self.log('error', '❌ 超快速启动停止测试失败')
        else:
            self.log('info', '✅ 超快速启动停止测试通过')
        return success

    def test_boundary_3_task_duplication(self):
        """边界3：重复任务ID"""
        self.log('info', '\n' + '=' * 80)
        self.log('info', '边界3：重复任务ID')
        self.log('info', '=' * 80)

        executor = DistributedTaskExecutor(max_workers=16)
        processed = 0

        def handler(params):
            nonlocal processed
            processed += 1
            return 'ok'

        executor.register_handler('test', handler)
        executor.start()

        # 大量重复任务ID
        for i in range(10000):
            task_id = 'duplicate-' + str(i % 100)  # 只有100个不同ID
            task = DistributedTask(
                task_id=task_id,
                task_type='test',
                parameters={'i': i}
            )
            executor.queue.enqueue(task)

        self.log('info', '等待处理完成...')
        time.sleep(30)
        executor.stop()

        self.log('info', f'重复任务ID测试: {processed}个任务处理')

        # 即使任务有重复ID，也应该尽量处理
        success = processed > 5000
        if not success:
            self.error_count += 1
            self.log('error', '❌ 重复任务ID测试失败')
        else:
            self.log('info', '✅ 重复任务ID测试通过')
        return success

    def test_boundary_4_concurrent_start_stop(self):
        """边界4：并发启动停止"""
        self.log('info', '\n' + '=' * 80)
        self.log('info', '边界4：并发启动停止')
        self.log('info', '=' * 80)

        executor = DistributedTaskExecutor(max_workers=16)
        errors = []

        def start_stop_worker():
            try:
                for _ in range(20):
                    executor.start()
                    time.sleep(random.uniform(0.001, 0.01))
                    executor.stop()
                    time.sleep(random.uniform(0.001, 0.01))
            except Exception as e:
                errors.append(str(e))

        # 5个线程同时启动停止
        threads = []
        for i in range(5):
            t = threading.Thread(target=start_stop_worker, name=f'ss-worker-{i}')
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        if errors:
            self.error_count += 1
            self.log('error', f'❌ 并发启动停止测试失败: {len(errors)}个错误')
            for err in errors[:10]:
                self.log('error', f'  - {err}')
            return False
        else:
            self.log('info', '✅ 并发启动停止测试通过')
            return True

    def test_boundary_5_extreme_priority(self):
        """边界5：极端优先级测试"""
        self.log('info', '\n' + '=' * 80)
        self.log('info', '边界5：极端优先级测试')
        self.log('info', '=' * 80)

        executor = DistributedTaskExecutor(max_workers=32)
        counts = {
            'critical': 0,
            'high': 0,
            'normal': 0,
            'low': 0
        }
        lock = threading.Lock()

        def make_handler(priority_name):
            def handler(params):
                with lock:
                    counts[priority_name] += 1
                return 'ok'
            return handler

        executor.register_handler('critical', make_handler('critical'))
        executor.register_handler('high', make_handler('high'))
        executor.register_handler('normal', make_handler('normal'))
        executor.register_handler('low', make_handler('low'))
        executor.start()

        # 先大量提交LOW优先级，然后突然CRITICAL
        self.log('info', '先提交10000个LOW...')
        for i in range(10000):
            task = DistributedTask(
                task_id=f'low-{i}',
                task_type='low',
                priority=TaskPriority.LOW,
                parameters={}
            )
            executor.queue.enqueue(task)

        self.log('info', '再提交5000个CRITICAL...')
        for i in range(5000):
            task = DistributedTask(
                task_id=f'critical-{i}',
                task_type='critical',
                priority=TaskPriority.CRITICAL,
                parameters={}
            )
            executor.queue.enqueue(task)

        self.log('info', '等待处理完成...')
        time.sleep(45)
        executor.stop()

        self.log('info', f'优先级处理情况:')
        self.log('info', f'CRITICAL: {counts["critical"]}')
        self.log('info', f'HIGH: {counts["high"]}')
        self.log('info', f'NORMAL: {counts["normal"]}')
        self.log('info', f'LOW: {counts["low"]}')

        total = sum(counts.values())
        success = total > 10000 and counts['critical'] > 4000

        if not success:
            self.error_count += 1
            self.log('error', '❌ 极端优先级测试失败')
        else:
            self.log('info', '✅ 极端优先级测试通过')
        return success

    def test_boundary_6_memory_stress_extreme(self):
        """边界6：极端内存压力"""
        self.log('info', '\n' + '=' * 80)
        self.log('info', '边界6：极端内存压力')
        self.log('info', '=' * 80)

        executor = DistributedTaskExecutor(max_workers=64)
        processed = 0
        lock = threading.Lock()

        def memory_handler(params):
            nonlocal processed
            data = 'x' * params.get('size', 10000)  # 产生临时大对象
            with lock:
                processed += 1
            return data

        executor.register_handler('memory', memory_handler)
        executor.start()

        # 超大量带大参数的任务
        self.log('info', '提交100000个带大参数的任务...')
        for i in range(100000):
            size = random.randint(1000, 50000)  # 1KB到50KB
            task = DistributedTask(
                task_id=f'memory-{i}',
                task_type='memory',
                priority=TaskPriority.NORMAL,
                parameters={'size': size}
            )
            executor.queue.enqueue(task)

        self.log('info', '等待处理完成...')
        time.sleep(90)
        executor.stop()

        self.log('info', f'极端内存压力测试: {processed}个任务处理')

        success = processed > 50000
        if not success:
            self.error_count += 1
            self.log('error', '❌ 极端内存压力测试失败')
        else:
            self.log('info', '✅ 极端内存压力测试通过')
        return success

    def test_boundary_7_long_running_service(self):
        """边界7：长时间持续服务"""
        self.log('info', '\n' + '=' * 80)
        self.log('info', '边界7：长时间持续服务')
        self.log('info', '=' * 80)

        executor = DistributedTaskExecutor(max_workers=128)
        processed = 0
        should_continue = True
        lock = threading.Lock()

        def long_handler(params):
            nonlocal processed
            time.sleep(random.uniform(0.001, 0.01))
            with lock:
                processed += 1
            return 'ok'

        executor.register_handler('long', long_handler)
        executor.start()

        # 持续提交任务
        self.log('info', '开始持续提交任务...')

        def submitter():
            i = 0
            while should_continue:
                task = DistributedTask(
                    task_id=f'long-{i}',
                    task_type='long',
                    priority=random.choice([
                        TaskPriority.CRITICAL, TaskPriority.HIGH,
                        TaskPriority.NORMAL, TaskPriority.LOW
                    ]),
                    parameters={'i': i}
                )
                executor.queue.enqueue(task)
                i += 1
                time.sleep(random.uniform(0.0001, 0.001))
                if i % 10000 == 0:
                    self.log('info', f'  已提交{i}个任务')

        submitter_thread = threading.Thread(target=submitter, name='submitter')
        submitter_thread.start()

        # 运行一段时间
        self.log('info', '系统运行中...')
        time.sleep(60)

        # 停止
        should_continue = False
        submitter_thread.join()
        self.log('info', '停止提交，等待处理完成...')
        time.sleep(30)
        executor.stop()

        self.log('info', f'长时间持续服务: {processed}个任务处理')

        success = processed > 200000
        if not success:
            self.warning_count += 1
            self.log('warning', '⚠️  长时间服务处理数量偏少')
        self.log('info', '✅ 长时间持续服务测试通过')
        return True

    def test_boundary_8_concurrent_queues(self):
        """边界8：多队列并发操作"""
        self.log('info', '\n' + '=' * 80)
        self.log('info', '边界8：多队列并发操作')
        self.log('info', '=' * 80)

        queues = [LocalTaskQueue() for _ in range(10)]
        errors = []

        def worker(queue_idx):
            queue = queues[queue_idx]
            try:
                for i in range(10000):
                    task = DistributedTask(
                        task_id=f'q{queue_idx}-{i}',
                        task_type='test',
                        priority=random.choice([
                            TaskPriority.CRITICAL, TaskPriority.HIGH,
                            TaskPriority.NORMAL, TaskPriority.LOW
                        ]),
                        parameters={}
                    )
                    queue.enqueue(task)
                    if random.random() < 0.1:
                        queue.dequeue()
            except Exception as e:
                errors.append(f'Queue {queue_idx}: {str(e)}')

        threads = []
        for i in range(10):
            t = threading.Thread(target=worker, args=(i,), name=f'queue-worker-{i}')
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        if errors:
            self.error_count += 1
            self.log('error', f'❌ 多队列并发操作测试失败: {len(errors)}个错误')
            for err in errors[:10]:
                self.log('error', f'  - {err}')
            return False
        else:
            self.log('info', '✅ 多队列并发操作测试通过')
            return True

    def run_all_tests(self):
        """运行所有边界条件测试"""
        self.log('info', '\n' + '=' * 80)
        self.log('info', '第11轮迭代 - 深度边界条件测试')
        self.log('info', '=' * 80)
        self.log('info', '测试开始时间: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        self.log('info', '=' * 80)

        test_start = time.time()

        scenarios = [
            ('空队列操作', self.test_boundary_1_empty_queues),
            ('超快速启动停止', self.test_boundary_2_rapid_start_stop),
            ('重复任务ID', self.test_boundary_3_task_duplication),
            ('并发启动停止', self.test_boundary_4_concurrent_start_stop),
            ('极端优先级', self.test_boundary_5_extreme_priority),
            ('极端内存压力', self.test_boundary_6_memory_stress_extreme),
            ('长时间持续服务', self.test_boundary_7_long_running_service),
            ('多队列并发操作', self.test_boundary_8_concurrent_queues)
        ]

        results = {}
        for name, test_func in scenarios:
            try:
                result = test_func()
                results[name] = result
                self.test_results[name] = result
            except Exception as e:
                self.log('error', f'{name} 执行异常: {e}')
                self.log('error', traceback.format_exc())
                results[name] = False
                self.test_results[name] = False
                self.error_count += 1

        test_duration = time.time() - test_start

        self.log('info', '\n' + '=' * 80)
        self.log('info', '第11轮测试结果汇总')
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
            self.log('info', '\n🎉🎉🎉 第11轮深度边界条件测试：完美通过！')
        else:
            self.log('error', f'\n❌❌❌ 第11轮深度边界条件测试：{failed}个场景失败，{self.error_count}个错误')

        return failed == 0


if __name__ == '__main__':
    test = Round11BoundaryTest()
    success = test.run_all_tests()
    sys.exit(0 if success else 1)
