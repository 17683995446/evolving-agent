"""
Task Execution Engine - 任务执行层 (Optimized & Working)
Core execution module for task processing and management
"""
import threading
import time
import logging
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, Future
from collections import deque
import traceback

from core.data_models_universal import (
    TaskExecution, TaskMetrics, TaskStatus, 
    generate_task_id, ExecutionLevel,
    ExecutionContext
)


class OptimizedTaskQueue:
    """Optimized task queue with proper priority handling"""
    def __init__(self):
        self.pending: deque = deque()
        self.running: Dict[str, TaskExecution] = {}
        self.completed: deque = deque(maxlen=1000)
        self.failed: deque = deque(maxlen=100)
        self.lock = threading.RLock()
        
    def add_task(self, task: TaskExecution):
        with self.lock:
            self.pending.append(task)
            
    def get_next_task(self) -> Optional[TaskExecution]:
        with self.lock:
            if self.pending:
                return self.pending.popleft()
            return None
            
    def complete_task(self, task_id: str, result: Any):
        with self.lock:
            if task_id in self.running:
                task = self.running.pop(task_id)
                task.mark_completed(result)
                self.completed.append(task)
                return True
        return False
            
    def fail_task(self, task_id: str, error: str):
        with self.lock:
            if task_id in self.running:
                task = self.running.pop(task_id)
                task.mark_failed(error)
                self.failed.append(task)
                return True
        return False
        
    def mark_running(self, task: TaskExecution) -> bool:
        with self.lock:
            if task.task_id not in self.running:
                task.mark_running()
                self.running[task.task_id] = task
                return True
        return False
            
    def get_queue_stats(self) -> Dict[str, int]:
        with self.lock:
            return {
                'pending': len(self.pending),
                'running': len(self.running),
                'completed': len(self.completed),
                'failed': len(self.failed)
            }


class TaskExecutionEngine:
    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        self.task_queue = OptimizedTaskQueue()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.active_tasks: Dict[str, Future] = {}
        self.task_handlers: Dict[str, Callable] = {}
        self.execution_history: List[TaskMetrics] = []
        self.is_running = False
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
        self.context = ExecutionContext(
            context_id=generate_task_id(),
            execution_level=ExecutionLevel.STANDARD
        )

    def register_handler(self, task_type: str, handler: Callable):
        self.task_handlers[task_type] = handler
        self.logger.info(f"Handler registered for task type: {task_type}")

    def add_task(self, task_type: str, description: str, 
                 parameters: Dict[str, Any] = None,
                 priority: int = 5) -> str:
        task_id = generate_task_id()
        task = TaskExecution(
            task_id=task_id,
            task_type=task_type,
            description=description,
            status=TaskStatus.PENDING,
            priority=priority,
            parameters=parameters or {}
        )
        task.metrics = TaskMetrics(task_id=task_id, start_time=datetime.now())

        with self.lock:
            self.task_queue.add_task(task)
            self.context.current_tasks.append(task)

        self.logger.debug(f"Task added: {task_id} ({task_type})")
        return task_id

    def start(self):
        if not self.is_running:
            self.is_running = True
            threading.Thread(target=self._execution_loop, daemon=True).start()
            self.logger.info("Task execution engine started")

    def stop(self):
        self.is_running = False
        try:
            self.executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self.logger.info("Task execution engine stopped")

    def _execution_loop(self):
        while self.is_running:
            try:
                task = self.task_queue.get_next_task()
                if task:
                    self._execute_task(task)
                else:
                    time.sleep(0.01)
            except Exception as e:
                self.logger.error(f"Execution loop error: {e}")

    def _execute_task(self, task: TaskExecution):
        try:
            self.task_queue.mark_running(task)
            future = self.executor.submit(self._task_worker, task)
            with self.lock:
                self.active_tasks[task.task_id] = future
            future.add_done_callback(lambda f: self._on_task_complete(task.task_id, f))
        except Exception as e:
            self.task_queue.fail_task(task.task_id, str(e))
            self.logger.error(f"Task submission failed: {e}")

    def _task_worker(self, task: TaskExecution) -> Any:
        start_time = time.time()
        try:
            if task.task_type in self.task_handlers:
                handler = self.task_handlers[task.task_type]
                result = handler(task.parameters)

                if task.metrics:
                    task.metrics.duration = time.time() - start_time
                    task.metrics.success_count = 1
                    task.metrics.efficiency_score = 1.0

                return result
            else:
                raise ValueError(f"No handler for task type: {task.task_type}")
        except Exception as e:
            if task.metrics:
                task.metrics.duration = time.time() - start_time
                task.metrics.failure_count = 1
                task.metrics.efficiency_score = 0.0
            raise e

    def _on_task_complete(self, task_id: str, future: Future):
        try:
            result = future.result()
            self.task_queue.complete_task(task_id, result)
            self._record_metrics(task_id)
            self.logger.debug(f"Task completed: {task_id}")
        except Exception as e:
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            self.task_queue.fail_task(task_id, error_msg)
            self.logger.debug(f"Task failed: {task_id} - {e}")
        finally:
            with self.lock:
                if task_id in self.active_tasks:
                    del self.active_tasks[task_id]

    def _record_metrics(self, task_id: str):
        for task in list(self.task_queue.completed):
            if task.task_id == task_id and task.metrics:
                self.execution_history.append(task.metrics)
                self.context.total_execution_time += task.metrics.duration
                break

    def get_statistics(self) -> Dict[str, Any]:
        return {
            'queue_stats': self.task_queue.get_queue_stats(),
            'active_tasks': len(self.active_tasks),
            'execution_history': len(self.execution_history),
            'registered_handlers': list(self.task_handlers.keys()),
            'total_execution_time': self.context.total_execution_time
        }

    def optimize_execution_level(self, new_level: ExecutionLevel):
        self.context.execution_level = new_level
        if new_level == ExecutionLevel.MAXIMUM:
            self.max_workers = max(self.max_workers * 2, 8)
        elif new_level == ExecutionLevel.MINIMAL:
            self.max_workers = max(1, self.max_workers // 2)

        self.logger.info(f"Execution level optimized to: {new_level}")


class AdaptiveTaskScheduler:
    def __init__(self, execution_engine: TaskExecutionEngine):
        self.execution_engine = execution_engine
        self.priority_boost_enabled = True
        self.adaptive_threading_enabled = True
        self.logger = logging.getLogger(__name__)

    def schedule_with_priority(self, task_type: str, description: str,
                               parameters: Dict[str, Any] = None,
                               priority: int = 5) -> str:
        if self.priority_boost_enabled:
            priority = self._adjust_priority_based_on_load(priority)

        return self.execution_engine.add_task(
            task_type, description, parameters, priority
        )

    def _adjust_priority_based_on_load(self, base_priority: int) -> int:
        queue_stats = self.execution_engine.task_queue.get_queue_stats()
        pending_load = queue_stats['pending']

        if pending_load > 20:
            return min(base_priority + 2, 10)
        elif pending_load < 5:
            return max(base_priority - 1, 1)
        return base_priority

    def enable_adaptive_scaling(self):
        self.adaptive_threading_enabled = True
        threading.Thread(target=self._scaling_loop, daemon=True).start()

    def _scaling_loop(self):
        while self.adaptive_threading_enabled:
            try:
                queue_stats = self.execution_engine.task_queue.get_queue_stats()
                load = queue_stats['pending'] + queue_stats['running']

                if load > 15 and self.execution_engine.max_workers < 8:
                    self.execution_engine.max_workers += 1
                    self.logger.info(f"Increased workers to {self.execution_engine.max_workers}")
                elif load < 3 and self.execution_engine.max_workers > 2:
                    self.execution_engine.max_workers -= 1
                    self.logger.info(f"Decreased workers to {self.execution_engine.max_workers}")

                time.sleep(30)
            except Exception as e:
                self.logger.error(f"Scaling loop error: {e}")


class TaskBatchingProcessor:
    def __init__(self, batch_size: int = 10):
        self.batch_size = batch_size
        self.pending_batches: List[List[TaskExecution]] = []
        self.lock = threading.RLock()

    def add_to_batch(self, task: TaskExecution):
        with self.lock:
            if not self.pending_batches or len(self.pending_batches[-1]) >= self.batch_size:
                self.pending_batches.append([])
            self.pending_batches[-1].append(task)

    def get_next_batch(self) -> Optional[List[TaskExecution]]:
        with self.lock:
            if self.pending_batches:
                return self.pending_batches.pop(0)
            return None
