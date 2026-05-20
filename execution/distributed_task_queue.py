"""
分布式任务队列系统 - 完全优化版本
支持本地和Redis两种模式
支持任务优先级、权重调度、持久化和失败恢复
"""
import threading
import time
import logging
import json
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable
from enum import Enum
from collections import deque
from dataclasses import dataclass, field

# 尝试导入Redis，如果不可用则使用本地模式
try:
    import redis
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False


class TaskPriority(Enum):
    """任务优先级枚举"""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3


@dataclass
class DistributedTask:
    """分布式任务数据结构"""
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str = ""
    description: str = ""
    parameters: Dict[str, Any] = field(default_factory=dict)
    priority: TaskPriority = TaskPriority.NORMAL
    weight: float = 1.0
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    status: str = "pending"
    result: Optional[Any] = None
    error: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3

    def to_dict(self) -> Dict[str, Any]:
        """序列化任务为字典"""
        return {
            'task_id': self.task_id,
            'task_type': self.task_type,
            'description': self.description,
            'parameters': self.parameters,
            'priority': self.priority.value,
            'weight': self.weight,
            'created_at': self.created_at.isoformat(),
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'status': self.status,
            'result': json.dumps(self.result) if self.result else None,
            'error': self.error,
            'retry_count': self.retry_count,
            'max_retries': self.max_retries
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DistributedTask':
        """从字典反序列化任务"""
        task = cls()
        task.task_id = data.get('task_id', str(uuid.uuid4()))
        task.task_type = data.get('task_type', '')
        task.description = data.get('description', '')
        task.parameters = data.get('parameters', {})
        task.priority = TaskPriority(data.get('priority', 2))
        task.weight = data.get('weight', 1.0)
        task.created_at = datetime.fromisoformat(data['created_at'])
        if data.get('started_at'):
            task.started_at = datetime.fromisoformat(data['started_at'])
        if data.get('completed_at'):
            task.completed_at = datetime.fromisoformat(data['completed_at'])
        task.status = data.get('status', 'pending')
        if data.get('result'):
            task.result = json.loads(data['result'])
        task.error = data.get('error')
        task.retry_count = data.get('retry_count', 0)
        task.max_retries = data.get('max_retries', 3)
        return task


class BaseTaskQueue:
    """任务队列基类"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def enqueue(self, task: DistributedTask) -> bool:
        """入队任务"""
        raise NotImplementedError

    def dequeue(self) -> Optional[DistributedTask]:
        """出队任务"""
        raise NotImplementedError

    def complete(self, task_id: str, result: Any) -> bool:
        """标记任务完成"""
        raise NotImplementedError

    def fail(self, task_id: str, error: str) -> bool:
        """标记任务失败"""
        raise NotImplementedError

    def get_stats(self) -> Dict[str, Any]:
        """获取队列统计"""
        raise NotImplementedError

    def get_pending_count(self) -> int:
        """获取待处理任务数"""
        raise NotImplementedError


class LocalTaskQueue(BaseTaskQueue):
    """本地任务队列实现 - 线程安全，支持优先级"""
    
    def __init__(self):
        super().__init__()
        # 按优先级分离的队列
        self.queues: Dict[TaskPriority, deque] = {
            TaskPriority.CRITICAL: deque(),
            TaskPriority.HIGH: deque(),
            TaskPriority.NORMAL: deque(),
            TaskPriority.LOW: deque()
        }
        self.running: Dict[str, DistributedTask] = {}
        self.completed: deque = deque(maxlen=10000)
        self.failed: deque = deque(maxlen=1000)
        
        # 添加计数器来跟踪完整的历史
        self.total_completed = 0
        self.total_failed = 0
        
        self.lock = threading.RLock()

    def enqueue(self, task: DistributedTask) -> bool:
        with self.lock:
            task.status = "pending"
            self.queues[task.priority].append(task)
            self.logger.debug(f"Task enqueued: {task.task_id} (priority: {task.priority.name})")
            return True

    def dequeue(self) -> Optional[DistributedTask]:
        with self.lock:
            # 按优先级顺序获取任务
            for priority in [TaskPriority.CRITICAL, TaskPriority.HIGH,
                            TaskPriority.NORMAL, TaskPriority.LOW]:
                if self.queues[priority]:
                    task = self.queues[priority].popleft()
                    task.status = "running"
                    task.started_at = datetime.now()
                    self.running[task.task_id] = task
                    return task
            return None

    def complete(self, task_id: str, result: Any) -> bool:
        with self.lock:
            if task_id in self.running:
                task = self.running.pop(task_id)
                task.status = "completed"
                task.completed_at = datetime.now()
                task.result = result
                self.completed.append(task)
                self.total_completed += 1  # 增加完整计数器
                self.logger.debug(f"Task completed: {task_id}")
                return True
        return False

    def fail(self, task_id: str, error: str) -> bool:
        with self.lock:
            if task_id in self.running:
                task = self.running.pop(task_id)
                task.error = error
                task.retry_count += 1
                
                # 检查是否需要重试
                if task.retry_count < task.max_retries:
                    task.status = "pending"
                    task.started_at = None
                    self.queues[task.priority].append(task)
                    # 减少日志输出频率
                else:
                    task.status = "failed"
                    task.completed_at = datetime.now()
                    self.failed.append(task)
                    self.total_failed += 1  # 增加完整计数器
                return True
        return False

    def get_stats(self) -> Dict[str, Any]:
        with self.lock:
            return {
                'pending': {
                    'critical': len(self.queues[TaskPriority.CRITICAL]),
                    'high': len(self.queues[TaskPriority.HIGH]),
                    'normal': len(self.queues[TaskPriority.NORMAL]),
                    'low': len(self.queues[TaskPriority.LOW]),
                    'total': (len(self.queues[TaskPriority.CRITICAL]) +
                             len(self.queues[TaskPriority.HIGH]) +
                             len(self.queues[TaskPriority.NORMAL]) +
                             len(self.queues[TaskPriority.LOW]))
                },
                'running': len(self.running),
                'completed': self.total_completed,  # 使用完整计数器
                'failed': self.total_failed         # 使用完整计数器
            }

    def get_pending_count(self) -> int:
        with self.lock:
            total = 0
            for q in self.queues.values():
                total += len(q)
            return total


class RedisTaskQueue(BaseTaskQueue):
    """Redis 分布式任务队列实现"""
    
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        super().__init__()
        if not HAS_REDIS:
            raise RuntimeError("Redis not available. Install with 'pip install redis'")
        
        self.redis_client = redis.from_url(redis_url)
        self.namespace = "distributed_queue:"
        self.queue_prefixes = {
            TaskPriority.CRITICAL: "critical",
            TaskPriority.HIGH: "high",
            TaskPriority.NORMAL: "normal",
            TaskPriority.LOW: "low"
        }
        self.logger.info("Redis task queue initialized")

    def _get_queue_key(self, priority: TaskPriority) -> str:
        return f"{self.namespace}queue:{self.queue_prefixes[priority]}"

    def _get_task_key(self, task_id: str) -> str:
        return f"{self.namespace}task:{task_id}"

    def enqueue(self, task: DistributedTask) -> bool:
        try:
            task_key = self._get_task_key(task.task_id)
            queue_key = self._get_queue_key(task.priority)
            
            # 存储任务数据
            self.redis_client.setex(task_key, 86400, json.dumps(task.to_dict()))
            # 入队
            self.redis_client.lpush(queue_key, task.task_id)
            
            self.logger.debug(f"Task enqueued in Redis: {task.task_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to enqueue to Redis: {e}")
            return False

    def dequeue(self) -> Optional[DistributedTask]:
        try:
            # 按优先级顺序尝试获取任务
            for priority in [TaskPriority.CRITICAL, TaskPriority.HIGH,
                            TaskPriority.NORMAL, TaskPriority.LOW]:
                queue_key = self._get_queue_key(priority)
                task_id = self.redis_client.rpop(queue_key)
                
                if task_id:
                    task_id = task_id.decode('utf-8')
                    task_key = self._get_task_key(task_id)
                    task_data = self.redis_client.get(task_key)
                    
                    if task_data:
                        task = DistributedTask.from_dict(json.loads(task_data))
                        task.status = "running"
                        task.started_at = datetime.now()
                        self.redis_client.setex(task_key, 86400, json.dumps(task.to_dict()))
                        return task
            return None
        except Exception as e:
            self.logger.error(f"Failed to dequeue from Redis: {e}")
            return None

    def complete(self, task_id: str, result: Any) -> bool:
        try:
            task_key = self._get_task_key(task_id)
            task_data = self.redis_client.get(task_key)
            
            if task_data:
                task = DistributedTask.from_dict(json.loads(task_data))
                task.status = "completed"
                task.completed_at = datetime.now()
                task.result = result
                self.redis_client.setex(task_key, 86400, json.dumps(task.to_dict()))
                return True
            return False
        except Exception as e:
            self.logger.error(f"Failed to mark complete in Redis: {e}")
            return False

    def fail(self, task_id: str, error: str) -> bool:
        try:
            task_key = self._get_task_key(task_id)
            task_data = self.redis_client.get(task_key)
            
            if task_data:
                task = DistributedTask.from_dict(json.loads(task_data))
                task.status = "failed"
                task.completed_at = datetime.now()
                task.error = error
                task.retry_count += 1
                
                if task.retry_count < task.max_retries:
                    task.status = "pending"
                    task.started_at = None
                    self.redis_client.setex(task_key, 86400, json.dumps(task.to_dict()))
                    queue_key = self._get_queue_key(task.priority)
                    self.redis_client.lpush(queue_key, task_id)
                else:
                    task.status = "failed"
                    task.completed_at = datetime.now()
                    self.redis_client.setex(task_key, 86400, json.dumps(task.to_dict()))
                
                return True
            return False
        except Exception as e:
            self.logger.error(f"Failed to mark failed in Redis: {e}")
            return False

    def get_stats(self) -> Dict[str, Any]:
        try:
            stats = {
                'pending': {},
                'running': 0,
                'completed': 0,
                'failed': 0
            }
            
            total_pending = 0
            for priority in TaskPriority:
                queue_key = self._get_queue_key(priority)
                count = self.redis_client.llen(queue_key)
                stats['pending'][priority.name.lower()] = count
                total_pending += count
            
            stats['pending']['total'] = total_pending
            return stats
        except Exception as e:
            self.logger.error(f"Failed to get stats from Redis: {e}")
            return {'pending': {'total': 0}, 'running': 0, 'completed': 0, 'failed': 0}

    def get_pending_count(self) -> int:
        stats = self.get_stats()
        return stats['pending']['total']


class DistributedTaskExecutor:
    """分布式任务执行器"""
    
    def __init__(self, use_redis: bool = False, max_workers: int = 128,
                 redis_url: str = "redis://localhost:6379/0"):
        self.max_workers = max_workers
        self.task_handlers: Dict[str, Callable] = {}
        self.is_running = False
        self.worker_threads: List[threading.Thread] = []
        
        # 选择队列实现
        if use_redis and HAS_REDIS:
            self.queue = RedisTaskQueue(redis_url)
            self.logger = logging.getLogger(__name__)
            self.logger.info("Using Redis distributed task queue")
        else:
            self.queue = LocalTaskQueue()
            self.logger = logging.getLogger(__name__)
            self.logger.info("Using local task queue")
        
        self.lock = threading.RLock()

    def register_handler(self, task_type: str, handler: Callable):
        """注册任务处理器"""
        self.task_handlers[task_type] = handler
        self.logger.info(f"Handler registered for: {task_type}")

    def submit_task(self, task_type: str, description: str = "",
                     parameters: Dict[str, Any] = None,
                     priority: TaskPriority = TaskPriority.NORMAL,
                     weight: float = 1.0,
                     max_retries: int = 3) -> str:
        """提交任务到队列"""
        task = DistributedTask(
            task_type=task_type,
            description=description,
            parameters=parameters or {},
            priority=priority,
            weight=weight,
            max_retries=max_retries
        )
        self.queue.enqueue(task)
        return task.task_id

    def start(self, num_workers: int = 32):
        """启动工作线程"""
        if self.is_running:
            return
        
        self.is_running = True
        self.logger.info(f"Starting distributed task executor with {num_workers} workers")
        
        # 启动多个工作线程
        for i in range(num_workers):
            t = threading.Thread(target=self._worker_loop, daemon=True, name=f"Worker-{i}")
            t.start()
            self.worker_threads.append(t)

    def stop(self):
        """停止执行器"""
        self.is_running = False
        self.logger.info("Stopping distributed task executor")

    def _worker_loop(self):
        """工作线程循环"""
        while self.is_running:
            try:
                task = self.queue.dequeue()
                if task:
                    self._execute_task(task)
                else:
                    time.sleep(0.0001)  # 极短时间等待，提高吞吐量
            except Exception as e:
                self.logger.error(f"Worker error: {e}")

    def _execute_task(self, task: DistributedTask):
        """执行单个任务"""
        try:
            if task.task_type in self.task_handlers:
                handler = self.task_handlers[task.task_type]
                result = handler(task.parameters)
                self.queue.complete(task.task_id, result)
            else:
                raise ValueError(f"No handler for task type: {task.task_type}")
        except Exception as e:
            self.queue.fail(task.task_id, str(e))

    def get_statistics(self) -> Dict[str, Any]:
        """获取执行器统计"""
        stats = self.queue.get_stats()
        return stats

    def get_pending_count(self) -> int:
        """获取待处理任务数"""
        return self.queue.get_pending_count()
