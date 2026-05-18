"""
Universal Data Models for Evolving Agent System
Provides standardized data structures for all components
"""
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Callable
from enum import Enum
from datetime import datetime
import uuid


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    OPTIMIZING = "optimizing"


class OptimizationStatus(Enum):
    ANALYZING = "analyzing"
    GENERATING = "generating"
    APPLYING = "applying"
    COMPLETED = "completed"


class ExecutionLevel(Enum):
    MINIMAL = 1
    STANDARD = 2
    ENHANCED = 3
    MAXIMUM = 4


@dataclass
class TaskMetrics:
    task_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration: float = 0.0
    success_count: int = 0
    failure_count: int = 0
    efficiency_score: float = 0.0
    resource_usage: Dict[str, float] = field(default_factory=dict)
    custom_metrics: Dict[str, Any] = field(default_factory=dict)

    def calculate_efficiency(self) -> float:
        if self.duration > 0:
            return self.success_count / (self.success_count + self.failure_count + 0.001)
        return 0.0


@dataclass
class OptimizationRule:
    rule_id: str
    name: str
    description: str
    target_component: str
    optimization_type: str
    parameters: Dict[str, Any]
    effectiveness_score: float = 0.0
    usage_count: int = 0
    created_at: datetime = field(default_factory=datetime.now)
    last_used: Optional[datetime] = None
    is_active: bool = True

    def activate(self):
        self.is_active = True
        self.last_used = datetime.now()
        self.usage_count += 1

    def deactivate(self):
        self.is_active = False


@dataclass
class TaskExecution:
    task_id: str
    task_type: str
    description: str
    status: TaskStatus
    priority: int = 5
    parameters: Dict[str, Any] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)
    result: Optional[Any] = None
    error: Optional[str] = None
    metrics: Optional[TaskMetrics] = None
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

    def mark_running(self):
        self.status = TaskStatus.RUNNING
        self.updated_at = datetime.now()

    def mark_completed(self, result: Any):
        self.status = TaskStatus.COMPLETED
        self.result = result
        self.updated_at = datetime.now()

    def mark_failed(self, error: str):
        self.status = TaskStatus.FAILED
        self.error = error
        self.updated_at = datetime.now()


@dataclass
class KnowledgeNode:
    node_id: str
    title: str
    content: str
    node_type: str
    tags: List[str] = field(default_factory=list)
    relationships: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    confidence: float = 1.0
    created_at: datetime = field(default_factory=datetime.now)
    last_accessed: datetime = field(default_factory=datetime.now)
    access_count: int = 0

    def touch(self):
        self.last_accessed = datetime.now()
        self.access_count += 1


@dataclass
class ExecutionContext:
    context_id: str
    current_tasks: List[TaskExecution] = field(default_factory=list)
    active_rules: List[OptimizationRule] = field(default_factory=list)
    execution_level: ExecutionLevel = ExecutionLevel.STANDARD
    iteration_count: int = 0
    optimization_iteration: int = 0
    total_execution_time: float = 0.0
    start_time: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GuardianState:
    is_alive: bool = True
    restart_count: int = 0
    last_health_check: datetime = field(default_factory=datetime.now)
    active_threads: List[str] = field(default_factory=list)
    exceptions_caught: List[str] = field(default_factory=list)
    system_health: float = 1.0


class TaskQueue:
    def __init__(self):
        self.pending: List[TaskExecution] = []
        self.running: List[TaskExecution] = []
        self.completed: List[TaskExecution] = []
        self.failed: List[TaskExecution] = []

    def add_task(self, task: TaskExecution):
        self.pending.append(task)

    def get_next_task(self) -> Optional[TaskExecution]:
        if self.pending:
            task = self.pending.pop(0)
            self.running.append(task)
            return task
        return None

    def complete_task(self, task_id: str, result: Any):
        for task in self.running:
            if task.task_id == task_id:
                task.mark_completed(result)
                self.running.remove(task)
                self.completed.append(task)
                return True
        return False

    def fail_task(self, task_id: str, error: str):
        for task in self.running:
            if task.task_id == task_id:
                task.mark_failed(error)
                self.running.remove(task)
                self.failed.append(task)
                return True
        return False

    def get_queue_stats(self) -> Dict[str, int]:
        return {
            "pending": len(self.pending),
            "running": len(self.running),
            "completed": len(self.completed),
            "failed": len(self.failed)
        }


def generate_task_id() -> str:
    return f"task_{uuid.uuid4().hex[:12]}"


def generate_rule_id() -> str:
    return f"rule_{uuid.uuid4().hex[:12]}"


def generate_node_id() -> str:
    return f"node_{uuid.uuid4().hex[:12]}"
