"""
Persistent Evolution Agent - Package Initialization
"""

from core.dual_loop_drive_engine import DualLoopDriveEngine, PerpetualEvolutionController
from core.data_models_universal import (
    TaskExecution, TaskMetrics, OptimizationRule,
    KnowledgeNode, ExecutionContext, ExecutionLevel
)
from knowledge.knowledge_graph_integration import KnowledgeGraphIntegration
from execution.task_execution_engine import TaskExecutionEngine
from optimization.intelligent_optimization_system import IntelligenceOptimizationLayer
from synchronization.synchronization_landing_layer import SynchronizationLandingLayer
from core.guardian_perpetual_watcher import GuardianPerpetualWatcher

__version__ = "1.0.0"
__all__ = [
    'DualLoopDriveEngine',
    'PerpetualEvolutionController',
    'TaskExecution',
    'TaskMetrics',
    'OptimizationRule',
    'KnowledgeNode',
    'ExecutionContext',
    'ExecutionLevel',
    'KnowledgeGraphIntegration',
    'TaskExecutionEngine',
    'IntelligenceOptimizationLayer',
    'SynchronizationLandingLayer',
    'GuardianPerpetualWatcher'
]
