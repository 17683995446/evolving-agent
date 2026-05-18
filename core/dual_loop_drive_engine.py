"""
Dual Loop Drive Engine - 双循环驱动引擎
Core orchestration engine for perpetual execution + optimization
"""
import threading
import time
import logging
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
from collections import deque
import traceback

from core.guardian_perpetual_watcher import (
    GuardianPerpetualWatcher, 
    MultiLevelGuardian,
    ComponentHealthMonitor
)
from execution.task_execution_engine import (
    TaskExecutionEngine,
    AdaptiveTaskScheduler,
    TaskBatchingProcessor
)
from optimization.intelligent_optimization_system import (
    IntelligenceOptimizationLayer,
    ContinuousImprovementEngine,
    AdaptiveParameterTuner
)
from synchronization.synchronization_landing_layer import (
    SynchronizationLandingLayer,
    RealTimeRuleEmitter,
    RuleEffectivenessTracker
)
from knowledge.knowledge_graph_integration import (
    KnowledgeGraphIntegration,
    ExperienceExtractor
)
from core.data_models_universal import (
    ExecutionContext,
    ExecutionLevel,
    OptimizationRule,
    generate_task_id
)


class DualLoopDriveEngine:
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.guardian = MultiLevelGuardian()
        self.task_engine = TaskExecutionEngine(
            max_workers=self.config.get('max_workers', 4)
        )
        self.task_scheduler = AdaptiveTaskScheduler(self.task_engine)
        self.task_batcher = TaskBatchingProcessor(
            batch_size=self.config.get('batch_size', 10)
        )
        self.optimization_layer = IntelligenceOptimizationLayer(
            analysis_interval=self.config.get('optimization_interval', 60.0)
        )
        self.improvement_engine = ContinuousImprovementEngine(self.optimization_layer)
        self.parameter_tuner = AdaptiveParameterTuner()
        self.synchronization_layer = SynchronizationLandingLayer()
        self.rule_emitter = RealTimeRuleEmitter(self.synchronization_layer)
        self.effectiveness_tracker = RuleEffectivenessTracker()
        self.knowledge_graph = KnowledgeGraphIntegration(
            db_path=self.config.get('knowledge_db_path', 'knowledge_graph.db')
        )
        self.experience_extractor = ExperienceExtractor(self.knowledge_graph)
        self.context = ExecutionContext(
            context_id=generate_task_id(),
            execution_level=ExecutionLevel.STANDARD
        )
        self.is_running = False
        self.main_loop_thread: Optional[threading.Thread] = None
        self.optimization_thread: Optional[threading.Thread] = None
        self.logger = logging.getLogger(__name__)
        self.execution_metrics: deque = deque(maxlen=1000)
        self.optimization_cycles = 0
        self.total_tasks_completed = 0

    def initialize(self):
        self.logger.info("Initializing Dual Loop Drive Engine...")
        self.knowledge_graph.initialize()
        self._setup_layer_connections()
        self._register_components_with_guardian()
        self._register_optimization_callbacks()
        self._register_target_handlers()
        self.logger.info("Dual Loop Drive Engine initialized successfully")

    def _setup_layer_connections(self):
        self.task_engine.register_handler('optimization_task', self._optimization_task_handler)
        self.task_engine.register_handler('knowledge_query', self._knowledge_query_handler)

    def _register_components_with_guardian(self):
        self.guardian.register_primary_component(
            'task_execution',
            ComponentHealthMonitor(
                name='task_execution',
                check_fn=lambda: len(self.task_engine.task_queue.running) < 100,
                restart_fn=self.task_engine.stop
            )
        )
        self.guardian.register_primary_component(
            'optimization_layer',
            ComponentHealthMonitor(
                name='optimization_layer',
                check_fn=lambda: self.optimization_layer.is_optimizing,
                restart_fn=lambda: self.optimization_layer.start_optimization()
            )
        )
        self.guardian.register_primary_component(
            'synchronization_layer',
            ComponentHealthMonitor(
                name='synchronization_layer',
                check_fn=lambda: self.synchronization_layer.is_active,
                restart_fn=lambda: self.synchronization_layer.start_synchronization()
            )
        )

    def _register_optimization_callbacks(self):
        def on_optimization_complete(rules: List[OptimizationRule]):
            self.logger.info(f"New optimization rules generated: {len(rules)}")
            self.rule_emitter.emit_rules(rules)
        self.optimization_layer.register_callback(on_optimization_complete)

    def _register_target_handlers(self):
        def handle_task_engine_rules(rule: OptimizationRule) -> bool:
            if rule.target_component == 'task_execution':
                if 'max_workers' in rule.parameters:
                    self.task_engine.max_workers = rule.parameters['max_workers']
                return True
            return False
        self.synchronization_layer.register_target_handler('task_execution', handle_task_engine_rules)

    def start(self):
        if self.is_running:
            self.logger.warning("Engine is already running")
            return
        self.is_running = True
        self.logger.info("Starting Dual Loop Drive Engine...")
        # Start components first so health checks pass
        self.task_engine.start()
        self.optimization_layer.start_optimization()
        self.synchronization_layer.start_synchronization()
        # Start guardian after components are healthy
        self.guardian.start()
        self.main_loop_thread = threading.Thread(target=self._main_loop, daemon=True)
        self.main_loop_thread.start()
        self.logger.info("Dual Loop Drive Engine started - Perpetual execution mode")

    def stop(self):
        self.is_running = False
        self.logger.info("Stopping Dual Loop Drive Engine...")
        self.task_engine.stop()
        self.optimization_layer.stop_optimization()
        self.synchronization_layer.stop_synchronization()
        self.guardian.stop()
        if self.main_loop_thread:
            self.main_loop_thread.join(timeout=10)
        self.logger.info("Dual Loop Drive Engine stopped")

    def _main_loop(self):
        iteration = 0
        while self.is_running:
            try:
                iteration += 1
                self.context.iteration_count = iteration
                self._execute_tasks_phase()
                self._optimization_phase()
                self._record_metrics()
                time.sleep(0.1)
            except Exception as e:
                self._handle_main_loop_exception(iteration, e)

    def _execute_tasks_phase(self):
        queue_stats = self.task_engine.task_queue.get_queue_stats()
        if queue_stats['pending'] > 0:
            completed_before = queue_stats['completed']
            time.sleep(0.05)
            queue_stats_after = self.task_engine.task_queue.get_queue_stats()
            self.total_tasks_completed = queue_stats_after['completed']

    def _optimization_phase(self):
        self.optimization_cycles += 1
        self.context.optimization_iteration = self.optimization_cycles
        self.improvement_engine.run_improvement_cycle()
        active_rules = self.optimization_layer.get_active_rules()
        self.context.active_rules = active_rules

    def _record_metrics(self):
        metrics = {
            'iteration': self.context.iteration_count,
            'optimization_cycle': self.optimization_cycles,
            'total_tasks': self.total_tasks_completed,
            'active_tasks': len(self.task_engine.task_queue.running),
            'pending_tasks': len(self.task_engine.task_queue.pending),
            'active_rules': len(self.context.active_rules),
            'timestamp': datetime.now()
        }
        self.execution_metrics.append(metrics)

    def _handle_main_loop_exception(self, iteration: int, exception: Exception):
        error_msg = f"Main loop iteration {iteration} failed: {str(exception)}\n{traceback.format_exc()}"
        self.logger.error(error_msg)
        self.knowledge_graph.add_node(
            title=f"Error in iteration {iteration}",
            content=error_msg,
            node_type="error",
            tags=["error", "exception"]
        )

    def _optimization_task_handler(self, parameters: Dict[str, Any]) -> Any:
        return self.improvement_engine.run_improvement_cycle()

    def _knowledge_query_handler(self, parameters: Dict[str, Any]) -> Any:
        query = parameters.get('query', '')
        max_results = parameters.get('max_results', 5)
        return self.knowledge_graph.query(query, max_results)

    def add_task(self, task_type: str, description: str, 
                parameters: Dict[str, Any] = None) -> str:
        return self.task_scheduler.schedule_with_priority(
            task_type, description, parameters
        )

    def query_knowledge(self, query: str, max_results: int = 5) -> List[Any]:
        return self.knowledge_graph.query(query, max_results)

    def integrate_experience(self, experience_type: str, content: str):
        return self.knowledge_graph.integrate_experience(
            experience_type, content
        )

    def integrate_project(self, project_name: str, description: str,
                        technologies: List[str], outcomes: str):
        return self.knowledge_graph.integrate_project(
            project_name, description, technologies, outcomes
        )

    def get_system_status(self) -> Dict[str, Any]:
        return {
            'is_running': self.is_running,
            'guardian_status': self.guardian.get_full_status(),
            'task_engine_stats': self.task_engine.get_statistics(),
            'optimization_stats': self.optimization_layer.get_optimization_stats(),
            'synchronization_stats': self.synchronization_layer.get_synchronization_status(),
            'knowledge_stats': self.knowledge_graph.get_statistics(),
            'execution_context': {
                'iteration': self.context.iteration_count,
                'optimization_cycle': self.context.optimization_iteration,
                'execution_level': self.context.execution_level.name
            }
        }

    def get_execution_metrics(self, limit: int = 100) -> List[Dict[str, Any]]:
        return list(self.execution_metrics)[-limit:]

    def adjust_execution_level(self, level: ExecutionLevel):
        self.context.execution_level = level
        self.task_engine.optimize_execution_level(level)
        self.logger.info(f"Execution level adjusted to: {level.name}")


class PerpetualEvolutionController:
    def __init__(self, engine: DualLoopDriveEngine):
        self.engine = engine
        self.evolution_enabled = True
        self.evolution_thread: Optional[threading.Thread] = None
        self.logger = logging.getLogger(__name__)

    def start_evolution(self):
        if not self.evolution_enabled:
            return
        self.evolution_thread = threading.Thread(target=self._evolution_loop, daemon=True)
        self.evolution_thread.start()
        self.logger.info("Perpetual evolution controller started")

    def _evolution_loop(self):
        while self.evolution_enabled:
            try:
                self._evolve_execution_strategy()
                self._evolve_optimization_parameters()
                self._evolve_knowledge_integration()
                time.sleep(300)
            except Exception as e:
                self.logger.error(f"Evolution loop error: {e}")

    def _evolve_execution_strategy(self):
        metrics = self.engine.get_execution_metrics(10)
        if metrics:
            avg_pending = sum(m['pending_tasks'] for m in metrics) / len(metrics)
            if avg_pending > 15:
                self.engine.adjust_execution_level(ExecutionLevel.ENHANCED)
            elif avg_pending < 3:
                self.engine.adjust_execution_level(ExecutionLevel.MINIMAL)

    def _evolve_optimization_parameters(self):
        opt_stats = self.engine.optimization_layer.get_optimization_stats()
        if opt_stats['active_rules'] > 20:
            self.engine.parameter_tuner.tune_parameter(
                'optimization_interval',
                self.engine.optimization_layer.analysis_interval,
                0.8
            )

    def _evolve_knowledge_integration(self):
        know_stats = self.engine.knowledge_graph.get_statistics()
        if know_stats['total_nodes'] < 10:
            self._seed_initial_knowledge()

    def _seed_initial_knowledge(self):
        self.engine.integrate_experience(
            "system_initialization",
            "Evolved Agent System initialized and ready for continuous operation"
        )


class TaskFactory:
    @staticmethod
    def create_analysis_task(query: str) -> Dict[str, Any]:
        return {
            'task_type': 'analysis',
            'description': f"Analyze: {query}",
            'parameters': {'query': query}
        }

    @staticmethod
    def create_optimization_task(target: str) -> Dict[str, Any]:
        return {
            'task_type': 'optimization',
            'description': f"Optimize: {target}",
            'parameters': {'target': target}
        }

    @staticmethod
    def create_learning_task(content: str) -> Dict[str, Any]:
        return {
            'task_type': 'learning',
            'description': f"Learn: {content[:50]}",
            'parameters': {'content': content}
        }
