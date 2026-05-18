"""
Intelligent Optimization System - 智能优化层
Real-time analysis and continuous improvement engine
"""
import threading
import time
import logging
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime, timedelta
from collections import deque, defaultdict
import statistics
import json

from core.data_models_universal import (
    OptimizationRule, TaskMetrics, OptimizationStatus,
    generate_rule_id
)


class IntelligenceOptimizationLayer:
    def __init__(self, analysis_interval: float = 60.0):
        self.analysis_interval = analysis_interval
        self.optimization_rules: List[OptimizationRule] = []
        self.performance_history: deque = deque(maxlen=500)
        self.bottleneck_detector = BottleneckDetector()
        self.efficiency_analyzer = EfficiencyAnalyzer()
        self.strategy_generator = StrategyGenerator()
        self.is_optimizing = False
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
        self.optimization_callbacks: List[Callable] = []
        self.metrics_collectors: List[Callable] = []

    def start_optimization(self):
        if not self.is_optimizing:
            self.is_optimizing = True
            threading.Thread(target=self._optimization_loop, daemon=True).start()
            self.logger.info("Intelligence optimization layer started")

    def stop_optimization(self):
        self.is_optimizing = False
        self.logger.info("Intelligence optimization layer stopped")

    def register_callback(self, callback: Callable):
        self.optimization_callbacks.append(callback)

    def register_metrics_collector(self, collector: Callable):
        self.metrics_collectors.append(collector)

    def _optimization_loop(self):
        while self.is_optimizing:
            try:
                metrics_data = self._collect_metrics()
                self._analyze_performance(metrics_data)
                bottlenecks = self.bottleneck_detector.detect(metrics_data)
                if bottlenecks:
                    rules = self.strategy_generator.generate_rules(bottlenecks)
                    for rule in rules:
                        self._apply_optimization_rule(rule)
                self._notify_callbacks()
                time.sleep(self.analysis_interval)
            except Exception as e:
                self.logger.error(f"Optimization loop error: {e}")

    def _collect_metrics(self) -> Dict[str, Any]:
        metrics = {}
        for collector in self.metrics_collectors:
            try:
                data = collector()
                metrics.update(data)
            except Exception as e:
                self.logger.error(f"Metrics collection error: {e}")
        return metrics

    def _analyze_performance(self, metrics_data: Dict[str, Any]):
        analysis_result = self.efficiency_analyzer.analyze(metrics_data)
        self.performance_history.append({
            'timestamp': datetime.now(),
            'metrics': metrics_data,
            'analysis': analysis_result
        })
        return analysis_result

    def _apply_optimization_rule(self, rule: OptimizationRule):
        with self.lock:
            existing = [r for r in self.optimization_rules if r.name == rule.name]
            if existing:
                existing[0].parameters.update(rule.parameters)
                existing[0].effectiveness_score = rule.effectiveness_score
            else:
                rule.rule_id = generate_rule_id()
                self.optimization_rules.append(rule)
        self.logger.info(f"Optimization rule applied: {rule.name}")

    def _notify_callbacks(self):
        for callback in self.optimization_callbacks:
            try:
                callback(self.get_active_rules())
            except Exception as e:
                self.logger.error(f"Callback error: {e}")

    def get_active_rules(self) -> List[OptimizationRule]:
        with self.lock:
            return [r for r in self.optimization_rules if r.is_active]

    def get_optimization_stats(self) -> Dict[str, Any]:
        with self.lock:
            return {
                'total_rules': len(self.optimization_rules),
                'active_rules': len(self.get_active_rules()),
                'performance_history': len(self.performance_history),
                'bottleneck_stats': self.bottleneck_detector.get_stats()
            }


class BottleneckDetector:
    def __init__(self, threshold: float = 0.7):
        self.threshold = threshold
        self.bottleneck_history: deque = deque(maxlen=100)
        self.detection_count = defaultdict(int)
        self.logger = logging.getLogger(__name__)

    def detect(self, metrics_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        bottlenecks = []
        if 'execution_time' in metrics_data:
            if metrics_data['execution_time'] > 5.0:
                bottlenecks.append({
                    'type': 'slow_execution',
                    'severity': 'high',
                    'metric': 'execution_time',
                    'value': metrics_data['execution_time']
                })
                self.detection_count['slow_execution'] += 1
        if 'failure_rate' in metrics_data:
            if metrics_data['failure_rate'] > self.threshold:
                bottlenecks.append({
                    'type': 'high_failure_rate',
                    'severity': 'critical',
                    'metric': 'failure_rate',
                    'value': metrics_data['failure_rate']
                })
                self.detection_count['high_failure_rate'] += 1
        if 'queue_size' in metrics_data:
            if metrics_data['queue_size'] > 20:
                bottlenecks.append({
                    'type': 'queue_overflow',
                    'severity': 'medium',
                    'metric': 'queue_size',
                    'value': metrics_data['queue_size']
                })
                self.detection_count['queue_overflow'] += 1
        if 'memory_usage' in metrics_data:
            if metrics_data['memory_usage'] > 0.8:
                bottlenecks.append({
                    'type': 'memory_pressure',
                    'severity': 'high',
                    'metric': 'memory_usage',
                    'value': metrics_data['memory_usage']
                })
                self.detection_count['memory_pressure'] += 1
        for bottleneck in bottlenecks:
            self.bottleneck_history.append(bottleneck)
        return bottlenecks

    def get_stats(self) -> Dict[str, int]:
        return dict(self.detection_count)


class EfficiencyAnalyzer:
    def __init__(self):
        self.analysis_window = timedelta(hours=1)
        self.efficiency_scores: deque = deque(maxlen=100)

    def analyze(self, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        efficiency = 1.0
        improvements = []
        if 'success_count' in metrics_data and 'failure_count' in metrics_data:
            total = metrics_data['success_count'] + metrics_data['failure_count']
            if total > 0:
                efficiency = metrics_data['success_count'] / total
                self.efficiency_scores.append(efficiency)
        if 'execution_time' in metrics_data:
            if metrics_data['execution_time'] > 10.0:
                improvements.append('optimize_execution_path')
        if 'resource_usage' in metrics_data:
            if metrics_data['resource_usage'] > 0.9:
                improvements.append('reduce_resource_consumption')
        return {
            'efficiency': efficiency,
            'improvements': improvements,
            'average_efficiency': statistics.mean(self.efficiency_scores) if self.efficiency_scores else 1.0
        }


class StrategyGenerator:
    def __init__(self):
        self.rule_templates = {
            'slow_execution': {
                'name': 'execution_speed_optimization',
                'description': 'Optimize slow execution paths',
                'optimization_type': 'performance',
                'parameters': {'timeout_adjustment': 0.8}
            },
            'high_failure_rate': {
                'name': 'reliability_improvement',
                'description': 'Improve system reliability',
                'optimization_type': 'reliability',
                'parameters': {'retry_limit': 3}
            },
            'queue_overflow': {
                'name': 'queue_management_optimization',
                'description': 'Optimize queue processing',
                'optimization_type': 'throughput',
                'parameters': {'batch_size': 10}
            },
            'memory_pressure': {
                'name': 'memory_efficiency_optimization',
                'description': 'Reduce memory usage',
                'optimization_type': 'resource',
                'parameters': {'gc_frequency': 0.5}
            }
        }

    def generate_rules(self, bottlenecks: List[Dict[str, Any]]) -> List[OptimizationRule]:
        rules = []
        for bottleneck in bottlenecks:
            bottleneck_type = bottleneck['type']
            if bottleneck_type in self.rule_templates:
                template = self.rule_templates[bottleneck_type]
                rule = OptimizationRule(
                    rule_id=generate_rule_id(),
                    name=template['name'],
                    description=template['description'],
                    target_component=bottleneck_type,
                    optimization_type=template['optimization_type'],
                    parameters=template['parameters'].copy(),
                    effectiveness_score=0.8
                )
                rules.append(rule)
        return rules


class ContinuousImprovementEngine:
    def __init__(self, optimization_layer: IntelligenceOptimizationLayer):
        self.optimization_layer = optimization_layer
        self.improvement_history: deque = deque(maxlen=50)
        self.iteration_count = 0
        self.logger = logging.getLogger(__name__)

    def run_improvement_cycle(self):
        self.iteration_count += 1
        current_rules = self.optimization_layer.get_active_rules()
        metrics = self._gather_current_metrics()
        improvements = self._evaluate_improvements(current_rules, metrics)
        for improvement in improvements:
            self._apply_improvement(improvement)
        self.improvement_history.append({
            'iteration': self.iteration_count,
            'improvements': len(improvements),
            'timestamp': datetime.now()
        })
        return improvements

    def _gather_current_metrics(self) -> Dict[str, Any]:
        return {
            'timestamp': datetime.now(),
            'iteration': self.iteration_count
        }

    def _evaluate_improvements(self, rules: List[OptimizationRule],
                              metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        improvements = []
        for rule in rules:
            if rule.effectiveness_score < 0.7:
                improvements.append({
                    'rule_id': rule.rule_id,
                    'action': 'adjust_parameters',
                    'reason': 'low_effectiveness'
                })
        return improvements

    def _apply_improvement(self, improvement: Dict[str, Any]):
        self.logger.info(f"Applying improvement: {improvement}")


class AdaptiveParameterTuner:
    def __init__(self):
        self.parameters: Dict[str, float] = {}
        self.tuning_history: deque = deque(maxlen=100)
        self.convergence_threshold = 0.01

    def tune_parameter(self, name: str, current_value: float,
                      performance_score: float) -> float:
        if name not in self.parameters:
            self.parameters[name] = current_value
        adjustment = 0.1 if performance_score < 0.8 else -0.05
        new_value = current_value * (1 + adjustment)
        self.parameters[name] = new_value
        self.tuning_history.append({
            'parameter': name,
            'old_value': current_value,
            'new_value': new_value,
            'score': performance_score,
            'timestamp': datetime.now()
        })
        return new_value

    def get_optimal_parameters(self) -> Dict[str, float]:
        return self.parameters.copy()
