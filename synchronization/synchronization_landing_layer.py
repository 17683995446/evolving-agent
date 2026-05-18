"""
Synchronization Landing Layer - 同步落地层
Real-time rule application and execution integration
"""
import threading
import time
import logging
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime, timedelta
from collections import deque
import json

from core.data_models_universal import OptimizationRule


class SynchronizationLandingLayer:
    def __init__(self):
        self.pending_applications: deque = deque(maxlen=100)
        self.active_applications: Dict[str, 'RuleApplication'] = {}
        self.application_history: deque = deque(maxlen=500)
        self.target_handlers: Dict[str, Callable] = {}
        self.is_active = False
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
        self.sync_interval = 1.0
        self.conflict_resolver = ConflictResolver()

    def register_target_handler(self, component_name: str, handler: Callable):
        self.target_handlers[component_name] = handler
        self.logger.info(f"Target handler registered: {component_name}")

    def start_synchronization(self):
        if not self.is_active:
            self.is_active = True
            threading.Thread(target=self._synchronization_loop, daemon=True).start()
            self.logger.info("Synchronization landing layer started")

    def stop_synchronization(self):
        self.is_active = False
        self.logger.info("Synchronization landing layer stopped")

    def queue_rule_application(self, rule: OptimizationRule, priority: int = 5):
        application = RuleApplication(
            application_id=f"app_{len(self.pending_applications)}",
            rule=rule,
            priority=priority,
            queued_at=datetime.now()
        )
        with self.lock:
            self.pending_applications.append(application)
        self.logger.debug(f"Rule queued for application: {rule.name}")

    def _synchronization_loop(self):
        while self.is_active:
            try:
                self._process_pending_applications()
                self._monitor_active_applications()
                self._cleanup_completed_applications()
                time.sleep(self.sync_interval)
            except Exception as e:
                self.logger.error(f"Synchronization loop error: {e}")

    def _process_pending_applications(self):
        with self.lock:
            applications = sorted(
                self.pending_applications,
                key=lambda x: (x.priority, x.queued_at),
                reverse=True
            )

            for application in applications[:5]:
                self._apply_rule(application)
                self.pending_applications.remove(application)

    def _apply_rule(self, application: 'RuleApplication'):
        rule = application.rule
        application.status = 'applying'
        application.started_at = datetime.now()

        try:
            conflicts = self._detect_conflicts(rule)
            if conflicts:
                resolved_rule = self.conflict_resolver.resolve(rule, conflicts)
                rule = resolved_rule

            if rule.target_component in self.target_handlers:
                handler = self.target_handlers[rule.target_component]
                success = handler(rule)

                if success:
                    application.status = 'completed'
                    application.completed_at = datetime.now()
                    application.success = True
                    rule.activate()
                    self.logger.info(f"Rule successfully applied: {rule.name}")
                else:
                    application.status = 'failed'
                    application.error = 'Handler returned failure'
            else:
                application.status = 'failed'
                application.error = f'No handler for: {rule.target_component}'

        except Exception as e:
            application.status = 'failed'
            application.error = str(e)
            self.logger.error(f"Rule application failed: {rule.name} - {e}")
        finally:
            self.active_applications[application.application_id] = application
            self.application_history.append(application)

    def _detect_conflicts(self, rule: OptimizationRule) -> List[OptimizationRule]:
        conflicts = []
        for app in self.active_applications.values():
            if app.rule.target_component == rule.target_component:
                if self._rules_conflict(rule, app.rule):
                    conflicts.append(app.rule)
        return conflicts

    def _rules_conflict(self, rule1: OptimizationRule, rule2: OptimizationRule) -> bool:
        if rule1.optimization_type != rule2.optimization_type:
            return False
        for key in rule1.parameters:
            if key in rule2.parameters:
                if rule1.parameters[key] != rule2.parameters[key]:
                    return True
        return False

    def _monitor_active_applications(self):
        timeout_threshold = datetime.now() - timedelta(seconds=30)
        for app_id, application in list(self.active_applications.items()):
            if application.status == 'applying' and application.started_at < timeout_threshold:
                application.status = 'timeout'
                application.error = 'Application timed out'

    def _cleanup_completed_applications(self):
        completed_statuses = ['completed', 'failed', 'timeout']
        to_remove = [
            app_id for app_id, app in self.active_applications.items()
            if app.status in completed_statuses
        ]
        for app_id in to_remove:
            del self.active_applications[app_id]

    def get_synchronization_status(self) -> Dict[str, Any]:
        with self.lock:
            return {
                'pending_count': len(self.pending_applications),
                'active_count': len(self.active_applications),
                'total_applications': len(self.application_history),
                'is_active': self.is_active,
                'recent_failures': len([a for a in self.application_history if a.status == 'failed'])
            }


class RuleApplication:
    def __init__(self, application_id: str, rule: OptimizationRule, priority: int, queued_at: datetime):
        self.application_id = application_id
        self.rule = rule
        self.priority = priority
        self.status = 'queued'
        self.queued_at = queued_at
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.success = False
        self.error: Optional[str] = None


class ConflictResolver:
    def __init__(self):
        self.resolution_strategies = {
            'newer_wins': self._newer_wins,
            'higher_priority_wins': self._higher_priority_wins,
            'merge_parameters': self._merge_parameters
        }

    def resolve(self, new_rule: OptimizationRule, conflicts: List[OptimizationRule]) -> OptimizationRule:
        if not conflicts:
            return new_rule
        strategy = self.resolution_strategies['merge_parameters']
        return strategy(new_rule, conflicts)

    def _newer_wins(self, new_rule: OptimizationRule, conflicts: List[OptimizationRule]) -> OptimizationRule:
        return new_rule

    def _higher_priority_wins(self, new_rule: OptimizationRule, conflicts: List[OptimizationRule]) -> OptimizationRule:
        max_priority = max(conflicts, key=lambda r: r.effectiveness_score)
        if max_priority.effectiveness_score > new_rule.effectiveness_score:
            return max_priority
        return new_rule

    def _merge_parameters(self, new_rule: OptimizationRule, conflicts: List[OptimizationRule]) -> OptimizationRule:
        merged_params = new_rule.parameters.copy()
        for conflict in conflicts:
            for key, value in conflict.parameters.items():
                if key not in merged_params:
                    merged_params[key] = value
        new_rule.parameters = merged_params
        return new_rule


class RealTimeRuleEmitter:
    def __init__(self, landing_layer: SynchronizationLandingLayer):
        self.landing_layer = landing_layer
        self.emitters: List[Callable] = []
        self.emission_history: deque = deque(maxlen=100)
        self.logger = logging.getLogger(__name__)

    def register_emitter(self, emitter: Callable):
        self.emitters.append(emitter)

    def emit_rules(self, rules: List[OptimizationRule]):
        for rule in rules:
            self.landing_layer.queue_rule_application(rule)
            self.emission_history.append({
                'rule': rule.name,
                'timestamp': datetime.now()
            })
            self.logger.debug(f"Rule emitted: {rule.name}")

    def emit_instant(self, rule: OptimizationRule):
        self.landing_layer.queue_rule_application(rule, priority=10)


class RuleEffectivenessTracker:
    def __init__(self):
        self.tracked_rules: Dict[str, List[float]] = {}
        self.effectiveness_threshold = 0.7

    def track(self, rule: OptimizationRule, performance_delta: float):
        rule_id = rule.rule_id
        if rule_id not in self.tracked_rules:
            self.tracked_rules[rule_id] = []
        self.tracked_rules[rule_id].append(performance_delta)

    def get_effectiveness(self, rule: OptimizationRule) -> float:
        rule_id = rule.rule_id
        if rule_id not in self.tracked_rules or not self.tracked_rules[rule_id]:
            return 0.5
        return sum(self.tracked_rules[rule_id]) / len(self.tracked_rules[rule_id])

    def should_deactivate(self, rule: OptimizationRule) -> bool:
        effectiveness = self.get_effectiveness(rule)
        return effectiveness < self.effectiveness_threshold
