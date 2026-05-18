"""
Guardian Perpetual Watcher - 常驻守护层
Continuous health monitoring and automatic recovery system
"""
import threading
import time
import logging
from datetime import datetime
from typing import Dict, List, Callable, Any, Optional
from collections import deque
import traceback

from .data_models_universal import GuardianState


class GuardianPerpetualWatcher:
    def __init__(self, health_check_interval: float = 5.0):
        self.state = GuardianState()
        self.health_check_interval = health_check_interval
        self.components: Dict[str, 'HealthMonitorable'] = {}
        self.health_history: deque = deque(maxlen=100)
        self.exception_history: deque = deque(maxlen=50)
        self.is_monitoring = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
        self.on_critical_failure: Optional[Callable] = None
        self.recovery_strategies: Dict[str, Callable] = {}

    def register_component(self, name: str, component: 'HealthMonitorable'):
        with self.lock:
            self.components[name] = component
            self.logger.info(f"Component registered: {name}")

    def register_recovery_strategy(self, component_name: str, strategy: Callable):
        with self.lock:
            self.recovery_strategies[component_name] = strategy
            self.logger.info(f"Recovery strategy registered for: {component_name}")

    def start_monitoring(self):
        if not self.is_monitoring:
            self.is_monitoring = True
            self.monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
            self.monitor_thread.start()
            self.logger.info("Guardian monitoring started")

    def stop_monitoring(self):
        self.is_monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=10)
        self.logger.info("Guardian monitoring stopped")

    def _monitoring_loop(self):
        while self.is_monitoring:
            try:
                self._perform_health_check()
                time.sleep(self.health_check_interval)
            except Exception as e:
                self._handle_exception("monitoring_loop", e)

    def _perform_health_check(self):
        with self.lock:
            all_healthy = True
            component_health: Dict[str, float] = {}

            for name, component in self.components.items():
                try:
                    is_healthy, metrics = component.check_health()
                    component_health[name] = 1.0 if is_healthy else 0.0

                    if not is_healthy:
                        all_healthy = False
                        self.logger.warning(f"Unhealthy component detected: {name}")
                        self._attempt_recovery(name, component, metrics)
                except Exception as e:
                    self._handle_exception(f"health_check_{name}", e)
                    component_health[name] = 0.0
                    all_healthy = False

            overall_health = sum(component_health.values()) / len(component_health) if component_health else 1.0
            self.state.system_health = overall_health
            self.state.last_health_check = datetime.now()
            self.health_history.append({
                'timestamp': datetime.now(),
                'health': overall_health,
                'components': component_health.copy()
            })

            if not all_healthy:
                self.logger.warning(f"System health: {overall_health:.2%}")

    def _attempt_recovery(self, component_name: str, component: 'HealthMonitorable', metrics: Dict[str, Any]):
        try:
            if component_name in self.recovery_strategies:
                self.logger.info(f"Attempting recovery for: {component_name}")
                success = self.recovery_strategies[component_name](component, metrics)
                if success:
                    self.logger.info(f"Recovery successful for: {component_name}")
                else:
                    self.logger.error(f"Recovery failed for: {component_name}")
                    self._handle_critical_failure(component_name)
            else:
                self.logger.info(f"Auto-restarting component: {component_name}")
                component.restart()
                self.state.restart_count += 1
        except Exception as e:
            self._handle_exception(f"recovery_{component_name}", e)
            self._handle_critical_failure(component_name)

    def _handle_critical_failure(self, component_name: str):
        self.logger.critical(f"Critical failure in component: {component_name}")
        self.exception_history.append({
            'timestamp': datetime.now(),
            'component': component_name,
            'error': 'Critical failure'
        })

        if self.on_critical_failure:
            try:
                self.on_critical_failure(component_name)
            except Exception as e:
                self.logger.error(f"Critical failure handler error: {e}")

    def _handle_exception(self, context: str, exception: Exception):
        error_msg = f"{context}: {str(exception)}\n{traceback.format_exc()}"
        self.logger.error(error_msg)
        self.exception_history.append({
            'timestamp': datetime.now(),
            'context': context,
            'error': str(exception)
        })
        self.state.exceptions_caught.append(error_msg)

    def get_system_status(self) -> Dict[str, Any]:
        with self.lock:
            return {
                'is_alive': self.state.is_alive,
                'health': self.state.system_health,
                'restart_count': self.state.restart_count,
                'registered_components': list(self.components.keys()),
                'recent_exceptions': len(self.exception_history),
                'health_history': len(self.health_history)
            }

    def force_recovery_all(self):
        with self.lock:
            for name, component in self.components.items():
                try:
                    component.restart()
                    self.state.restart_count += 1
                except Exception as e:
                    self._handle_exception(f"force_recovery_{name}", e)


class HealthMonitorable:
    def check_health(self) -> tuple[bool, Dict[str, Any]]:
        raise NotImplementedError

    def restart(self):
        raise NotImplementedError


class ComponentHealthMonitor(HealthMonitorable):
    def __init__(self, name: str, check_fn: Callable[[], bool], restart_fn: Callable[[], None]):
        self.name = name
        self.check_fn = check_fn
        self.restart_fn = restart_fn
        self.last_check_time: Optional[datetime] = None
        self.consecutive_failures = 0

    def check_health(self) -> tuple[bool, Dict[str, Any]]:
        try:
            is_healthy = self.check_fn()
            self.last_check_time = datetime.now()

            if not is_healthy:
                self.consecutive_failures += 1
            else:
                self.consecutive_failures = 0

            return is_healthy, {
                'consecutive_failures': self.consecutive_failures,
                'last_check': self.last_check_time
            }
        except Exception as e:
            self.consecutive_failures += 1
            return False, {'error': str(e), 'consecutive_failures': self.consecutive_failures}

    def restart(self):
        self.consecutive_failures = 0
        self.restart_fn()


class MultiLevelGuardian:
    def __init__(self):
        self.primary_guardian = GuardianPerpetualWatcher(health_check_interval=5.0)
        self.secondary_guardian = GuardianPerpetualWatcher(health_check_interval=30.0)
        self.is_running = False
        self.logger = logging.getLogger(__name__)

    def register_primary_component(self, name: str, component: HealthMonitorable):
        self.primary_guardian.register_component(name, component)

    def register_secondary_component(self, name: str, component: HealthMonitorable):
        self.secondary_guardian.register_component(name, component)

    def start(self):
        self.is_running = True
        self.primary_guardian.start_monitoring()
        self.secondary_guardian.start_monitoring()
        self.logger.info("Multi-level guardian system started")

    def stop(self):
        self.is_running = False
        self.primary_guardian.stop_monitoring()
        self.secondary_guardian.stop_monitoring()
        self.logger.info("Multi-level guardian system stopped")

    def get_full_status(self) -> Dict[str, Any]:
        return {
            'primary': self.primary_guardian.get_system_status(),
            'secondary': self.secondary_guardian.get_system_status(),
            'is_running': self.is_running
        }
