"""
高级监控与指标收集系统
提供性能指标、统计数据、告警和可视化支持
"""
import time
import logging
import threading
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable
from collections import deque
from dataclasses import dataclass, field
from enum import Enum


class MetricType(Enum):
    """指标类型"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


@dataclass
class Metric:
    """指标数据结构"""
    name: str
    type: MetricType
    value: float = 0.0
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    history: deque = field(default_factory=lambda: deque(maxlen=1000))

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'type': self.type.value,
            'value': self.value,
            'labels': self.labels,
            'timestamp': self.timestamp.isoformat()
        }


class AlertLevel(Enum):
    """告警级别"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class Alert:
    """告警数据结构"""
    id: str
    level: AlertLevel
    message: str
    source: str
    timestamp: datetime = field(default_factory=datetime.now)
    resolved: bool = False
    resolved_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'level': self.level.value,
            'message': self.message,
            'source': self.source,
            'timestamp': self.timestamp.isoformat(),
            'resolved': self.resolved,
            'resolved_at': self.resolved_at.isoformat() if self.resolved_at else None
        }


class MetricsCollector:
    """指标收集器"""
    
    def __init__(self):
        self.metrics: Dict[str, Metric] = {}
        self.alerts: List[Alert] = []
        self.alert_history: deque = deque(maxlen=1000)
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
        self.callbacks: List[Callable] = []

    def register_callback(self, callback: Callable[[Metric], None]):
        """注册指标回调"""
        with self.lock:
            self.callbacks.append(callback)

    def counter(self, name: str, value: float = 1.0, labels: Dict[str, str] = None):
        """增加计数器"""
        with self.lock:
            if name not in self.metrics:
                self.metrics[name] = Metric(name=name, type=MetricType.COUNTER, labels=labels or {})
            
            self.metrics[name].value += value
            self.metrics[name].timestamp = datetime.now()
            self.metrics[name].history.append((datetime.now(), self.metrics[name].value))
            self._notify_callbacks(self.metrics[name])

    def gauge(self, name: str, value: float, labels: Dict[str, str] = None):
        """设置仪表值"""
        with self.lock:
            if name not in self.metrics:
                self.metrics[name] = Metric(name=name, type=MetricType.GAUGE, labels=labels or {})
            
            self.metrics[name].value = value
            self.metrics[name].timestamp = datetime.now()
            self.metrics[name].history.append((datetime.now(), value))
            self._notify_callbacks(self.metrics[name])

    def histogram(self, name: str, value: float, labels: Dict[str, str] = None):
        """记录直方图值"""
        with self.lock:
            if name not in self.metrics:
                self.metrics[name] = Metric(name=name, type=MetricType.HISTOGRAM, labels=labels or {})
            
            self.metrics[name].value = value
            self.metrics[name].timestamp = datetime.now()
            self.metrics[name].history.append((datetime.now(), value))
            self._notify_callbacks(self.metrics[name])

    def timer(self, name: str, duration: float, labels: Dict[str, str] = None):
        """记录计时器"""
        self.histogram(name, duration, labels)

    def create_alert(self, level: AlertLevel, message: str, source: str = "system") -> str:
        """创建告警"""
        import uuid
        alert_id = str(uuid.uuid4())
        
        with self.lock:
            alert = Alert(
                id=alert_id,
                level=level,
                message=message,
                source=source
            )
            self.alerts.append(alert)
            self.alert_history.append(alert)
        
        level_logger = {
            AlertLevel.INFO: self.logger.info,
            AlertLevel.WARNING: self.logger.warning,
            AlertLevel.ERROR: self.logger.error,
            AlertLevel.CRITICAL: self.logger.critical
        }
        level_logger[level](f"ALERT [{level.name}]: {message} (source: {source})")
        
        return alert_id

    def resolve_alert(self, alert_id: str):
        """解决告警"""
        with self.lock:
            for alert in self.alerts:
                if alert.id == alert_id:
                    alert.resolved = True
                    alert.resolved_at = datetime.now()
                    self.logger.info(f"Alert resolved: {alert_id}")
                    return

    def get_metric(self, name: str) -> Optional[Metric]:
        """获取单个指标"""
        with self.lock:
            return self.metrics.get(name)

    def get_all_metrics(self) -> Dict[str, Metric]:
        """获取所有指标"""
        with self.lock:
            return dict(self.metrics)

    def get_active_alerts(self) -> List[Alert]:
        """获取活动告警"""
        with self.lock:
            return [a for a in self.alerts if not a.resolved]

    def get_all_alerts(self) -> List[Alert]:
        """获取所有告警历史"""
        with self.lock:
            return list(self.alert_history)

    def get_statistics(self) -> Dict[str, Any]:
        """获取统计摘要"""
        with self.lock:
            active_alerts = [a for a in self.alerts if not a.resolved]
            
            return {
                'total_metrics': len(self.metrics),
                'active_alerts': len(active_alerts),
                'alerts_by_level': {
                    'info': len([a for a in active_alerts if a.level == AlertLevel.INFO]),
                    'warning': len([a for a in active_alerts if a.level == AlertLevel.WARNING]),
                    'error': len([a for a in active_alerts if a.level == AlertLevel.ERROR]),
                    'critical': len([a for a in active_alerts if a.level == AlertLevel.CRITICAL])
                },
                'metrics': {name: metric.to_dict() for name, metric in self.metrics.items()},
                'timestamp': datetime.now().isoformat()
            }

    def _notify_callbacks(self, metric: Metric):
        """通知所有注册的回调"""
        for callback in self.callbacks:
            try:
                callback(metric)
            except Exception as e:
                self.logger.error(f"Metric callback error: {e}")


class PrometheusExporter:
    """Prometheus 指标导出器（简单实现）"""
    
    def __init__(self, collector: MetricsCollector):
        self.collector = collector
        self.logger = logging.getLogger(__name__)

    def export(self) -> str:
        """导出 Prometheus 格式的指标"""
        lines = []
        metrics = self.collector.get_all_metrics()
        
        for name, metric in metrics.items():
            labels_str = ""
            if metric.labels:
                label_parts = [f'{k}="{v}"' for k, v in metric.labels.items()]
                labels_str = "{" + ",".join(label_parts) + "}"
            
            lines.append(f"# HELP {name} {metric.type.value}")
            lines.append(f"# TYPE {name} {metric.type.value}")
            lines.append(f"{name}{labels_str} {metric.value}")
        
        return "\n".join(lines)


class PerformanceMonitor:
    """性能监控器 - 定时收集系统指标"""
    
    def __init__(self, collector: MetricsCollector, interval: float = 5.0):
        self.collector = collector
        self.interval = interval
        self.is_running = False
        self.thread: Optional[threading.Thread] = None
        self.logger = logging.getLogger(__name__)
        self.start_time = datetime.now()

    def start(self):
        """启动监控"""
        if self.is_running:
            return
        
        self.is_running = True
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()
        self.logger.info("Performance monitor started")

    def stop(self):
        """停止监控"""
        self.is_running = False
        if self.thread:
            self.thread.join(timeout=10)
        self.logger.info("Performance monitor stopped")

    def _monitor_loop(self):
        """监控循环"""
        while self.is_running:
            try:
                self._collect_system_metrics()
                time.sleep(self.interval)
            except Exception as e:
                self.logger.error(f"Monitor loop error: {e}")

    def _collect_system_metrics(self):
        """收集系统指标"""
        try:
            import psutil
            process = psutil.Process()
            
            # CPU 使用率
            cpu_percent = process.cpu_percent()
            self.collector.gauge("system_cpu_percent", cpu_percent)
            
            # 内存使用
            memory_mb = process.memory_info().rss / (1024 * 1024)
            self.collector.gauge("system_memory_mb", memory_mb)
            
            # 线程数
            num_threads = process.num_threads()
            self.collector.gauge("system_threads", num_threads)
            
            # 运行时间
            uptime_seconds = (datetime.now() - self.start_time).total_seconds()
            self.collector.gauge("system_uptime_seconds", uptime_seconds)
            
        except ImportError:
            # 没有 psutil，记录简单指标
            self.collector.gauge("system_uptime_seconds", 
                                (datetime.now() - self.start_time).total_seconds())
        except Exception as e:
            self.logger.debug(f"Could not collect system metrics: {e}")


class SystemMonitor:
    """系统监控主类 - 整合所有监控功能"""
    
    def __init__(self):
        self.collector = MetricsCollector()
        self.performance = PerformanceMonitor(self.collector)
        self.prometheus = PrometheusExporter(self.collector)
        self.logger = logging.getLogger(__name__)
        self.custom_stats: Dict[str, Any] = {}

    def start(self):
        """启动监控系统"""
        self.performance.start()
        self.logger.info("System monitor started")

    def stop(self):
        """停止监控系统"""
        self.performance.stop()
        self.logger.info("System monitor stopped")

    def record_task_start(self, task_type: str):
        """记录任务开始"""
        self.collector.counter("tasks_started_total", labels={'task_type': task_type})

    def record_task_complete(self, task_type: str, duration: float):
        """记录任务完成"""
        self.collector.counter("tasks_completed_total", labels={'task_type': task_type})
        self.collector.timer("task_duration_seconds", duration, labels={'task_type': task_type})

    def record_task_failed(self, task_type: str):
        """记录任务失败"""
        self.collector.counter("tasks_failed_total", labels={'task_type': task_type})

    def update_queue_stats(self, pending: int, running: int, completed: int, failed: int):
        """更新队列统计"""
        self.collector.gauge("queue_pending_tasks", pending)
        self.collector.gauge("queue_running_tasks", running)
        self.collector.gauge("queue_completed_tasks", completed)
        self.collector.gauge("queue_failed_tasks", failed)

    def get_dashboard_data(self) -> Dict[str, Any]:
        """获取仪表板数据"""
        stats = self.collector.get_statistics()
        
        return {
            'system': stats,
            'active_alerts': [a.to_dict() for a in self.collector.get_active_alerts()],
            'custom_stats': self.custom_stats,
            'timestamp': datetime.now().isoformat()
        }

    def to_json(self) -> str:
        """导出为 JSON"""
        return json.dumps(self.get_dashboard_data(), indent=2)
