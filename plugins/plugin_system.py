"""
插件系统与扩展架构
支持动态加载、插件接口和扩展机制
"""
import importlib
import os
import sys
import logging
import threading
from typing import Dict, List, Any, Optional, Callable, Type
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from datetime import datetime


@dataclass
class PluginInfo:
    """插件信息"""
    name: str
    version: str
    author: str
    description: str
    capabilities: List[str] = field(default_factory=list)
    plugin_type: str = "generic"
    enabled: bool = True


class BasePlugin(ABC):
    """插件基类"""
    
    def __init__(self):
        self.plugin_info = PluginInfo(
            name="Unnamed Plugin",
            version="0.1.0",
            author="Unknown",
            description="No description"
        )
        self.is_initialized = False
        self.logger = logging.getLogger(f"Plugin.{self.__class__.__name__}")

    @abstractmethod
    def initialize(self, context: Dict[str, Any] = None) -> bool:
        """初始化插件"""
        pass

    @abstractmethod
    def execute(self, parameters: Dict[str, Any] = None) -> Any:
        """执行插件功能"""
        pass

    def shutdown(self):
        """关闭插件"""
        pass


class TaskHandlerPlugin(BasePlugin):
    """任务处理器插件"""
    
    def __init__(self):
        super().__init__()
        self.handled_task_types: List[str] = []
        self.plugin_info.plugin_type = "task_handler"

    def register_task_type(self, task_type: str):
        """注册任务类型"""
        self.handled_task_types.append(task_type)

    def can_handle(self, task_type: str) -> bool:
        """检查是否能处理任务类型"""
        return task_type in self.handled_task_types


class OptimizationPlugin(BasePlugin):
    """优化策略插件"""
    
    def __init__(self):
        super().__init__()
        self.plugin_info.plugin_type = "optimization"

    @abstractmethod
    def analyze(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """分析数据并返回优化建议"""
        pass


class PluginManager:
    """插件管理器"""
    
    def __init__(self, plugin_directories: List[str] = None):
        self.plugin_directories = plugin_directories or []
        self.plugins: Dict[str, BasePlugin] = {}
        self.plugin_classes: Dict[str, Type[BasePlugin]] = {}
        self.plugin_info: Dict[str, PluginInfo] = {}
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
        self.loaded_modules = set()

        # 添加默认插件目录
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(current_dir)
        if os.path.exists(os.path.join(parent_dir, 'plugins')):
            self.plugin_directories.append(os.path.join(parent_dir, 'plugins'))

    def add_directory(self, directory: str):
        """添加插件目录"""
        if directory not in self.plugin_directories:
            self.plugin_directories.append(directory)
            self.logger.info(f"Added plugin directory: {directory}")

    def register_plugin_class(self, plugin_class: Type[BasePlugin]):
        """注册插件类"""
        with self.lock:
            plugin_name = plugin_class.__name__
            self.plugin_classes[plugin_name] = plugin_class
            self.logger.info(f"Registered plugin class: {plugin_name}")

    def create_plugin(self, plugin_name: str, context: Dict[str, Any] = None) -> Optional[BasePlugin]:
        """创建插件实例"""
        with self.lock:
            if plugin_name in self.plugin_classes:
                plugin = self.plugin_classes[plugin_name]()
                if plugin.initialize(context):
                    plugin.is_initialized = True
                    self.plugins[plugin_name] = plugin
                    self.plugin_info[plugin_name] = plugin.plugin_info
                    self.logger.info(f"Created and initialized plugin: {plugin_name}")
                    return plugin
                else:
                    self.logger.error(f"Failed to initialize plugin: {plugin_name}")
            return None

    def load_from_directory(self, directory: str):
        """从目录加载插件"""
        if not os.path.exists(directory):
            self.logger.warning(f"Plugin directory does not exist: {directory}")
            return
        
        self.logger.info(f"Loading plugins from: {directory}")
        
        # 添加目录到路径
        if directory not in sys.path:
            sys.path.insert(0, directory)
        
        # 扫描文件
        for filename in os.listdir(directory):
            if filename.endswith('.py') and not filename.startswith('_'):
                module_name = filename[:-3]  # 去掉 .py
                self._load_plugin_module(module_name, directory)

    def _load_plugin_module(self, module_name: str, directory: str):
        """加载插件模块"""
        try:
            if module_name in self.loaded_modules:
                return
            
            # 导入模块
            module = importlib.import_module(module_name)
            self.loaded_modules.add(module_name)
            
            # 查找插件类
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if (isinstance(attr, type) and 
                    issubclass(attr, BasePlugin) and 
                    attr != BasePlugin):
                    self.register_plugin_class(attr)
            
            self.logger.info(f"Loaded plugin module: {module_name}")
        except Exception as e:
            self.logger.error(f"Failed to load plugin module {module_name}: {e}")

    def load_all(self):
        """加载所有插件"""
        self.logger.info("Loading all plugins...")
        for directory in self.plugin_directories:
            self.load_from_directory(directory)

    def get_plugin(self, plugin_name: str) -> Optional[BasePlugin]:
        """获取插件"""
        with self.lock:
            return self.plugins.get(plugin_name)

    def get_plugins_by_type(self, plugin_type: str) -> List[BasePlugin]:
        """按类型获取插件"""
        with self.lock:
            return [p for p in self.plugins.values() 
                   if p.plugin_info.plugin_type == plugin_type]

    def enable_plugin(self, plugin_name: str):
        """启用插件"""
        with self.lock:
            if plugin_name in self.plugin_info:
                self.plugin_info[plugin_name].enabled = True
                self.logger.info(f"Enabled plugin: {plugin_name}")

    def disable_plugin(self, plugin_name: str):
        """禁用插件"""
        with self.lock:
            if plugin_name in self.plugin_info:
                self.plugin_info[plugin_name].enabled = False
                self.logger.info(f"Disabled plugin: {plugin_name}")

    def shutdown_all(self):
        """关闭所有插件"""
        with self.lock:
            for plugin in self.plugins.values():
                try:
                    plugin.shutdown()
                except Exception as e:
                    self.logger.error(f"Error shutting down plugin {plugin.plugin_info.name}: {e}")
            self.plugins.clear()
            self.logger.info("All plugins shut down")

    def get_plugin_status(self) -> Dict[str, Any]:
        """获取插件状态"""
        with self.lock:
            return {
                'total_plugins': len(self.plugins),
                'available_classes': list(self.plugin_classes.keys()),
                'plugins': {
                    name: info.__dict__ for name, info in self.plugin_info.items()
                }
            }


# 示例插件 - 内置示例
class ExampleTaskPlugin(TaskHandlerPlugin):
    """示例任务处理器插件"""
    
    def __init__(self):
        super().__init__()
        self.plugin_info = PluginInfo(
            name="Example Task Plugin",
            version="1.0.0",
            author="System",
            description="Example task handler plugin",
            capabilities=["example_task", "demo_task"]
        )
        self.register_task_type("example_task")
        self.register_task_type("demo_task")

    def initialize(self, context: Dict[str, Any] = None) -> bool:
        self.logger.info("Example plugin initialized")
        return True

    def execute(self, parameters: Dict[str, Any] = None) -> Any:
        params = parameters or {}
        return {
            'status': 'success',
            'message': 'Example plugin executed successfully',
            'parameters': params
        }


class ExampleOptimizationPlugin(OptimizationPlugin):
    """示例优化插件"""
    
    def __init__(self):
        super().__init__()
        self.plugin_info = PluginInfo(
            name="Example Optimization Plugin",
            version="1.0.0",
            author="System",
            description="Example optimization strategy plugin",
            capabilities=["efficiency_analysis"]
        )

    def initialize(self, context: Dict[str, Any] = None) -> bool:
        self.logger.info("Example optimization plugin initialized")
        return True

    def analyze(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [
            {
                'type': 'suggestion',
                'priority': 'medium',
                'message': 'Consider optimizing task batching',
                'confidence': 0.75
            }
        ]

    def execute(self, parameters: Dict[str, Any] = None) -> Any:
        return self.analyze(parameters or {})


class ExtensionPoint:
    """扩展点 - 允许插件集成到系统的特定位置"""
    
    def __init__(self, name: str):
        self.name = name
        self.extensions: List[Callable] = []
        self.lock = threading.RLock()
        self.logger = logging.getLogger(f"Extension.{name}")

    def register(self, extension: Callable):
        """注册扩展"""
        with self.lock:
            self.extensions.append(extension)
            self.logger.debug(f"Registered extension to {self.name}")

    def unregister(self, extension: Callable):
        """注销扩展"""
        with self.lock:
            if extension in self.extensions:
                self.extensions.remove(extension)
                self.logger.debug(f"Unregistered extension from {self.name}")

    def execute_all(self, *args, **kwargs) -> List[Any]:
        """执行所有扩展"""
        results = []
        with self.lock:
            for extension in self.extensions:
                try:
                    result = extension(*args, **kwargs)
                    results.append(result)
                except Exception as e:
                    self.logger.error(f"Extension execution error: {e}")
        return results


class ExtensionManager:
    """扩展管理器 - 管理所有扩展点"""
    
    def __init__(self):
        self.extension_points: Dict[str, ExtensionPoint] = {}
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)

    def get_extension_point(self, name: str) -> ExtensionPoint:
        """获取或创建扩展点"""
        with self.lock:
            if name not in self.extension_points:
                self.extension_points[name] = ExtensionPoint(name)
            return self.extension_points[name]

    def register_extension(self, point_name: str, extension: Callable):
        """注册扩展"""
        point = self.get_extension_point(point_name)
        point.register(extension)

    def execute_extensions(self, point_name: str, *args, **kwargs) -> List[Any]:
        """执行扩展点的所有扩展"""
        if point_name in self.extension_points:
            return self.extension_points[point_name].execute_all(*args, **kwargs)
        return []

    def get_all_points(self) -> Dict[str, ExtensionPoint]:
        """获取所有扩展点"""
        with self.lock:
            return dict(self.extension_points)
