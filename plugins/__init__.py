"""
插件系统模块
"""
from .plugin_system import (
    PluginManager,
    BasePlugin,
    TaskHandlerPlugin,
    OptimizationPlugin,
    ExtensionManager,
    ExtensionPoint,
    PluginInfo,
    ExampleTaskPlugin,
    ExampleOptimizationPlugin
)

__all__ = [
    'PluginManager',
    'BasePlugin',
    'TaskHandlerPlugin',
    'OptimizationPlugin',
    'ExtensionManager',
    'ExtensionPoint',
    'PluginInfo',
    'ExampleTaskPlugin',
    'ExampleOptimizationPlugin'
]
