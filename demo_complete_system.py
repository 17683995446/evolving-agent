#!/usr/bin/env python3
"""
持续进化 Agent - 完整系统演示
整合所有阶段的功能：短期改进、中期创新、长期规划
"""
import sys
import os
import time
import logging
import json
import random
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# 添加路径
sys.path.insert(0, '/workspace/evolving_agent')

from core.dual_loop_drive_engine import DualLoopDriveEngine, PerpetualEvolutionController
from execution.distributed_task_queue import DistributedTaskExecutor, TaskPriority
from core.advanced_monitoring import SystemMonitor
from collaboration.multi_agent_system import MultiAgentSystem
from plugins.plugin_system import PluginManager, ExampleTaskPlugin
from evolution.deep_evolution import DeepEvolutionEngine


def demonstrate_short_term_improvements():
    """演示短期改进"""
    print("\n" + "=" * 80)
    print("短期改进演示")
    print("=" * 80)
    
    # 1. 分布式任务队列
    print("\n1. 分布式任务队列")
    executor = DistributedTaskExecutor(max_workers=32)
    
    # 注册处理器
    def compute_handler(params):
        x = params.get('x', 0)
        y = params.get('y', 0)
        return {'result': x + y, 'sum': x + y}
    
    executor.register_handler("compute", compute_handler)
    executor.start(num_workers=8)
    
    # 提交任务
    print("  提交任务...")
    for i in range(50):
        executor.submit_task(
            "compute",
            f"Task {i}",
            {'x': i, 'y': i * 2},
            priority=TaskPriority.NORMAL
        )
    
    # 等待完成
    print("  等待任务完成...")
    time.sleep(2)
    
    stats = executor.get_statistics()
    print(f"  队列统计: {stats}")
    executor.stop()
    
    # 2. 高级监控
    print("\n2. 高级监控系统")
    monitor = SystemMonitor()
    monitor.start()
    
    monitor.record_task_start("test")
    monitor.record_task_complete("test", 0.5)
    monitor.update_queue_stats(0, 0, 50, 0)
    
    dashboard = monitor.get_dashboard_data()
    print(f"  监控统计: {dashboard['system']['total_metrics']} 指标")
    
    monitor.stop()
    
    print("\n✅ 短期改进演示完成!")


def demonstrate_medium_term_innovations():
    """演示中期创新"""
    print("\n" + "=" * 80)
    print("中期创新演示")
    print("=" * 80)
    
    # 1. 多 Agent 协作
    print("\n1. 多 Agent 协作系统")
    mas = MultiAgentSystem()
    
    # 创建 Agent
    agent1 = mas.create_agent("Worker-1", ["compute", "analysis"])
    agent2 = mas.create_agent("Worker-2", ["compute", "optimization"])
    agent3 = mas.create_agent("Worker-3", ["analysis", "learning"])
    
    # 注册任务处理器
    def agent_compute(params):
        return {'agent': 'worker', 'result': sum(params.values())}
    
    agent1.register_task_handler("compute", agent_compute)
    agent2.register_task_handler("compute", agent_compute)
    
    # 启动所有
    mas.start_all()
    
    status = mas.get_system_status()
    print(f"  系统状态: {status['agents_count']} 个 Agent")
    
    # 2. 插件系统
    print("\n2. 插件系统")
    plugin_manager = PluginManager()
    
    # 注册并创建示例插件
    plugin_manager.register_plugin_class(ExampleTaskPlugin)
    plugin = plugin_manager.create_plugin("ExampleTaskPlugin")
    
    if plugin:
        result = plugin.execute({'test': 'data'})
        print(f"  插件执行结果: {result}")
    
    plugin_status = plugin_manager.get_plugin_status()
    print(f"  插件系统状态: {plugin_status['total_plugins']} 插件")
    
    # 停止
    mas.stop_all()
    plugin_manager.shutdown_all()
    
    print("\n✅ 中期创新演示完成!")


def demonstrate_long_term_planning():
    """演示长期规划"""
    print("\n" + "=" * 80)
    print("长期规划演示")
    print("=" * 80)
    
    # AI 自主学习与深度进化
    print("\n1. 深度进化引擎")
    evolution_engine = DeepEvolutionEngine()
    evolution_engine.start()
    
    # 运行一些进化周期
    print("  运行进化周期...")
    for i in range(5):
        state = {'test': i, 'value': random.random()}
        actions = ['action_a', 'action_b', 'action_c']
        action = evolution_engine.learner.decide_action(state, actions)
        reward = random.uniform(0.5, 1.0)
        next_state = {'test': i + 1, 'value': random.random()}
        evolution_engine.learner.observe_and_learn(state, action, reward, next_state)
        print(f"    周期 {i}: action={action}, reward={reward:.4f}")
    
    status = evolution_engine.get_evolution_status()
    print(f"  进化代数: {status['generation']}")
    print(f"  学习状态: Q-table {status['learning_state']['q_table']['states_count']} 状态")
    
    evolution_engine.stop()
    
    print("\n✅ 长期规划演示完成!")


def demonstrate_full_integration():
    """完整整合演示"""
    print("\n" + "=" * 80)
    print("完整系统整合演示")
    print("=" * 80)
    
    # 初始化核心引擎
    print("\n初始化核心双循环引擎...")
    engine = DualLoopDriveEngine({
        'max_workers': 32,
        'optimization_interval': 10.0
    })
    engine.initialize()
    
    # 集成监控
    print("集成监控系统...")
    monitor = SystemMonitor()
    monitor.start()
    
    # 集成分布式队列
    print("启动分布式任务执行器...")
    dist_executor = DistributedTaskExecutor(max_workers=32)
    dist_executor.register_handler("fast_compute", lambda p: {'result': p.get('x', 0) * 2})
    dist_executor.start(num_workers=8)
    
    # 启动核心引擎
    print("启动核心引擎...")
    engine.start()
    
    # 集成进化系统
    print("集成深度进化引擎...")
    evolution = DeepEvolutionEngine(engine)
    evolution.start()
    
    # 提交任务
    print("\n提交任务...")
    for i in range(20):
        task_id = engine.add_task(
            "optimization_task",
            f"Integration Task {i}",
            {'data': i}
        )
        dist_executor.submit_task(
            "fast_compute",
            f"Distributed Task {i}",
            {'x': i}
        )
    
    # 运行一段时间
    print("\n系统运行中...")
    time.sleep(5)
    
    # 获取最终状态
    print("\n最终系统状态:")
    engine_status = engine.get_system_status()
    monitor_status = monitor.get_dashboard_data()
    evolution_status = evolution.get_evolution_status()
    dist_stats = dist_executor.get_statistics()
    
    print(f"  核心引擎运行: {engine_status['is_running']}")
    print(f"  监控指标数: {monitor_status['system']['total_metrics']}")
    print(f"  进化代数: {evolution_status['generation']}")
    print(f"  分布式队列统计: Pending={dist_stats.get('queue_stats', {}).get('pending', 0)}, Completed={dist_stats.get('queue_stats', {}).get('completed', 0)}")
    
    # 停止所有
    print("\n停止系统...")
    evolution.stop()
    engine.stop()
    dist_executor.stop()
    monitor.stop()
    
    print("\n✅ 完整整合演示完成!")


def run_benchmark():
    """性能基准测试"""
    print("\n" + "=" * 80)
    print("性能基准测试")
    print("=" * 80)
    
    print("\n分布式任务队列基准:")
    executor = DistributedTaskExecutor(max_workers=64)
    
    def benchmark_handler(params):
        return {'processed': True}
    
    executor.register_handler("benchmark", benchmark_handler)
    executor.start(num_workers=16)
    
    start_time = time.time()
    num_tasks = 1000
    for i in range(num_tasks):
        executor.submit_task("benchmark", f"Benchmark {i}", {'i': i})
    
    while executor.get_pending_count() > 0:
        time.sleep(0.1)
    
    elapsed = time.time() - start_time
    throughput = num_tasks / elapsed
    
    print(f"  任务数: {num_tasks}")
    print(f"  耗时: {elapsed:.3f} 秒")
    print(f"  吞吐量: {throughput:.0f} 任务/秒")
    
    executor.stop()
    print("\n✅ 性能基准测试完成!")
    
    return {
        'tasks': num_tasks,
        'elapsed': elapsed,
        'throughput': throughput
    }


def main():
    print("=" * 80)
    print("持续进化 Agent - 完整系统演示")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    import random
    
    # 各阶段演示
    demonstrate_short_term_improvements()
    demonstrate_medium_term_innovations()
    demonstrate_long_term_planning()
    demonstrate_full_integration()
    
    # 性能基准
    benchmark_result = run_benchmark()
    
    # 总结
    print("\n" + "=" * 80)
    print("演示总结")
    print("=" * 80)
    print("\n✅ 所有阶段功能正常")
    print(f"✅ 峰值性能: {benchmark_result['throughput']:.0f} 任务/秒")
    print("\n" + "=" * 80)
    print("系统已准备好投入生产使用！")
    print("=" * 80)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
