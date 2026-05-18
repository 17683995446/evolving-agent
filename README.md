# 专属持续进化Agent系统

## 系统概述

高度模块化的持续进化Agent系统，实现**任务执行**与**智能优化**双并行运转，永久循环不停歇。

### 核心特点

1. **一开即全速运转** - 不存在等待、休眠、闲置状态
2. **任务越做越熟练** - 流程越跑越精简
3. **全自动自驱进化** - 无需人工干预调整
4. **全程闭环** - 执行与进化深度绑定

## 四层架构

```
┌─────────────────────────────────────────────────────────┐
│                  常驻守护层 (Guardian Layer)              │
│            健康监控 · 异常自动修复 · 永不宕机                │
├─────────────────────────────────────────────────────────┤
│                  任务执行层 (Task Execution Layer)          │
│            核心干活模块 · 满负荷推进任务进度                  │
├─────────────────────────────────────────────────────────┤
│                  智能优化层 (Intelligence Optimization)     │
│            实时复盘 · 找短板 · 改流程 · 提效率                │
├─────────────────────────────────────────────────────────┤
│                  同步落地层 (Synchronization Landing)        │
│            优化规则即时生效 · 无缝衔接下一轮执行               │
└─────────────────────────────────────────────────────────┘
```

## 项目结构

```
evolving_agent/
├── core/                          # 核心模块
│   ├── data_models_universal.py   # 通用数据模型
│   ├── guardian_perpetual_watcher.py  # 常驻守护层
│   └── dual_loop_drive_engine.py  # 双循环驱动引擎
├── execution/                     # 任务执行层
│   └── task_execution_engine.py  # 任务执行引擎
├── optimization/                   # 智能优化层
│   └── intelligent_optimization_system.py  # 优化系统
├── synchronization/               # 同步落地层
│   └── synchronization_landing_layer.py  # 同步层
├── knowledge/                     # 知识图谱
│   └── knowledge_graph_integration.py  # 知识图谱集成
└── demo_persistent_evolution.py   # 演示脚本
```

## 核心模块说明

### 1. 常驻守护层 (GuardianPerpetualWatcher)

- 多级健康监控
- 组件异常自动恢复
- 系统状态实时追踪

### 2. 任务执行层 (TaskExecutionEngine)

- 线程池并发执行
- 自适应任务调度
- 优先级队列管理

### 3. 智能优化层 (IntelligenceOptimizationLayer)

- 实时性能分析
- 瓶颈检测
- 策略自动生成
- 参数自适应调优

### 4. 同步落地层 (SynchronizationLandingLayer)

- 规则即时应用
- 冲突自动消解
- 规则有效性追踪

### 5. 知识图谱集成 (KnowledgeGraphIntegration)

- 经验持久化存储
- 语义关联检索
- 项目信息集成
- NetworkX图结构支持

### 6. 双循环驱动引擎 (DualLoopDriveEngine)

- 主线：任务执行循环
- 副线：优化迭代循环
- 双线联动，齐头并进

## 永久运转流程

```
启动Agent 
    ↓
持续执行任务 ───────────────────────────────────┐
    ↓                                          ↑
实时采集执行数据                                │
    ↓                                          ↑
分析不足、梳理优化方向                          │
    ↓                                          ↑
更新任务执行规则                                │
    ↓                                          ↑
新规则直接投入下一轮执行 ────────────────────────┘
    ↓
捕获异常，仅修正不终止全局流程
```

## 安装依赖

```bash
pip install networkx>=2.8
```

## 快速开始

```python
from core.dual_loop_drive_engine import DualLoopDriveEngine, PerpetualEvolutionController

# 初始化引擎
engine = DualLoopDriveEngine({
    'max_workers': 4,
    'optimization_interval': 30.0
})
engine.initialize()

# 启动永久运转
engine.start()

# 添加任务
engine.add_task('analysis', '分析系统性能')
engine.add_task('optimization', '优化执行策略')

# 查询知识图谱
results = engine.query_knowledge('optimization', max_results=5)

# 集成经验
engine.integrate_experience('task_success', '成功完成任务优化')

# 查看系统状态
status = engine.get_system_status()
print(status)

# 停止
engine.stop()
```

## 演示运行

```bash
python demo_persistent_evolution.py
```

## 设计亮点

- **零空闲**: 执行与优化同时进行，无任何等待状态
- **自进化**: 每次迭代都基于历史数据自我改进
- **容错强**: 局部故障不影响整体运转
- **知识沉淀**: 所有经验自动存入知识图谱
- **高度解耦**: 各层独立运作，通过接口通信

## 开源工具集

- `threading`: 多线程并发控制
- `networkx`: 知识图谱图结构
- `sqlite3`: 持久化存储
- `concurrent.futures`: 线程池执行器
- `dataclasses`: 数据结构定义
