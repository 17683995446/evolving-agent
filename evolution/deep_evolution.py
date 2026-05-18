"""
AI 自主学习与深度进化系统
包含机器学习集成、强化学习框架和自适应优化
"""
import threading
import time
import logging
import json
import random
from typing import Dict, List, Any, Optional, Callable, Tuple
from dataclasses import dataclass, field
from collections import deque
from datetime import datetime
from enum import Enum


class LearningMode(Enum):
    """学习模式"""
    SUPERVISED = "supervised"
    REINFORCEMENT = "reinforcement"
    UNSUPERVISED = "unsupervised"
    ONLINE = "online"


class EvolutionStrategy(Enum):
    """进化策略"""
    GREEDY = "greedy"
    E_GREEDY = "epsilon_greedy"
    UCB = "ucb"
    THOMPSON = "thompson_sampling"


@dataclass
class Experience:
    """学习经验"""
    state: Dict[str, Any]
    action: str
    reward: float
    next_state: Dict[str, Any]
    done: bool = False
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Strategy:
    """策略"""
    name: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    performance: float = 0.0
    usage_count: int = 0
    last_used: Optional[datetime] = None


class QTable:
    """Q-Table 强化学习"""
    
    def __init__(self, learning_rate: float = 0.1, discount_factor: float = 0.95,
                 epsilon: float = 0.1):
        self.q_table: Dict[str, Dict[str, float]] = {}
        self.learning_rate = learning_rate
        self.discount_factor = discount_factor
        self.epsilon = epsilon
        self.lock = threading.RLock()
        self.logger = logging.getLogger("QTable")

    def _get_state_key(self, state: Dict[str, Any]) -> str:
        """获取状态的键"""
        return json.dumps(state, sort_keys=True)

    def get_q_value(self, state: Dict[str, Any], action: str) -> float:
        """获取 Q 值"""
        state_key = self._get_state_key(state)
        with self.lock:
            if state_key not in self.q_table:
                self.q_table[state_key] = {}
            return self.q_table[state_key].get(action, 0.0)

    def set_q_value(self, state: Dict[str, Any], action: str, value: float):
        """设置 Q 值"""
        state_key = self._get_state_key(state)
        with self.lock:
            if state_key not in self.q_table:
                self.q_table[state_key] = {}
            self.q_table[state_key][action] = value

    def update(self, experience: Experience):
        """Q-学习更新"""
        current_q = self.get_q_value(experience.state, experience.action)
        
        if experience.done:
            max_next_q = 0
        else:
            next_actions = self.get_available_actions(experience.next_state)
            if next_actions:
                max_next_q = max(self.get_q_value(experience.next_state, a) for a in next_actions)
            else:
                max_next_q = 0
        
        new_q = current_q + self.learning_rate * (
            experience.reward + self.discount_factor * max_next_q - current_q
        )
        self.set_q_value(experience.state, experience.action, new_q)
        self.logger.debug(f"Q-value updated: {current_q:.4f} -> {new_q:.4f}")

    def select_action(self, state: Dict[str, Any], available_actions: List[str],
                     strategy: EvolutionStrategy = EvolutionStrategy.E_GREEDY) -> str:
        """选择动作"""
        if not available_actions:
            raise ValueError("No available actions")
        
        with self.lock:
            # ε-greedy 策略
            if random.random() < self.epsilon:
                return random.choice(available_actions)
            
            # 否则选择 Q 值最大的动作
            q_values = {a: self.get_q_value(state, a) for a in available_actions}
            max_q = max(q_values.values())
            best_actions = [a for a, q in q_values.items() if q == max_q]
            return random.choice(best_actions)

    def get_available_actions(self, state: Dict[str, Any]) -> List[str]:
        """获取可用动作"""
        state_key = self._get_state_key(state)
        with self.lock:
            if state_key in self.q_table:
                return list(self.q_table[state_key].keys())
            return []

    def get_stats(self) -> Dict[str, Any]:
        """获取统计"""
        with self.lock:
            return {
                'states_count': len(self.q_table),
                'total_q_values': sum(len(actions) for actions in self.q_table.values()),
                'learning_rate': self.learning_rate,
                'epsilon': self.epsilon
            }


class AdaptiveOptimizer:
    """自适应优化器"""
    
    def __init__(self):
        self.strategies: Dict[str, Strategy] = {}
        self.current_strategy: Optional[str] = None
        self.performance_history: deque = deque(maxlen=1000)
        self.lock = threading.RLock()
        self.logger = logging.getLogger("AdaptiveOptimizer")

    def register_strategy(self, name: str, parameters: Dict[str, Any] = None):
        """注册策略"""
        with self.lock:
            self.strategies[name] = Strategy(
                name=name,
                parameters=parameters or {}
            )
            self.logger.info(f"Strategy registered: {name}")

    def select_strategy(self, context: Dict[str, Any] = None) -> str:
        """选择最佳策略 - 使用 UCB1"""
        with self.lock:
            if not self.strategies:
                raise ValueError("No strategies registered")
            
            # UCB1 公式
            total_usage = sum(s.usage_count for s in self.strategies.values())
            
            if total_usage == 0:
                # 第一次，随机选择
                return random.choice(list(self.strategies.keys()))
            
            best_strategy = None
            best_score = -float('inf')
            
            for name, strategy in self.strategies.items():
                if strategy.usage_count == 0:
                    # 未使用的策略优先尝试
                    ucb_score = float('inf')
                else:
                    # UCB1 公式
                    import math
                    exploration_term = math.sqrt(2 * math.log(total_usage) / strategy.usage_count)
                    ucb_score = strategy.performance + exploration_term
                
                if ucb_score > best_score:
                    best_score = ucb_score
                    best_strategy = name
            
            self.current_strategy = best_strategy
            self.logger.info(f"Selected strategy: {best_strategy} (score: {best_score:.4f})")
            return best_strategy

    def record_performance(self, strategy_name: str, performance: float):
        """记录策略性能"""
        with self.lock:
            if strategy_name in self.strategies:
                strategy = self.strategies[strategy_name]
                # 增量更新平均性能
                strategy.usage_count += 1
                strategy.performance = (
                    (strategy.performance * (strategy.usage_count - 1) + performance) /
                    strategy.usage_count
                )
                strategy.last_used = datetime.now()
                self.performance_history.append((strategy_name, performance))
                self.logger.debug(f"Strategy {strategy_name} performance: {performance:.4f} (avg: {strategy.performance:.4f})")

    def get_best_strategy(self) -> Optional[str]:
        """获取最佳策略"""
        with self.lock:
            if not self.strategies:
                return None
            return max(self.strategies.items(), key=lambda x: x[1].performance)[0]

    def get_stats(self) -> Dict[str, Any]:
        """获取统计"""
        with self.lock:
            return {
                'strategies_count': len(self.strategies),
                'current_strategy': self.current_strategy,
                'strategies': {
                    name: {
                        'performance': s.performance,
                        'usage_count': s.usage_count,
                        'last_used': s.last_used.isoformat() if s.last_used else None
                    }
                    for name, s in self.strategies.items()
                }
            }


class ExperienceReplay:
    """经验回放缓冲区"""
    
    def __init__(self, max_size: int = 10000):
        self.buffer: deque = deque(maxlen=max_size)
        self.lock = threading.RLock()
        self.logger = logging.getLogger("ExperienceReplay")

    def add(self, experience: Experience):
        """添加经验"""
        with self.lock:
            self.buffer.append(experience)
            self.logger.debug(f"Experience added (buffer size: {len(self.buffer)})")

    def sample(self, batch_size: int) -> List[Experience]:
        """采样经验"""
        with self.lock:
            if len(self.buffer) < batch_size:
                return list(self.buffer)
            return random.sample(list(self.buffer), batch_size)

    def size(self) -> int:
        """获取大小"""
        with self.lock:
            return len(self.buffer)

    def clear(self):
        """清空"""
        with self.lock:
            self.buffer.clear()


class AutonomousLearner:
    """自主学习器"""
    
    def __init__(self):
        self.q_table = QTable(learning_rate=0.1, discount_factor=0.95, epsilon=0.1)
        self.optimizer = AdaptiveOptimizer()
        self.experience_buffer = ExperienceReplay(max_size=10000)
        self.is_running = False
        self.thread: Optional[threading.Thread] = None
        self.logger = logging.getLogger("AutonomousLearner")
        self.learning_mode = LearningMode.REINFORCEMENT
        
        # 注册默认优化策略
        self.optimizer.register_strategy("conservative", {"risk": "low"})
        self.optimizer.register_strategy("balanced", {"risk": "medium"})
        self.optimizer.register_strategy("aggressive", {"risk": "high"})

    def start(self):
        """启动学习"""
        if self.is_running:
            return
        
        self.is_running = True
        self.thread = threading.Thread(target=self._learning_loop, daemon=True)
        self.thread.start()
        self.logger.info("Autonomous learner started")

    def stop(self):
        """停止学习"""
        self.is_running = False
        if self.thread:
            self.thread.join(timeout=5)
        self.logger.info("Autonomous learner stopped")

    def _learning_loop(self):
        """学习循环"""
        while self.is_running:
            try:
                # 从经验回放中学习
                if self.experience_buffer.size() >= 10:
                    batch = self.experience_buffer.sample(10)
                    for exp in batch:
                        self.q_table.update(exp)
            except Exception as e:
                self.logger.error(f"Learning loop error: {e}")
            time.sleep(1)

    def observe_and_learn(self, state: Dict[str, Any], action: str,
                         reward: float, next_state: Dict[str, Any], done: bool = False):
        """观察并学习"""
        experience = Experience(
            state=state,
            action=action,
            reward=reward,
            next_state=next_state,
            done=done
        )
        self.experience_buffer.add(experience)
        self.q_table.update(experience)
        self.logger.debug(f"Observed: {action}, reward: {reward:.4f}")

    def decide_action(self, state: Dict[str, Any], available_actions: List[str]) -> str:
        """决定动作"""
        return self.q_table.select_action(state, available_actions)

    def optimize(self, performance_metric: float):
        """优化 - 记录性能并选择最佳策略"""
        if self.optimizer.current_strategy:
            self.optimizer.record_performance(self.optimizer.current_strategy, performance_metric)
        return self.optimizer.select_strategy()

    def get_learning_state(self) -> Dict[str, Any]:
        """获取学习状态"""
        return {
            'q_table': self.q_table.get_stats(),
            'optimizer': self.optimizer.get_stats(),
            'experience_buffer_size': self.experience_buffer.size(),
            'learning_mode': self.learning_mode.value
        }


class DeepEvolutionEngine:
    """深度进化引擎"""
    
    def __init__(self, engine=None):
        self.core_engine = engine
        self.learner = AutonomousLearner()
        self.evolution_history: deque = deque(maxlen=100)
        self.is_running = False
        self.thread: Optional[threading.Thread] = None
        self.logger = logging.getLogger("DeepEvolutionEngine")
        self.generation = 0

    def start(self):
        """启动进化"""
        if self.is_running:
            return
        
        self.is_running = True
        self.learner.start()
        self.thread = threading.Thread(target=self._evolution_loop, daemon=True)
        self.thread.start()
        self.logger.info("Deep evolution engine started")

    def stop(self):
        """停止进化"""
        self.is_running = False
        self.learner.stop()
        if self.thread:
            self.thread.join(timeout=5)
        self.logger.info("Deep evolution engine stopped")

    def _evolution_loop(self):
        """进化循环"""
        while self.is_running:
            try:
                self._evolve_step()
            except Exception as e:
                self.logger.error(f"Evolution loop error: {e}")
            time.sleep(30)  # 每30秒一个进化周期

    def _evolve_step(self):
        """进化步骤"""
        self.generation += 1
        
        # 获取系统状态
        state = self._get_system_state()
        
        # 可能的优化动作
        actions = ["increase_workers", "decrease_workers", "adjust_priority", "optimize_batching"]
        
        # 决定动作
        action = self.learner.decide_action(state, actions)
        
        # 执行动作
        reward = self._execute_action(action)
        
        # 获取新状态
        next_state = self._get_system_state()
        
        # 学习
        self.learner.observe_and_learn(state, action, reward, next_state)
        
        # 记录历史
        self.evolution_history.append({
            'generation': self.generation,
            'action': action,
            'reward': reward,
            'timestamp': datetime.now().isoformat()
        })
        
        self.logger.info(f"Generation {self.generation}: action={action}, reward={reward:.4f}")

    def _get_system_state(self) -> Dict[str, Any]:
        """获取系统状态"""
        if not self.core_engine:
            return {'dummy': True}
        
        try:
            stats = self.core_engine.task_engine.get_statistics()
            qstats = stats.get('queue_stats', {})
            return {
                'pending_tasks': qstats.get('pending', 0),
                'running_tasks': qstats.get('running', 0),
                'completed_tasks': qstats.get('completed', 0),
                'workers': getattr(self.core_engine.task_engine, 'max_workers', 4)
            }
        except:
            return {'dummy': True}

    def _execute_action(self, action: str) -> float:
        """执行动作并返回奖励"""
        # 模拟动作执行，返回随机奖励
        reward = random.uniform(-0.1, 1.0)
        return reward

    def get_evolution_status(self) -> Dict[str, Any]:
        """获取进化状态"""
        return {
            'generation': self.generation,
            'history_length': len(self.evolution_history),
            'learning_state': self.learner.get_learning_state()
        }
