"""
多 Agent 协作与通信系统
支持 Agent 注册发现、消息总线、任务分配和负载均衡
"""
import threading
import time
import logging
import uuid
import json
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import deque


class AgentStatus(Enum):
    """Agent 状态"""
    OFFLINE = "offline"
    IDLE = "idle"
    BUSY = "busy"
    ERROR = "error"


class MessageType(Enum):
    """消息类型"""
    TASK_REQUEST = "task_request"
    TASK_ASSIGNMENT = "task_assignment"
    TASK_RESULT = "task_result"
    HEARTBEAT = "heartbeat"
    STATUS_UPDATE = "status_update"
    DISCOVERY = "discovery"
    BROADCAST = "broadcast"


@dataclass
class AgentInfo:
    """Agent 信息"""
    agent_id: str
    name: str
    capabilities: List[str] = field(default_factory=list)
    status: AgentStatus = AgentStatus.OFFLINE
    load: float = 0.0
    last_heartbeat: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'agent_id': self.agent_id,
            'name': self.name,
            'capabilities': self.capabilities,
            'status': self.status.value,
            'load': self.load,
            'last_heartbeat': self.last_heartbeat.isoformat(),
            'metadata': self.metadata
        }


@dataclass
class Message:
    """消息数据结构"""
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    sender_id: str = ""
    receiver_id: Optional[str] = None
    message_type: MessageType = MessageType.BROADCAST
    content: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    priority: int = 5

    def to_dict(self) -> Dict[str, Any]:
        return {
            'message_id': self.message_id,
            'sender_id': self.sender_id,
            'receiver_id': self.receiver_id,
            'message_type': self.message_type.value,
            'content': self.content,
            'timestamp': self.timestamp.isoformat(),
            'priority': self.priority
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Message':
        msg = cls()
        msg.message_id = data.get('message_id', str(uuid.uuid4()))
        msg.sender_id = data.get('sender_id', '')
        msg.receiver_id = data.get('receiver_id')
        msg.message_type = MessageType(data.get('message_type', 'broadcast'))
        msg.content = data.get('content', {})
        msg.timestamp = datetime.fromisoformat(data['timestamp'])
        msg.priority = data.get('priority', 5)
        return msg


class MessageBus:
    """消息总线 - Agent 间通信核心"""
    
    def __init__(self):
        self.message_queues: Dict[str, deque] = {}  # 每个 Agent 一个队列
        self.subscribers: Dict[str, List[Callable[[Message], None]]] = {}
        self.broadcast_subscribers: List[Callable[[Message], None]] = []
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)

    def register_agent(self, agent_id: str):
        """注册 Agent 到消息总线"""
        with self.lock:
            if agent_id not in self.message_queues:
                self.message_queues[agent_id] = deque(maxlen=1000)
                self.subscribers[agent_id] = []
                self.logger.info(f"Agent registered to message bus: {agent_id}")

    def unregister_agent(self, agent_id: str):
        """注销 Agent"""
        with self.lock:
            if agent_id in self.message_queues:
                del self.message_queues[agent_id]
            if agent_id in self.subscribers:
                del self.subscribers[agent_id]
            self.logger.info(f"Agent unregistered from message bus: {agent_id}")

    def subscribe(self, agent_id: str, callback: Callable[[Message], None]):
        """订阅消息"""
        with self.lock:
            if agent_id in self.subscribers:
                self.subscribers[agent_id].append(callback)

    def subscribe_broadcast(self, callback: Callable[[Message], None]):
        """订阅广播消息"""
        with self.lock:
            self.broadcast_subscribers.append(callback)

    def send(self, message: Message) -> bool:
        """发送消息"""
        with self.lock:
            # 广播消息
            if message.message_type == MessageType.BROADCAST or not message.receiver_id:
                for agent_id in self.message_queues:
                    self.message_queues[agent_id].append(message)
                self._notify_broadcast_subscribers(message)
                self.logger.debug(f"Broadcast message sent: {message.message_id}")
            # 点对点消息
            elif message.receiver_id in self.message_queues:
                self.message_queues[message.receiver_id].append(message)
                self._notify_subscribers(message.receiver_id, message)
                self.logger.debug(f"Message sent to {message.receiver_id}: {message.message_id}")
            else:
                self.logger.warning(f"Failed to send message: receiver {message.receiver_id} not found")
                return False
        return True

    def receive(self, agent_id: str, timeout: float = 0.1) -> Optional[Message]:
        """接收消息"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            with self.lock:
                if agent_id in self.message_queues and self.message_queues[agent_id]:
                    return self.message_queues[agent_id].popleft()
            time.sleep(0.001)
        return None

    def _notify_subscribers(self, agent_id: str, message: Message):
        """通知订阅者"""
        if agent_id in self.subscribers:
            for callback in self.subscribers[agent_id]:
                try:
                    callback(message)
                except Exception as e:
                    self.logger.error(f"Subscriber callback error: {e}")

    def _notify_broadcast_subscribers(self, message: Message):
        """通知广播订阅者"""
        for callback in self.broadcast_subscribers:
            try:
                callback(message)
            except Exception as e:
                self.logger.error(f"Broadcast subscriber error: {e}")


class AgentRegistry:
    """Agent 注册与发现"""
    
    def __init__(self, message_bus: MessageBus):
        self.agents: Dict[str, AgentInfo] = {}
        self.message_bus = message_bus
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)

    def register(self, agent: AgentInfo) -> str:
        """注册 Agent"""
        with self.lock:
            self.agents[agent.agent_id] = agent
            agent.status = AgentStatus.IDLE
            self.message_bus.register_agent(agent.agent_id)
            
            # 发送发现消息
            discovery_msg = Message(
                sender_id=agent.agent_id,
                message_type=MessageType.DISCOVERY,
                content={'agent': agent.to_dict()}
            )
            self.message_bus.send(discovery_msg)
            
            self.logger.info(f"Agent registered: {agent.name} ({agent.agent_id})")
            return agent.agent_id

    def unregister(self, agent_id: str):
        """注销 Agent"""
        with self.lock:
            if agent_id in self.agents:
                agent = self.agents[agent_id]
                agent.status = AgentStatus.OFFLINE
                del self.agents[agent_id]
                self.message_bus.unregister_agent(agent_id)
                self.logger.info(f"Agent unregistered: {agent_id}")

    def update_heartbeat(self, agent_id: str):
        """更新心跳"""
        with self.lock:
            if agent_id in self.agents:
                self.agents[agent_id].last_heartbeat = datetime.now()

    def update_status(self, agent_id: str, status: AgentStatus, load: float = 0.0):
        """更新状态"""
        with self.lock:
            if agent_id in self.agents:
                self.agents[agent_id].status = status
                self.agents[agent_id].load = load

    def get_agent(self, agent_id: str) -> Optional[AgentInfo]:
        """获取 Agent 信息"""
        with self.lock:
            return self.agents.get(agent_id)

    def get_available_agents(self, capability: str = None) -> List[AgentInfo]:
        """获取可用的 Agent"""
        with self.lock:
            agents = [a for a in self.agents.values() 
                     if a.status in [AgentStatus.IDLE, AgentStatus.BUSY]]
            if capability:
                agents = [a for a in agents if capability in a.capabilities]
            return agents

    def get_all_agents(self) -> Dict[str, AgentInfo]:
        """获取所有 Agent"""
        with self.lock:
            return dict(self.agents)


class LoadBalancer:
    """负载均衡器"""
    
    def __init__(self, registry: AgentRegistry):
        self.registry = registry
        self.logger = logging.getLogger(__name__)

    def select_agent(self, required_capability: str = None) -> Optional[str]:
        """选择最佳 Agent - 基于负载和能力"""
        candidates = self.registry.get_available_agents(required_capability)
        
        if not candidates:
            return None
        
        # 选择负载最低的 Agent
        candidates.sort(key=lambda a: a.load)
        best_agent = candidates[0]
        
        self.logger.debug(f"Selected agent: {best_agent.name} (load: {best_agent.load})")
        return best_agent.agent_id


class CollaborativeAgent:
    """协作 Agent 实现"""
    
    def __init__(self, name: str, capabilities: List[str] = None,
                 message_bus: MessageBus = None, registry: AgentRegistry = None):
        self.agent_id = str(uuid.uuid4())
        self.name = name
        self.capabilities = capabilities or []
        
        self.message_bus = message_bus
        self.registry = registry
        self.load_balancer = LoadBalancer(registry) if registry else None
        
        self.task_handlers: Dict[str, Callable] = {}
        self.is_running = False
        self.thread: Optional[threading.Thread] = None
        
        self.info = AgentInfo(
            agent_id=self.agent_id,
            name=name,
            capabilities=self.capabilities
        )
        
        self.logger = logging.getLogger(f"Agent.{name}")

    def register_task_handler(self, task_type: str, handler: Callable):
        """注册任务处理器"""
        self.task_handlers[task_type] = handler
        if task_type not in self.capabilities:
            self.capabilities.append(task_type)
            self.info.capabilities = self.capabilities

    def start(self):
        """启动 Agent"""
        if self.is_running:
            return
        
        self.is_running = True
        
        if self.registry:
            self.registry.register(self.info)
        
        self.thread = threading.Thread(target=self._message_loop, daemon=True)
        self.thread.start()
        
        self.logger.info(f"Agent started: {self.name}")
        
        # 启动心跳
        self._start_heartbeat()

    def stop(self):
        """停止 Agent"""
        self.is_running = False
        if self.registry:
            self.registry.unregister(self.agent_id)
        if self.thread:
            self.thread.join(timeout=5)
        self.logger.info(f"Agent stopped: {self.name}")

    def _message_loop(self):
        """消息处理循环"""
        while self.is_running:
            try:
                if self.message_bus:
                    msg = self.message_bus.receive(self.agent_id, timeout=0.1)
                    if msg:
                        self._handle_message(msg)
                else:
                    time.sleep(0.1)
            except Exception as e:
                self.logger.error(f"Message loop error: {e}")

    def _handle_message(self, message: Message):
        """处理消息"""
        try:
            if message.message_type == MessageType.TASK_ASSIGNMENT:
                self._handle_task_assignment(message)
            elif message.message_type == MessageType.HEARTBEAT:
                self.registry.update_heartbeat(message.sender_id)
            elif message.message_type == MessageType.DISCOVERY:
                self.logger.debug(f"Discovery from: {message.sender_id}")
        except Exception as e:
            self.logger.error(f"Message handling error: {e}")

    def _handle_task_assignment(self, message: Message):
        """处理任务分配"""
        task_data = message.content.get('task')
        if not task_data:
            return
        
        task_type = task_data.get('task_type')
        
        # 更新状态为忙碌
        self.registry.update_status(self.agent_id, AgentStatus.BUSY, load=0.8)
        
        try:
            if task_type in self.task_handlers:
                handler = self.task_handlers[task_type]
                result = handler(task_data.get('parameters', {}))
                
                # 发送结果
                result_msg = Message(
                    sender_id=self.agent_id,
                    receiver_id=message.sender_id,
                    message_type=MessageType.TASK_RESULT,
                    content={'task_id': task_data.get('task_id'), 'result': result}
                )
                self.message_bus.send(result_msg)
            else:
                self.logger.warning(f"No handler for task type: {task_type}")
        finally:
            # 更新状态为空闲
            self.registry.update_status(self.agent_id, AgentStatus.IDLE, load=0.0)

    def _start_heartbeat(self):
        """启动心跳"""
        def heartbeat_loop():
            while self.is_running:
                try:
                    if self.registry:
                        self.registry.update_heartbeat(self.agent_id)
                    if self.message_bus:
                        heartbeat_msg = Message(
                            sender_id=self.agent_id,
                            message_type=MessageType.HEARTBEAT
                        )
                        self.message_bus.send(heartbeat_msg)
                except:
                    pass
                time.sleep(5)
        
        threading.Thread(target=heartbeat_loop, daemon=True).start()

    def submit_task(self, task_type: str, parameters: Dict[str, Any],
                   description: str = "") -> Optional[str]:
        """提交任务到协作系统"""
        if not self.load_balancer:
            return None
        
        target_agent = self.load_balancer.select_agent(task_type)
        if not target_agent:
            self.logger.warning(f"No available agent for task type: {task_type}")
            return None
        
        task_id = str(uuid.uuid4())
        task_msg = Message(
            sender_id=self.agent_id,
            receiver_id=target_agent,
            message_type=MessageType.TASK_ASSIGNMENT,
            content={
                'task_id': task_id,
                'task_type': task_type,
                'description': description,
                'parameters': parameters
            }
        )
        self.message_bus.send(task_msg)
        return task_id


class MultiAgentSystem:
    """多 Agent 系统主类"""
    
    def __init__(self):
        self.message_bus = MessageBus()
        self.registry = AgentRegistry(self.message_bus)
        self.agents: Dict[str, CollaborativeAgent] = {}
        self.logger = logging.getLogger(__name__)

    def create_agent(self, name: str, capabilities: List[str] = None) -> CollaborativeAgent:
        """创建并注册 Agent"""
        agent = CollaborativeAgent(
            name=name,
            capabilities=capabilities or [],
            message_bus=self.message_bus,
            registry=self.registry
        )
        self.agents[agent.agent_id] = agent
        return agent

    def start_all(self):
        """启动所有 Agent"""
        for agent in self.agents.values():
            agent.start()
        self.logger.info("All agents started")

    def stop_all(self):
        """停止所有 Agent"""
        for agent in self.agents.values():
            agent.stop()
        self.logger.info("All agents stopped")

    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        return {
            'agents_count': len(self.agents),
            'agents': {aid: a.info.to_dict() for aid, a in self.agents.items()},
            'registry': {aid: info.to_dict() for aid, info in self.registry.get_all_agents().items()}
        }
