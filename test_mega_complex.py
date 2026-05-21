"""
超级复杂场景测试 - Mega Complex Test Suite
包含：
1. 百万级任务规模
2. 复杂任务依赖链
3. 极端异常场景
4. 并发竞态条件
5. 资源限制模拟
6. 真实业务流程模拟
"""
import sys
import os
import time
import threading
import logging
import random
import traceback
from datetime import datetime
from typing import Dict, Any, List, Optional
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from execution.distributed_task_queue import (
    DistributedTaskExecutor, TaskPriority, DistributedTask, LocalTaskQueue
)

# 配置详细日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("MegaComplex")

class MegaComplexTest:
    def __init__(self):
        self.executor = None
        self.running = False
        self.total_submitted = 0
        self.total_expected = 0
        self.start_time = None
        
        # 统计追踪
        self.stats = {
            'success': 0,
            'failed': 0,
            'total': 0,
            'errors': [],
            'warnings': []
        }
        
        # 任务依赖追踪
        self.dependencies = {}
        self.completed_dependencies = set()
        
        # 模拟资源
        self.simulated_resources = {
            'cpu_usage': 0,
            'memory_usage': 0,
            'network_latency': 0
        }
        
        self.lock = threading.Lock()
        
    # ============ 复杂处理器定义 ============
    
    def handler_data_processing(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """复杂数据处理任务"""
        data = params.get('data', [])
        if not data:
            raise ValueError("No data provided")
        
        result = {
            'count': len(data),
            'sum': sum(data),
            'avg': sum(data) / len(data),
            'max': max(data),
            'min': min(data),
            'processed_at': datetime.now().isoformat()
        }
        
        # 模拟处理延迟
        time.sleep(random.uniform(0.001, 0.005))
        return result
    
    def handler_dependency_task(self, params: Dict[str, Any]) -> str:
        """依赖其他任务的任务"""
        depends_on = params.get('depends_on', [])
        
        # 等待依赖完成
        wait_count = 0
        max_wait = 100
        for dep_id in depends_on:
            while dep_id not in self.completed_dependencies:
                time.sleep(0.001)
                wait_count += 1
                if wait_count > max_wait:
                    raise RuntimeError(f"Dependency timeout: {dep_id}")
        
        return f"Dependency task completed: {params.get('name', 'unknown')}"
    
    def handler_resource_hungry(self, params: Dict[str, Any]) -> str:
        """资源密集型任务"""
        resource_type = params.get('type', 'cpu')
        
        if resource_type == 'cpu':
            # CPU密集计算
            result = 0
            n = 100000
            for i in range(n):
                result += i * i * i
            return f"CPU intensive done: {result}"
            
        elif resource_type == 'memory':
            # 内存密集操作
            data = []
            for i in range(10000):
                data.append({'id': i, 'data': 'x' * 100})
            result = len(data)
            del data
            return f"Memory intensive done: {result} items"
            
        elif resource_type == 'network':
            # 模拟网络延迟
            latency = random.uniform(0.01, 0.05)
            time.sleep(latency)
            return f"Network done: {latency:.3f}s"
        
        return "Resource task done"
    
    def handler_state_machine(self, params: Dict[str, Any]) -> str:
        """复杂状态机任务"""
        state = params.get('state', 'start')
        transitions = {
            'start': 'processing',
            'processing': 'validating',
            'validating': 'completed',
            'completed': 'done'
        }
        
        next_state = transitions.get(state)
        if not next_state:
            raise ValueError(f"Invalid state: {state}")
        
        time.sleep(0.002)
        return next_state
    
    def handler_error_prone(self, params: Dict[str, Any]) -> str:
        """高错误率任务"""
        error_rate = params.get('error_rate', 0.3)
        
        if random.random() < error_rate:
            error_type = random.choice([
                ValueError("Invalid input"),
                RuntimeError("Runtime exception"),
                ConnectionError("Connection failed"),
                TimeoutError("Operation timed out")
            ])
            raise error_type
        
        return "Success despite risk"
    
    def handler_batch_processor(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """批量处理器"""
        items = params.get('items', [])
        results = []
        
        for item in items:
            try:
                processed = {
                    'original': item,
                    'transformed': str(item).upper() if isinstance(item, str) else item * 2,
                    'timestamp': datetime.now().isoformat()
                }
                results.append(processed)
            except Exception as e:
                results.append({'error': str(e), 'original': item})
        
        return {'processed': results, 'total': len(results)}
    
    # ============ 复杂场景生成器 ============
    
    def generate_complex_workflow(self, workflow_id: int):
        """生成复杂工作流"""
        dependencies = []
        
        # 阶段1: 数据采集
        collect_task_id = self.executor.submit_task(
            'data_processing',
            f"Workflow-{workflow_id}-Collect",
            {'data': [random.randint(1, 100) for _ in range(50)]},
            priority=TaskPriority.HIGH
        )
        dependencies.append(collect_task_id)
        
        # 阶段2: 数据处理（依赖阶段1）
        process_task_id = self.executor.submit_task(
            'dependency_task',
            f"Workflow-{workflow_id}-Process",
            {'depends_on': [collect_task_id], 'name': 'Process'},
            priority=TaskPriority.NORMAL
        )
        dependencies.append(process_task_id)
        
        # 阶段3: 资源密集操作
        resource_task_id = self.executor.submit_task(
            'resource_hungry',
            f"Workflow-{workflow_id}-Resource",
            {'type': random.choice(['cpu', 'memory', 'network'])},
            priority=TaskPriority.LOW
        )
        dependencies.append(resource_task_id)
        
        # 阶段4: 状态机验证（依赖阶段2和阶段3）
        state_task_id = self.executor.submit_task(
            'state_machine',
            f"Workflow-{workflow_id}-State",
            {'state': 'start'},
            priority=TaskPriority.NORMAL
        )
        dependencies.append(state_task_id)
        
        # 阶段5: 批量处理（依赖阶段4）
        batch_task_id = self.executor.submit_task(
            'batch_processor',
            f"Workflow-{workflow_id}-Batch",
            {'items': [f"item_{i}" for i in range(20)]},
            priority=TaskPriority.HIGH
        )
        dependencies.append(batch_task_id)
        
        # 记录依赖关系
        self.dependencies[workflow_id] = dependencies
        
        with self.lock:
            self.total_submitted += 5
            self.total_expected += 5
    
    def generate_error_scenario(self, scenario_id: int):
        """生成错误场景"""
        # 高错误率任务
        self.executor.submit_task(
            'error_prone',
            f"Error-{scenario_id}-HighRisk",
            {'error_rate': 0.7},
            priority=TaskPriority.NORMAL
        )
        
        # 资源限制任务
        for i in range(3):
            self.executor.submit_task(
                'resource_hungry',
                f"Error-{scenario_id}-Resource-{i}",
                {'type': 'cpu'},
                priority=TaskPriority.LOW
            )
        
        with self.lock:
            self.total_submitted += 4
            self.total_expected += 4
    
    def generate_massive_load(self, count: int):
        """生成大规模简单任务"""
        for i in range(count):
            priority = random.choice([
                TaskPriority.CRITICAL,
                TaskPriority.HIGH,
                TaskPriority.NORMAL,
                TaskPriority.LOW
            ])
            
            self.executor.submit_task(
                'data_processing',
                f"Massive-{i}",
                {'data': [1, 2, 3, 4, 5]},
                priority=priority
            )
        
        with self.lock:
            self.total_submitted += count
            self.total_expected += count
    
    # ============ 主测试流程 ============
    
    def run(self, duration_minutes=5):
        """运行超级复杂测试"""
        logger.info("=" * 100)
        logger.info("🔥🔥🔥 超级复杂场景测试 - Mega Complex Test Suite 🔥🔥🔥")
        logger.info("=" * 100)
        logger.info(f"⏱️  运行时间: {duration_minutes} 分钟")
        logger.info("=" * 100)
        
        self.running = True
        self.start_time = time.time()
        
        try:
            # 1. 初始化执行器（大规模配置）
            logger.info("🚀 初始化执行器 (128 workers)...")
            self.executor = DistributedTaskExecutor(
                use_redis=False,
                max_workers=128
            )
            
            # 2. 注册所有处理器
            handlers = [
                ('data_processing', self.handler_data_processing),
                ('dependency_task', self.handler_dependency_task),
                ('resource_hungry', self.handler_resource_hungry),
                ('state_machine', self.handler_state_machine),
                ('error_prone', self.handler_error_prone),
                ('batch_processor', self.handler_batch_processor)
            ]
            
            for name, handler in handlers:
                self.executor.register_handler(name, handler)
            logger.info(f"✅ 注册了 {len(handlers)} 个处理器")
            
            # 3. 启动执行器
            self.executor.start(num_workers=64)
            logger.info("✅ 执行器启动 (64 workers)")
            
            # 4. 启动监控线程
            monitor_thread = threading.Thread(
                target=self.monitor_progress,
                daemon=True,
                name="Monitor"
            )
            monitor_thread.start()
            
            # 5. 启动工作流生成线程
            workflow_thread = threading.Thread(
                target=self.workflow_generator,
                daemon=True,
                name="WorkflowGenerator"
            )
            workflow_thread.start()
            
            # 6. 启动错误场景生成线程
            error_thread = threading.Thread(
                target=self.error_scenario_generator,
                daemon=True,
                name="ErrorGenerator"
            )
            error_thread.start()
            
            # 7. 启动大规模负载线程
            load_thread = threading.Thread(
                target=self.massive_load_generator,
                daemon=True,
                name="LoadGenerator"
            )
            load_thread.start()
            
            # 8. 等待测试完成
            logger.info(f"\n⏱️  开始超级复杂场景测试 ({duration_minutes} 分钟)...")
            time.sleep(duration_minutes * 60)
            
            # 9. 停止测试
            logger.info("\n" + "=" * 100)
            logger.info("🛑 停止测试...")
            self.running = False
            
            # 等待线程
            workflow_thread.join(timeout=5)
            error_thread.join(timeout=5)
            load_thread.join(timeout=5)
            
            # 等待队列清空
            logger.info("⏳ 等待队列清空...")
            max_wait = 60
            wait_count = 0
            
            while wait_count < max_wait:
                stats = self.executor.get_statistics()
                pending = stats['pending']['total']
                running = stats['running']
                
                if pending == 0 and running == 0:
                    break
                
                logger.info(f"  - 待处理: {pending:>10,}, 运行中: {running:>3}")
                
                if wait_count > 30 and pending < 100:
                    break
                
                time.sleep(1)
                wait_count += 1
            
            # 获取最终统计
            stats = self.executor.get_statistics()
            
            # 停止执行器
            self.executor.stop()
            
            # 显示最终结果
            end_time = time.time()
            duration = end_time - self.start_time
            
            with self.lock:
                total_submitted = self.total_submitted
            
            completed = stats['completed']
            failed = stats['failed']
            total_processed = completed + failed
            success_rate = (completed / total_processed * 100) if total_processed > 0 else 0
            avg_throughput = total_processed / duration if duration > 0 else 0
            
            logger.info("=" * 100)
            logger.info("📊 最终结果 - 超级复杂场景测试")
            logger.info("=" * 100)
            logger.info(f"📋 执行统计:")
            logger.info(f"  总提交:     {total_submitted:>15,}")
            logger.info(f"  完成:       {completed:>15,}")
            logger.info(f"  失败:       {failed:>15,} ({failed/total_processed*100:.2f}%)")
            logger.info(f"  总处理:     {total_processed:>15,}")
            logger.info(f"  成功率:     {success_rate:>15.2f}%")
            logger.info(f"  总耗时:     {duration:>15.1f}s ({duration/60:.1f} 分钟)")
            logger.info(f"  平均吞吐:   {avg_throughput:>15,.0f} 任务/秒")
            logger.info("=" * 100)
            
            # 验证标准
            passed = True
            
            # 标准1: 完成率 >= 90%
            if success_rate < 90:
                logger.warning(f"❌ 标准1 不通过: 成功率 {success_rate:.1f}% < 90%")
                passed = False
            else:
                logger.info("✅ 标准1 通过: 成功率 >= 90%")
            
            # 标准2: 无严重错误
            if len(self.stats['errors']) > 10:
                logger.warning(f"❌ 标准2 不通过: 严重错误过多 ({len(self.stats['errors'])}个)")
                passed = False
            else:
                logger.info("✅ 标准2 通过: 错误数量在正常范围")
            
            # 标准3: 无崩溃
            logger.info("✅ 标准3 通过: 系统未崩溃")
            
            if passed:
                logger.info("\n🎉 超级复杂场景测试通过！系统表现优秀！")
                return True
            else:
                logger.warning("\n⚠️  超级复杂场景测试部分不通过，需要优化！")
                return False
            
        except KeyboardInterrupt:
            logger.info("⛔ 测试被中断")
            self.running = False
            if self.executor:
                self.executor.stop()
            return False
            
        except Exception as e:
            logger.error(f"❌ 测试崩溃: {e}", exc_info=True)
            self.running = False
            if self.executor:
                try:
                    self.executor.stop()
                except:
                    pass
            return False
    
    def monitor_progress(self):
        """监控进度"""
        last_time = time.time()
        last_total = 0
        
        while self.running:
            try:
                stats = self.executor.get_statistics()
                
                pending = stats['pending']['total']
                running = stats['running']
                completed = stats['completed']
                failed = stats['failed']
                total = completed + failed
                
                now = time.time()
                delta_time = now - last_time
                delta_total = total - last_total
                throughput = delta_total / delta_time if delta_time > 0 else 0
                
                last_time = now
                last_total = total
                
                timestamp = datetime.now().strftime("%H:%M:%S")
                success_rate = (completed / total * 100) if total > 0 else 0
                
                logger.info(
                    f"[{timestamp}] "
                    f"📊 提交: {self.total_submitted:>10,}, "
                    f"完成: {completed:>10,}, "
                    f"失败: {failed:>8,}, "
                    f"待: {pending:>8,}, "
                    f"运行: {running:>3}, "
                    f"速率: {throughput:>8,.0f}/s, "
                    f"成功率: {success_rate:>5.1f}%"
                )
                
                # 检查异常情况
                if pending > 10000 and running == 0:
                    logger.warning("⚠️  警告：队列积压严重但无运行中任务！")
                
                if failed > completed * 0.2:
                    logger.warning("⚠️  警告：失败率超过 20%！")
                
                time.sleep(2)
                
            except Exception as e:
                with self.lock:
                    self.stats['errors'].append(f"Monitor error: {e}")
                logger.error(f"监控错误: {e}")
                time.sleep(1)
    
    def workflow_generator(self):
        """生成复杂工作流"""
        workflow_id = 0
        while self.running:
            try:
                self.generate_complex_workflow(workflow_id)
                workflow_id += 1
                
                # 随机延迟
                time.sleep(random.uniform(0.1, 0.5))
                
            except Exception as e:
                with self.lock:
                    self.stats['errors'].append(f"Workflow generator error: {e}")
                time.sleep(0.5)
    
    def error_scenario_generator(self):
        """生成错误场景"""
        scenario_id = 0
        while self.running:
            try:
                if random.random() < 0.3:  # 30%概率生成错误场景
                    self.generate_error_scenario(scenario_id)
                    scenario_id += 1
                
                time.sleep(random.uniform(0.5, 2.0))
                
            except Exception as e:
                with self.lock:
                    self.stats['errors'].append(f"Error scenario generator error: {e}")
                time.sleep(1.0)
    
    def massive_load_generator(self):
        """生成大规模负载"""
        batch = 0
        while self.running:
            try:
                batch_size = random.randint(100, 500)
                self.generate_massive_load(batch_size)
                batch += 1
                
                time.sleep(random.uniform(0.05, 0.2))
                
            except Exception as e:
                with self.lock:
                    self.stats['errors'].append(f"Load generator error: {e}")
                time.sleep(0.5)

if __name__ == "__main__":
    test = MegaComplexTest()
    duration = 10  # 10分钟大规模测试
    success = test.run(duration_minutes=duration)
    sys.exit(0 if success else 1)
