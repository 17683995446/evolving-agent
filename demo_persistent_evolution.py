#!/usr/bin/env python3
"""
Persistent Evolution Agent - Demonstration
持续进化Agent系统演示
"""

import sys
import time
import logging
import threading
from datetime import datetime

sys.path.insert(0, '/workspace/evolving_agent')

from core.dual_loop_drive_engine import DualLoopDriveEngine, PerpetualEvolutionController
from core.data_models_universal import ExecutionLevel

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def simple_compute_task(params):
    n = params.get('n', 100)
    result = sum(i * i for i in range(n))
    time.sleep(0.05)
    return result


def analysis_task(params):
    time.sleep(0.02)
    return {'analysis': 'complete', 'query': params.get('query')}


def optimization_task(params):
    time.sleep(0.03)
    return {'optimized': True, 'target': params.get('target')}


def learning_task(params):
    time.sleep(0.04)
    return {'learned': True, 'content': params.get('content', '')}


class PersistentEvolutionDemo:
    def __init__(self):
        self.engine = None
        self.evolution_controller = None
        self.is_demo_running = False
        self.logger = logging.getLogger(__name__)

    def setup_engine(self):
        self.engine = DualLoopDriveEngine({'max_workers': 6, 'optimization_interval': 10.0})
        self.engine.initialize()
        
        self.engine.task_engine.register_handler('compute', simple_compute_task)
        self.engine.task_engine.register_handler('analysis', analysis_task)
        self.engine.task_engine.register_handler('optimization', optimization_task)
        self.engine.task_engine.register_handler('learning', learning_task)
        
        self.evolution_controller = PerpetualEvolutionController(self.engine)
        self.logger.info("Engine setup complete")

    def add_sample_tasks(self):
        tasks = [
            ('analysis', 'Analyze system performance metrics'),
            ('optimization', 'Optimize query execution'),
            ('learning', 'Learn from recent optimizations'),
            ('analysis', 'Analyze resource utilization'),
            ('optimization', 'Optimize memory management'),
        ]
        for task_type, desc in tasks:
            self.engine.add_task(task_type, desc, {'target': 'test', 'query': 'test'})
            self.logger.info(f"Added task: {desc}")

    def run_demo_cycle(self, duration_seconds=20):
        self.logger.info(f"Starting demo cycle for {duration_seconds} seconds...")
        self.is_demo_running = True
        self.engine.start()
        self.evolution_controller.start_evolution()
        start_time = time.time()
        task_counter = 0
        
        while time.time() - start_time < duration_seconds:
            if task_counter < 10:
                task_counter += 1
                self.engine.add_task(
                    'compute',
                    f'Compute task #{task_counter}',
                    {'n': 1000 + task_counter * 100}
                )
            
            stats = self.engine.task_engine.get_statistics()
            queue_stats = stats['queue_stats']
            self.logger.info(
                f"Status - Tasks: "
                f"P={queue_stats['pending']} "
                f"R={queue_stats['running']} "
                f"C={queue_stats['completed']} "
                f"F={queue_stats['failed']}"
            )
            time.sleep(2)

        self.is_demo_running = False
        self.engine.stop()
        self.logger.info("Demo cycle complete")

    def demonstrate_knowledge_graph(self):
        self.logger.info("Demonstrating knowledge graph integration...")
        self.engine.integrate_experience(
            experience_type="demo_run",
            content="Successfully ran full demo of evolving agent system",
        )
        self.engine.integrate_project(
            project_name="Evolved System",
            description="Self-evolving autonomous agent",
            technologies=["Python", "AI", "Concurrent"],
            outcomes="System successfully runs",
        )
        results = self.engine.query_knowledge("demo", max_results=5)
        self.logger.info(f"Knowledge query returned {len(results)} results")

    def demonstrate_adaptive_behavior(self):
        self.logger.info("Demonstrating adaptive execution behavior...")
        self.engine.adjust_execution_level(ExecutionLevel.ENHANCED)
        self.logger.info("Adjusted to ENHANCED execution level")
        
        for i in range(10):
            self.engine.add_task('compute', f'Dynamic task {i}', {'n': 500})
            time.sleep(0.1)
            
        self.engine.adjust_execution_level(ExecutionLevel.STANDARD)
        self.logger.info("Returned to STANDARD execution level")

    def run_full_demonstration(self):
        print("=" * 60)
        print("PERSISTENT EVOLUTION AGENT - FULL DEMONSTRATION")
        print("=" * 60)
        print()

        print("Step 1: Initializing Engine...")
        self.setup_engine()
        print()

        print("Step 2: Running Main Demo Cycle (20 seconds)...")
        print("  - Engine starts in perpetual execution mode")
        print("  - Tasks execute continuously")
        print("  - Optimization runs in parallel")
        print()
        self.run_demo_cycle(duration_seconds=20)
        print()

        print("Step 3: Demonstrating Knowledge Integration...")
        self.demonstrate_knowledge_graph()
        print()

        print("Step 4: Demonstrating Adaptive Behavior...")
        self.demonstrate_adaptive_behavior()
        print()

        final_stats = self.engine.task_engine.get_statistics()
        print("Step 5: Final System Status...")
        print(f"  Total Completed Tasks: {final_stats['queue_stats']['completed']}")
        print(f"  Total Failed Tasks: {final_stats['queue_stats']['failed']}")
        print(f"  Knowledge Graph Stats: {self.engine.knowledge_graph.get_statistics()}")
        print()

        print("=" * 60)
        print("DEMONSTRATION COMPLETE!")
        print("=" * 60)
        print()


def demonstrate_concurrent_tasks():
    print("\n" + "=" * 60)
    print("CONCURRENT TASK EXECUTION DEMO")
    print("=" * 60 + "\n")
    
    engine = DualLoopDriveEngine({'max_workers': 4})
    engine.initialize()
    
    engine.task_engine.register_handler('compute', simple_compute_task)
    engine.start()
    
    print("Launching 50 concurrent compute tasks...")
    for i in range(50):
        engine.add_task('compute', f'Concurrent task {i}', {'n': 200 + i * 50})
    
    time.sleep(10)
    stats = engine.task_engine.get_statistics()
    print(f"\nConcurrent Execution Results:")
    print(f"  Completed: {stats['queue_stats']['completed']}")
    print(f"  Failed: {stats['queue_stats']['failed']}")
    engine.stop()


if __name__ == '__main__':
    demo = PersistentEvolutionDemo()

    try:
        demo.run_full_demonstration()
        demonstrate_concurrent_tasks()

    except KeyboardInterrupt:
        print("\n\nDemo interrupted by user")
        if demo.engine:
            demo.engine.stop()
    except Exception as e:
        print(f"\n\nDemo error: {e}")
        import traceback
        traceback.print_exc()
        if demo.engine:
            demo.engine.stop()
