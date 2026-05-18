#!/usr/bin/env python3
"""
快速诊断测试 - 快速发现系统缺陷
Quick Diagnostic Test
"""

import sys
import time
import logging
import threading
import traceback
from datetime import datetime
import random

sys.path.insert(0, '/workspace/evolving_agent')

from core.dual_loop_drive_engine import DualLoopDriveEngine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def quick_diagnostic_test():
    print("=" * 80)
    print("QUICK DIAGNOSTIC TEST - FINDING SYSTEM DEFECTS")
    print("=" * 80)
    
    engine = None
    issues_found = []
    
    try:
        print("\n1. Initializing engine...")
        engine = DualLoopDriveEngine()
        engine.initialize()
        print("   ✓ Engine initialized")
        
        print("\n2. Starting engine...")
        engine.start()
        print("   ✓ Engine started")
        
        print("\n3. Testing task execution...")
        test_task_result = None
        
        def simple_task(params):
            return f"Simple result: {params}"
        
        engine.task_engine.register_handler('simple', simple_task)
        
        engine.add_task('simple', 'Test simple task', {'test': 'data'})
        
        time.sleep(2)
        
        queue_stats = engine.task_engine.task_queue.get_queue_stats()
        print(f"   Queue stats: {queue_stats}")
        
        if queue_stats['pending'] + queue_stats['running'] > 0:
            print("   ✓ Tasks are being processed")
        else:
            issues_found.append({
                'severity': 'high',
                'type': 'task_execution_not_issue',
                'description': 'Tasks not being processed properly'
            })
            print("   ✗ Task processing issue detected")
            
        print("\n4. Testing knowledge graph...")
        engine.integrate_experience('test', 'This is a test experience')
        results = engine.query_knowledge('test', max_results=5)
        print(f"   ✓ Knowledge query results: {len(results)}")
        
        print("\n5. Testing system status...")
        status = engine.get_system_status()
        print(f"   ✓ System status retrieved")
        
        print("\n6. Testing rapid task flooding test...")
        
        def comp_task(params):
            time.sleep(0.1)
            return sum(i*i for i in range(params.get('n', 1000)))
        
        engine.task_engine.register_handler('compute', comp_task)
        
        for i in range(100):
            engine.add_task('compute', f'Quick compute {i}', {'n': random.randint(100, 1000)})
            time.sleep(0.05)
            
        print("   ✓ 100 tasks submitted")
        time.sleep(10)
        
        final_stats = engine.task_engine.task_queue.get_queue_stats()
        print(f"   Final queue stats: {final_stats}")
        
        if final_stats['completed'] < 50:
            issues_found.append({
                'severity': 'medium',
                'type': 'low_task_throughput',
                'description': f'Only {final_stats["completed"]} tasks completed in 10s'
            })
            print("   ✗ Low throughput issue detected")
            
        print("\n7. Stopping engine...")
        engine.stop()
        print("   ✓ Engine stopped")
        
        print("\n" + "=" * 80)
        print("DIAGNOSTIC SUMMARY")
        print("=" * 80)
        
        if issues_found:
            print("\nISSUES FOUND:")
            for i, issue in enumerate(issues_found):
                print(f"\n{i+1}. [{issue['severity']}] {issue['type']}")
                print(f"   {issue['description']}")
        else:
            print("\n✓ No major issues found in quick test!")
            
    except Exception as e:
        print(f"\n✗ FATAL ERROR: {e}")
        traceback.print_exc()
        issues_found.append({
            'severity': 'critical',
            'type': 'fatal_error',
            'description': str(e)
        })
    finally:
        if engine:
            try:
                engine.stop()
            except:
                pass
                
    return issues_found


if __name__ == '__main__':
    issues = quick_diagnostic_test()
    sys.exit(1 if issues else 0)
