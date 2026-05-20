"""
Comprehensive Test Suite for Distributed Task Queue
Covers:
- Unit tests for LocalTaskQueue
- Boundary condition tests
- Integration tests
- Performance tests
- Edge case testing
"""
import sys
import os
import time
import threading
import logging
import random
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from execution.distributed_task_queue import (
    DistributedTaskExecutor,
    TaskPriority,
    DistributedTask,
    LocalTaskQueue
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TestResults:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.total = 0
        self.errors = []

test_results = TestResults()

def assert_true(condition, message):
    test_results.total += 1
    if condition:
        test_results.passed += 1
        logger.info(f"✓ {message}")
    else:
        test_results.failed += 1
        test_results.errors.append(message)
        logger.error(f"✗ {message}")

def test_task_basics():
    """Test basic task creation and serialization"""
    logger.info("\n=== Test 1: Task Basics ===")
    
    # Test task creation
    task = DistributedTask(
        task_type="test",
        description="test task",
        parameters={"x": 10},
        priority=TaskPriority.HIGH,
        weight=2.0
    )
    
    assert_true(task.task_id is not None, "Task ID should be generated")
    assert_true(task.task_type == "test", "Task type should be 'test'")
    assert_true(task.priority == TaskPriority.HIGH, "Task priority should be HIGH")
    
    # Test serialization
    data = task.to_dict()
    assert_true(data['task_id'] == task.task_id, "Serialized task ID should match")
    
    task2 = DistributedTask.from_dict(data)
    assert_true(task2.task_type == task.task_type, "Deserialized task should match")
    
    logger.info(f"Task basics: {test_results.passed}/{test_results.total} passed")

def test_local_queue_enqueue_dequeue():
    """Test LocalTaskQueue enqueue and dequeue"""
    logger.info("\n=== Test 2: Local Queue Enqueue/Dequeue ===")
    
    queue = LocalTaskQueue()
    
    # Enqueue tasks
    task1 = DistributedTask(task_type="a", priority=TaskPriority.LOW)
    task2 = DistributedTask(task_type="b", priority=TaskPriority.CRITICAL)
    task3 = DistributedTask(task_type="c", priority=TaskPriority.HIGH)
    
    queue.enqueue(task1)
    queue.enqueue(task2)
    queue.enqueue(task3)
    
    # Dequeue should be priority ordered: CRITICAL, HIGH, LOW
    dequeued1 = queue.dequeue()
    assert_true(dequeued1 is not None, "Should dequeue first task")
    assert_true(dequeued1.priority == TaskPriority.CRITICAL, "First should be CRITICAL")
    
    dequeued2 = queue.dequeue()
    assert_true(dequeued2 is not None, "Should dequeue second task")
    assert_true(dequeued2.priority == TaskPriority.HIGH, "Second should be HIGH")
    
    dequeued3 = queue.dequeue()
    assert_true(dequeued3 is not None, "Should dequeue third task")
    assert_true(dequeued3.priority == TaskPriority.LOW, "Third should be LOW")
    
    assert_true(queue.dequeue() is None, "Queue should be empty now")
    
    logger.info(f"Local queue enqueue/dequeue: {test_results.passed}/{test_results.total} passed")

def test_local_queue_complete_fail():
    """Test completing and failing tasks"""
    logger.info("\n=== Test 3: Local Queue Complete/Fail ===")
    
    queue = LocalTaskQueue()
    
    task1 = DistributedTask(task_type="success", max_retries=1)
    queue.enqueue(task1)
    
    # Complete
    dequeued_task = queue.dequeue()
    result = queue.complete(dequeued_task.task_id, "success_result")
    
    assert_true(result, "Complete should succeed")
    assert_true(queue.total_completed == 1, "total_completed should be 1")
    
    # Fail with retry
    task2 = DistributedTask(task_type="fail", max_retries=1)
    queue.enqueue(task2)
    dequeued_task2 = queue.dequeue()
    result = queue.fail(dequeued_task2.task_id, "test error")
    
    assert_true(result, "Fail should succeed")
    
    # Should be retried - back in queue
    retry_task = queue.dequeue()
    assert_true(retry_task is not None, "Should have retry task in queue")
    
    # Fail again - should go to failed
    result = queue.fail(retry_task.task_id, "test error 2")
    assert_true(result, "Second fail should succeed")
    assert_true(queue.total_failed == 1, "total_failed should be 1")
    
    logger.info(f"Local queue complete/fail: {test_results.passed}/{test_results.total} passed")

def test_local_queue_stats():
    """Test stats tracking"""
    logger.info("\n=== Test 4: Local Queue Statistics ===")
    
    queue = LocalTaskQueue()
    
    # Add some tasks
    for i in range(10):
        queue.enqueue(DistributedTask(task_type="test"))
    
    # Process some
    for i in range(5):
        task = queue.dequeue()
        queue.complete(task.task_id, f"result{i}")
    
    # Fail some
    for i in range(3):
        task = queue.dequeue()
        queue.fail(task.task_id, f"error{i}")
    
    stats = queue.get_stats()
    
    assert_true(stats['running'] == 0, "Running should be 0")
    assert_true(stats['completed'] == 5, "Completed should be 5")
    assert_true(stats['failed'] == 3, "Failed should be 3")
    assert_true(stats['pending']['total'] == 2, "Pending should be 2 (10-5-3)")
    
    logger.info(f"Local queue stats: {test_results.passed}/{test_results.total} passed")

def test_executor_basic():
    """Test basic executor functionality"""
    logger.info("\n=== Test 5: Executor Basic ===")
    
    executor = DistributedTaskExecutor(use_redis=False, max_workers=4)
    
    # Register a handler
    def square(params):
        return params.get('x', 0) ** 2
    
    executor.register_handler('square', square)
    
    # Start
    executor.start(num_workers=4)
    
    # Submit tasks
    task_ids = []
    for i in range(20):
        task_id = executor.submit_task('square', f"task{i}", {'x': i})
        task_ids.append(task_id)
    
    # Wait for completion
    time.sleep(1)
    
    # Check stats
    stats = executor.get_statistics()
    
    assert_true(stats['completed'] == 20, "Should have completed 20 tasks")
    assert_true(stats['failed'] == 0, "Should have 0 failed tasks")
    
    executor.stop()
    logger.info(f"Executor basic: {test_results.passed}/{test_results.total} passed")

def test_executor_multiple_handlers():
    """Test multiple handler types"""
    logger.info("\n=== Test 6: Multiple Handlers ===")
    
    executor = DistributedTaskExecutor(use_redis=False, max_workers=8)
    
    def add(params):
        return params.get('a', 0) + params.get('b', 0)
    
    def multiply(params):
        return params.get('a', 1) * params.get('b', 1)
    
    def delay(params):
        time.sleep(0.001)
        return "delayed"
    
    executor.register_handler('add', add)
    executor.register_handler('multiply', multiply)
    executor.register_handler('delay', delay)
    
    executor.start(num_workers=8)
    
    for i in range(30):
        if i % 3 == 0:
            executor.submit_task('add', parameters={'a': i, 'b': i+1})
        elif i % 3 == 1:
            executor.submit_task('multiply', parameters={'a': i, 'b': i+1})
        else:
            executor.submit_task('delay', parameters={})
    
    time.sleep(2)
    
    stats = executor.get_statistics()
    assert_true(stats['completed'] == 30, "Should have 30 completed")
    assert_true(stats['failed'] == 0, "Should have 0 failed")
    
    executor.stop()
    logger.info(f"Multiple handlers: {test_results.passed}/{test_results.total} passed")

def test_executor_unknown_handler():
    """Test handling unknown task types"""
    logger.info("\n=== Test 7: Unknown Handlers ===")
    
    executor = DistributedTaskExecutor(use_redis=False, max_workers=2)
    executor.start(num_workers=2)
    
    # Submit task with no handler
    executor.submit_task('unknown', "test", {})
    
    time.sleep(0.5)
    
    stats = executor.get_statistics()
    assert_true(stats['failed'] == 1, "Should have failed 1 task")
    
    executor.stop()
    logger.info(f"Unknown handler: {test_results.passed}/{test_results.total} passed")

def test_boundary_conditions():
    """Test various boundary conditions"""
    logger.info("\n=== Test 8: Boundary Conditions ===")
    
    queue = LocalTaskQueue()
    
    # Test many tasks
    for i in range(1000):
        queue.enqueue(DistributedTask(
            task_type=f"task_{i}",
            priority=random.choice(list(TaskPriority))
        ))
    
    assert_true(queue.get_pending_count() == 1000, "Should have 1000 pending tasks")
    
    # Process all
    completed = 0
    while True:
        task = queue.dequeue()
        if task:
            queue.complete(task.task_id, "result")
            completed += 1
        else:
            break
    
    stats = queue.get_stats()
    assert_true(stats['completed'] == 1000, "Should have 1000 completed")
    
    logger.info(f"Boundary conditions: {test_results.passed}/{test_results.total} passed")

def test_concurrent_access():
    """Test concurrent access to queue"""
    logger.info("\n=== Test 9: Concurrent Access ===")
    
    queue = LocalTaskQueue()
    errors = []
    lock = threading.Lock()
    
    def producer():
        try:
            for i in range(100):
                queue.enqueue(DistributedTask(task_type="test"))
        except Exception as e:
            with lock:
                errors.append(str(e))
    
    def consumer():
        try:
            for i in range(100):
                task = queue.dequeue()
                if task:
                    queue.complete(task.task_id, "done")
        except Exception as e:
            with lock:
                errors.append(str(e))
    
    # Create threads
    threads = []
    for i in range(5):
        t = threading.Thread(target=producer)
        threads.append(t)
        t = threading.Thread(target=consumer)
        threads.append(t)
    
    # Start
    for t in threads:
        t.start()
    
    # Wait
    for t in threads:
        t.join(timeout=5)
    
    # Check
    assert_true(len(errors) == 0, f"No concurrent errors, found {len(errors)} errors")
    
    logger.info(f"Concurrent access: {test_results.passed}/{test_results.total} passed")

def test_priority_ordering():
    """Test strict priority ordering"""
    logger.info("\n=== Test 10: Priority Ordering ===")
    
    queue = LocalTaskQueue()
    
    # Enqueue in reverse priority
    queue.enqueue(DistributedTask(task_type="low1", priority=TaskPriority.LOW))
    queue.enqueue(DistributedTask(task_type="normal1", priority=TaskPriority.NORMAL))
    queue.enqueue(DistributedTask(task_type="high1", priority=TaskPriority.HIGH))
    queue.enqueue(DistributedTask(task_type="critical1", priority=TaskPriority.CRITICAL))
    queue.enqueue(DistributedTask(task_type="critical2", priority=TaskPriority.CRITICAL))
    queue.enqueue(DistributedTask(task_type="high2", priority=TaskPriority.HIGH))
    queue.enqueue(DistributedTask(task_type="low2", priority=TaskPriority.LOW))
    
    # Dequeue and check order
    order = []
    while True:
        task = queue.dequeue()
        if task:
            order.append(task.task_type)
        else:
            break
    
    expected_start = ['critical1', 'critical2', 'high1', 'high2', 'normal1']
    assert_true(order[:5] == expected_start, 
                f"Priority order should be critical first, got {order[:5]}")
    
    logger.info(f"Priority ordering: {test_results.passed}/{test_results.total} passed")

def test_stress_mini():
    """Mini stress test"""
    logger.info("\n=== Test 11: Mini Stress ===")
    
    executor = DistributedTaskExecutor(use_redis=False, max_workers=16)
    
    def fast_handler(params):
        return "ok"
    
    executor.register_handler('fast', fast_handler)
    executor.start(num_workers=16)
    
    # Submit 1000 tasks
    num_tasks = 1000
    for i in range(num_tasks):
        executor.submit_task('fast')
    
    start = time.time()
    time.sleep(2)
    end = time.time()
    
    stats = executor.get_statistics()
    assert_true(stats['completed'] >= num_tasks, 
                f"Should have completed at least {num_tasks} tasks")
    
    logger.info(f"Mini stress: {test_results.passed}/{test_results.total} passed")
    logger.info(f"  Throughput: {stats['completed']/(end-start):.0f} tasks/sec")
    
    executor.stop()

def run_all_tests():
    """Run all comprehensive tests"""
    logger.info("=" * 80)
    logger.info("COMPREHENSIVE TEST SUITE STARTING")
    logger.info("=" * 80)
    
    try:
        test_task_basics()
        test_local_queue_enqueue_dequeue()
        test_local_queue_complete_fail()
        test_local_queue_stats()
        test_executor_basic()
        test_executor_multiple_handlers()
        test_executor_unknown_handler()
        test_boundary_conditions()
        test_concurrent_access()
        test_priority_ordering()
        test_stress_mini()
        
    except Exception as e:
        logger.error(f"Test suite failed with exception: {e}", exc_info=True)
        test_results.failed += 1
        test_results.errors.append(f"Fatal error: {e}")
    
    logger.info("\n" + "=" * 80)
    logger.info("COMPREHENSIVE TEST SUITE COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Total tests: {test_results.total}")
    logger.info(f"Passed:      {test_results.passed}")
    logger.info(f"Failed:      {test_results.failed}")
    
    if test_results.errors:
        logger.warning("\nErrors:")
        for i, err in enumerate(test_results.errors):
            logger.warning(f"  {i+1}. {err}")
    
    success = test_results.failed == 0
    logger.info(f"\n{'✓ ALL TESTS PASSED!' if success else '✗ SOME TESTS FAILED!'}")
    logger.info("=" * 80)
    
    return success

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
