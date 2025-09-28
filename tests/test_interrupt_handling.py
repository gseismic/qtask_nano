#!/usr/bin/env python3
"""
测试KeyboardInterrupt处理逻辑
"""
import time
import signal
import sys
from qtask_nano.task_queue import TaskQueue, Task, Worker

def test_task_handler(params):
    """测试任务处理器"""
    print(f"处理任务: {params}")
    time.sleep(2)  # 模拟长时间处理
    print(f"任务完成: {params}")

def signal_handler(signum, frame):
    """信号处理器"""
    print(f"\n收到信号 {signum}，模拟KeyboardInterrupt")
    raise KeyboardInterrupt()

def main():
    print("=== 测试KeyboardInterrupt处理逻辑 ===")
    
    # 创建队列和worker
    queue = TaskQueue("test_queue", "redis://localhost:6379/0")
    queue.clear_all_queues(dry_run=False)
    worker = Worker(queue, "test_worker")
    
    # 注册任务处理器
    worker.register_task("test_task", test_task_handler)
    
    # 添加一些测试任务
    for i in range(5):
        task = Task("test_task", {"id": i, "message": f"测试任务 {i}"})
        queue.add_task(task)
        print(f"添加任务: {task.task_id}")
    
    print("\n启动worker，将在3秒后模拟KeyboardInterrupt...")
    
    # 设置信号处理器
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(3)  # 3秒后发送SIGALRM信号
    
    try:
        # 启动worker
        worker.run(poll_timeout=1, delay=0.1)
    except KeyboardInterrupt:
    # finally:
        # print("\n捕获到KeyboardInterrupt，检查任务状态...")
        
        # 检查doing状态的任务
        doing_tasks = queue.get_doing_tasks()
        print(f"doing状态任务数量: {len(doing_tasks)}")
        
        # 检查todo状态的任务
        # 注意：这里需要直接访问底层队列来检查todo状态
        todo_count = queue.queue.redis.llen(queue.queue._todo_rkey)
        print(f"todo状态任务数量: {todo_count}")
        
        if len(doing_tasks) == 0 and todo_count == 5:
            print("✅ 测试通过：所有doing任务都已移回todo队列")
        else:
            print("❌ 测试失败：任务状态不正确")
    
    print("\n测试完成")

if __name__ == "__main__":
    main()
