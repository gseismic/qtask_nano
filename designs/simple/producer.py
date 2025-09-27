#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通用队列生产者示例
Universal Queue Producer Example
"""

import time
from task_queue import TaskQueue, Task

def main():
    """生产者主函数"""
    print("🚀 启动生产者...")
    
    # 选择队列类型 (Redis或PostgreSQL)
    queue_uri = "redis://localhost:6379/0"
    # queue_uri = "postgresql://user:password@localhost:5432/taskdb"
    
    # 创建队列
    queue = TaskQueue(namespace="universal_demo", uri=queue_uri)
    
    # 创建任务
    tasks = [
        Task("demo_task", {"message": "Hello Universal Queue!", "timestamp": time.time()}),
        Task("demo_task", {"message": "Supports multiple backends", "timestamp": time.time()}),
        Task("long_task", {"duration": 10}),  # 这个任务会超时
    ]
    
    # 添加任务到队列
    for i, task in enumerate(tasks, 1):
        queue.add_task(task)
        print(f"✅ 已添加任务 {i}: {task.task_id} ({task.task_type})")
        time.sleep(0.5)
    
    print(f"\n📊 总共添加了 {len(tasks)} 个任务到队列")

if __name__ == "__main__":
    main()