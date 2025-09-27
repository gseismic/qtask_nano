#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
支持Key过期的队列生产者示例
"""

import time
from qtask_nano import TaskQueue, Task

def main():
    """生产者主函数"""
    print("🚀 启动生产者...")
    
    # 配置Key过期时间（秒）
    key_expire = {
        'todo': 3600,    # todo状态Key过期时间1小时
        'doing': 7200,   # doing状态Key过期时间2小时
        'done': 86400,   # done状态Key过期时间1天
        'error': 86400,  # error状态Key过期时间1天
        'null': 86400    # null状态Key过期时间1天
    }
    
    # 选择队列类型
    queue_uri = "redis://localhost:6379/0"
    # queue_uri = "postgresql://user:password@localhost:5432/taskdb"
    
    # 创建队列（传入key_expire参数）
    queue = TaskQueue(
        namespace="expire_demo", 
        uri=queue_uri,
        key_expire=key_expire
    )
    
    # 创建任务
    tasks = [
        Task("demo_task", {"message": "Hello Expiring Queue!", "timestamp": time.time()}),
        Task("demo_task", {"message": "Keys will expire automatically", "timestamp": time.time()}),
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