#!/usr/bin/env python3
"""
最简单的 result_callback 使用示例
"""

import time
from qtask_nano import TaskQueue, Worker, Task


def simple_task_handler(params):
    """简单的任务处理器"""
    result = params['a'] + params['b']
    print(f"计算 {params['a']} + {params['b']} = {result}")
    return result


def simple_callback(task, result):
    """简单的结果回调函数"""
    print(f"任务 {task.task_id} 完成，结果: {result}")


def main():
    # 创建任务队列和工作器
    queue = TaskQueue("simple_demo", "redis://localhost:6379/0")
    worker = Worker(queue, "simple_worker")
    
    # 注册任务处理器，带回调函数
    worker.register_task('add_task', simple_task_handler, result_callback=simple_callback)
    
    # 创建一个测试任务
    task = Task('add_task', {'a': 10, 'b': 5})
    queue.add_task(task)
    
    # 运行工作器处理任务
    print("开始处理任务...")
    try:
        worker.run()
    except KeyboardInterrupt:
        print("停止工作器")


if __name__ == '__main__':
    main()