#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通用队列消费者示例
Universal Queue Consumer Example
"""

import time
from task_queue import TaskQueue, Worker

def demo_task_handler(params):
    """处理演示任务"""
    message = params.get("message", "默认消息")
    timestamp = params.get("timestamp", time.time())
    
    print(f"📝 任务内容: {message}")
    print(f"   时间戳: {time.ctime(timestamp)}")
    print(f"   处理时间: {time.ctime()}")
    time.sleep(1)  # 模拟处理时间

def long_task_handler(params):
    """处理长时间任务（会超时）"""
    duration = params.get("duration", 5)
    print(f"⏳ 开始执行长时间任务，预计耗时: {duration}秒")
    time.sleep(duration)
    print("✅ 长时间任务完成")

def main():
    """消费者主函数"""
    print("🔄 启动消费者...")
    
    # 选择队列类型 (Redis或PostgreSQL)
    queue_uri = "redis://localhost:6379/0"
    # queue_uri = "postgresql://user:password@localhost:5432/taskdb"
    
    # 创建队列
    queue = TaskQueue(namespace="universal_demo", uri=queue_uri)
    
    # 创建worker
    worker = Worker(queue, "universal_consumer")
    worker.timeout_seconds = 3  # 设置超时时间为3秒
    
    # 注册任务处理器
    worker.register_task("demo_task", demo_task_handler, priority=1)
    worker.register_task("long_task", long_task_handler, priority=2)
    
    print("📋 已注册的任务处理器:")
    print("  - demo_task: 处理演示任务")
    print("  - long_task: 处理长时间任务")
    
    print("\n🔄 开始处理任务...")
    print("💡 提示: 按 Ctrl+C 停止消费者")
    
    try:
        # 运行worker
        worker.run(poll_timeout=1)
    except KeyboardInterrupt:
        print("\n⏹️  消费者被用户中断")
    except Exception as e:
        print(f"\n❌ 消费者发生错误: {e}")

if __name__ == "__main__":
    main()