#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
支持Key过期的队列消费者示例
"""

import time
from qtask_nano import TaskQueue, Worker

def demo_task_handler(params): 
    """处理演示任务""" 
    message = params.get("message", "默认消息") 
    timestamp = params.get("timestamp", time.time()) 
    
    print(f"📝 任务内容: {message}") 
    print(f"   时间戳: {time.ctime(timestamp)}") 
    print(f"   处理时间: {time.ctime()}") 
    time.sleep(1)  # 模拟处理时间 
    return {"status": "success"}

def long_task_handler(params): 
    """处理长时间任务（会超时）""" 
    duration = params.get("duration", 5) 
    print(f"⏳ 开始执行长时间任务，预计耗时: {duration}秒") 
    time.sleep(duration)
    print("✅ 长时间任务完成")
    return {"status": "success"}

def main():
    """消费者主函数"""
    print("🔄 启动消费者...")
    
    # 配置Key过期时间（与生产者一致）
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
    
    # 创建worker
    worker = Worker(queue, "expire_consumer")
    worker.timeout_seconds = 3  # 设置超时时间为3秒
    
    # 注册任务处理器
    worker.register_task("demo_task", demo_task_handler, weight=1.0)
    worker.register_task("long_task", long_task_handler, weight=1.0)
    
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