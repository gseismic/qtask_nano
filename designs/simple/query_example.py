#!/usr/bin/env python3
"""
任务查询使用示例
演示解耦后的TaskQuery类的使用方法
"""

import json
import time
from datetime import datetime, timedelta
from task_queue import TaskQueue, Task
from task_query import TaskQuery, TaskQueryCLI

def demo_redis_query():
    """演示Redis后端的查询功能"""
    print("=== Redis后端查询示例 ===")
    
    # 创建Redis队列
    queue = TaskQueue("demo_redis", "redis://localhost:6379/0")
    
    # 创建查询器
    query = TaskQuery(queue)
    cli = TaskQueryCLI(queue)
    
    # 添加一些测试任务
    print("添加测试任务...")
    for i in range(5):
        task = Task(f"demo_task_{i}", {"data": f"test_data_{i}", "index": i})
        queue.add_task(task)
    
    # 获取一个任务进行处理
    task = queue.get_task()
    if task:
        print(f"处理任务: {task.task_id}")
        queue.mark_done(task)
    
    # 1. 获取队列统计信息
    print("\n1. 队列统计信息:")
    cli.print_stats()
    
    # 2. 获取健康状态
    print("\n2. 队列健康状态:")
    cli.print_health()
    
    # 3. 查看各状态的任务
    print("\n3. 各状态任务:")
    for status in ['todo', 'doing', 'done', 'error', 'null']:
        tasks = query.get_tasks_by_status(status, 5)
        if tasks:
            print(f"\n{status.upper()} 状态任务:")
            for task in tasks:
                print(f"  - {task['task_id']}: {task['task_type']}")
    
    # 4. 搜索任务
    print("\n4. 搜索任务:")
    cli.search_and_print(task_type="demo_task_0", limit=3)
    
    # 5. 导出任务数据
    print("\n5. 导出任务数据:")
    export_file = query.export_tasks(status='todo', format='json')
    print(f"任务数据已导出到: {export_file}")

def demo_postgresql_query():
    """演示PostgreSQL后端的查询功能"""
    print("\n=== PostgreSQL后端查询示例 ===")
    
    try:
        # 创建PostgreSQL队列
        queue = TaskQueue("demo_postgresql", "postgresql://user:password@localhost:5432/testdb")
        
        # 创建查询器
        query = TaskQuery(queue)
        cli = TaskQueryCLI(queue)
        
        # 添加一些测试任务
        print("添加测试任务...")
        for i in range(3):
            task = Task(f"pg_task_{i}", {"data": f"postgresql_data_{i}", "index": i})
            queue.add_task(task)
        
        # 获取队列统计信息
        print("\nPostgreSQL队列统计信息:")
        cli.print_stats()
        
        # 查看任务详情
        print("\nPostgreSQL任务详情:")
        tasks = query.get_tasks_by_status('todo', 10)
        for task in tasks:
            print(f"  - {task['task_id']}: {task['task_type']}")
            if 'db_created_at' in task:
                print(f"    数据库创建时间: {task['db_created_at']}")
        
    except Exception as e:
        print(f"PostgreSQL示例需要数据库连接: {str(e)}")

def demo_unified_interface():
    """演示统一接口的使用"""
    print("\n=== 统一接口示例 ===")
    
    # 定义后端配置
    backends = [
        ("Redis", "redis://localhost:6379/0"),
        # ("PostgreSQL", "postgresql://user:password@localhost:5432/testdb")
    ]
    
    for backend_name, uri in backends:
        try:
            print(f"\n使用 {backend_name} 后端:")
            
            # 创建队列和查询器
            queue = TaskQueue(f"demo_{backend_name.lower()}", uri)
            query = TaskQuery(queue)
            
            # 添加测试任务
            task = Task("unified_test", {"backend": backend_name, "timestamp": time.time()})
            queue.add_task(task)
            
            # 使用统一的查询接口
            stats = query.get_queue_stats()
            print(f"  统计信息: {stats['todo_count']} 个待处理任务")
            
            # 获取任务详情
            tasks = query.get_tasks_by_status('todo', 1)
            if tasks:
                task_info = tasks[0]
                print(f"  任务ID: {task_info['task_id']}")
                print(f"  任务类型: {task_info['task_type']}")
                print(f"  参数: {task_info['params']}")
                
                # 显示元数据（如果有的话）
                if 'create_time' in task_info:
                    create_time = datetime.fromtimestamp(task_info['create_time'])
                    print(f"  创建时间: {create_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
        except Exception as e:
            print(f"  {backend_name} 后端不可用: {str(e)}")

def demo_advanced_queries():
    """演示高级查询功能"""
    print("\n=== 高级查询示例 ===")
    
    queue = TaskQueue("demo_advanced", "redis://localhost:6379/0")
    query = TaskQuery(queue)
    
    # 添加不同类型的任务
    task_types = ["email", "notification", "report", "cleanup"]
    for i, task_type in enumerate(task_types):
        for j in range(2):
            task = Task(task_type, {
                "priority": i,
                "batch": j,
                "created_at": time.time()
            })
            queue.add_task(task)
    
    # 1. 按任务类型搜索
    print("1. 按任务类型搜索:")
    email_tasks = query.search_tasks(task_type="email", limit=5)
    print(f"  找到 {len(email_tasks)} 个邮件任务")
    
    # 2. 按时间范围搜索
    print("\n2. 按时间范围搜索:")
    recent_time = datetime.now() - timedelta(hours=1)
    recent_tasks = query.search_tasks(created_after=recent_time, limit=10)
    print(f"  最近1小时创建的任务: {len(recent_tasks)} 个")
    
    # 3. 获取队列健康状态
    print("\n3. 队列健康状态:")
    health = query.get_queue_health()
    print(f"  健康状态: {'健康' if health['is_healthy'] else '异常'}")
    if health['warnings']:
        print("  警告信息:")
        for warning in health['warnings']:
            print(f"    - {warning}")
    
    # 4. 导出数据
    print("\n4. 数据导出:")
    export_file = query.export_tasks(format='json')
    print(f"  所有任务已导出到: {export_file}")

if __name__ == '__main__':
    print("任务查询系统演示")
    print("=" * 50)
    
    # 演示Redis查询
    demo_redis_query()
    
    # 演示PostgreSQL查询（如果可用）
    demo_postgresql_query()
    
    # 演示统一接口
    demo_unified_interface()
    
    # 演示高级查询
    demo_advanced_queries()
    
    print("\n演示完成！")
    print("\n使用CLI工具:")
    print("python query_cli.py --uri redis://localhost:6379/0 --namespace demo --action stats")
    print("python query_cli.py --uri redis://localhost:6379/0 --namespace demo --action tasks --status todo")
    print("python query_cli.py --uri redis://localhost:6379/0 --namespace demo --action health")
