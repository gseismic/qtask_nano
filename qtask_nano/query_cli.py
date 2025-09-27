#!/usr/bin/env python3
"""
任务查询命令行工具
支持Redis和PostgreSQL后端的统一查询接口
"""

import argparse
import json
import sys
from datetime import datetime
from qtask_nano import TaskQueue
from qtask_nano import TaskQuery, TaskQueryCLI

def main():
    parser = argparse.ArgumentParser(description='任务队列查询工具')
    parser.add_argument('--uri', required=True, help='队列URI (redis://host:port/db 或 postgresql://user:pass@host:port/db)')
    parser.add_argument('--namespace', required=True, help='队列命名空间')
    parser.add_argument('--action', choices=['stats', 'tasks', 'health', 'search', 'doing'], 
                       default='stats', help='查询操作')
    parser.add_argument('--status', choices=['todo', 'doing', 'done', 'error', 'null'], 
                       help='任务状态过滤')
    parser.add_argument('--task-type', help='任务类型过滤')
    parser.add_argument('--limit', type=int, default=10, help='返回数量限制')
    parser.add_argument('--format', choices=['json', 'table'], default='table', help='输出格式')
    
    args = parser.parse_args()
    
    try:
        # 创建队列
        queue = TaskQueue(args.namespace, args.uri)
        
        # 创建查询器
        query = TaskQuery(queue)
        cli = TaskQueryCLI(queue)
        
        if args.action == 'stats':
            if args.format == 'json':
                stats = query.get_queue_stats()
                print(json.dumps(stats, ensure_ascii=False, indent=2))
            else:
                cli.print_stats()
                
        elif args.action == 'tasks':
            if not args.status:
                print("错误: 查询任务时必须指定 --status 参数")
                sys.exit(1)
            
            if args.format == 'json':
                tasks = query.get_tasks_by_status(args.status, args.limit)
                print(json.dumps(tasks, ensure_ascii=False, indent=2))
            else:
                cli.print_tasks_by_status(args.status, args.limit)
                
        elif args.action == 'health':
            if args.format == 'json':
                health = query.get_queue_health()
                print(json.dumps(health, ensure_ascii=False, indent=2))
            else:
                cli.print_health()
                
        elif args.action == 'search':
            if args.format == 'json':
                tasks = query.search_tasks(
                    task_type=args.task_type,
                    status=args.status,
                    limit=args.limit
                )
                print(json.dumps(tasks, ensure_ascii=False, indent=2))
            else:
                cli.search_and_print(
                    task_type=args.task_type,
                    status=args.status,
                    limit=args.limit
                )
                
        elif args.action == 'doing':
            if args.format == 'json':
                tasks = query.get_doing_tasks_with_duration()
                print(json.dumps(tasks, ensure_ascii=False, indent=2))
            else:
                cli.print_doing_tasks()
                
    except Exception as e:
        print(f"错误: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
