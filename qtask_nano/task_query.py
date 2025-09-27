import json
import time
# import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from qtask_nano import TaskQueue, Task, RedisQueue, PostgreSQLQueue
from qtask_nano.logger import logger

__all__ = ['TaskQuery', 'RedisQueryBackend', 'PostgreSQLQueryBackend']

# 设置日志
# logger = logging.getLogger('TaskQuery')

class BaseQueryBackend(ABC):
    """查询后端基类"""
    
    @abstractmethod
    def get_stats(self, queue_id: str) -> Dict[str, int]:
        """获取队列统计信息"""
        pass
    
    @abstractmethod
    def get_tasks_by_status(self, queue_id: str, status: str, limit: int) -> List[str]:
        """获取指定状态的任务数据"""
        pass
    
    @abstractmethod
    def get_task_metadata(self, queue_id: str, task_key: str) -> Dict[str, Any]:
        """获取任务元数据（如创建时间、开始时间等）"""
        pass

class RedisQueryBackend(BaseQueryBackend):
    """Redis查询后端实现"""
    
    def __init__(self, redis_client):
        self.redis = redis_client
    
    def get_stats(self, queue_id: str) -> Dict[str, int]:
        """获取Redis队列统计信息"""
        return {
            'todo_count': self.redis.llen(f"{queue_id}:todo"),
            'doing_count': self.redis.scard(f"{queue_id}:doing"),
            'done_count': self.redis.llen(f"{queue_id}:done"),
            'error_count': self.redis.llen(f"{queue_id}:error"),
            'null_count': self.redis.llen(f"{queue_id}:null")
        }
    
    def get_tasks_by_status(self, queue_id: str, status: str, limit: int) -> List[str]:
        """从Redis获取指定状态的任务"""
        if status == 'todo':
            return self.redis.lrange(f"{queue_id}:todo", 0, limit - 1)
        elif status == 'doing':
            return list(self.redis.smembers(f"{queue_id}:doing"))[:limit]
        elif status == 'done':
            return self.redis.lrange(f"{queue_id}:done", 0, limit - 1)
        elif status == 'error':
            return self.redis.lrange(f"{queue_id}:error", 0, limit - 1)
        elif status == 'null':
            return self.redis.lrange(f"{queue_id}:null", 0, limit - 1)
        return []
    
    def get_task_metadata(self, queue_id: str, task_key: str) -> Dict[str, Any]:
        """获取Redis任务元数据"""
        metadata = {}
        
        # 获取创建时间
        create_time = self.redis.hget(f"{queue_id}:create_time", task_key)
        if create_time:
            metadata['create_time'] = int(create_time) / 1000
        
        # 获取开始时间（仅对doing状态有效）
        start_time = self.redis.zscore(f"{queue_id}:doing_time", task_key)
        if start_time:
            metadata['start_time'] = start_time / 1000
        
        return metadata

class PostgreSQLQueryBackend(BaseQueryBackend):
    """PostgreSQL查询后端实现"""
    
    def __init__(self, cursor):
        self.cursor = cursor
    
    def get_stats(self, queue_id: str) -> Dict[str, int]:
        """获取PostgreSQL队列统计信息"""
        self.cursor.execute(
            "SELECT status, COUNT(*) FROM tasks WHERE queue_id = %s GROUP BY status",
            (queue_id,)
        )
        
        stats = {'todo_count': 0, 'doing_count': 0, 'done_count': 0, 'error_count': 0, 'null_count': 0}
        
        for status, count in self.cursor.fetchall():
            stats[f'{status}_count'] = count
            
        return stats
    
    def get_tasks_by_status(self, queue_id: str, status: str, limit: int) -> List[str]:
        """从PostgreSQL获取指定状态的任务"""
        self.cursor.execute(
            """
            SELECT key FROM tasks 
            WHERE queue_id = %s AND status = %s 
            ORDER BY created_at DESC 
            LIMIT %s
            """,
            (queue_id, status, limit)
        )
        
        return [row[0] for row in self.cursor.fetchall()]
    
    def get_task_metadata(self, queue_id: str, task_key: str) -> Dict[str, Any]:
        """获取PostgreSQL任务元数据"""
        self.cursor.execute(
            """
            SELECT created_at, updated_at, start_time 
            FROM tasks 
            WHERE queue_id = %s AND key = %s
            """,
            (queue_id, task_key)
        )
        
        result = self.cursor.fetchone()
        metadata = {}
        
        if result:
            created_at, updated_at, start_time = result
            if created_at:
                metadata['create_time'] = created_at.timestamp()
            if updated_at:
                metadata['update_time'] = updated_at.timestamp()
            if start_time:
                metadata['start_time'] = start_time.timestamp()
        
        return metadata

class TaskQuery:
    """任务状态查询类（解耦版本）"""
    
    def __init__(self, queue: TaskQueue):
        """
        初始化任务查询器
        
        Args:
            queue: TaskQueue实例
        """
        self.queue = queue
        self.namespace = queue.namespace
        
        # 根据队列类型选择后端
        if isinstance(queue.queue, RedisQueue):
            self.backend = RedisQueryBackend(queue.queue.redis)
        elif isinstance(queue.queue, PostgreSQLQueue):
            self.backend = PostgreSQLQueryBackend(queue.queue.cursor)
        else:
            raise ValueError(f"不支持的队列类型: {type(queue.queue)}")
        
    def get_queue_stats(self) -> Dict[str, Any]:
        """
        获取队列统计信息
        
        Returns:
            包含各状态任务数量的字典
        """
        stats = {
            'namespace': self.namespace,
            'timestamp': datetime.now().isoformat(),
            'todo_count': 0,
            'doing_count': 0,
            'done_count': 0,
            'error_count': 0,
            'null_count': 0,
            'total_count': 0
        }
        
        try:
            # 使用统一的后端接口
            backend_stats = self.backend.get_stats(self.queue.queue.queue_id)
            stats.update(backend_stats)
            
            stats['total_count'] = sum([
                stats['todo_count'], 
                stats['doing_count'], 
                stats['done_count'], 
                stats['error_count'], 
                stats['null_count']
            ])
            
        except Exception as e:
            logger.error(f"获取队列统计信息失败: {str(e)}")
            stats['error'] = str(e)
            
        return stats
    
    def get_tasks_by_status(self, status: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        根据状态获取任务列表
        
        Args:
            status: 任务状态 ('todo', 'doing', 'done', 'error', 'null')
            limit: 返回任务数量限制
            
        Returns:
            任务信息列表
        """
        if status not in ['todo', 'doing', 'done', 'error', 'null']:
            raise ValueError(f"无效的状态: {status}")
        
        tasks = []
        
        try:
            # 使用统一的后端接口获取任务数据
            task_keys = self.backend.get_tasks_by_status(self.queue.queue.queue_id, status, limit)
            
            for key in task_keys:
                try:
                    task_data = json.loads(key)
                    
                    # 获取任务元数据
                    metadata = self.backend.get_task_metadata(self.queue.queue.queue_id, key)
                    
                    task_info = {
                        'task_id': task_data.get('task_id'),
                        'task_type': task_data.get('task_type'),
                        'params': task_data.get('params'),
                        'created_at': task_data.get('created_at'),
                        'status': status,
                        'raw_data': key
                    }
                    
                    # 添加元数据
                    task_info.update(metadata)
                    
                    tasks.append(task_info)
                except json.JSONDecodeError:
                    logger.warning(f"无法解析任务数据: {key}")
                    
        except Exception as e:
            logger.error(f"获取{status}状态任务失败: {str(e)}")
            
        return tasks
    
    def search_tasks(self, 
                    task_type: Optional[str] = None,
                    status: Optional[str] = None,
                    created_after: Optional[datetime] = None,
                    created_before: Optional[datetime] = None,
                    limit: int = 100) -> List[Dict[str, Any]]:
        """
        搜索任务
        
        Args:
            task_type: 任务类型过滤
            status: 状态过滤
            created_after: 创建时间之后
            created_before: 创建时间之前
            limit: 返回数量限制
            
        Returns:
            匹配的任务列表
        """
        all_tasks = []
        
        # 获取所有状态的任务
        if status:
            statuses = [status]
        else:
            statuses = ['todo', 'doing', 'done', 'error', 'null']
        
        for st in statuses:
            tasks = self.get_tasks_by_status(st, limit)
            all_tasks.extend(tasks)
        
        # 应用过滤条件
        filtered_tasks = []
        
        for task in all_tasks:
            # 任务类型过滤
            if task_type and task.get('task_type') != task_type:
                continue
                
            # 创建时间过滤
            if created_after or created_before:
                task_created = task.get('created_at')
                if task_created:
                    task_time = datetime.fromtimestamp(task_created)
                    if created_after and task_time < created_after:
                        continue
                    if created_before and task_time > created_before:
                        continue
                else:
                    # 如果没有创建时间，跳过时间过滤
                    pass
            
            filtered_tasks.append(task)
            
            if len(filtered_tasks) >= limit:
                break
        
        return filtered_tasks
    
    def get_task_by_id(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        根据任务ID获取任务信息
        
        Args:
            task_id: 任务ID
            
        Returns:
            任务信息，如果未找到返回None
        """
        # 在所有状态中搜索
        for status in ['todo', 'doing', 'done', 'error', 'null']:
            tasks = self.get_tasks_by_status(status, 1000)  # 获取更多任务进行搜索
            for task in tasks:
                if task.get('task_id') == task_id:
                    return task
        return None
    
    def get_doing_tasks_with_duration(self) -> List[Dict[str, Any]]:
        """
        获取正在执行的任务及其执行时长
        
        Returns:
            包含执行时长的doing任务列表
        """
        doing_tasks = self.get_tasks_by_status('doing')
        
        for task in doing_tasks:
            # 使用统一的后端接口获取开始时间
            start_time = task.get('start_time')
            if start_time:
                duration = time.time() - start_time
                task['duration_seconds'] = duration
        
        return doing_tasks
    
    def get_timeout_tasks(self, timeout_seconds: int = 300) -> List[Dict[str, Any]]:
        """
        获取超时任务
        
        Args:
            timeout_seconds: 超时时间（秒）
            
        Returns:
            超时任务列表
        """
        timeout_tasks = self.queue.get_timeout_tasks(timeout_seconds)
        
        tasks = []
        for task in timeout_tasks:
            task_info = {
                'task_id': task.task_id,
                'task_type': task.task_type,
                'params': task.params,
                'created_at': task.created_at,
                'status': 'doing',
                'timeout_seconds': timeout_seconds
            }
            tasks.append(task_info)
        
        return tasks
    
    def get_queue_health(self) -> Dict[str, Any]:
        """
        获取队列健康状态
        
        Returns:
            队列健康状态信息
        """
        stats = self.get_queue_stats()
        
        health = {
            'namespace': self.namespace,
            'timestamp': datetime.now().isoformat(),
            'is_healthy': True,
            'warnings': [],
            'stats': stats
        }
        
        # 检查是否有异常情况
        if stats['error_count'] > stats['done_count'] * 0.1:  # 错误率超过10%
            health['warnings'].append(f"错误率过高: {stats['error_count']} 个错误任务")
            health['is_healthy'] = False
        
        if stats['doing_count'] > 100:  # 执行中任务过多
            health['warnings'].append(f"执行中任务过多: {stats['doing_count']} 个")
        
        if stats['total_count'] == 0:
            health['warnings'].append("队列为空")
        
        # 检查超时任务
        timeout_tasks = self.get_timeout_tasks()
        if timeout_tasks:
            health['warnings'].append(f"发现 {len(timeout_tasks)} 个超时任务")
            health['is_healthy'] = False
        
        return health
    
    def export_tasks(self, 
                    status: Optional[str] = None,
                    format: str = 'json',
                    filename: Optional[str] = None) -> str:
        """
        导出任务数据
        
        Args:
            status: 指定状态，None表示所有状态
            format: 导出格式 ('json', 'csv')
            filename: 输出文件名，None表示自动生成
            
        Returns:
            导出文件路径
        """
        if status:
            tasks = self.get_tasks_by_status(status, 10000)
        else:
            tasks = []
            for st in ['todo', 'doing', 'done', 'error', 'null']:
                tasks.extend(self.get_tasks_by_status(st, 10000))
        
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"tasks_export_{timestamp}.{format}"
        
        if format == 'json':
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(tasks, f, ensure_ascii=False, indent=2)
        elif format == 'csv':
            import csv
            if tasks:
                with open(filename, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=tasks[0].keys())
                    writer.writeheader()
                    writer.writerows(tasks)
        
        logger.info(f"任务数据已导出到: {filename}")
        return filename


class TaskQueryCLI:
    """任务查询命令行接口"""
    
    def __init__(self, queue: TaskQueue):
        self.query = TaskQuery(queue)
    
    def print_stats(self):
        """打印队列统计信息"""
        stats = self.query.get_queue_stats()
        
        print(f"\n=== 队列统计信息 ===")
        print(f"命名空间: {stats['namespace']}")
        print(f"时间戳: {stats['timestamp']}")
        print(f"待处理: {stats['todo_count']}")
        print(f"执行中: {stats['doing_count']}")
        print(f"已完成: {stats['done_count']}")
        print(f"错误: {stats['error_count']}")
        print(f"空任务: {stats['null_count']}")
        print(f"总计: {stats['total_count']}")
        
        if 'error' in stats:
            print(f"错误信息: {stats['error']}")
    
    def print_tasks_by_status(self, status: str, limit: int = 10):
        """打印指定状态的任务"""
        tasks = self.query.get_tasks_by_status(status, limit)
        
        print(f"\n=== {status.upper()} 状态任务 (显示前{limit}个) ===")
        if not tasks:
            print("无任务")
            return
        
        for i, task in enumerate(tasks, 1):
            print(f"\n{i}. 任务ID: {task.get('task_id')}")
            print(f"   类型: {task.get('task_type')}")
            print(f"   参数: {task.get('params')}")
            if task.get('created_at'):
                created_time = datetime.fromtimestamp(task['created_at'])
                print(f"   创建时间: {created_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    def print_doing_tasks(self):
        """打印正在执行的任务及其执行时长"""
        tasks = self.query.get_doing_tasks_with_duration()
        
        print(f"\n=== 正在执行的任务 ===")
        if not tasks:
            print("无正在执行的任务")
            return
        
        for i, task in enumerate(tasks, 1):
            print(f"\n{i}. 任务ID: {task.get('task_id')}")
            print(f"   类型: {task.get('task_type')}")
            print(f"   参数: {task.get('params')}")
            if task.get('duration_seconds'):
                duration = task['duration_seconds']
                print(f"   执行时长: {duration:.2f} 秒")
    
    def print_health(self):
        """打印队列健康状态"""
        health = self.query.get_queue_health()
        
        print(f"\n=== 队列健康状态 ===")
        print(f"命名空间: {health['namespace']}")
        print(f"健康状态: {'健康' if health['is_healthy'] else '异常'}")
        
        if health['warnings']:
            print("警告信息:")
            for warning in health['warnings']:
                print(f"  - {warning}")
        else:
            print("无警告信息")
    
    def search_and_print(self, task_type: str = None, status: str = None, limit: int = 10):
        """搜索并打印任务"""
        tasks = self.query.search_tasks(
            task_type=task_type,
            status=status,
            limit=limit
        )
        
        print(f"\n=== 搜索结果 ===")
        if not tasks:
            print("未找到匹配的任务")
            return
        
        print(f"找到 {len(tasks)} 个匹配的任务:")
        for i, task in enumerate(tasks, 1):
            print(f"\n{i}. 任务ID: {task.get('task_id')}")
            print(f"   类型: {task.get('task_type')}")
            print(f"   状态: {task.get('status')}")
            print(f"   参数: {task.get('params')}")


# 使用示例
if __name__ == "__main__":
    # 创建队列
    queue = TaskQueue("test_queue", "redis://localhost:6379/0")
    
    # 创建查询器
    query = TaskQuery(queue)
    cli = TaskQueryCLI(queue)
    
    # 打印统计信息
    cli.print_stats()
    
    # 打印健康状态
    cli.print_health()
    
    # 搜索任务
    cli.search_and_print(limit=5)
