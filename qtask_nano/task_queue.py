import json
from typing import Dict, List, Optional
from .logger import logger
from .queue import RedisQueue, PostgreSQLQueue
from .task import Task


class TaskQueue:
    """通用任务队列封装（支持Key过期时间）"""
    def __init__(self, namespace: str, uri: str, key_expire: Dict[str, int] = None, cleanup_interval: int = 60):
        """
        key_expire: 各状态Key的过期时间配置（秒）
        格式: {'todo': 3600, 'doing': 7200, 'done': 86400, 'error': 86400, 'null': 86400}
        cleanup_interval: 清理间隔时间（秒），默认60秒（1分钟）
        """
        self.namespace = namespace
        self.uri = uri
        self.key_expire = key_expire
        self.cleanup_interval = cleanup_interval
        self.queues = {}
        # self.queue = self.make_queue(uri) 
    
    def get_or_make_queue(self, task_type: str):
        if task_type not in self.queues:
            self.queues[task_type] = self.make_queue(self.uri)
        return self.queues[task_type]
    
    def make_queue(self, uri: str):
        if uri.startswith("redis://"):
            return RedisQueue(self.namespace, uri, self.key_expire, self.cleanup_interval)
        elif uri.startswith("postgresql://") or uri.startswith("postgres://"):
            return PostgreSQLQueue(self.namespace, uri, self.key_expire, self.cleanup_interval)
        else:
            raise ValueError(f"Unsupported URI scheme: {uri}")
    
    def clear_all_queues(self, 
                         task_type: str,
                         dry_run: bool = False,
                         todo: bool = True,
                         doing: bool = True,
                         done: bool = True,
                         error: bool = True,
                         null: bool = True):
        if dry_run:
            logger.info(f"Dry run mode: will reset queues")
            return
        
        logger.info(f"Resetting queues")
        queue = self.get_or_make_queue(task_type)
        queue.reset(todo=todo, doing=doing, done=done, error=error, null=null)

    def add_task(self, task: Task): 
        if not task: 
            logger.warning(f"Task is None") 
            return 
        # print('add_task:', task)
        task_data = json.dumps(task.to_dict())
        queue = self.get_or_make_queue(task.task_type)
        queue.push_key(task_data)
    
    def add_tasks(self, tasks: List[Task]):
        for task in tasks:
            self.add_task(task)

    def get_task(self, task_type: str) -> Optional[Task]:
        queue = self.get_or_make_queue(task_type)
        task_data = queue.pop_key()
        if task_data:
            return Task.from_dict(json.loads(task_data))
        return None

    def mark_done(self, task: Task):
        task_data = json.dumps(task.to_dict())
        queue = self.get_or_make_queue(task.task_type)
        queue.doing_to_done(task_data)

    def mark_error(self, task: Task): 
        task_data = json.dumps(task.to_dict()) 
        queue = self.get_or_make_queue(task.task_type) 
        queue.doing_to_error(task_data) 

    def mark_null(self, task: Task):
        task_data = json.dumps(task.to_dict())
        queue = self.get_or_make_queue(task.task_type)
        queue.doing_to_null(task_data)

    def requeue_task(self, task: Task):
        task_data = json.dumps(task.to_dict())
        queue = self.get_or_make_queue(task.task_type)
        return queue.doing_to_todo(task_data)

    def get_timeout_tasks(self, task_type: str, timeout_seconds: float) -> List[Task]:
        queue = self.get_or_make_queue(task_type)
        timeout_data = queue.get_timeout_doing_keys(timeout_seconds)
        return [Task.from_dict(json.loads(data)) for data in timeout_data]

    def requeue_timeout_tasks(self, task_type: str, timeout_seconds: float) -> int:
        queue = self.get_or_make_queue(task_type) 
        return queue.move_timeout_to_todo(timeout_seconds) 

    def get_doing_tasks(self, task_type: str) -> List[Task]:
        """获取某个`任务类型`的所有doing状态的任务
        """
        queue = self.get_or_make_queue(task_type)
        doing_data = queue.get_doing_keys()
        tasks = []
        for data in doing_data:
            try:
                task = Task.from_dict(json.loads(data)) 
                tasks.append(task) 
            except Exception as e: 
                logger.warning(f"Failed to parse doing task: {data}, error: {e}")
        return tasks

    def get_info(self, task_type: str, simple: bool = False):
        queue = self.get_or_make_queue(task_type)
        return queue.get_info(simple)