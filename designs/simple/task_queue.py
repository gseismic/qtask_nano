import json
import time
import re
from typing import Dict, Any, List, Optional, Union
# import logging
from loguru import logger

# 设置日志
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# logger = logging.getLogger('TaskQueue')

class BaseQueue:
    """队列基类，定义接口规范"""
    def push_key(self, key):
        raise NotImplementedError
        
    def pop_key(self):
        raise NotImplementedError
        
    def doing_to_done(self, key):
        raise NotImplementedError
        
    def doing_to_error(self, key):
        raise NotImplementedError
        
    def doing_to_null(self, key):
        raise NotImplementedError
        
    def doing_to_todo(self, key):
        raise NotImplementedError
        
    def get_doing_keys(self) -> List[str]:
        raise NotImplementedError
        
    def get_timeout_doing_keys(self, timeout_seconds: int) -> List[str]:
        raise NotImplementedError
        
    def move_timeout_to_todo(self, timeout_seconds: int) -> int:
        raise NotImplementedError

class RedisQueue(BaseQueue):
    '''
    Redis队列实现
    '''
    def __init__(self, queue_id, redis_uri=None):
        import redis
        
        self._todo_rkey = queue_id + ':todo'
        self._doing_rkey = queue_id + ':doing'
        self._done_rkey = queue_id + ':done'
        self._error_rkey = queue_id + ':error'
        self._null_rkey = queue_id + ':null'
        self._control_rkey = queue_id + ':control'
        self._state_rkey = queue_id + ':state'
        self._doing_time_rkey = queue_id + ':doing_time'

        if isinstance(redis_uri, str):
            # 解析Redis URI
            parts = redis_uri.split("://")[1].split("@")
            if len(parts) > 1:
                user_pass, host_port_db = parts
                user, password = user_pass.split(":")
            else:
                host_port_db = parts[0]
                password = None
            host_port, db = host_port_db.split("/")
            host, port = host_port.split(":")
            port = int(port)
            db = int(db)
        else:
            config = redis_uri or {}
            host = config.get('host', 'localhost')
            port = config.get('port', 6379)
            password = config.get('password', None)
            db = config.get('db', 0)
            
        pool = redis.ConnectionPool(
            host=host, port=port, password=password, 
            db=db, decode_responses=True
        )
        self.redis = redis.Redis(connection_pool=pool)
        self.set_state({"status": "active"})
        logger.info(f"Initialized Redis queue: {queue_id}")

    def reset(self, todo=True, doing=True, done=True, error=True, null=True):
        if todo:
            self.redis.delete(self._todo_rkey)
        if doing:
            self.redis.delete(self._doing_rkey)
            self.redis.delete(self._doing_time_rkey)
        if done:
            self.redis.delete(self._done_rkey)
        if error:
            self.redis.delete(self._error_rkey)
        if null:
            self.redis.delete(self._null_rkey)
        logger.info("Queue reset")

    def set_state(self, state: Dict[str, Any]):
        text = json.dumps(state)
        self.redis.set(self._state_rkey, text)

    def get_status(self) -> Dict[str, Any]:
        text = self.redis.get(self._state_rkey)
        return json.loads(text) if text else {}

    def push_key(self, key: str):
        if key:
            self.redis.lpush(self._todo_rkey, key)
            logger.debug(f"Pushed key: {key}")

    def pop_key(self) -> Optional[str]:
        key = self.redis.rpop(self._todo_rkey)
        if key:
            timestamp = int(time.time() * 1000)
            self.redis.sadd(self._doing_rkey, key)
            self.redis.zadd(self._doing_time_rkey, {key: timestamp})
            logger.debug(f"Popped key: {key}")
        return key

    def doing_to_done(self, key: str) -> bool:
        return self._doing_to_xxx(key, self._done_rkey)

    def doing_to_error(self, key: str) -> bool:
        return self._doing_to_xxx(key, self._error_rkey)

    def doing_to_null(self, key: str) -> bool:
        return self._doing_to_xxx(key, self._null_rkey)

    def doing_to_todo(self, key: str) -> bool:
        return self._doing_to_xxx(key, self._todo_rkey)

    def _doing_to_xxx(self, key: str, redis_key: str) -> bool:
        pipe = self.redis.pipeline()
        pipe.srem(self._doing_rkey, key)
        pipe.zrem(self._doing_time_rkey, key)
        pipe.lpush(redis_key, key)
        results = pipe.execute()
        success = results[0] > 0
        if success:
            logger.debug(f"Moved key {key} to {redis_key}")
        return success

    def get_doing_keys(self) -> List[str]:
        return list(self.redis.smembers(self._doing_rkey))

    def get_timeout_doing_keys(self, timeout_seconds: int) -> List[str]:
        cutoff = int(time.time() * 1000) - timeout_seconds * 1000
        return self.redis.zrangebyscore(self._doing_time_rkey, 0, cutoff)

    def move_timeout_to_todo(self, timeout_seconds: int) -> int:
        timeout_keys = self.get_timeout_doing_keys(timeout_seconds)
        moved_count = 0
        
        for key in timeout_keys:
            if self.doing_to_todo(key):
                moved_count += 1
                
        logger.info(f"Moved {moved_count} timeout keys to todo")
        return moved_count

class PostgreSQLQueue(BaseQueue):
    '''
    PostgreSQL队列实现
    '''
    def __init__(self, queue_id, pg_uri=None):
        import psycopg2
        from psycopg2 import sql
        
        self.queue_id = queue_id
        self.conn = None
        
        # 解析PostgreSQL URI
        if isinstance(pg_uri, str):
            # postgresql://user:password@host:port/dbname
            pattern = r'postgres(?:ql)?://(?:([^:]+):([^@]+)@)?([^:/]+)(?::(\d+))?/([^?]+)'
            match = re.match(pattern, pg_uri)
            if match:
                user = match.group(1)
                password = match.group(2)
                host = match.group(3)
                port = match.group(4) or '5432'
                dbname = match.group(5)
            else:
                raise ValueError("Invalid PostgreSQL URI format")
        else:
            config = pg_uri or {}
            user = config.get('user', 'postgres')
            password = config.get('password', None)
            host = config.get('host', 'localhost')
            port = config.get('port', 5432)
            dbname = config.get('dbname', 'postgres')
        
        # 连接数据库
        self.conn = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            dbname=dbname
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        
        # 初始化数据库表
        self._init_db()
        logger.info(f"Initialized PostgreSQL queue: {queue_id}")

    def _init_db(self):
        """初始化数据库表结构"""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id SERIAL PRIMARY KEY,
                queue_id VARCHAR(255) NOT NULL,
                key TEXT NOT NULL,
                status VARCHAR(20) NOT NULL CHECK(status IN ('todo', 'doing', 'done', 'error', 'null')),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                start_time TIMESTAMP
            )
        """)
        
        # 创建索引
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_queue_status ON tasks (queue_id, status)
        """)
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_start_time ON tasks (start_time)
        """)
        self.conn.commit()

    def reset(self, todo=True, doing=True, done=True, error=True, null=True):
        conditions = []
        if todo: conditions.append("status = 'todo'")
        if doing: conditions.append("status = 'doing'")
        if done: conditions.append("status = 'done'")
        if error: conditions.append("status = 'error'")
        if null: conditions.append("status = 'null'")
        
        if conditions:
            condition_str = " OR ".join(conditions)
            self.cursor.execute(
                f"DELETE FROM tasks WHERE queue_id = %s AND ({condition_str})",
                (self.queue_id,)
            )
            self.conn.commit()
            logger.info("Queue reset")

    def push_key(self, key: str):
        if key:
            self.cursor.execute(
                "INSERT INTO tasks (queue_id, key, status) VALUES (%s, %s, 'todo')",
                (self.queue_id, key)
            )
            self.conn.commit()
            logger.debug(f"Pushed key: {key}")

    def pop_key(self) -> Optional[str]:
        # 获取最早创建的todo任务
        self.cursor.execute(
            "SELECT id, key FROM tasks WHERE status = 'todo' AND queue_id = %s ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED",
            (self.queue_id,)
        )
        task = self.cursor.fetchone()
        
        if task:
            task_id, key = task
            # 更新任务状态为doing并记录开始时间
            self.cursor.execute(
                "UPDATE tasks SET status = 'doing', updated_at = CURRENT_TIMESTAMP, start_time = CURRENT_TIMESTAMP WHERE id = %s",
                (task_id,)
            )
            self.conn.commit()
            logger.debug(f"Popped key: {key}")
            return key
        return None

    def doing_to_done(self, key: str) -> bool:
        return self._change_status(key, 'done')

    def doing_to_error(self, key: str) -> bool:
        return self._change_status(key, 'error')

    def doing_to_null(self, key: str) -> bool:
        return self._change_status(key, 'null')

    def doing_to_todo(self, key: str) -> bool:
        return self._change_status(key, 'todo')

    def _change_status(self, key: str, new_status: str) -> bool:
        self.cursor.execute(
            "UPDATE tasks SET status = %s, updated_at = CURRENT_TIMESTAMP, start_time = %s WHERE key = %s AND status = 'doing' AND queue_id = %s",
            (new_status, None if new_status == 'todo' else "CURRENT_TIMESTAMP", key, self.queue_id)
        )
        self.conn.commit()
        success = self.cursor.rowcount > 0
        if success:
            logger.debug(f"Moved key {key} to {new_status}")
        return success

    def get_doing_keys(self) -> List[str]:
        self.cursor.execute(
            "SELECT key FROM tasks WHERE status = 'doing' AND queue_id = %s",
            (self.queue_id,)
        )
        return [row[0] for row in self.cursor.fetchall()]

    def get_timeout_doing_keys(self, timeout_seconds: int) -> List[str]:
        self.cursor.execute(
            "SELECT key FROM tasks WHERE status = 'doing' AND start_time < (CURRENT_TIMESTAMP - INTERVAL '%s seconds') AND queue_id = %s",
            (timeout_seconds, self.queue_id)
        )
        return [row[0] for row in self.cursor.fetchall()]

    def move_timeout_to_todo(self, timeout_seconds: int) -> int:
        timeout_keys = self.get_timeout_doing_keys(timeout_seconds)
        moved_count = 0
        
        for key in timeout_keys:
            if self.doing_to_todo(key):
                moved_count += 1
                
        logger.info(f"Moved {moved_count} timeout keys to todo")
        return moved_count

    def __del__(self):
        if self.conn:
            self.conn.close()

class Task:
    """任务类"""
    def __init__(self, task_type: str, params: Dict[str, Any]):
        self.task_type = task_type
        self.params = params
        self.task_id = f"{task_type}_{int(time.time() * 1000)}"
        self.created_at = time.time()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "task_type": self.task_type,
            "params": self.params,
            "created_at": self.created_at
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        task = cls(data["task_type"], data["params"])
        task.task_id = data["task_id"]
        task.created_at = data["created_at"]
        return task

class TaskQueue:
    """通用任务队列封装"""
    def __init__(self, namespace: str, uri: str):
        self.namespace = namespace
        
        # 根据URI选择队列类型
        if uri.startswith("redis://"):
            self.queue = RedisQueue(namespace, uri)
            logger.info(f"Using Redis backend for queue: {namespace}")
        elif uri.startswith("postgresql://") or uri.startswith("postgres://"):
            self.queue = PostgreSQLQueue(namespace, uri)
            logger.info(f"Using PostgreSQL backend for queue: {namespace}")
        else:
            raise ValueError(f"Unsupported URI scheme: {uri}")

    def add_task(self, task: Task):
        task_data = json.dumps(task.to_dict())
        self.queue.push_key(task_data)

    def get_task(self) -> Optional[Task]:
        task_data = self.queue.pop_key()
        if task_data:
            return Task.from_dict(json.loads(task_data))
        return None

    def mark_done(self, task: Task):
        task_data = json.dumps(task.to_dict())
        self.queue.doing_to_done(task_data)

    def mark_error(self, task: Task):
        task_data = json.dumps(task.to_dict())
        self.queue.doing_to_error(task_data)

    def mark_null(self, task: Task):
        task_data = json.dumps(task.to_dict())
        self.queue.doing_to_null(task_data)

    def requeue_task(self, task: Task):
        task_data = json.dumps(task.to_dict())
        self.queue.doing_to_todo(task_data)

    def get_timeout_tasks(self, timeout_seconds: int) -> List[Task]:
        timeout_data = self.queue.get_timeout_doing_keys(timeout_seconds)
        return [Task.from_dict(json.loads(data)) for data in timeout_data]

    def requeue_timeout_tasks(self, timeout_seconds: int) -> int:
        return self.queue.move_timeout_to_todo(timeout_seconds)

class Worker:
    """任务处理器"""
    def __init__(self, queue: TaskQueue, worker_id: str):
        self.queue = queue
        self.worker_id = worker_id
        self.task_handlers = {}
        self.running = False
        self.timeout_seconds = 300  # 默认超时时间5分钟
        logger.info(f"Worker {worker_id} initialized")

    def register_task(self, task_type: str, handler, priority: int = 1):
        self.task_handlers[task_type] = {
            "handler": handler,
            "priority": priority
        }
        logger.info(f"Registered task handler for: {task_type}")

    def run(self, poll_timeout: int = 1):
        self.running = True
        logger.info(f"Worker {self.worker_id} started")
        
        try:
            while self.running:
                self._handle_timeout_tasks()
                
                task = self.queue.get_task()
                if task:
                    logger.info(f"Processing task: {task.task_id} ({task.task_type})")
                    self._process_task(task)
                else:
                    time.sleep(poll_timeout)
        except KeyboardInterrupt:
            logger.info("Worker stopped by user")
        except Exception as e:
            logger.error(f"Worker error: {str(e)}")
        finally:
            self.running = False
            logger.info(f"Worker {self.worker_id} stopped")

    def stop(self):
        self.running = False

    def _process_task(self, task: Task):
        try:
            handler_info = self.task_handlers.get(task.task_type)
            if not handler_info:
                logger.warning(f"No handler for task type: {task.task_type}")
                self.queue.mark_null(task)
                return
                
            start_time = time.time()
            handler_info["handler"](task.params)
            self.queue.mark_done(task)
            elapsed = time.time() - start_time
            logger.info(f"Task completed: {task.task_id} (duration: {elapsed:.2f}s)")
            
        except Exception as e:
            logger.error(f"Task failed: {task.task_id} - {str(e)}")
            self.queue.mark_error(task)

    def _handle_timeout_tasks(self):
        timeout_tasks = self.queue.get_timeout_tasks(self.timeout_seconds)
        if timeout_tasks:
            logger.info(f"Found {len(timeout_tasks)} timeout tasks")
            requeued = self.queue.requeue_timeout_tasks(self.timeout_seconds)
            logger.info(f"Requeued {requeued} timeout tasks")