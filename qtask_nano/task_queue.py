import json
import time
import re
# import logging
import threading
from typing import Dict, Any, List, Optional, Union
import datetime 
from qtask_nano.logger import logger

# 设置日志
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# logger = logging.getLogger('TaskQueue')

class BaseQueue:
    """队列基类，定义接口规范"""
    def __init__(self, queue_id, uri=None, key_expire: Dict[str, int] = None, cleanup_interval: int = 60):
        """
        key_expire: 各状态Key的过期时间配置（秒）
        格式: {'todo': 3600, 'doing': 7200, 'done': 86400, 'error': 86400, 'null': 86400}
        cleanup_interval: 清理间隔时间（秒），默认60秒（1分钟）
        """
        self.queue_id = queue_id
        self.key_expire = key_expire or {}
        self.cleanup_interval = cleanup_interval  # 可配置的清理间隔
        self.cleanup_thread = None
        self.running = False
        
    def start_cleanup_thread(self):
        """启动过期Key清理线程"""
        if not self.key_expire:
            return
            
        self.running = True
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()
        logger.info("Started key cleanup thread")
        
        # 立即执行一次清理
        try:
            self.cleanup_expired_keys()
        except Exception as e:
            logger.error(f"Initial cleanup error: {str(e)}")
        
    def stop_cleanup_thread(self):
        """停止过期Key清理线程"""
        self.running = False
        if self.cleanup_thread and self.cleanup_thread.is_alive():
            self.cleanup_thread.join(timeout=5)
        logger.info("Stopped key cleanup thread")
        
    def _cleanup_loop(self):
        """定期清理过期Key"""
        while self.running:
            try:
                self.cleanup_expired_keys()
            except Exception as e:
                logger.error(f"Cleanup error: {str(e)}")
            time.sleep(self.cleanup_interval)
            
    def cleanup_expired_keys(self):
        """清理过期Key（子类实现）"""
        raise NotImplementedError
        
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
    Redis队列实现（支持Key过期时间）
    '''
    def __init__(self, queue_id, redis_uri=None, key_expire: Dict[str, int] = None, cleanup_interval: int = 60):
        import redis
        super().__init__(queue_id, redis_uri, key_expire, cleanup_interval)
        
        self._todo_rkey = queue_id + ':todo'
        self._doing_rkey = queue_id + ':doing'
        self._done_rkey = queue_id + ':done'
        self._error_rkey = queue_id + ':error'
        self._null_rkey = queue_id + ':null'
        self._control_rkey = queue_id + ':control'
        self._state_rkey = queue_id + ':state'
        self._doing_time_rkey = queue_id + ':doing_time'
        self._create_time_rkey = queue_id + ':create_time'  # 新增：记录Key创建时间

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
        
        # 启动清理线程
        if self.key_expire:
            self.start_cleanup_thread()

    def __del__(self):
        self.stop_cleanup_thread()

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
        # 新增：清理创建时间记录
        self.redis.delete(self._create_time_rkey)
        logger.info("Queue reset")

    def set_state(self, state: Dict[str, Any]):
        text = json.dumps(state)
        self.redis.set(self._state_rkey, text)

    def get_status(self) -> Dict[str, Any]:
        text = self.redis.get(self._state_rkey)
        return json.loads(text) if text else {}

    def push_key(self, key: str):
        if key:
            # 记录Key创建时间
            timestamp = int(time.time() * 1000)
            self.redis.hset(self._create_time_rkey, key, timestamp)
            
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
        
    def cleanup_expired_keys(self):
        """清理过期Key"""
        if not self.key_expire:
            return 0
            
        now = int(time.time() * 1000)
        total_removed = 0
        
        # 清理todo状态的过期Key
        if 'todo' in self.key_expire:
            expire_ms = self.key_expire['todo'] * 1000
            todo_keys = self.redis.lrange(self._todo_rkey, 0, -1)
            for key in todo_keys:
                create_time = self.redis.hget(self._create_time_rkey, key)
                if create_time:
                    age_ms = now - int(create_time)
                    if age_ms > expire_ms:
                        # 原子操作：从todo列表和创建时间记录中移除
                        pipe = self.redis.pipeline()
                        pipe.lrem(self._todo_rkey, 0, key)  # 移除所有匹配项
                        pipe.hdel(self._create_time_rkey, key)
                        pipe.execute()
                        total_removed += 1
                        logger.debug(f"【Removed expired】Todo key {key}: create_time={create_time}, age_ms={age_ms}, expire_ms={expire_ms}")
                        # logger.debug(f"Removed expired todo key: {key}")
                else:
                    logger.warning(f"Todo key {key}: no create_time found, skipping cleanup")
                    # 如果没有创建时间记录，跳过清理，保留Key
                    # 这种情况可能是由于数据不一致或并发操作导致的
                    # 不应该删除这些Key，因为它们可能是有效的任务
        
        # 清理doing状态的过期Key
        if 'doing' in self.key_expire:
            expire_ms = self.key_expire['doing'] * 1000
            doing_keys = list(self.redis.smembers(self._doing_rkey))
            for key in doing_keys:
                start_time = self.redis.zscore(self._doing_time_rkey, key)
                if start_time and (now - int(start_time)) > expire_ms:
                    # 原子操作：从doing集合和时间记录中移除
                    pipe = self.redis.pipeline()
                    pipe.srem(self._doing_rkey, key)
                    pipe.zrem(self._doing_time_rkey, key)
                    pipe.hdel(self._create_time_rkey, key)
                    pipe.execute()
                    total_removed += 1
                    logger.debug(f"Removed expired doing key: {key}")
        
        # 清理done/error/null状态的过期Key
        for status in ['done', 'error', 'null']:
            if status in self.key_expire:
                expire_ms = self.key_expire[status] * 1000
                rkey = getattr(self, f'_{status}_rkey')
                keys = self.redis.lrange(rkey, 0, -1)
                for key in keys:
                    # 对于done/error/null状态，使用创建时间计算过期
                    create_time = self.redis.hget(self._create_time_rkey, key)
                    if create_time and (now - int(create_time)) > expire_ms:
                        # 原子操作：从状态列表和创建时间记录中移除
                        pipe = self.redis.pipeline()
                        pipe.lrem(rkey, 0, key)
                        pipe.hdel(self._create_time_rkey, key)
                        pipe.execute()
                        total_removed += 1
                        logger.debug(f"Removed expired {status} key: {key}")
        
        logger.info(f"Cleaned up {total_removed} expired keys")
        return total_removed

class PostgreSQLQueue(BaseQueue):
    '''
    PostgreSQL队列实现（支持Key过期时间）
    '''
    def __init__(self, queue_id, pg_uri=None, key_expire: Dict[str, int] = None, cleanup_interval: int = 60):
        import psycopg2
        super().__init__(queue_id, pg_uri, key_expire, cleanup_interval)
        
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
        
        # 启动清理线程
        if self.key_expire:
            self.start_cleanup_thread()
            
    def __del__(self):
        self.stop_cleanup_thread()
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()

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
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_created_at ON tasks (created_at)
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
            (new_status, "CURRENT_TIMESTAMP" if new_status != 'todo' else "NULL", key, self.queue_id)
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
        
    def cleanup_expired_keys(self):
        """清理过期Key"""
        if not self.key_expire:
            return 0
            
        total_removed = 0
        now = datetime.datetime.now()
        
        # 清理todo状态的过期Key
        if 'todo' in self.key_expire:
            expire_time = now - datetime.timedelta(seconds=self.key_expire['todo'])
            self.cursor.execute(
                "DELETE FROM tasks WHERE status = 'todo' AND created_at < %s AND queue_id = %s",
                (expire_time, self.queue_id)
            )
            removed = self.cursor.rowcount
            total_removed += removed
            if removed:
                logger.debug(f"Removed {removed} expired todo keys")
        
        # 清理doing状态的过期Key
        if 'doing' in self.key_expire:
            expire_time = now - datetime.timedelta(seconds=self.key_expire['doing'])
            self.cursor.execute(
                "DELETE FROM tasks WHERE status = 'doing' AND start_time < %s AND queue_id = %s",
                (expire_time, self.queue_id)
            )
            removed = self.cursor.rowcount
            total_removed += removed
            if removed:
                logger.debug(f"Removed {removed} expired doing keys")
        
        # 清理done/error/null状态的过期Key
        for status in ['done', 'error', 'null']:
            if status in self.key_expire:
                expire_time = now - datetime.timedelta(seconds=self.key_expire[status])
                self.cursor.execute(
                    f"DELETE FROM tasks WHERE status = %s AND created_at < %s AND queue_id = %s",
                    (status, expire_time, self.queue_id)
                )
                removed = self.cursor.rowcount
                total_removed += removed
                if removed:
                    logger.debug(f"Removed {removed} expired {status} keys")
        
        self.conn.commit()
        logger.info(f"Cleaned up {total_removed} expired keys")
        return total_removed

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
    """通用任务队列封装（支持Key过期时间）"""
    def __init__(self, namespace: str, uri: str, key_expire: Dict[str, int] = None, cleanup_interval: int = 60):
        """
        key_expire: 各状态Key的过期时间配置（秒）
        格式: {'todo': 3600, 'doing': 7200, 'done': 86400, 'error': 86400, 'null': 86400}
        cleanup_interval: 清理间隔时间（秒），默认60秒（1分钟）
        """
        self.namespace = namespace
        
        # 根据URI选择队列类型
        if uri.startswith("redis://"):
            self.queue = RedisQueue(namespace, uri, key_expire, cleanup_interval)
            logger.info(f"Using Redis backend for queue: {namespace}")
        elif uri.startswith("postgresql://") or uri.startswith("postgres://"):
            self.queue = PostgreSQLQueue(namespace, uri, key_expire, cleanup_interval)
            logger.info(f"Using PostgreSQL backend for queue: {namespace}")
        else:
            raise ValueError(f"Unsupported URI scheme: {uri}")
    
    def clear_all_queues(self, dry_run: bool = False,
                         todo: bool = True,
                         doing: bool = True,
                         done: bool = True,
                         error: bool = True,
                         null: bool = True):
        if dry_run:
            logger.info(f"Dry run mode: will reset queues")
            return
        
        logger.info(f"Resetting queues")
        self.queue.reset(todo=todo, doing=doing, done=done, error=error, null=null)

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
        return self.queue.doing_to_todo(task_data)

    def get_timeout_tasks(self, timeout_seconds: int) -> List[Task]:
        timeout_data = self.queue.get_timeout_doing_keys(timeout_seconds)
        return [Task.from_dict(json.loads(data)) for data in timeout_data]

    def requeue_timeout_tasks(self, timeout_seconds: int) -> int:
        return self.queue.move_timeout_to_todo(timeout_seconds)

    def get_doing_tasks(self) -> List[Task]:
        """获取所有doing状态的任务"""
        doing_data = self.queue.get_doing_keys()
        tasks = []
        for data in doing_data:
            try:
                task = Task.from_dict(json.loads(data))
                tasks.append(task)
            except Exception as e:
                logger.warning(f"Failed to parse doing task: {data}, error: {e}")
        return tasks

class Worker:
    """任务处理器（支持Key过期时间）"""
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

    def run(self, poll_timeout: int = 1, delay: int = 0.001):
        self.running = True
        logger.info(f"Worker {self.worker_id} started")
        
        try:
            while self.running:
                self._handle_timeout_tasks()
                
                task = self.queue.get_task()
                if task:
                    logger.info(f"Processing task: {task.task_id} ({task.task_type})")
                    self._process_task(task)
                    time.sleep(delay)
                else:
                    time.sleep(poll_timeout)
                    
        except KeyboardInterrupt:
            logger.info("Worker stopped by user")
            # 处理KeyboardInterrupt：将doing状态的任务移回todo队列末尾
            self._handle_interrupt_cleanup()
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

    def _handle_interrupt_cleanup(self):
        """处理KeyboardInterrupt：将doing状态的任务移回todo队列末尾"""
        try:
            print('--------------')
            # 获取所有doing状态的任务
            doing_tasks = self.queue.get_doing_tasks()
            if doing_tasks:
                logger.info(f"KeyboardInterrupt: Found {len(doing_tasks)} tasks in doing state, moving to todo queue")
                
                # 将doing任务移回todo队列末尾
                requeued_count = 0
                for task in doing_tasks:
                    logger.info(f"Requeuing task: {task.task_id} ({task.task_type})")
                    if self.queue.requeue_task(task):
                        requeued_count += 1
                        logger.debug(f"Requeued task {task.task_id} to todo queue")
                    else:
                        logger.error(f"Failed to requeue task {task.task_id} to todo queue")
                
                logger.info(f"KeyboardInterrupt: Successfully requeued {requeued_count} tasks to todo queue")
            else:
                logger.info("KeyboardInterrupt: No tasks in doing state")
        except Exception as e:
            logger.error(f"KeyboardInterrupt cleanup failed: {str(e)}")