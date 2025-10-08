import datetime
from typing import Dict, List, Optional
import re
import logging
from .base import BaseQueue
from ..logger import logger

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

    def get_todo_keys(self) -> List[str]:
        self.cursor.execute(
            "SELECT key FROM tasks WHERE status = 'todo' AND queue_id = %s",
            (self.queue_id,)
        )
        return [row[0] for row in self.cursor.fetchall()]

    def get_done_keys(self) -> List[str]:
        self.cursor.execute(
            "SELECT key FROM tasks WHERE status = 'done' AND queue_id = %s",
            (self.queue_id,)
        )
        return [row[0] for row in self.cursor.fetchall()]

    def get_error_keys(self) -> List[str]:
        self.cursor.execute(
            "SELECT key FROM tasks WHERE status = 'error' AND queue_id = %s",
            (self.queue_id,)
        )
        return [row[0] for row in self.cursor.fetchall()]

    def get_null_keys(self) -> List[str]:
        self.cursor.execute(
            "SELECT key FROM tasks WHERE status = 'null' AND queue_id = %s",
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
