import time
from typing import Dict, List, Optional, Any
import json
from .base import BaseQueue
from ..logger import logger


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
            self.clear_todo_keys()
        if doing:
            self.clear_doing_keys()
        if done:
            self.clear_done_keys()
        if error:
            self.clear_error_keys()
        if null:
            self.clear_null_keys()
            
        self.clear_create_time_keys()
        logger.info("Queue reset")
    
    def clear_todo_keys(self):
        self.redis.delete(self._todo_rkey)
    
    def clear_done_keys(self):
        self.redis.delete(self._done_rkey)
    
    def clear_error_keys(self):
        self.redis.delete(self._error_rkey)
    
    def clear_null_keys(self):
        self.redis.delete(self._null_rkey)
    
    def clear_create_time_keys(self):
        self.redis.delete(self._create_time_rkey)
    
    def clear_doing_keys(self):
        self.redis.delete(self._doing_rkey)
        self.redis.delete(self._doing_time_rkey)    
    
    def clear_all_keys(self):
        self.clear_todo_keys()
        self.clear_doing_keys()
        self.clear_done_keys()
        self.clear_error_keys()
        self.clear_null_keys()
        self.clear_create_time_keys()

    def set_state(self, state: Dict[str, Any]):
        text = json.dumps(state)
        self.redis.set(self._state_rkey, text)

    def get_state(self) -> Dict[str, Any]:
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
    
    def get_todo_keys(self) -> List[str]:
        return list(self.redis.lrange(self._todo_rkey, 0, -1))
    
    def get_done_keys(self) -> List[str]:
        return list(self.redis.lrange(self._done_rkey, 0, -1))
    
    def get_error_keys(self) -> List[str]:
        return list(self.redis.lrange(self._error_rkey, 0, -1))
    
    def get_null_keys(self) -> List[str]:
        return list(self.redis.lrange(self._null_rkey, 0, -1))
    
    def get_info(self, simple: bool = False) -> Dict[str, Any]:
        todo_keys = self.get_todo_keys()
        doing_keys = self.get_doing_keys()
        done_keys = self.get_done_keys()
        error_keys = self.get_error_keys()
        null_keys = self.get_null_keys()
        info = {
            'state': self.get_state(),
            'todo_count': len(todo_keys),
            'doing_count': len(doing_keys),
            'done_count': len(done_keys),
            'error_count': len(error_keys),
            'null_count': len(null_keys),
            'total_count': len(todo_keys) + len(doing_keys) + len(done_keys) + len(error_keys) + len(null_keys),
        }
        print(f'simple: {simple}')
        if simple:
            return info
        else:
            return info | {
                'todo_keys': todo_keys,
                'doing_keys': doing_keys,
                'done_keys': done_keys,
                'error_keys': error_keys,
                'null_keys': null_keys,
            } 

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
        
        logger.debug(f"Cleaned up {total_removed} expired keys")
        return total_removed

    def get_total_memory_usage(self) -> float:
        """
        获取本队列相关key占用的总内存大小（MB）
        
        Returns:
            float: 总内存使用量（MB）
        """
        try:
            # 获取本队列相关的所有key
            queue_keys = [
                self._todo_rkey,
                self._doing_rkey,
                self._done_rkey,
                self._error_rkey,
                self._null_rkey,
                self._control_rkey,
                self._state_rkey,
                self._doing_time_rkey,
                self._create_time_rkey
            ]
            
            total_memory_bytes = 0
            
            for key in queue_keys:
                try:
                    memory = self.redis.memory_usage(key)
                    total_memory_bytes += memory
                except Exception as e:
                    logger.warning(f"获取key {key} 内存使用量失败: {e}")
                    continue
            
            # 转换为MB
            total_memory_mb = total_memory_bytes / (1024 * 1024)
            return round(total_memory_mb, 2)
        except Exception as e:
            logger.warning(f"计算总内存使用量失败: {e}")
            return 0.0
