import time
from typing import Dict, List
import threading
import logging

logger = logging.getLogger(__name__)

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
        
    def get_timeout_doing_keys(self, timeout_seconds: int) -> List[str]:
        raise NotImplementedError
        
    def move_timeout_to_todo(self, timeout_seconds: int) -> int:
        raise NotImplementedError
    
    # TODO: get_todo_keys/get_done_keys/get_error_keys/get_null_keys
    # 和get_doing_keys/get_timeout_doing_keys/move_timeout_to_todo 保持一致
    def get_todo_keys(self) -> List[str]:
        raise NotImplementedError
        
    def get_doing_keys(self) -> List[str]:
        raise NotImplementedError
    
    def get_done_keys(self) -> List[str]:
        raise NotImplementedError
        
    def get_error_keys(self) -> List[str]:
        raise NotImplementedError
        
    def get_null_keys(self) -> List[str]:
        raise NotImplementedError