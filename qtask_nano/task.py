import time
from typing import Dict, Any, Union
import uuid
import hashlib
import json
from .logger import logger

class Task:
    """任务类"""
    def __init__(self, task_type: str, params: Dict[str, Any]=None):
        self.task_type = task_type
        self.params = params
        tag = str(uuid.uuid4())[:8]
        # 不用task_type+param_hash作为唯一ID的原因: 同一个任务可能被多次重试
        param_hash = Task.format_task_params(params)
        self.task_id = f"{task_type}-{param_hash}-{int(time.time_ns()/1000)}-{tag}"
        self.created_at = time.time()
    
    @staticmethod
    def format_task_params(params: Dict[str, Any]=None) -> str:
        if params is None:
            param_hash = ''
        else:
            param_hash = hashlib.md5(json.dumps(params).encode('utf-8')).hexdigest()
        return param_hash

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
    
    def __repr__(self):
        return f"Task(task_id={self.task_id}, task_type={self.task_type}, params={self.params}, created_at={self.created_at})"