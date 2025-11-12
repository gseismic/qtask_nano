import time
from typing import Dict, Any, Union, Optional
import uuid
import hashlib
import json
from .logger import logger

class Task: 
    """任务类"""
    def __init__(self, task_type: str, params: Dict[str, Any]=None):
        self.task_type = task_type
        self.params = params
        self.task_id = Task.make_task_id(task_type, params)
        self.created_at = time.time()
        
    @staticmethod
    def make_task_id(task_type: str, params: Dict[str, Any]=None) -> str:
        param_hash = Task.format_task_params(params)
        tag = str(uuid.uuid4())[:8]
        # 同一个任务可能被多次重试
        return f"{task_type}-{param_hash}-{int(time.time_ns()/1000)}-{tag}"
    
    @staticmethod
    def format_task_params(params: Dict[str, Any]=None) -> str:
        params = params or {}
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
    
    
# class TaskResult:
#     """任务结果类"""
#     def __init__(self, task: Task, result: Any, created_at: Optional[float]=None): 
#         self.task = task 
#         self.result = result 
#         if created_at is None: 
#             self.created_at = time.time() 
#         else: 
#             self.created_at = created_at
    
#     def to_dict(self) -> Dict[str, Any]:
#         return {
#             "task": self.task.to_dict(),
#             "result": self.result,
#             "created_at": self.created_at
#         }
    
#     @classmethod
#     def from_dict(cls, data: Dict[str, Any]):
#         return cls(Task.from_dict(data["task"]), data["result"], data["created_at"])
    
#     def __repr__(self):
#         return f"TaskResult(task={self.task}, result={self.result}, created_at={self.created_at})"