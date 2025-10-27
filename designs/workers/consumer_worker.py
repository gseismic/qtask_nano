"""
只消费的Worker - 当前实现
"""
import time
import random
import json
from ..task_queue import TaskQueue, Task
from ..logger import logger


class ConsumerWorker:
    """只消费任务的Worker（当前实现）"""
    
    def __init__(self, queue: TaskQueue, worker_id: str):
        self.queue = queue
        self.worker_id = worker_id
        self.task_handlers = {}
        self.running = False
        self.timeout_seconds = 300
        self.weights = []
        logger.info(f"ConsumerWorker {worker_id} initialized")

    def register_task(self, task_type: str, handler, weight: int = 1):
        """注册任务处理函数"""
        if task_type in self.task_handlers:
            logger.warning(f"Task handler for {task_type} already registered, will be overwritten")
        
        self.task_handlers[task_type] = {
            "handler": handler,
            "weight": weight
        }
        self.weights = [self.task_handlers[task_type]["weight"] for task_type in self.task_handlers.keys()]
        handler_name = handler.__name__ if hasattr(handler, '__name__') else handler.__class__.__name__
        logger.info(f"Registered task: {task_type}, weight: {weight}: {handler_name}")

    def run(self, poll_timeout: int = 1, delay: int = 0.001):
        """运行Worker，只消费任务"""
        self.running = True
        logger.info(f"ConsumerWorker {self.worker_id} started")
        
        try: 
            task = None
            while self.running: 
                task_types = list(self.task_handlers.keys())
                task_type = random.choices(task_types, weights=self.weights, k=1)[0]
                task = self.queue.get_task(task_type)
                if task:
                    logger.info(f"[ConsumerWorker {self.worker_id}] Processing task: {task.task_id} ({task.task_type})")
                    self._process_task(task)
                    task = None
                    time.sleep(delay)
                else:
                    time.sleep(poll_timeout)
                    
        except KeyboardInterrupt:
            if task:
                self.queue.requeue_task(task)
                logger.info(f'[ConsumerWorker {self.worker_id}]: Stopped and Requeued task: {task.task_id}')
            else:
                logger.info(f'[ConsumerWorker {self.worker_id}]: Stopped and No task to requeue')
        except Exception as e:
            logger.error(f"[ConsumerWorker {self.worker_id}]: Error: {str(e)}")
        finally:
            self.running = False

    def _process_task(self, task: Task): 
        """处理任务"""
        try:
            handler_info = self.task_handlers.get(task.task_type)
            if not handler_info: 
                logger.error(f"No handler for task type: {task.task_type}")
                return 
                
            start_time = time.time()
            handler_info["handler"](task.params)
            self.queue.mark_done(task)
            elapsed = time.time() - start_time
            logger.info(f"Task completed: {task.task_id} (duration: {elapsed:.2f}s)")
            
        except Exception as e:
            logger.error(f"Task failed: {task.task_id} - {str(e)}")
            self.queue.mark_error(task)

    def stop(self):
        self.running = False
