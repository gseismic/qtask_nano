import time
import random
import json
from .task_queue import TaskQueue, Task
from .logger import logger


class Worker:
    """任务处理器（支持Key过期时间）"""
    def __init__(self, queue: TaskQueue, worker_id: str):
        self.queue = queue
        self.worker_id = worker_id
        self.task_handlers = {}
        self.running = False
        self.timeout_seconds = 300  # 默认超时时间5分钟
        self.weights = []
        logger.info(f"Worker {worker_id} initialized")

    def register_task(self, task_type: str, handler, weight: int = 1, result_callback=None):
        """注册任务处理函数 
        
        Args: 
            task_type: 任务类型 
            handler: 任务处理函数 
            weight: 任务权重 这里是概率权重，用于调度本worker在task_types中被选中的概率 
            result_callback: 任务完成后的回调函数，接收 (task, result) 参数
        Note: 
            - 权重越大，被选中的概率越大 
            - 任务可能被覆盖 
        """
        if task_type in self.task_handlers: 
            logger.warning(f"Task handler for {task_type} already registered, will be overwritten")
        
        self.task_handlers[task_type] = { 
            "handler": handler,
            "weight": weight,
            "result_callback": result_callback
        }
        # 这里不应注册到queue，因为queue是全局共享的，如果worker宕机不会删除注册的task，不符合预期
        # self.queue.on_register_task(self.worker_id, task_type, weight)
        self.weights = [self.task_handlers[task_type]["weight"] for task_type in self.task_handlers.keys()]
        handler_name = handler.__name__ if hasattr(handler, '__name__') else handler.__class__.__name__
        logger.info(f"Registered task: {task_type}, weight: {weight}: {handler_name}")

    def run(self, poll_timeout: int = 1, delay: int = 0.001):
        self.running = True
        logger.info(f"Worker {self.worker_id} started")
        
        try: 
            task = None # None: 当前worker没有未处理完的任务
            while self.running: 
                # start_time = time.time()
                # self._handle_timeout_tasks(task_type) 
                
                task_types = list(self.task_handlers.keys())
                task_type = random.choices(task_types, weights=self.weights, k=1)[0]
                task = self.queue.get_task(task_type)
                if task:
                    logger.info(f"[Worker {self.worker_id}] Processing task: {task.task_id} ({task.task_type})")
                    self._process_task(task)
                    task = None
                    time.sleep(delay)
                else:
                    # 当前系统没有任务，防止过度消耗cpu
                    time.sleep(poll_timeout)
                    
        except KeyboardInterrupt:
            # logger.info(f"Worker {self.worker_id} stopped by user")
            # 处理KeyboardInterrupt：将doing状态的任务移回todo队列末尾
            if task:
                self.queue.requeue_task(task)
                logger.info(f'[Worker {self.worker_id}]: Stopped and Requeued task: {task.task_id} ({task.task_type})')
            else:
                logger.info(f'[Worker {self.worker_id}]: Stopped and No task to requeue')
        except Exception as e:
            logger.error(f"[Worker {self.worker_id}]: Error: {str(e)}")
        finally:
            self.running = False
            # logger.info(f"Worker {self.worker_id} stopped")
            all_info = self.get_task_queue_info(simple=True)
            formatted_info = json.dumps(all_info, indent=4)
            logger.info(f"[Worker {self.worker_id}]: Stopped, task queue info: {formatted_info}")

    def get_task_queue_info(self, simple: bool = False):
        all_info = {}
        for task_type in self.task_handlers.keys():
            info = self.queue.get_info(task_type, simple=simple)
            info['task_type'] = task_type
            info['worker_id'] = self.worker_id
            all_info[task_type] = info
        return all_info

    def stop(self):
        self.running = False

    def _process_task(self, task: Task): 
        try:
            handler_info = self.task_handlers.get(task.task_type)
            if not handler_info: 
                # 说明系统推送过来一个错误任务 
                logger.error(f"No handler for task type: {task.task_type}") 
                return 
                
            start_time = time.time() 
            result = handler_info["handler"](task.params) 
            
            # 调用结果回调函数（如果存在）
            result_callback = handler_info.get("result_callback")
            if result_callback:
                result_callback(task, result)
            
            # 只有在任务处理和回调都成功后才标记为完成
            self.queue.mark_done(task)
            elapsed = time.time() - start_time
            logger.info(f"Task completed: {task.task_id} (duration: {elapsed:.2f}s)")
            
        except Exception as e:
            logger.error(f"Task failed: {task.task_id} - {str(e)}")
            self.queue.mark_error(task) 

    def _handle_timeout_tasks(self, task_type: str): 
        # 这个应该是后台管理时处理，而非在worker中处理 
        timeout_tasks = self.queue.get_timeout_tasks(task_type, self.timeout_seconds)
        if timeout_tasks:
            logger.info(f"Found {len(timeout_tasks)} timeout tasks")
            requeued = self.queue.requeue_timeout_tasks(task_type, self.timeout_seconds)
            logger.info(f"[Worker {self.worker_id}] Requeued {requeued} timeout tasks")
            