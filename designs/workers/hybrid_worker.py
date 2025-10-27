"""
同时支持消费和生产的Worker - 3种设计方案
"""
import time
import random
import json
from ..task_queue import TaskQueue, Task
from ..logger import logger


# ========== 方案1: Callback + Queue绑定 ==========
class HybridWorkerV1:
    """混合Worker - 方案1: Callback + Queue绑定"""
    
    def __init__(self, queue: TaskQueue, worker_id: str):
        self.queue = queue
        self.worker_id = worker_id
        self.task_handlers = {}
        self.running = False
        self.weights = []
        logger.info(f"HybridWorkerV1 {worker_id} initialized")

    def register_task(self, task_type: str, handler, weight: int = 1, 
                     result_queue: TaskQueue = None, result_task_type: str = None):
        """注册任务处理函数
        
        Args:
            task_type: 任务类型
            handler: 任务处理函数
            weight: 权重
            result_queue: 结果发布队列
            result_task_type: 结果任务类型
        """
        if task_type in self.task_handlers:
            logger.warning(f"Task handler for {task_type} already registered, will be overwritten")
        
        self.task_handlers[task_type] = {
            "handler": handler,
            "weight": weight,
            "result_queue": result_queue,
            "result_task_type": result_task_type
        }
        self.weights = [self.task_handlers[task_type]["weight"] for task_type in self.task_handlers.keys()]
        handler_name = handler.__name__ if hasattr(handler, '__name__') else handler.__class__.__name__
        logger.info(f"Registered task: {task_type}, weight: {weight}: {handler_name}")

    def run(self, poll_timeout: int = 1, delay: int = 0.001):
        """运行Worker"""
        self.running = True
        logger.info(f"HybridWorkerV1 {self.worker_id} started")
        
        try: 
            task = None
            while self.running: 
                task_types = list(self.task_handlers.keys())
                task_type = random.choices(task_types, weights=self.weights, k=1)[0]
                task = self.queue.get_task(task_type)
                if task:
                    logger.info(f"[HybridWorkerV1 {self.worker_id}] Processing task: {task.task_id} ({task.task_type})")
                    self._process_task(task)
                    task = None
                    time.sleep(delay)
                else:
                    time.sleep(poll_timeout)
                    
        except KeyboardInterrupt:
            if task:
                self.queue.requeue_task(task)
                logger.info(f'[HybridWorkerV1 {self.worker_id}]: Stopped and Requeued task: {task.task_id}')
            else:
                logger.info(f'[HybridWorkerV1 {self.worker_id}]: Stopped and No task to requeue')
        except Exception as e:
            logger.error(f"[HybridWorkerV1 {self.worker_id}]: Error: {str(e)}")
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
            result = handler_info["handler"](task.params)
            
            # 如果有结果且配置了结果队列，则发布结果
            if result and handler_info.get("result_queue") and handler_info.get("result_task_type"):
                result_task = Task(
                    task_id=f"result_{task.task_id}_{int(time.time())}",
                    task_type=handler_info["result_task_type"],
                    params=result
                )
                handler_info["result_queue"].add_task(result_task)
                logger.info(f"Published result task: {result_task.task_id}")
            
            self.queue.mark_done(task)
            elapsed = time.time() - start_time
            logger.info(f"Task completed: {task.task_id} (duration: {elapsed:.2f}s)")
            
        except Exception as e:
            logger.error(f"Task failed: {task.task_id} - {str(e)}")
            self.queue.mark_error(task)

    def stop(self):
        self.running = False


# ========== 方案2: Callback函数 ==========
class HybridWorkerV2:
    """混合Worker - 方案2: Callback函数"""
    
    def __init__(self, queue: TaskQueue, worker_id: str):
        self.queue = queue
        self.worker_id = worker_id
        self.task_handlers = {}
        self.running = False
        self.weights = []
        logger.info(f"HybridWorkerV2 {worker_id} initialized")

    def register_task(self, task_type: str, handler, weight: int = 1, 
                     result_callback=None):
        """注册任务处理函数
        
        Args:
            task_type: 任务类型
            handler: 任务处理函数
            weight: 权重
            result_callback: 结果回调函数，接收(result, original_task)参数
        """
        if task_type in self.task_handlers:
            logger.warning(f"Task handler for {task_type} already registered, will be overwritten")
        
        self.task_handlers[task_type] = {
            "handler": handler,
            "weight": weight,
            "result_callback": result_callback
        }
        self.weights = [self.task_handlers[task_type]["weight"] for task_type in self.task_handlers.keys()]
        handler_name = handler.__name__ if hasattr(handler, '__name__') else handler.__class__.__name__
        logger.info(f"Registered task: {task_type}, weight: {weight}: {handler_name}")

    def run(self, poll_timeout: int = 1, delay: int = 0.001):
        """运行Worker"""
        self.running = True
        logger.info(f"HybridWorkerV2 {self.worker_id} started")
        
        try: 
            task = None
            while self.running: 
                task_types = list(self.task_handlers.keys())
                task_type = random.choices(task_types, weights=self.weights, k=1)[0]
                task = self.queue.get_task(task_type)
                if task:
                    logger.info(f"[HybridWorkerV2 {self.worker_id}] Processing task: {task.task_id} ({task.task_type})")
                    self._process_task(task)
                    task = None
                    time.sleep(delay)
                else:
                    time.sleep(poll_timeout)
                    
        except KeyboardInterrupt:
            if task:
                self.queue.requeue_task(task)
                logger.info(f'[HybridWorkerV2 {self.worker_id}]: Stopped and Requeued task: {task.task_id}')
            else:
                logger.info(f'[HybridWorkerV2 {self.worker_id}]: Stopped and No task to requeue')
        except Exception as e:
            logger.error(f"[HybridWorkerV2 {self.worker_id}]: Error: {str(e)}")
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
            result = handler_info["handler"](task.params)
            
            # 如果有结果回调，则调用回调
            if result and handler_info.get("result_callback"):
                try:
                    handler_info["result_callback"](result, task)
                except Exception as e:
                    logger.error(f"Result callback failed: {str(e)}")
            
            self.queue.mark_done(task)
            elapsed = time.time() - start_time
            logger.info(f"Task completed: {task.task_id} (duration: {elapsed:.2f}s)")
            
        except Exception as e:
            logger.error(f"Task failed: {task.task_id} - {str(e)}")
            self.queue.mark_error(task)

    def stop(self):
        self.running = False


# ========== 方案3: 结果处理器 ==========
class HybridWorkerV3:
    """混合Worker - 方案3: 结果处理器"""
    
    def __init__(self, queue: TaskQueue, worker_id: str):
        self.queue = queue
        self.worker_id = worker_id
        self.task_handlers = {}
        self.result_processors = {}  # 结果处理器
        self.running = False
        self.weights = []
        logger.info(f"HybridWorkerV3 {worker_id} initialized")

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

    def register_result_processor(self, task_type: str, processor):
        """注册结果处理器
        
        Args:
            task_type: 任务类型
            processor: 结果处理器函数，接收(result, original_task)参数，返回Task或Task列表
        """
        self.result_processors[task_type] = processor
        processor_name = processor.__name__ if hasattr(processor, '__name__') else processor.__class__.__name__
        logger.info(f"Registered result processor for {task_type}: {processor_name}")

    def run(self, poll_timeout: int = 1, delay: int = 0.001):
        """运行Worker"""
        self.running = True
        logger.info(f"HybridWorkerV3 {self.worker_id} started")
        
        try: 
            task = None
            while self.running: 
                task_types = list(self.task_handlers.keys())
                task_type = random.choices(task_types, weights=self.weights, k=1)[0]
                task = self.queue.get_task(task_type)
                if task:
                    logger.info(f"[HybridWorkerV3 {self.worker_id}] Processing task: {task.task_id} ({task.task_type})")
                    self._process_task(task)
                    task = None
                    time.sleep(delay)
                else:
                    time.sleep(poll_timeout)
                    
        except KeyboardInterrupt:
            if task:
                self.queue.requeue_task(task)
                logger.info(f'[HybridWorkerV3 {self.worker_id}]: Stopped and Requeued task: {task.task_id}')
            else:
                logger.info(f'[HybridWorkerV3 {self.worker_id}]: Stopped and No task to requeue')
        except Exception as e:
            logger.error(f"[HybridWorkerV3 {self.worker_id}]: Error: {str(e)}")
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
            result = handler_info["handler"](task.params)
            
            # 如果有结果处理器，则处理结果
            if result and task.task_type in self.result_processors:
                try:
                    result_tasks = self.result_processors[task.task_type](result, task)
                    if result_tasks:
                        if isinstance(result_tasks, list):
                            for result_task in result_tasks:
                                self.queue.add_task(result_task)
                            logger.info(f"Processed {len(result_tasks)} result tasks")
                        else:
                            self.queue.add_task(result_tasks)
                            logger.info(f"Processed 1 result task")
                except Exception as e:
                    logger.error(f"Result processor failed: {str(e)}")
            
            self.queue.mark_done(task)
            elapsed = time.time() - start_time
            logger.info(f"Task completed: {task.task_id} (duration: {elapsed:.2f}s)")
            
        except Exception as e:
            logger.error(f"Task failed: {task.task_id} - {str(e)}")
            self.queue.mark_error(task)

    def stop(self):
        self.running = False
