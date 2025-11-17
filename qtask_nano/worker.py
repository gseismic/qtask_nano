import time
import random
import json
import threading
from .task_queue import TaskQueue, Task
import traceback
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

    def register_task(self, task_type: str, handler, 
                      weight: int = 1, 
                      result_callback=None, 
                      keep_alive_callback=None,
                      keep_alive_interval: int = 120):
        """注册任务处理函数 
        
        Args: 
            task_type: 任务类型 
            handler: 任务处理函数 
            weight: 任务权重 这里是概率权重，用于调度本worker在task_types中被选中的概率 
            result_callback: 任务完成后的回调函数，接收 (task, result) 参数
            keep_alive_callback: 任务保持活跃的回调函数，接收 (task, result) 参数
        Note: 
            - 权重越大，被选中的概率越大 
            - 任务可能被覆盖 
        """
        if task_type in self.task_handlers: 
            logger.warning(f"Task handler for {task_type} already registered, will be overwritten")
        
        self.task_handlers[task_type] = { 
            "handler": handler, 
            "weight": weight, 
            "result_callback": result_callback, 
            "keep_alive_callback": keep_alive_callback, 
            "keep_alive_interval": keep_alive_interval,
            "start_time": time.time()
        }
        # 这里不应注册到queue，因为queue是全局共享的，如果worker宕机不会删除注册的task，不符合预期
        # self.queue.on_register_task(self.worker_id, task_type, weight)
        self.weights = [self.task_handlers[task_type]["weight"] for task_type in self.task_handlers.keys()]
        handler_name = handler.__name__ if hasattr(handler, '__name__') else handler.__class__.__name__
        logger.info(f"Registered task: {task_type}, weight: {weight}: {handler_name}")

    def run(self, poll_timeout: int = 1, delay: float = 1.0):
        self.running = True
        logger.info(f"Worker {self.worker_id} started")
        all_info = self.get_task_queue_info(simple=True)
        formatted_info = json.dumps(all_info, indent=4)
        logger.info(f"[Worker {self.worker_id}]: Task queue info: {formatted_info}")
        
        cnt = 0 
        try: 
            task = None # None: 当前worker没有未处理完的任务
            while self.running: 
                # start_time = time.time()
                # self._handle_timeout_tasks(task_type) 
                task_types = list(self.task_handlers.keys()) 
                task_type = random.choices(task_types, weights=self.weights, k=1)[0]
                if self.task_handlers[task_type]['keep_alive_callback'] and time.time() > self.task_handlers[task_type]['start_time'] + self.task_handlers[task_type]['keep_alive_interval']:
                    if time.time() > self.task_handlers[task_type]['start_time'] + self.task_handlers[task_type]['keep_alive_interval']:
                        self.task_handlers[task_type]['start_time'] = time.time()
                        self.task_handlers[task_type]['keep_alive_callback'](task_type)
                        
                task = self.queue.get_task(task_type) 
                if task:
                    logger.info(f"[cnt={cnt}][Worker {self.worker_id}] Processing task: {task.task_id} ({task.task_type})")
                    self._process_task(task)
                    task = None
                    time.sleep(delay)
                    cnt += 1
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
            import traceback
            traceback.print_exc() 
            logger.error(f"[Worker {self.worker_id}]: Error: {str(e)}")
        finally:
            self.running = False
            # logger.info(f"Worker {self.worker_id} stopped")
            logger.info(f"Worker {self.worker_id} stopped, calling keep_alive_callback for task_types: {self.task_handlers.keys()}")
            for task_type in self.task_handlers.keys():
                if self.task_handlers[task_type]['keep_alive_callback']:
                    self.task_handlers[task_type]['keep_alive_callback'](task_type)
                    
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
            keep_alive_thread = None
            keep_alive_stop_event = None

            keep_alive_callback = handler_info.get("keep_alive_callback")
            keep_alive_interval = handler_info.get("keep_alive_interval", 120)

            if keep_alive_callback:
                keep_alive_stop_event = threading.Event()
                keep_alive_thread = threading.Thread(
                    target=self._keep_task_alive,
                    args=(keep_alive_callback, keep_alive_interval, keep_alive_stop_event, task),
                    daemon=True
                )
                keep_alive_thread.start()

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
            traceback.print_exc() 
            logger.error(f"Task failed: {task.task_id} - {str(e)}")
            self.queue.mark_error(task) 
        finally:
            if keep_alive_stop_event:
                keep_alive_stop_event.set()
            if keep_alive_thread:
                keep_alive_thread.join(timeout=keep_alive_interval)

    def _handle_timeout_tasks(self, task_type: str): 
        # 这个应该是后台管理时处理，而非在worker中处理 
        timeout_tasks = self.queue.get_timeout_tasks(task_type, self.timeout_seconds)
        if timeout_tasks:
            logger.info(f"Found {len(timeout_tasks)} timeout tasks")
            requeued = self.queue.requeue_timeout_tasks(task_type, self.timeout_seconds)
            logger.info(f"[Worker {self.worker_id}] Requeued {requeued} timeout tasks")

    def _keep_task_alive(self, callback, interval, stop_event: threading.Event, task: Task):
        """任务执行期间定时调用keep alive回调"""
        try:
            while not stop_event.is_set():
                stop_event.wait(interval)
                if stop_event.is_set():
                    break
                try:
                    callback(task)
                except Exception as callback_error:
                    logger.warning(f"Keep-alive callback error for task {task.task_id}: {callback_error}")
        except Exception as e:
            logger.error(f"Keep-alive thread unexpected error: {e}")
            