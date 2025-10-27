"""
只生产的Worker
"""
import time
import json
from ..task_queue import TaskQueue, Task
from ..logger import logger


class ProducerWorker:
    """只生产任务的Worker"""
    
    def __init__(self, queue: TaskQueue, worker_id: str):
        self.queue = queue
        self.worker_id = worker_id
        self.producers = {}  # 存储生产者函数
        self.running = False
        self.weights = []
        logger.info(f"ProducerWorker {worker_id} initialized")

    def register_producer(self, task_type: str, producer_func, weight: int = 1, interval: float = 1.0):
        """注册任务生产者
        
        Args:
            task_type: 任务类型
            producer_func: 生产者函数，应该返回Task或Task列表
            weight: 权重
            interval: 生产间隔（秒）
        """
        if task_type in self.producers:
            logger.warning(f"Producer for {task_type} already registered, will be overwritten")
        
        self.producers[task_type] = {
            "producer": producer_func,
            "weight": weight,
            "interval": interval,
            "last_produce_time": 0
        }
        self.weights = [self.producers[task_type]["weight"] for task_type in self.producers.keys()]
        producer_name = producer_func.__name__ if hasattr(producer_func, '__name__') else producer_func.__class__.__name__
        logger.info(f"Registered producer: {task_type}, weight: {weight}, interval: {interval}s: {producer_name}")

    def run(self, poll_timeout: int = 1):
        """运行Worker，只生产任务"""
        self.running = True
        logger.info(f"ProducerWorker {self.worker_id} started")
        
        try: 
            while self.running: 
                current_time = time.time()
                task_types = list(self.producers.keys())
                
                # 按权重随机选择要检查的生产者
                task_type = random.choices(task_types, weights=self.weights, k=1)[0]
                producer_info = self.producers[task_type]
                
                # 检查是否到了生产时间
                if current_time - producer_info["last_produce_time"] >= producer_info["interval"]:
                    try:
                        tasks = producer_info["producer"]()
                        if tasks:
                            if isinstance(tasks, list):
                                for task in tasks:
                                    self.queue.add_task(task)
                                logger.info(f"[ProducerWorker {self.worker_id}] Produced {len(tasks)} tasks of type: {task_type}")
                            else:
                                self.queue.add_task(tasks)
                                logger.info(f"[ProducerWorker {self.worker_id}] Produced 1 task of type: {task_type}")
                            
                            producer_info["last_produce_time"] = current_time
                    except Exception as e:
                        logger.error(f"[ProducerWorker {self.worker_id}] Producer error for {task_type}: {str(e)}")
                
                time.sleep(poll_timeout)
                    
        except KeyboardInterrupt:
            logger.info(f'[ProducerWorker {self.worker_id}]: Stopped by user')
        except Exception as e:
            logger.error(f"[ProducerWorker {self.worker_id}]: Error: {str(e)}")
        finally:
            self.running = False

    def stop(self):
        self.running = False
