"""
Worker使用示例
"""
import time
from qtask_nano.task_queue import TaskQueue, Task
from qtask_nano.workers import ConsumerWorker, ProducerWorker, HybridWorkerV1, HybridWorkerV2, HybridWorkerV3


def example_consumer_worker():
    """示例：只消费的Worker"""
    print("=== ConsumerWorker 示例 ===")
    
    # 创建队列
    queue = TaskQueue("test", "redis://localhost:6379/0")
    
    # 创建消费者Worker
    consumer = ConsumerWorker(queue, "consumer_1") 
    
    def process_data(params):
        print(f"处理数据: {params}")
        time.sleep(0.1)  # 模拟处理时间
    
    # 注册任务处理器
    consumer.register_task("data_processing", process_data, weight=1)
    
    # 添加一些任务
    for i in range(3):
        task = Task(f"task_{i}", "data_processing", {"data": f"value_{i}"})
        queue.add_task(task)
    
    # 运行消费者（实际使用中会在单独线程中运行）
    print("添加了3个任务到队列")


def example_producer_worker():
    """示例：只生产的Worker"""
    print("=== ProducerWorker 示例 ===")
    
    # 创建队列
    queue = TaskQueue("test", "redis://localhost:6379/0")
    
    # 创建生产者Worker
    producer = ProducerWorker(queue, "producer_1")
    
    def generate_tasks():
        """生成任务的生产者函数"""
        tasks = []
        for i in range(2):
            task = Task(f"generated_{int(time.time())}_{i}", "generated_task", {"value": i})
            tasks.append(task)
        return tasks
    
    # 注册生产者
    producer.register_producer("generated_task", generate_tasks, weight=1, interval=2.0)
    
    print("生产者已注册，每2秒生成2个任务")


def example_hybrid_worker_v1():
    """示例：混合Worker - 方案1"""
    print("=== HybridWorkerV1 示例 ===")
    
    # 创建输入和输出队列
    input_queue = TaskQueue("input", "redis://localhost:6379/0")
    output_queue = TaskQueue("output", "redis://localhost:6379/0")
    
    # 创建混合Worker
    hybrid = HybridWorkerV1(input_queue, "hybrid_1")
    
    def process_and_transform(params):
        """处理并转换数据"""
        result = {"processed": params["data"], "timestamp": time.time()}
        return result
    
    # 注册任务处理器，绑定结果队列
    hybrid.register_task(
        "transform_task", 
        process_and_transform, 
        weight=1,
        result_queue=output_queue,
        result_task_type="result_task"
    )
    
    print("混合Worker已配置，处理结果将发布到输出队列")


def example_hybrid_worker_v2():
    """示例：混合Worker - 方案2"""
    print("=== HybridWorkerV2 示例 ===")
    
    # 创建队列
    queue = TaskQueue("test", "redis://localhost:6379/0")
    
    # 创建混合Worker
    hybrid = HybridWorkerV2(queue, "hybrid_2")
    
    def process_data(params):
        """处理数据"""
        return {"result": params["data"] * 2}
    
    def result_callback(result, original_task):
        """结果回调函数"""
        print(f"处理结果: {result}, 原任务: {original_task.task_id}")
        # 这里可以发布到其他队列或进行其他处理
    
    # 注册任务处理器和回调
    hybrid.register_task("process_task", process_data, weight=1, result_callback=result_callback)
    
    print("混合Worker已配置，使用回调函数处理结果")


def example_hybrid_worker_v3():
    """示例：混合Worker - 方案3"""
    print("=== HybridWorkerV3 示例 ===")
    
    # 创建队列
    queue = TaskQueue("test", "redis://localhost:6379/0")
    
    # 创建混合Worker
    hybrid = HybridWorkerV3(queue, "hybrid_3")
    
    def process_data(params):
        """处理数据"""
        return {"result": params["data"] * 3}
    
    def result_processor(result, original_task):
        """结果处理器"""
        # 根据结果生成新任务
        new_task = Task(
            f"followup_{original_task.task_id}",
            "followup_task",
            {"processed_result": result}
        )
        return new_task
    
    # 注册任务处理器
    hybrid.register_task("process_task", process_data, weight=1)
    # 注册结果处理器
    hybrid.register_result_processor("process_task", result_processor)
    
    print("混合Worker已配置，使用结果处理器生成后续任务")


if __name__ == "__main__":
    # 运行示例
    example_consumer_worker()
    example_producer_worker()
    example_hybrid_worker_v1()
    example_hybrid_worker_v2()
    example_hybrid_worker_v3()
