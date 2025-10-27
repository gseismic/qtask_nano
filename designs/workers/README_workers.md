# Worker设计文档

## 概述

基于你的需求，我设计了3种类型的Worker，每种都有不同的使用场景和设计思路：

## 1. ConsumerWorker - 只消费的Worker

**特点：** 只从队列中消费任务，不产生新任务
**适用场景：** 数据处理、API调用、文件处理等纯消费型任务

```python
from qtask_nano.workers import ConsumerWorker

# 创建消费者
consumer = ConsumerWorker(queue, "consumer_1")

# 注册任务处理器
consumer.register_task("data_processing", process_function, weight=1)

# 运行
consumer.run()
```

## 2. ProducerWorker - 只生产的Worker

**特点：** 只向队列中生产任务，不消费任务
**适用场景：** 定时任务生成、数据采集、监控告警等

```python
from qtask_nano.workers import ProducerWorker

# 创建生产者
producer = ProducerWorker(queue, "producer_1")

# 注册生产者函数
producer.register_producer("scheduled_task", generate_tasks, weight=1, interval=60.0)

# 运行
producer.run()
```

## 3. HybridWorker - 混合型Worker（3种设计方案）

### 方案1: Callback + Queue绑定
**设计思路：** 在注册任务时直接绑定结果队列和任务类型
**优点：** 配置简单，结果发布自动化
**缺点：** 不够灵活，结果队列固定

```python
from qtask_nano.workers import HybridWorkerV1

# 创建混合Worker
hybrid = HybridWorkerV1(input_queue, "hybrid_1")

# 注册任务，绑定结果队列 
hybrid.register_task(
    "transform_task", 
    process_function, 
    weight=1,
    result_queue=output_queue,
    result_task_type="result_task"
) 
```

### 方案2: Callback函数
**设计思路：** 使用回调函数处理结果，更灵活
**优点：** 灵活性高，可以自定义结果处理逻辑
**缺点：** 需要手动管理队列

```python
from qtask_nano.workers import HybridWorkerV2

# 创建混合Worker
hybrid = HybridWorkerV2(queue, "hybrid_2")

# 定义结果回调
def result_callback(result, original_task):
    # 自定义结果处理逻辑
    new_task = Task("new_task", "new_type", result)
    output_queue.add_task(new_task)

# 注册任务和回调
hybrid.register_task("process_task", process_function, weight=1, result_callback=result_callback)
```

### 方案3: 结果处理器
**设计思路：** 分离结果处理逻辑，使用专门的处理器
**优点：** 职责分离，易于测试和维护
**缺点：** 配置稍复杂

```python
from qtask_nano.workers import HybridWorkerV3

# 创建混合Worker
hybrid = HybridWorkerV3(queue, "hybrid_3")

# 注册任务处理器
hybrid.register_task("process_task", process_function, weight=1)

# 注册结果处理器
def result_processor(result, original_task):
    return Task("followup_task", "followup_type", {"data": result})

hybrid.register_result_processor("process_task", result_processor)
```

## 设计对比

| 方案 | 灵活性 | 易用性 | 可测试性 | 适用场景 |
|------|--------|--------|----------|----------|
| 方案1 | 低 | 高 | 中 | 简单的结果发布 |
| 方案2 | 高 | 中 | 高 | 复杂的结果处理 |
| 方案3 | 高 | 中 | 高 | 需要结果转换的场景 |

## 推荐使用场景

- **ConsumerWorker**: 纯数据处理任务
- **ProducerWorker**: 定时任务、数据采集
- **HybridWorkerV1**: 简单的数据转换管道
- **HybridWorkerV2**: 复杂的结果处理逻辑
- **HybridWorkerV3**: 需要结果转换和后续任务生成的场景

## 使用建议

1. **简单场景**: 使用方案1，配置简单
2. **复杂逻辑**: 使用方案2，灵活性高
3. **结果转换**: 使用方案3，职责分离清晰

每种方案都保持了极简设计，可以根据具体需求选择合适的方案。
