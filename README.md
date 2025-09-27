# qtask_nano

一个轻量级的 Python 任务队列系统，支持 Redis 和 PostgreSQL 后端，具有自动过期清理、任务查询和监控功能。

## ✨ 特性

- 🚀 **轻量级设计**: 简洁的 API，易于使用和集成
- 🔄 **多后端支持**: 支持 Redis 和 PostgreSQL 作为存储后端
- ⏰ **自动过期清理**: 支持任务 Key 自动过期，避免数据积累
- 📊 **任务查询系统**: 提供统一的任务查询接口和 CLI 工具
- 🔍 **健康监控**: 内置队列健康状态检查
- 📝 **详细日志**: 基于 loguru 的日志系统
- 🛠️ **CLI 工具**: 命令行界面支持任务查询和管理

## 📦 安装

### 环境要求

- Python 3.7+
- Redis 或 PostgreSQL

### 安装依赖

```bash
# 克隆项目
git clone <repository-url>
cd qtask_nano

# 安装依赖
pip install -r requirements.txt

# 开发模式安装
pip install -e .
```

### 数据库安装

#### macOS

```bash
# 安装 PostgreSQL
brew install postgresql
brew services start postgresql

# 初始化数据库
sudo mkdir -p /usr/local/var/postgresql@14
sudo chown -R $(whoami) /usr/local/var/postgresql@14
initdb -D /usr/local/var/postgresql@14 -E utf8

# 连接数据库
psql -U $(whoami) -d postgres
```

```bash
# 安装 Redis
brew install redis
brew services start redis
```

#### Ubuntu

```bash
# 安装 PostgreSQL
sudo apt-get install postgresql
sudo systemctl start postgresql

# 安装 Redis
sudo apt-get install redis-server
sudo systemctl start redis-server
```

## 🚀 快速开始

### 基本使用

```python
from qtask_nano import TaskQueue, Task, Worker

# 创建队列（支持过期时间配置）
key_expire = {
    'todo': 3600,    # todo状态Key过期时间1小时
    'doing': 7200,   # doing状态Key过期时间2小时
    'done': 86400,   # done状态Key过期时间1天
    'error': 86400,  # error状态Key过期时间1天
}

queue = TaskQueue(
    namespace="demo_queue",
    uri="redis://localhost:6379/0",
    key_expire=key_expire
)

# 添加任务
task = Task("process_data", {"data": "example", "priority": 1})
queue.add_task(task)

# 获取任务
retrieved_task = queue.get_task()
if retrieved_task:
    print(f"获取到任务: {retrieved_task.task_type}")

# 处理任务
def process_data(params):
    print(f"处理数据: {params['data']}")

worker = Worker(queue, "demo_worker")
worker.register_task("process_data", process_data)
worker.run()
```

### 生产者示例

```python
#!/usr/bin/env python3
from qtask_nano import TaskQueue, Task
import time

def main():
    # 配置 Key 过期时间
    key_expire = {
        'todo': 3600,    # 1小时
        'doing': 7200,   # 2小时
        'done': 86400,   # 1天
        'error': 86400,  # 1天
    }
    
    queue = TaskQueue(
        namespace="demo_queue",
        uri="redis://localhost:6379/0",
        key_expire=key_expire
    )
    
    # 添加任务
    for i in range(3):
        task = Task("demo_task", {"message": f"任务 {i+1}", "timestamp": time.time()})
        queue.add_task(task)
        print(f"✅ 已添加任务 {i+1}")

if __name__ == "__main__":
    main()
```

### 消费者示例

```python
#!/usr/bin/env python3
from qtask_nano import TaskQueue, Worker
import time

def demo_task_handler(params):
    """处理演示任务"""
    message = params.get("message", "默认消息")
    timestamp = params.get("timestamp", time.time())
    print(f"📝 任务内容: {message}")
    print(f"   时间戳: {time.ctime(timestamp)}")
    time.sleep(1)

def main():
    # 配置 Key 过期时间（与生产者一致）
    key_expire = {
        'todo': 3600,    # 1小时
        'doing': 7200,   # 2小时
        'done': 86400,   # 1天
        'error': 86400,  # 1天
    }
    
    queue = TaskQueue(
        namespace="demo_queue",
        uri="redis://localhost:6379/0",
        key_expire=key_expire
    )
    
    # 创建工作者
    worker = Worker(queue, "demo_consumer")
    worker.timeout_seconds = 3  # 设置超时时间
    
    # 注册任务处理器
    worker.register_task("demo_task", demo_task_handler, priority=1)
    
    # 开始处理任务
    print("🔄 开始处理任务...")
    try:
        worker.run(poll_timeout=1)
    except KeyboardInterrupt:
        print("⏹️ 消费者被用户中断")

if __name__ == "__main__":
    main()
```

## 🔧 CLI 工具

qtask_nano 提供了强大的命令行工具来查询和管理任务队列。

### 基本用法

```bash
# 查看帮助
qtask_nano --help

# 检查队列健康状态
qtask_nano --uri redis://localhost:6379/0 --namespace demo --action health

# 查看队列统计信息
qtask_nano --uri redis://localhost:6379/0 --namespace demo --action stats

# 查看所有任务
qtask_nano --uri redis://localhost:6379/0 --namespace demo --action tasks

# 查看特定状态的任务
qtask_nano --uri redis://localhost:6379/0 --namespace demo --action tasks --status doing

# 搜索特定类型的任务
qtask_nano --uri redis://localhost:6379/0 --namespace demo --action search --task-type demo_task
```

### CLI 参数说明

- `--uri`: 队列 URI (redis://host:port/db 或 postgresql://user:pass@host:port/db)
- `--namespace`: 队列命名空间
- `--action`: 查询操作 (stats, tasks, health, search, doing)
- `--status`: 任务状态过滤 (todo, doing, done, error, null)
- `--task-type`: 任务类型过滤
- `--limit`: 返回数量限制
- `--format`: 输出格式 (json, table)

## 📊 任务查询系统

### 编程接口

```python
from qtask_nano import TaskQuery

# 创建查询对象
query = TaskQuery(
    uri="redis://localhost:6379/0",
    namespace="demo"
)

# 获取队列统计
stats = query.get_stats()
print(f"总任务数: {stats['total']}")
print(f"待处理: {stats['todo']}")
print(f"处理中: {stats['doing']}")
print(f"已完成: {stats['done']}")
print(f"错误: {stats['error']}")

# 查询任务
tasks = query.get_tasks(status="doing", limit=10)
for task in tasks:
    print(f"任务ID: {task['task_id']}")
    print(f"任务类型: {task['task_type']}")
    print(f"状态: {task['status']}")
```

## 🏗️ 架构设计

### 核心组件

- **TaskQueue**: 任务队列主类，支持 Redis 和 PostgreSQL 后端
- **Task**: 任务对象，包含任务类型、参数和元数据
- **Worker**: 任务处理器，支持多任务类型注册
- **TaskQuery**: 统一的任务查询接口
- **TaskQueryCLI**: 命令行查询工具

### 后端支持

- **Redis**: 高性能内存存储，适合高并发场景
- **PostgreSQL**: 持久化存储，适合需要数据持久化的场景

## 📝 日志系统

基于 loguru 的日志系统，支持结构化日志和日志文件输出。

```python
from qtask_nano import logger, get_logger

# 使用全局 logger
logger.info("这是一条信息日志")

# 使用带上下文的 logger
task_logger = get_logger("task_processor")
task_logger.info("处理任务", task_id="12345")
```

## 🧪 测试

```bash
# 运行测试
python -m pytest tests/

# 运行特定测试
python tests/test_redis_queue.py
```

## 📚 示例

项目包含多个示例文件：

- `examples/producer.py`: 生产者示例
- `examples/consumer.py`: 消费者示例
- `examples/query_example.py`: 查询系统示例

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📄 许可证

MIT License

## 🔗 相关链接

- [查询系统使用指南](QUERY_GUIDE.md)
- [任务查询 CLI 指南](TASK_QUERY_GUIDE.md)