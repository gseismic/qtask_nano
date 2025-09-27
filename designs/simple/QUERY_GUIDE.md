# 任务查询系统使用指南

## 概述

任务查询系统提供了一个统一的接口来查询任务队列的状态，支持Redis和PostgreSQL两种后端存储。通过抽象化设计，用户可以使用相同的API来查询不同后端的数据。

## 架构设计

### 解耦架构

```
TaskQuery (统一接口)
    ├── BaseQueryBackend (抽象基类)
    ├── RedisQueryBackend (Redis实现)
    └── PostgreSQLQueryBackend (PostgreSQL实现)
```

### 核心组件

1. **BaseQueryBackend**: 定义统一的后端接口
2. **RedisQueryBackend**: Redis后端的具体实现
3. **PostgreSQLQueryBackend**: PostgreSQL后端的具体实现
4. **TaskQuery**: 统一的查询接口，自动选择后端
5. **TaskQueryCLI**: 命令行界面

## 使用方法

### 1. 基本使用

```python
from task_queue import TaskQueue
from task_query import TaskQuery, TaskQueryCLI

# 创建队列（支持Redis和PostgreSQL）
queue = TaskQueue("my_queue", "redis://localhost:6379/0")
# 或者
# queue = TaskQueue("my_queue", "postgresql://user:pass@localhost:5432/db")

# 创建查询器
query = TaskQuery(queue)
cli = TaskQueryCLI(queue)

# 获取统计信息
stats = query.get_queue_stats()
print(f"待处理任务: {stats['todo_count']}")

# 使用CLI打印
cli.print_stats()
```

### 2. 查询任务

```python
# 获取指定状态的任务
todo_tasks = query.get_tasks_by_status('todo', limit=10)
doing_tasks = query.get_tasks_by_status('doing', limit=5)

# 搜索任务
email_tasks = query.search_tasks(task_type='email', status='todo')
recent_tasks = query.search_tasks(created_after=datetime.now() - timedelta(hours=1))

# 获取执行中的任务及其执行时长
doing_with_duration = query.get_doing_tasks_with_duration()
for task in doing_with_duration:
    print(f"任务 {task['task_id']} 已执行 {task['duration_seconds']:.2f} 秒")
```

### 3. 健康检查

```python
# 获取队列健康状态
health = query.get_queue_health()
if not health['is_healthy']:
    print("队列状态异常:")
    for warning in health['warnings']:
        print(f"  - {warning}")
```

### 4. 数据导出

```python
# 导出所有任务
export_file = query.export_tasks(format='json')

# 导出指定状态的任务
todo_export = query.export_tasks(status='todo', format='csv')
```

## 命令行工具

### 基本用法

```bash
# 查看队列统计
python query_cli.py --uri redis://localhost:6379/0 --namespace my_queue --action stats

# 查看指定状态的任务
python query_cli.py --uri redis://localhost:6379/0 --namespace my_queue --action tasks --status todo

# 查看队列健康状态
python query_cli.py --uri redis://localhost:6379/0 --namespace my_queue --action health

# 搜索任务
python query_cli.py --uri redis://localhost:6379/0 --namespace my_queue --action search --task-type email

# 查看执行中的任务
python query_cli.py --uri redis://localhost:6379/0 --namespace my_queue --action doing
```

### JSON输出

```bash
# 以JSON格式输出
python query_cli.py --uri redis://localhost:6379/0 --namespace my_queue --action stats --format json
```

## 后端实现细节

### Redis后端

- 使用Redis的List、Set、SortedSet等数据结构
- 支持Key过期时间配置
- 高性能，适合高并发场景

### PostgreSQL后端

- 使用关系型数据库存储
- 支持复杂查询和事务
- 数据持久化，适合重要业务场景

## 统一接口的优势

1. **代码复用**: 相同的查询代码可以用于不同后端
2. **易于扩展**: 添加新的后端实现只需继承BaseQueryBackend
3. **类型安全**: 统一的返回数据格式
4. **错误处理**: 统一的异常处理机制

## 扩展新后端

要添加新的后端支持，只需：

1. 继承`BaseQueryBackend`类
2. 实现三个抽象方法：
   - `get_stats()`: 获取统计信息
   - `get_tasks_by_status()`: 获取指定状态的任务
   - `get_task_metadata()`: 获取任务元数据

```python
class CustomQueryBackend(BaseQueryBackend):
    def __init__(self, custom_client):
        self.client = custom_client
    
    def get_stats(self, queue_id: str) -> Dict[str, int]:
        # 实现统计信息获取
        pass
    
    def get_tasks_by_status(self, queue_id: str, status: str, limit: int) -> List[str]:
        # 实现任务获取
        pass
    
    def get_task_metadata(self, queue_id: str, task_key: str) -> Dict[str, Any]:
        # 实现元数据获取
        pass
```

## 性能考虑

- **Redis**: 适合高并发、低延迟场景
- **PostgreSQL**: 适合复杂查询、数据一致性要求高的场景
- **缓存**: 可以考虑在应用层添加缓存机制
- **分页**: 大量数据查询时使用limit参数

## 故障排除

### 常见问题

1. **连接失败**: 检查URI格式和网络连接
2. **权限问题**: 确保数据库用户有相应权限
3. **数据格式**: 确保任务数据格式正确
4. **性能问题**: 考虑使用索引或优化查询

### 调试技巧

```python
# 启用详细日志
import logging
logging.basicConfig(level=logging.DEBUG)

# 检查队列状态
stats = query.get_queue_stats()
if 'error' in stats:
    print(f"错误: {stats['error']}")

# 验证任务格式
tasks = query.get_tasks_by_status('todo', 1)
if tasks:
    print(f"任务数据: {json.dumps(tasks[0], indent=2)}")
```

## 最佳实践

1. **使用连接池**: 避免频繁创建连接
2. **错误处理**: 始终处理可能的异常
3. **资源清理**: 及时关闭连接和资源
4. **监控**: 定期检查队列健康状态
5. **备份**: 重要数据定期备份

## 示例代码

完整的使用示例请参考 `query_example.py` 文件。
