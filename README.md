# Multi-AI Agent 协作系统

基于 Orchestrator (主从模式) 的多AI Agent协作系统，支持本地进程运行和消息队列通信。

## 系统架构

```
┌─────────────────────────────────────────────┐
│              Orchestrator (主节点)               │
│  ┌─────────────┬─────────────┬────────────┐ │
│  │ Task Planner │Task Scheduler│Heartbeat   │ │
│  │             │             │ Monitor    │ │
│  └─────────────┴─────────────┴────────────┘ │
└─────────────────────┬───────────────────────┘
                      │ Message Queue
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
   ┌─────────┐   ┌─────────┐   ┌─────────┐
   │ Worker A│   │ Worker B│   │ Worker C│
   │(分析)    │   │(研究)   │   │(编码)   │
   └─────────┘   └─────────┘   └─────────┘
```

## 核心组件

### 1. Orchestrator (主编排器)
- 负责任务调度和分配
- 监控 Worker 心跳健康状态
- 管理任务超时和重试
- 维护 Worker 注册表

### 2. WorkerAgent (工作节点)
- 向 Orchestrator 注册自己的能力
- 监听任务队列接收分配的任务
- 执行任务并返回结果
- 定期发送心跳保活

### 3. MessageQueue (消息队列)
- 基于 PriorityQueue 的线程安全队列
- 支持消息优先级
- 多队列类型：Orchestrator→Worker, Worker→Orchestrator, Heartbeat

## 快速开始

### 安装依赖
```bash
pip install pyyaml
```

### 运行示例

**1. 简单演示**
```bash
python3 examples/simple_demo.py
```

**2. API 服务器 (带5个Worker)**
```bash
python3 examples/api_server.py
# 访问 http://localhost:8080/api/status 查看状态
```

**3. 3D Dashboard**
```bash
python3 examples/api_server.py
# 在浏览器打开 /home/laodaboss/Desktop/3d_agent_office.html
```

## 使用方法

### 1. 创建 Orchestrator
```python
from multi_agent_system.common.queue import MessageQueueManager
from multi_agent_system.orchestrator.core import Orchestrator

mq = MessageQueueManager()
orch = Orchestrator(mq)
orch.start()
```

### 2. 创建 Worker
```python
from multi_agent_system.worker.agent import WorkerAgent

# 定义任务处理器
def analysis_handler(task_data):
    query = task_data.get("task_data", {}).get("query", "")
    return {"result": f"分析完成: {query}"}

# 创建 Worker 并注册处理器
worker = WorkerAgent(
    worker_id="worker_1",
    worker_type="Analysis",
    capabilities=["analysis"],  # 该Worker能处理的任务类型
    mq=mq
)
worker.register_handler("analysis", analysis_handler)
worker.start()
```

### 3. 提交任务
```python
# 提交任务
task_id = orch.submit_task("analysis", {"query": "Q1 revenue"})
print(f"Task ID: {task_id}")

# 查询任务状态
for _ in range(30):
    status = orch.get_task_status(task_id)
    if status["status"] in ("completed", "failed"):
        break
    time.sleep(0.5)

print(f"Status: {status['status']}")
print(f"Result: {status.get('result')}")
```

### 4. 获取系统状态
```python
# 获取所有 Worker 状态
workers = orch.get_workers_status()
for w in workers:
    print(f"{w['worker_id']}: {w['status']}")

# 获取所有任务状态
tasks = orch.get_task_status("*")  # 或指定 task_id
```

### 5. 停止系统
```python
worker.stop()
orch.stop()
```

## 任务类型和 Worker 能力

在 `api_server.py` 中预定义了5种 Worker:

| Worker ID | 类型 | 能力 | 说明 |
|-----------|------|------|------|
| worker_1 | Analysis | analysis | 数据分析 |
| worker_2 | Research | research | 市场研究 |
| worker_3 | Coding | coding | 代码编写 |
| worker_4 | Design | design | UI设计 |
| worker_5 | Data | data | 数据处理 |

## 消息流

```
1. Worker 启动 → 发送 REGISTER 到 Orchestrator
2. Orchestrator 收到 → 更新 Worker 状态为 "online"
3. 用户提交任务 → Orchestrator 分配给合适的 Worker
4. Worker 执行任务 → 发送 RESULT 回 Orchestrator
5. Orchestrator 更新任务状态为 "completed"
6. Worker 定期发送 HEARTBEAT 保活
```

## API 接口

启动 `api_server.py` 后可用:

```
GET /api/status
返回:
{
  "workers": [...],
  "tasks": {...}
}
```

## 项目结构

```
multi_agent_system/
├── src/multi_agent_system/
│   ├── common/
│   │   ├── message.py      # 消息格式定义
│   │   ├── queue.py        # 消息队列管理器
│   │   ├── errors.py       # 错误处理
│   │   └── timeout.py      # 超时管理
│   ├── orchestrator/
│   │   ├── core.py         # Orchestrator 主类
│   │   └── process_manager.py
│   ├── worker/
│   │   └── agent.py        # WorkerAgent 类
│   └── protocols/
│       └── api.py
├── examples/
│   ├── simple_demo.py       # 简单演示
│   ├── api_server.py       # API 服务器
│   └── agent_office_3d.py  # 3D 可视化
└── tests/
    ├── test_integration.py # 集成测试
    ├── test_orchestrator.py
    ├── test_queue.py
    └── test_worker.py
```

## 运行测试

```bash
python3 tests/test_integration.py
```

## License

MIT
