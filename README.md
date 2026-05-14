# Multi-AI Agent Collaboration System / 多AI Agent协作系统

基于 Orchestrator (主从模式) 的多AI Agent协作系统，支持本地进程运行和消息队列通信。

A multi-AI Agent collaboration system based on Orchestrator (master-worker) pattern, supporting local process execution and message queue communication.

---

## 系统架构 / Architecture

```
┌─────────────────────────────────────────────┐
│              Orchestrator (主节点/Master)          │
│  ┌─────────────┬─────────────┬────────────┐ │
│  │ Task Planner │Task Scheduler│Heartbeat   │ │
│  │ 任务规划器   │ 任务调度器    │ 心跳监控    │ │
│  └─────────────┴─────────────┴────────────┘ │
└─────────────────────┬───────────────────────┘
                      │ Message Queue / 消息队列
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
   ┌─────────┐   ┌─────────┐   ┌─────────┐
   │ Worker A│   │ Worker B│   │ Worker C│
   │ (分析)  │   │ (研究)  │   │ (编码)  │
   └─────────┘   └─────────┘   └─────────┘
```

---

## 核心组件 / Core Components

### 1. Orchestrator (主编排器)
- 负责任务调度和分配 / Task scheduling and assignment
- 监控 Worker 心跳健康状态 / Monitor worker heartbeat health
- 管理任务超时和重试 / Manage task timeout and retry
- 维护 Worker 注册表 / Maintain worker registry

### 2. WorkerAgent (工作节点)
- 向 Orchestrator 注册自己的能力 / Register capabilities with Orchestrator
- 监听任务队列接收分配的任务 / Listen to task queue for assigned tasks
- 执行任务并返回结果 / Execute tasks and return results
- 定期发送心跳保活 / Send periodic heartbeats

### 3. MessageQueue (消息队列)
- 基于 PriorityQueue 的线程安全队列 / Thread-safe queue based on PriorityQueue
- 支持消息优先级 / Support message priority
- 多队列类型 / Multiple queue types:
  - Orchestrator → Worker
  - Worker → Orchestrator
  - Heartbeat

---

## 快速开始 / Quick Start

### 安装依赖 / Install Dependencies
```bash
pip install pyyaml
```

### 运行示例 / Run Examples

**1. 简单演示 / Simple Demo**
```bash
python3 examples/simple_demo.py
```

**2. API 服务器 (带5个Worker) / API Server (with 5 Workers)**
```bash
python3 examples/api_server.py
# Visit http://localhost:8080/api/status for status
# 访问 http://localhost:8080/api/status 查看状态
```

**3. 3D Dashboard / 3D可视化**
```bash
python3 examples/api_server.py
# Open examples/3d_agent_office.html in browser
# 在浏览器打开 examples/3d_agent_office.html
```

---

## 使用方法 / Usage

### 1. 创建 Orchestrator / Create Orchestrator
```python
from multi_agent_system.common.queue import MessageQueueManager
from multi_agent_system.orchestrator.core import Orchestrator

mq = MessageQueueManager()
orch = Orchestrator(mq)
orch.start()
```

### 2. 创建 Worker / Create Worker
```python
from multi_agent_system.worker.agent import WorkerAgent

# 定义任务处理器 / Define task handler
def analysis_handler(task_data):
    query = task_data.get("task_data", {}).get("query", "")
    return {"result": f"分析完成: {query}"}

# 创建 Worker 并注册处理器 / Create Worker and register handler
worker = WorkerAgent(
    worker_id="worker_1",
    worker_type="Analysis",
    capabilities=["analysis"],  # 该Worker能处理的任务类型 / Task types this worker can handle
    mq=mq
)
worker.register_handler("analysis", analysis_handler)
worker.start()
```

### 3. 提交任务 / Submit Task
```python
# 提交任务 / Submit task
task_id = orch.submit_task("analysis", {"query": "Q1 revenue"})
print(f"Task ID: {task_id}")

# 查询任务状态 / Query task status
for _ in range(30):
    status = orch.get_task_status(task_id)
    if status["status"] in ("completed", "failed"):
        break
    time.sleep(0.5)

print(f"Status: {status['status']}")
print(f"Result: {status.get('result')}")
```

### 4. 获取系统状态 / Get System Status
```python
# 获取所有 Worker 状态 / Get all worker status
workers = orch.get_workers_status()
for w in workers:
    print(f"{w['worker_id']}: {w['status']}")
```

### 5. 停止系统 / Stop System
```python
worker.stop()
orch.stop()
```

---

## Worker 类型 / Worker Types

在 `api_server.py` 中预定义了5种 Worker / 5 Worker types predefined in api_server.py:

| Worker ID | 类型/Type | 能力/Capability | 说明/Description |
|-----------|-----------|------------------|-------------------|
| worker_1 | Analysis | analysis | 数据分析/Data Analysis |
| worker_2 | Research | research | 市场研究/Market Research |
| worker_3 | Coding | coding | 代码编写/Code Writing |
| worker_4 | Design | design | UI设计/UI Design |
| worker_5 | Data | data | 数据处理/Data Processing |

---

## 消息流程 / Message Flow

```
1. Worker 启动 → 发送 REGISTER 到 Orchestrator
   Worker starts → Send REGISTER to Orchestrator

2. Orchestrator 收到 → 更新 Worker 状态为 "online"
   Orchestrator receives → Update worker status to "online"

3. 用户提交任务 → Orchestrator 分配给合适的 Worker
   User submits task → Orchestrator assigns to suitable Worker

4. Worker 执行任务 → 发送 RESULT 回 Orchestrator
   Worker executes task → Send RESULT to Orchestrator

5. Orchestrator 更新任务状态为 "completed"
   Orchestrator updates task status to "completed"

6. Worker 定期发送 HEARTBEAT 保活
   Worker sends periodic HEARTBEAT for keep-alive
```

---

## API 接口 / API Endpoints

启动 `api_server.py` 后可用 / Available after starting api_server.py:

```
GET /api/status
返回 / Returns:
{
  "workers": [...],
  "tasks": {...}
}
```

---

## 项目结构 / Project Structure

```
multi_agent_system/
├── src/multi_agent_system/
│   ├── common/
│   │   ├── message.py      # 消息格式定义 / Message format definition
│   │   ├── queue.py        # 消息队列管理器 / Message queue manager
│   │   ├── errors.py       # 错误处理 / Error handling
│   │   └── timeout.py      # 超时管理 / Timeout management
│   ├── orchestrator/
│   │   ├── core.py         # Orchestrator 主类 / Orchestrator main class
│   │   └── process_manager.py
│   ├── worker/
│   │   └── agent.py        # WorkerAgent 类 / WorkerAgent class
│   └── protocols/
│       └── api.py
├── examples/
│   ├── simple_demo.py       # 简单演示 / Simple demo
│   ├── api_server.py       # API 服务器 / API server
│   └── agent_office_3d.py  # 3D 可视化 / 3D visualization
└── tests/
    ├── test_integration.py  # 集成测试 / Integration tests
    ├── test_orchestrator.py
    ├── test_queue.py
    └── test_worker.py
```

---

## 运行测试 / Run Tests

```bash
python3 tests/test_integration.py
```

---

## License

MIT
