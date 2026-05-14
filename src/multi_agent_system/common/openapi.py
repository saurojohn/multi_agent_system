"""OpenAPI/Swagger documentation generator."""

import json
import logging
from typing import Dict, List

logger = logging.getLogger('openapi')


class OpenAPIGenerator:
    """Generates OpenAPI 3.0 documentation for the multi-agent API."""

    def __init__(self, title: str = "Multi-Agent System API",
                 version: str = "1.0.0",
                 description: str = "REST API for Multi-Agent Orchestration System"):
        self.title = title
        self.version = version
        self.description = description

    def generate(self) -> Dict:
        """Generate complete OpenAPI specification."""
        return {
            "openapi": "3.0.3",
            "info": {
                "title": self.title,
                "version": self.version,
                "description": self.description,
                "contact": {
                    "name": "API Support",
                    "email": "support@example.com"
                }
            },
            "servers": [
                {"url": "http://localhost:8080", "description": "Local development server"}
            ],
            "paths": self._generate_paths(),
            "components": self._generate_components()
        }

    def _generate_paths(self) -> Dict:
        return {
            "/health": {
                "get": {
                    "summary": "Health check",
                    "description": "Returns system health status for load balancers",
                    "operationId": "getHealth",
                    "tags": ["System"],
                    "responses": {
                        "200": {
                            "description": "System is healthy",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/HealthStatus"}
                                }
                            }
                        }
                    }
                }
            },
            "/api/status": {
                "get": {
                    "summary": "Get system status",
                    "description": "Returns current workers and tasks status",
                    "operationId": "getStatus",
                    "tags": ["System"],
                    "responses": {
                        "200": {
                            "description": "System status",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/SystemStatus"}
                                }
                            }
                        }
                    }
                }
            },
            "/api/metrics": {
                "get": {
                    "summary": "Get Prometheus metrics",
                    "description": "Returns system metrics in Prometheus format",
                    "operationId": "getMetrics",
                    "tags": ["System"],
                    "responses": {
                        "200": {
                            "description": "Prometheus metrics",
                            "content": {
                                "text/plain": {
                                    "schema": {"type": "string"}
                                }
                            }
                        }
                    }
                }
            },
            "/api/tasks": {
                "get": {
                    "summary": "List all tasks",
                    "description": "Returns list of all tasks",
                    "operationId": "listTasks",
                    "tags": ["Tasks"],
                    "responses": {
                        "200": {
                            "description": "List of tasks",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/TaskList"}
                                }
                            }
                        }
                    }
                },
                "post": {
                    "summary": "Submit new task",
                    "description": "Submit a new task for execution",
                    "operationId": "submitTask",
                    "tags": ["Tasks"],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/TaskSubmit"}
                            }
                        }
                    },
                    "responses": {
                        "201": {
                            "description": "Task submitted successfully",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/TaskSubmitResponse"}
                                }
                            }
                        },
                        "400": {"description": "Invalid request"}
                    }
                }
            },
            "/api/tasks/batch": {
                "post": {
                    "summary": "Submit multiple tasks",
                    "description": "Submit multiple tasks in a single request",
                    "operationId": "submitBatch",
                    "tags": ["Tasks"],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/BatchSubmit"}
                            }
                        }
                    },
                    "responses": {
                        "201": {
                            "description": "Tasks submitted",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/BatchSubmitResponse"}
                                }
                            }
                        }
                    }
                }
            },
            "/api/tasks/{task_id}": {
                "get": {
                    "summary": "Get task status",
                    "description": "Get status of a specific task",
                    "operationId": "getTask",
                    "tags": ["Tasks"],
                    "parameters": [
                        {"name": "task_id", "in": "path", "required": True, "schema": {"type": "string"}}
                    ],
                    "responses": {
                        "200": {
                            "description": "Task status",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/TaskStatus"}
                                }
                            }
                        },
                        "404": {"description": "Task not found"}
                    }
                }
            },
            "/api/tasks/{task_id}/cancel": {
                "post": {
                    "summary": "Cancel task",
                    "description": "Request cancellation of a task",
                    "operationId": "cancelTask",
                    "tags": ["Tasks"],
                    "parameters": [
                        {"name": "task_id", "in": "path", "required": True, "schema": {"type": "string"}}
                    ],
                    "responses": {
                        "200": {"description": "Cancellation requested"}
                    }
                }
            },
            "/api/workers": {
                "get": {
                    "summary": "List all workers",
                    "description": "Returns list of all registered workers",
                    "operationId": "listWorkers",
                    "tags": ["Workers"],
                    "responses": {
                        "200": {
                            "description": "List of workers",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/WorkerList"}
                                }
                            }
                        }
                    }
                },
                "post": {
                    "summary": "Register new worker",
                    "description": "Register a new worker dynamically",
                    "operationId": "registerWorker",
                    "tags": ["Workers"],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/WorkerRegister"}
                            }
                        }
                    },
                    "responses": {
                        "201": {"description": "Worker registered"},
                        "409": {"description": "Worker already exists"}
                    }
                }
            },
            "/api/workers/{worker_id}": {
                "get": {
                    "summary": "Get worker status",
                    "description": "Get status of a specific worker",
                    "operationId": "getWorker",
                    "tags": ["Workers"],
                    "parameters": [
                        {"name": "worker_id", "in": "path", "required": True, "schema": {"type": "string"}}
                    ],
                    "responses": {
                        "200": {"description": "Worker status"},
                        "404": {"description": "Worker not found"}
                    }
                },
                "delete": {
                    "summary": "Unregister worker",
                    "description": "Remove a worker from the system",
                    "operationId": "unregisterWorker",
                    "tags": ["Workers"],
                    "parameters": [
                        {"name": "worker_id", "in": "path", "required": True, "schema": {"type": "string"}}
                    ],
                    "responses": {
                        "200": {"description": "Worker unregistered"},
                        "404": {"description": "Worker not found"}
                    }
                }
            },
            "/api/config/reload": {
                "post": {
                    "summary": "Hot reload configuration",
                    "description": "Reload configuration without restart",
                    "operationId": "reloadConfig",
                    "tags": ["System"],
                    "responses": {
                        "200": {"description": "Configuration reloaded"}
                    }
                }
            },
            "/api/events": {
                "get": {
                    "summary": "SSE real-time events",
                    "description": "Server-Sent Events stream for real-time updates",
                    "operationId": "getEvents",
                    "tags": ["System"],
                    "responses": {
                        "200": {
                            "description": "SSE stream",
                            "content": {
                                "text/event-stream": {
                                    "schema": {"type": "string"}
                                }
                            }
                        }
                    }
                }
            }
        }

    def _generate_components(self) -> Dict:
        return {
            "schemas": {
                "HealthStatus": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string", "example": "healthy"},
                        "timestamp": {"type": "number"},
                        "orchestrator": {"type": "string"},
                        "workers": {
                            "type": "object",
                            "properties": {
                                "total": {"type": "integer"},
                                "online": {"type": "integer"}
                            }
                        },
                        "tasks": {
                            "type": "object",
                            "properties": {
                                "pending": {"type": "integer"},
                                "running": {"type": "integer"},
                                "completed": {"type": "integer"},
                                "failed": {"type": "integer"}
                            }
                        }
                    }
                },
                "SystemStatus": {
                    "type": "object",
                    "properties": {
                        "workers": {"type": "array", "items": {"$ref": "#/components/schemas/Worker"}},
                        "tasks": {"type": "object"}
                    }
                },
                "TaskSubmit": {
                    "type": "object",
                    "required": ["task_type"],
                    "properties": {
                        "task_type": {"type": "string", "example": "analysis"},
                        "task_data": {"type": "object", "example": {"query": "analyze this"}},
                        "priority": {"type": "integer", "minimum": 1, "maximum": 3, "default": 2},
                        "timeout": {"type": "integer", "default": 300},
                        "dependencies": {"type": "array", "items": {"type": "string"}}
                    }
                },
                "TaskSubmitResponse": {
                    "type": "object",
                    "properties": {
                        "task_id": {"type": "string"},
                        "status": {"type": "string"}
                    }
                },
                "BatchSubmit": {
                    "type": "object",
                    "required": ["tasks"],
                    "properties": {
                        "tasks": {"type": "array", "items": {"$ref": "#/components/schemas/TaskSubmit"}}
                    }
                },
                "BatchSubmitResponse": {
                    "type": "object",
                    "properties": {
                        "task_ids": {"type": "array", "items": {"type": "string"}},
                        "count": {"type": "integer"}
                    }
                },
                "TaskStatus": {
                    "type": "object",
                    "properties": {
                        "task_id": {"type": "string"},
                        "status": {"type": "string", "enum": ["pending", "running", "completed", "failed"]},
                        "result": {"type": "object"},
                        "error": {"type": "string"},
                        "assigned_worker": {"type": "string"}
                    }
                },
                "TaskList": {
                    "type": "object",
                    "properties": {
                        "tasks": {"type": "array", "items": {"$ref": "#/components/schemas/TaskStatus"}},
                        "count": {"type": "integer"}
                    }
                },
                "Worker": {
                    "type": "object",
                    "properties": {
                        "worker_id": {"type": "string"},
                        "worker_type": {"type": "string"},
                        "status": {"type": "string"},
                        "capabilities": {"type": "array", "items": {"type": "string"}},
                        "current_task": {"type": "string"},
                        "completed_tasks": {"type": "integer"},
                        "failed_tasks": {"type": "integer"}
                    }
                },
                "WorkerList": {
                    "type": "object",
                    "properties": {
                        "workers": {"type": "array", "items": {"$ref": "#/components/schemas/Worker"}},
                        "count": {"type": "integer"}
                    }
                },
                "WorkerRegister": {
                    "type": "object",
                    "required": ["worker_id", "capabilities"],
                    "properties": {
                        "worker_id": {"type": "string"},
                        "worker_type": {"type": "string", "default": "general"},
                        "capabilities": {"type": "array", "items": {"type": "string"}}
                    }
                }
            },
            "securitySchemes": {
                "ApiKeyAuth": {
                    "type": "apiKey",
                    "in": "header",
                    "name": "X-API-Key"
                },
                "BearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                    "bearerFormat": "JWT"
                }
            }
        }

    def to_json(self) -> str:
        """Export as JSON string."""
        return json.dumps(self.generate(), indent=2)


# Global generator
_generator = OpenAPIGenerator()


def get_openapi_spec() -> Dict:
    return _generator.generate()


def get_openapi_json() -> str:
    return _generator.to_json()