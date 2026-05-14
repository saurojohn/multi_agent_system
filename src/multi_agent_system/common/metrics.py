"""Prometheus metrics exporter for multi-agent system."""

import time
from typing import Dict


class MetricsCollector:
    """Collects and exports Prometheus-format metrics."""

    def __init__(self):
        self._tasks_submitted = 0
        self._tasks_completed = 0
        self._tasks_failed = 0
        self._task_latencies = []
        self._workers_total = 0
        self._workers_online = 0
        self._start_time = time.time()

    def record_task_submitted(self):
        self._tasks_submitted += 1

    def record_task_completed(self, latency: float):
        self._tasks_completed += 1
        self._task_latencies.append(latency)
        # Keep only last 1000 latencies
        if len(self._task_latencies) > 1000:
            self._task_latencies = self._task_latencies[-1000:]

    def record_task_failed(self):
        self._tasks_failed += 1

    def update_workers(self, total: int, online: int):
        self._workers_total = total
        self._workers_online = online

    def export(self) -> str:
        """Export metrics in Prometheus format."""
        lines = [
            "# HELP multi_agent_tasks_submitted Total number of tasks submitted",
            "# TYPE multi_agent_tasks_submitted counter",
            f"multi_agent_tasks_submitted {self._tasks_submitted}",
            "",
            "# HELP multi_agent_tasks_completed Total number of tasks completed",
            "# TYPE multi_agent_tasks_completed counter",
            f"multi_agent_tasks_completed {self._tasks_completed}",
            "",
            "# HELP multi_agent_tasks_failed Total number of tasks failed",
            "# TYPE multi_agent_tasks_failed counter",
            f"multi_agent_tasks_failed {self._tasks_failed}",
            "",
            "# HELP multi_agent_task_success_rate Task success rate (0-1)",
            "# TYPE multi_agent_task_success_rate gauge",
        ]

        total = self._tasks_completed + self._tasks_failed
        if total > 0:
            rate = self._tasks_completed / total
        else:
            rate = 0.0
        lines.append(f"multi_agent_task_success_rate {rate}")

        # Task latency histogram (buckets)
        lines.extend([
            "",
            "# HELP multi_agent_task_latency_seconds Task completion latency in seconds",
            "# TYPE multi_agent_task_latency_seconds histogram",
        ])

        if self._task_latencies:
            latencies = sorted(self._task_latencies)
            buckets = [0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
            cumulative = 0
            for bucket in buckets:
                cumulative = sum(1 for l in latencies if l <= bucket)
                lines.append(f'multi_agent_task_latency_seconds_bucket{{le="{bucket}"}} {cumulative}')
            lines.append(f'multi_agent_task_latency_seconds_bucket{{le="+Inf"}} {len(latencies)}')

            # Average latency
            avg = sum(latencies) / len(latencies)
            lines.append(f'multi_agent_task_latency_seconds_avg {avg}')
            lines.append(f'multi_agent_task_latency_seconds_sum {sum(latencies)}')
            lines.append(f'multi_agent_task_latency_seconds_count {len(latencies)}')

        # Worker metrics
        lines.extend([
            "",
            "# HELP multi_agent_workers_total Total number of workers",
            "# TYPE multi_agent_workers_total gauge",
            f"multi_agent_workers_total {self._workers_total}",
            "",
            "# HELP multi_agent_workers_online Number of online workers",
            "# TYPE multi_agent_workers_online gauge",
            f"multi_agent_workers_online {self._workers_online}",
            "",
            "# HELP multi_agent_uptime_seconds System uptime in seconds",
            "# TYPE multi_agent_uptime_seconds counter",
            f"multi_agent_uptime_seconds {int(time.time() - self._start_time)}",
        ])

        return "\n".join(lines)


# Global metrics collector
_metrics = MetricsCollector()


def get_metrics() -> MetricsCollector:
    return _metrics