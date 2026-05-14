"""Process management for worker agents."""

import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class ProcessInfo:
    pid: int
    worker_id: str
    worker_type: str
    started_at: float
    command: List[str]
    process: Optional[subprocess.Popen] = None


class ProcessManager:
    def __init__(self, worker_dir: str = "./workers",
                 heartbeat_timeout: int = 30):
        self.worker_dir = worker_dir
        self.heartbeat_timeout = heartbeat_timeout
        self.processes: Dict[str, ProcessInfo] = {}
        self._lock = threading.Lock()

    def start_worker(self, worker_id: str, worker_type: str,
                     capabilities: List[str], env: Optional[Dict] = None) -> bool:
        with self._lock:
            if worker_id in self.processes:
                return False

            cmd = [
                sys.executable,
                f"{self.worker_dir}/runner.py",
                "--id", worker_id,
                "--type", worker_type,
                "--capabilities"
            ] + capabilities

            try:
                process = subprocess.Popen(
                    cmd,
                    env=env or {},
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                self.processes[worker_id] = ProcessInfo(
                    pid=process.pid,
                    worker_id=worker_id,
                    worker_type=worker_type,
                    started_at=time.time(),
                    command=cmd,
                    process=process
                )
                return True
            except Exception:
                return False

    def stop_worker(self, worker_id: str, timeout: int = 10) -> bool:
        with self._lock:
            if worker_id not in self.processes:
                return False
            proc_info = self.processes[worker_id]

        try:
            proc_info.process.terminate()
            try:
                proc_info.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                proc_info.process.kill()
                proc_info.process.wait()

            with self._lock:
                del self.processes[worker_id]
            return True
        except Exception:
            return False

    def stop_all_workers(self, timeout: int = 10):
        with self._lock:
            worker_ids = list(self.processes.keys())
        for worker_id in worker_ids:
            self.stop_worker(worker_id, timeout)

    def get_worker_status(self, worker_id: str) -> Optional[Dict]:
        with self._lock:
            if worker_id not in self.processes:
                return None
            proc_info = self.processes[worker_id]
            return {
                "worker_id": proc_info.worker_id,
                "pid": proc_info.pid,
                "type": proc_info.worker_type,
                "running": proc_info.process.poll() is None,
                "uptime": time.time() - proc_info.started_at
            }

    def monitor_processes(self) -> Dict[str, bool]:
        result = {}
        with self._lock:
            for worker_id, proc_info in list(self.processes.items()):
                result[worker_id] = proc_info.process.poll() is None
        return result