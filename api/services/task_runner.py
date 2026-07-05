"""Async task runner for managing scraper subprocesses."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from api.services import pg_service

log = logging.getLogger("api.task_runner")

REPO_ROOT = Path(__file__).resolve().parents[2]


class RunningTask:
    __slots__ = (
        "id", "type", "status", "config_path", "query", "instance_count",
        "pid", "exit_code", "logs", "metadata", "created_at", "started_at",
        "completed_at", "_process",
    )

    def __init__(
        self,
        task_type: str,
        config_path: Optional[str] = None,
        query: Optional[str] = None,
        instance_count: int = 1,
    ):
        self.id = uuid.uuid4().hex[:12]
        self.type = task_type
        self.status = "pending"
        self.config_path = config_path
        self.query = query
        self.instance_count = instance_count
        self.pid: Optional[int] = None
        self.exit_code: Optional[int] = None
        self.logs: list[str] = []
        self.metadata: dict = {}
        self.created_at = datetime.now(timezone.utc).isoformat()
        self.started_at: Optional[str] = None
        self.completed_at: Optional[str] = None
        self._process: Optional[asyncio.subprocess.Process] = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "type": self.type,
            "status": self.status,
            "config_path": self.config_path,
            "query": self.query,
            "instance_count": self.instance_count,
            "pid": self.pid,
            "exit_code": self.exit_code,
            "logs_tail": "\n".join(self.logs[-200:]),
            "metadata": self.metadata,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }

    async def run(self, command: list[str]):
        self.status = "running"
        self.started_at = datetime.now(timezone.utc).isoformat()
        await self._persist()

        try:
            self._process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                cwd=str(REPO_ROOT),
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
            )
            self.pid = self._process.pid
            await self._persist()

            # Read output line by line
            if self._process.stdout:
                while True:
                    line = await self._process.stdout.readline()
                    if not line:
                        break
                    decoded = line.decode(errors="replace").rstrip()
                    self.logs.append(decoded)
                    if len(self.logs) > 500:
                        self.logs = self.logs[-500:]

            await self._process.wait()
            self.exit_code = self._process.returncode or 0

        except asyncio.CancelledError:
            self._kill_process()
            self.status = "cancelled"
            self.exit_code = -15
        except Exception as e:
            self.logs.append(f"ERROR: {e}")
            self.status = "failed"
            self.exit_code = 1
        else:
            self.status = "completed" if self.exit_code == 0 else "failed"

        self.completed_at = datetime.now(timezone.utc).isoformat()
        await self._persist()

    def _kill_process(self):
        if self._process and self._process.returncode is None:
            try:
                self._process.send_signal(signal.SIGTERM)
            except ProcessLookupError:
                pass

    async def _persist(self):
        try:
            await pg_service.save_task(self.to_dict())
        except Exception:
            log.exception("Failed to persist task %s", self.id)


# ─── Global registry ────────────────────────────────────────────────────────

_tasks: dict[str, RunningTask] = {}
_poll_task: Optional[asyncio.Task] = None


async def launch_task(
    task_type: str,
    command: list[str],
    config_path: Optional[str] = None,
    query: Optional[str] = None,
    instance_count: int = 1,
) -> RunningTask:
    task = RunningTask(
        task_type=task_type,
        config_path=config_path,
        query=query,
        instance_count=instance_count,
    )
    _tasks[task.id] = task
    asyncio.create_task(task.run(command))
    return task


DEPRECATED_MSG = (
    "Task launching is deprecated — use systemd daemons: "
    "systemctl --user start infinitecrawler-search infinitecrawler-listing"
)


def get_task(task_id: str) -> Optional[RunningTask]:
    return _tasks.get(task_id)


def get_all_tasks(status: Optional[str] = None) -> list[RunningTask]:
    tasks = list(_tasks.values())
    if status:
        tasks = [t for t in tasks if t.status == status]
    return sorted(tasks, key=lambda t: t.created_at, reverse=True)


async def cancel_task(task_id: str) -> bool:
    task = _tasks.get(task_id)
    if not task:
        return False
    task._kill_process()
    task.status = "cancelled"
    task.completed_at = datetime.now(timezone.utc).isoformat()
    await task._persist()
    return True


async def restore_tasks():
    """Restore task records from PG (status info only, processes are dead on restart)."""
    try:
        tasks, _ = await pg_service.list_tasks(status="running", limit=100, offset=0)
        for t in tasks:
            if t["id"] not in _tasks:
                restored = RunningTask(t["type"])
                restored.id = t["id"]
                restored.status = "failed"
                restored.config_path = t.get("config_path")
                restored.query = t.get("query")
                restored.instance_count = t.get("instance_count", 1)
                restored.exit_code = -1
                restored.logs = ["[restarted] process killed by server restart"]
                restored.created_at = t.get("created_at") or restored.created_at
                restored.completed_at = datetime.now(timezone.utc).isoformat()
                _tasks[restored.id] = restored
                await restored._persist()
    except Exception:
        log.exception("Failed to restore tasks")


async def kill_all():
    for task in _tasks.values():
        task._kill_process()