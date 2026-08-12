"""api/services/tasks_repo.py — Internal task store (api_tasks table)."""

from __future__ import annotations

import json
from typing import Any, Optional

from api.services.pg_pool import get_pool

TASKS_TABLE = "api_tasks"
TASKS_SCHEMA = "scraper"


async def ensure_tasks_table():
    """Create the tasks table if it doesn't exist."""
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                CREATE SCHEMA IF NOT EXISTS {TASKS_SCHEMA}
            """)
            await cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TASKS_SCHEMA}.{TASKS_TABLE} (
                    id TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    config_path TEXT,
                    query TEXT,
                    instance_count INTEGER DEFAULT 1,
                    pid INTEGER,
                    exit_code INTEGER,
                    logs_tail TEXT DEFAULT '',
                    metadata JSONB DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    started_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ
                )
            """)


def _row_to_task(row, cur) -> dict:
    desc = [d[0] for d in cur.description]
    d = dict(zip(desc, row))
    d["metadata"] = json.loads(d.get("metadata") or "{}")
    for ts_field in ("created_at", "started_at", "completed_at"):
        if d.get(ts_field) and hasattr(d[ts_field], "isoformat"):
            d[ts_field] = d[ts_field].isoformat()
    return d


async def save_task(task: dict) -> dict:
    """Insert or update a task in PG."""
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            meta_json = json.dumps(task.get("metadata", {}))
            await cur.execute(f"""
                INSERT INTO {TASKS_SCHEMA}.{TASKS_TABLE}
                    (id, type, status, config_path, query, instance_count,
                     pid, exit_code, logs_tail, metadata, created_at,
                     started_at, completed_at)
                VALUES (%(id)s, %(type)s, %(status)s, %(config_path)s, %(query)s,
                        %(instance_count)s, %(pid)s, %(exit_code)s, %(logs_tail)s,
                        %(metadata)s::jsonb, %(created_at)s::timestamptz,
                        %(started_at)s::timestamptz, %(completed_at)s::timestamptz)
                ON CONFLICT (id) DO UPDATE SET
                    status = EXCLUDED.status,
                    pid = EXCLUDED.pid,
                    exit_code = EXCLUDED.exit_code,
                    logs_tail = EXCLUDED.logs_tail,
                    metadata = EXCLUDED.metadata,
                    started_at = EXCLUDED.started_at,
                    completed_at = EXCLUDED.completed_at
            """, {
                "id": task["id"],
                "type": task["type"],
                "status": task["status"],
                "config_path": task.get("config_path"),
                "query": task.get("query"),
                "instance_count": task.get("instance_count", 1),
                "pid": task.get("pid"),
                "exit_code": task.get("exit_code"),
                "logs_tail": task.get("logs_tail", ""),
                "metadata": meta_json,
                "created_at": task.get("created_at"),
                "started_at": task.get("started_at"),
                "completed_at": task.get("completed_at"),
            })
    return task


async def get_task(task_id: str) -> Optional[dict]:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT * FROM {TASKS_SCHEMA}.{TASKS_TABLE} WHERE id = %s",
                (task_id,),
            )
            row = await cur.fetchone()
            if not row:
                return None
            return _row_to_task(row, cur)


async def list_tasks(status: Optional[str] = None, limit: int = 20, offset: int = 0) -> tuple[list[dict], int]:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            where = "WHERE status = %s" if status else ""
            params: list[Any] = [status] if status else []

            await cur.execute(
                f"SELECT count(*) FROM {TASKS_SCHEMA}.{TASKS_TABLE} {where}",
                params,
            )
            total = (await cur.fetchone())[0]

            await cur.execute(
                f"SELECT * FROM {TASKS_SCHEMA}.{TASKS_TABLE} {where} "
                f"ORDER BY created_at DESC LIMIT %s OFFSET %s",
                [*params, limit, offset],
            )
            rows = await cur.fetchall()
            tasks = [_row_to_task(r, cur) for r in rows]
            return tasks, total


async def create_pipeline_task(task: dict) -> dict:
    return await save_task(task)


async def update_task_status(task_id: str, status: str) -> Optional[dict]:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"UPDATE {TASKS_SCHEMA}.{TASKS_TABLE} SET status = %s, "
                "completed_at = CASE WHEN %s IN ('completed','failed','cancelled') THEN NOW() ELSE completed_at END "
                "WHERE id = %s",
                (status, status, task_id),
            )
    return await get_task(task_id)


async def list_pipeline_tasks(
    task_type: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict], int]:
    where_parts = []
    params: list[Any] = []
    if task_type:
        where_parts.append("type = %s")
        params.append(task_type)
    if status:
        where_parts.append("status = %s")
        params.append(status)
    where_sql = " AND ".join(where_parts) if where_parts else "TRUE"

    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT count(*) FROM scraper.api_tasks WHERE {where_sql}",
                params,
            )
            total = (await cur.fetchone())[0]

            await cur.execute(
                f"SELECT * FROM scraper.api_tasks WHERE {where_sql} "
                f"ORDER BY created_at DESC LIMIT %s OFFSET %s",
                [*params, limit, offset],
            )
            rows = await cur.fetchall()
            tasks = [_row_to_task(r, cur) for r in rows]
            return tasks, total


async def get_active_pipeline_tasks() -> list[dict]:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM scraper.api_tasks WHERE status IN ('pending', 'running') "
                "ORDER BY created_at DESC"
            )
            rows = await cur.fetchall()
            return [_row_to_task(r, cur) for r in rows]


async def cancel_pipeline_task(task_id: str) -> Optional[dict]:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT status FROM scraper.api_tasks WHERE id = %s",
                (task_id,),
            )
            row = await cur.fetchone()
            if not row:
                return None
            if row[0] not in ("pending",):
                return None
            await cur.execute(
                "UPDATE scraper.api_tasks SET status = 'cancelled', completed_at = NOW() "
                "WHERE id = %s",
                (task_id,),
            )
    return await get_task(task_id)
