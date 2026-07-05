"""Tasks router — read-only task monitoring (subprocess launch deprecated)."""

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from api.dependencies import verify_token
from api.models.models import ScraperTask, TaskListResponse
from api.services import pg_service, task_runner

router = APIRouter(prefix="/api/tasks", tags=["tasks"])

DEPRECATED_MSG = (
    "Task API deprecated — use systemd daemons: "
    "systemctl --user start infinitecrawler-search infinitecrawler-listing"
)


def _gone():
    return JSONResponse(status_code=410, content={"detail": DEPRECATED_MSG})


@router.post("/search")
async def create_search_task(_user: str = Depends(verify_token)):
    return _gone()


@router.post("/crawl")
async def create_crawl_task(_user: str = Depends(verify_token)):
    return _gone()


@router.post("/pipeline")
async def create_pipeline_task(_user: str = Depends(verify_token)):
    return _gone()


@router.get("", response_model=TaskListResponse)
async def list_tasks(
    status: str | None = Query(None),
    type: str | None = Query(None),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    _user: str = Depends(verify_token),
):
    tasks_data, total = await pg_service.list_tasks(status=status, limit=limit, offset=offset)
    tasks = [ScraperTask(**t) for t in tasks_data]
    if type:
        tasks = [t for t in tasks if t.type == type]
        total = len(tasks)
    return TaskListResponse(tasks=tasks, total=total)


@router.get("/{task_id}", response_model=ScraperTask)
async def get_task(
    task_id: str,
    _user: str = Depends(verify_token),
):
    task_data = await pg_service.get_task(task_id)
    if not task_data:
        raise HTTPException(status_code=404, detail="Task not found")
    return ScraperTask(**task_data)


@router.delete("/{task_id}")
async def cancel_task(
    task_id: str,
    _user: str = Depends(verify_token),
):
    if await task_runner.cancel_task(task_id):
        return {"status": "cancelled", "task_id": task_id}
    raise HTTPException(status_code=404, detail="Task not found")


@router.post("/{task_id}/restart")
async def restart_task(_task_id: str, _user: str = Depends(verify_token)):
    return _gone()
