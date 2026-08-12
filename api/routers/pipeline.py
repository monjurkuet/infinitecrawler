"""Pipeline orchestration router — trigger enrichment tasks, check status."""

import uuid

from fastapi import APIRouter, Depends, Query, status

from api.dependencies import verify_token
from api.models.models import (
    PaginatedPipelineTasks,
    PipelineTaskCreate,
    PipelineTaskResponse,
)
from api.services import pg_service

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])


@router.post("/trigger/email-extract", response_model=PipelineTaskResponse, status_code=status.HTTP_201_CREATED)
async def trigger_email_extract(
    body: PipelineTaskCreate,
    _user: str = Depends(verify_token),
):
    task = {
        "id": uuid.uuid4().hex[:12],
        "type": "email_extract",
        "status": "pending",
        "config_path": body.config_path,
        "query": body.query,
        "metadata": body.metadata,
        "created_at": None,
    }
    result = await pg_service.create_pipeline_task(task)
    return PipelineTaskResponse(
        id=result["id"],
        type=result["type"],
        status=result["status"],
        query=result.get("query"),
        config_path=result.get("config_path"),
        metadata=result.get("metadata", {}),
        created_at=result.get("created_at"),
        started_at=result.get("started_at"),
        completed_at=result.get("completed_at"),
    )


@router.post("/trigger/linkedin-search", response_model=PipelineTaskResponse, status_code=status.HTTP_201_CREATED)
async def trigger_linkedin_search(
    body: PipelineTaskCreate,
    _user: str = Depends(verify_token),
):
    task = {
        "id": uuid.uuid4().hex[:12],
        "type": "linkedin_search",
        "status": "pending",
        "config_path": body.config_path,
        "query": body.query,
        "metadata": body.metadata,
        "created_at": None,
    }
    result = await pg_service.create_pipeline_task(task)
    return PipelineTaskResponse(
        id=result["id"],
        type=result["type"],
        status=result["status"],
        query=result.get("query"),
        config_path=result.get("config_path"),
        metadata=result.get("metadata", {}),
        created_at=result.get("created_at"),
        started_at=result.get("started_at"),
        completed_at=result.get("completed_at"),
    )


@router.post("/trigger/classification", response_model=PipelineTaskResponse, status_code=status.HTTP_201_CREATED)
async def trigger_classification(
    body: PipelineTaskCreate,
    _user: str = Depends(verify_token),
):
    task = {
        "id": uuid.uuid4().hex[:12],
        "type": "classification",
        "status": "pending",
        "config_path": body.config_path,
        "query": body.query,
        "metadata": body.metadata,
        "created_at": None,
    }
    result = await pg_service.create_pipeline_task(task)
    return PipelineTaskResponse(
        id=result["id"],
        type=result["type"],
        status=result["status"],
        query=result.get("query"),
        config_path=result.get("config_path"),
        metadata=result.get("metadata", {}),
        created_at=result.get("created_at"),
        started_at=result.get("started_at"),
        completed_at=result.get("completed_at"),
    )


@router.get("/tasks", response_model=PaginatedPipelineTasks)
async def list_tasks(
    task_type: str | None = Query(None, alias="type"),
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _user: str = Depends(verify_token),
):
    tasks, total = await pg_service.list_pipeline_tasks(
        task_type=task_type,
        status=status,
        limit=limit,
        offset=offset,
    )
    return PaginatedPipelineTasks(
        tasks=[PipelineTaskResponse(**t) for t in tasks],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/tasks/active", response_model=list[PipelineTaskResponse])
async def active_tasks(_user: str = Depends(verify_token)):
    tasks = await pg_service.get_active_pipeline_tasks()
    return [PipelineTaskResponse(**t) for t in tasks]


@router.get("/tasks/{task_id}", response_model=PipelineTaskResponse)
async def get_task(
    task_id: str,
    _user: str = Depends(verify_token),
):
    task = await pg_service.get_task(task_id)
    if not task:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Task not found")
    return PipelineTaskResponse(**task)


@router.delete("/tasks/{task_id}")
async def cancel_task(
    task_id: str,
    _user: str = Depends(verify_token),
):
    result = await pg_service.cancel_pipeline_task(task_id)
    if not result:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Task not found or already completed")
    return {"status": "cancelled", "task_id": task_id}