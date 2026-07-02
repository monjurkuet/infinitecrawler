"""Tasks router — create and manage scraper tasks."""

from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import verify_token
from api.models.models import (
    ScraperTask,
    TaskCreateCrawl,
    TaskCreatePipeline,
    TaskCreateSearch,
    TaskListResponse,
)
from api.services import pg_service, task_runner

router = APIRouter(prefix="/api/tasks", tags=["tasks"])


@router.post("/search", response_model=ScraperTask)
async def create_search_task(
    body: TaskCreateSearch,
    _user: str = Depends(verify_token),
):
    command = task_runner.build_search_command(body.config, body.query)
    task = await task_runner.launch_task(
        task_type="search",
        command=command,
        config_path=body.config,
        query=body.query,
    )
    return task.to_dict()


@router.post("/crawl", response_model=ScraperTask)
async def create_crawl_task(
    body: TaskCreateCrawl,
    _user: str = Depends(verify_token),
):
    # If from_uncrawled, export uncrawled URLs first
    url_count = 0
    if body.from_uncrawled:
        uncrawled = await pg_service.get_uncrawled_count()
        if uncrawled > 0:
            # Export uncrawled URLs
            import subprocess
            import os as _os
            env = _os.environ.copy()
            env["PGPASSWORD"] = _os.environ.get("POSTGRES_PASSWORD", "changeme")
            env["PGCONNECT_TIMEOUT"] = "10"
            result = subprocess.run(
                ["bash", "-c",
                 "PGPASSWORD=changeme psql -h 100.92.181.21 -U postgres "
                 "-d infinitecrawler -t -A -c "
                 "\"COPY (SELECT DISTINCT sr.payload->>'url' FROM scraper.gmaps_search_results sr "
                 "LEFT JOIN scraper.gmaps_listings gl ON gl.source_url = sr.payload->>'url' "
                 "WHERE sr.payload->>'url' IS NOT NULL AND gl.source_url IS NULL "
                 "ORDER BY 1) TO STDOUT;\" > input/uncrawled_urls.txt"],
                capture_output=True, text=True, timeout=60,
                cwd=str(task_runner.REPO_ROOT),
                env=env,
            )
            if result.returncode == 0:
                outfile = task_runner.REPO_ROOT / "input" / "uncrawled_urls.txt"
                url_count = len(outfile.read_text().strip().splitlines()) if outfile.exists() else 0
            else:
                raise HTTPException(status_code=500, detail=f"URL export failed: {result.stderr}")
        else:
            raise HTTPException(status_code=400, detail="No uncrawled URLs available")

    command = task_runner.build_crawl_command(body.config, body.instances)
    task = await task_runner.launch_task(
        task_type="crawl",
        command=command,
        config_path=body.config,
        instance_count=body.instances,
    )
    task.metadata = {"url_count": url_count}
    return task.to_dict()


@router.post("/pipeline", response_model=ScraperTask)
async def create_pipeline_task(
    body: TaskCreatePipeline,
    _user: str = Depends(verify_token),
):
    command = task_runner.build_pipeline_command(
        generate_only=body.generate_only,
        crawl_only=body.crawl_only,
        instances=body.instances,
    )
    task = await task_runner.launch_task(
        task_type="pipeline",
        command=command,
        instance_count=body.instances,
    )
    return task.to_dict()


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


@router.post("/{task_id}/restart", response_model=ScraperTask)
async def restart_task(
    task_id: str,
    _user: str = Depends(verify_token),
):
    old_data = await pg_service.get_task(task_id)
    if not old_data:
        raise HTTPException(status_code=404, detail="Task not found")

    tt = old_data["type"]
    if tt == "search":
        command = task_runner.build_search_command(
            old_data.get("config_path") or "config/gmaps_bd_business_search.yaml",
            old_data.get("query"),
        )
    elif tt == "crawl":
        command = task_runner.build_crawl_command(
            old_data.get("config_path") or "config/gmaps_listings_file_input.yaml",
            old_data.get("instance_count", 3),
        )
    elif tt == "pipeline":
        command = task_runner.build_pipeline_command()
    else:
        raise HTTPException(status_code=400, detail=f"Unknown task type: {tt}")

    task = await task_runner.launch_task(
        task_type=tt,
        command=command,
        config_path=old_data.get("config_path"),
        query=old_data.get("query"),
        instance_count=old_data.get("instance_count", 1),
    )
    return task.to_dict()