"""System router — log viewer."""

from fastapi import APIRouter, Depends, Query
from fastapi.responses import PlainTextResponse

from api.dependencies import verify_token
from api.services import config_loader

router = APIRouter(prefix="/api", tags=["system"])


@router.get("/logs")
async def get_logs(
    tail: int = Query(100, ge=1, le=1000),
    filter: str | None = Query(None),
    _user: str = Depends(verify_token),
):
    lines = config_loader.get_log_lines(tail, filter)
    return PlainTextResponse("\n".join(lines), media_type="text/plain")


@router.get("/logs/{crawler_name}")
async def get_crawler_log(
    crawler_name: str,
    tail: int = Query(100, ge=1, le=1000),
    _user: str = Depends(verify_token),
):
    content = config_loader.get_crawler_log(crawler_name, tail)
    if content is None:
        return PlainTextResponse("Log not found", status_code=404)
    return PlainTextResponse(content, media_type="text/plain")