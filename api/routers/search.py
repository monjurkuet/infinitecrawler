"""Search results router."""

from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import verify_token
from api.models.models import (
    PaginatedSearchResults,
    SearchResult,
    SearchResultStats,
)
from api.services import pg_service

router = APIRouter(prefix="/api/search-results", tags=["search-results"])


@router.get("", response_model=PaginatedSearchResults)
async def list_search_results(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    source_type: str | None = Query(None),
    _user: str = Depends(verify_token),
):
    results, total = await pg_service.query_search_results(limit, offset, source_type)
    return PaginatedSearchResults(
        results=[SearchResult(**r) for r in results],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/stats", response_model=SearchResultStats)
async def search_result_stats(_user: str = Depends(verify_token)):
    stats = await pg_service.get_search_result_stats()
    return SearchResultStats(**stats)


@router.get("/{result_id}", response_model=SearchResult)
async def get_search_result(
    result_id: int,
    _user: str = Depends(verify_token),
):
    result = await pg_service.get_search_result_by_id(result_id)
    if not result:
        raise HTTPException(status_code=404, detail="Search result not found")
    return SearchResult(**result)