"""Configs router — manage scraper YAML configs."""

from fastapi import APIRouter, Body, Depends, HTTPException, status

from api.dependencies import verify_token
from api.services import config_loader

router = APIRouter(prefix="/api/configs", tags=["configs"])


@router.get("")
async def list_configs(_user: str = Depends(verify_token)):
    return config_loader.list_configs()


@router.get("/{name}")
async def get_config(name: str, _user: str = Depends(verify_token)):
    config = config_loader.get_config(name)
    if config is None:
        raise HTTPException(status_code=404, detail="Config not found")
    return config


@router.post("/{name}", status_code=status.HTTP_201_CREATED)
async def create_config(
    name: str,
    body: dict = Body(...),
    _user: str = Depends(verify_token),
):
    success = config_loader.write_config(name, body)
    if not success:
        raise HTTPException(status_code=409, detail="Config already exists, use PUT to update")
    return {"status": "created", "name": name}


@router.put("/{name}")
async def update_config(
    name: str,
    body: dict = Body(...),
    _user: str = Depends(verify_token),
):
    success = config_loader.update_config(name, body)
    if not success:
        raise HTTPException(status_code=404, detail="Config not found")
    return {"status": "updated", "name": name}


@router.delete("/{name}")
async def delete_config(name: str, _user: str = Depends(verify_token)):
    success = config_loader.delete_config(name)
    if not success:
        raise HTTPException(status_code=404, detail="Config not found")
    return {"status": "deleted", "name": name}