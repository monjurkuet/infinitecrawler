"""Pydantic schemas for premium auth."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, EmailStr, Field


class RegisterIn(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=72)


class LoginIn(BaseModel):
    email: EmailStr
    password: str


class TokenOut(BaseModel):
    token: str
    user: "UserOut"


class UserOut(BaseModel):
    id: int
    email: str
    entitlement: dict[str, Any]
    created_at: datetime
    searches_run: int
    rows_exported: int

    class Config:
        from_attributes = True


class MeOut(UserOut):
    pass


class ChangePasswordIn(BaseModel):
    current_password: str
    new_password: str = Field(min_length=8, max_length=72)
