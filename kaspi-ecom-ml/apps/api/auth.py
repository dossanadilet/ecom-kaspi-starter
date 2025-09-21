from __future__ import annotations

from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import OAuth2PasswordRequestForm

from .core.security import create_token


router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/token")
def login(form: OAuth2PasswordRequestForm = Depends()):
    """Very basic auth: any username/password accepted for demo.
    TODO: replace with real user store.
    """
    if not form.username:
        raise HTTPException(status_code=400, detail="username required")
    token = create_token(sub=form.username)
    return {"access_token": token, "token_type": "bearer"}

