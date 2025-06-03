from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List

from app.models.schemas import ChatHistory
from app.services.database_service import DatabaseService
from app.core.dependencies import get_db, get_company_number, get_user_id

router = APIRouter()

@router.get("/history", response_model=List[ChatHistory])
async def get_history(
    company_number: str = Depends(get_company_number),
    user_id: str = Depends(get_user_id),
    db: Session = Depends(get_db)
):
    """Retrieve chat history for specific user and company"""
    database_service = DatabaseService()
    history = database_service.get_chat_history(db, company_number, user_id)
    return [ChatHistory(**item) for item in history]