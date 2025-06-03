from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from datetime import datetime
from typing import List, Dict

from app.models.schemas import BookmarkRequest
from app.core.dependencies import get_db, get_company_number, get_user_id
from app.utils.logging import logger

router = APIRouter()

@router.post("/bookmark")
async def bookmark_query(
    request: BookmarkRequest,
    company_number: str = Depends(get_company_number),
    user_id: str = Depends(get_user_id),
    db: Session = Depends(get_db)
):
    """Bookmark a query for later use"""
    try:
        # Insert bookmark
        db.execute(
            text("""
                INSERT INTO bookmarked_queries (query_id, question, company_number, user_id, created_timestamp)
                VALUES (:query_id, :question, :company_number, :user_id, :timestamp)
                ON CONFLICT (query_id, user_id) DO UPDATE SET
                question = EXCLUDED.question,
                created_timestamp = EXCLUDED.created_timestamp
            """),
            {
                "query_id": request.query_id,
                "question": request.question,
                "company_number": company_number,
                "user_id": user_id,
                "timestamp": datetime.utcnow()
            }
        )
        db.commit()

        return {"success": True, "message": "Query bookmarked successfully"}

    except Exception as e:
        logger.error(f"Bookmark error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error bookmarking query: {str(e)}")

@router.get("/bookmarks")
async def get_bookmarks(
    company_number: str = Depends(get_company_number),
    user_id: str = Depends(get_user_id),
    db: Session = Depends(get_db)
):
    """Get user's bookmarked queries"""
    try:
        result = db.execute(
            text("""
                SELECT query_id, question, created_timestamp
                FROM bookmarked_queries
                WHERE company_number = :company_number AND user_id = :user_id
                ORDER BY created_timestamp DESC
                LIMIT 50
            """),
            {"company_number": company_number, "user_id": user_id}
        )

        bookmarks = [dict(row) for row in result.mappings()]
        return bookmarks

    except Exception as e:
        logger.error(f"Bookmarks retrieval error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error retrieving bookmarks: {str(e)}")

@router.delete("/bookmark/{query_id}")
async def remove_bookmark(
    query_id: str,
    company_number: str = Depends(get_company_number),
    user_id: str = Depends(get_user_id),
    db: Session = Depends(get_db)
):
    """Remove a bookmarked query"""
    try:
        db.execute(
            text("""
                DELETE FROM bookmarked_queries
                WHERE query_id = :query_id AND company_number = :company_number AND user_id = :user_id
            """),
            {
                "query_id": query_id,
                "company_number": company_number,
                "user_id": user_id
            }
        )
        db.commit()

        return {"success": True, "message": "Bookmark removed successfully"}

    except Exception as e:
        logger.error(f"Bookmark removal error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error removing bookmark: {str(e)}")