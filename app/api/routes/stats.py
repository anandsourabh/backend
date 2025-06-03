from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from typing import List, Dict, Any

from app.models.schemas import FeedbackRequest
from app.core.dependencies import get_db, get_company_number, get_user_id
from app.utils.logging import logger  

router = APIRouter()

@router.get("/stats")
async def get_user_stats(
    company_number: str = Depends(get_company_number),
    user_id: str = Depends(get_user_id),
    db: Session = Depends(get_db)
):
    """Get user statistics and insights"""
    try:
        # Get query count by response type
        query_stats = db.execute(
            text("""
                SELECT response_type, COUNT(*) as count
                FROM chat_history
                WHERE company_number = :company_number AND user_id = :user_id
                AND timestamp >= :since
                GROUP BY response_type
            """),
            {
                "company_number": company_number,
                "user_id": user_id,
                "since": datetime.utcnow() - timedelta(days=30)
            }
        )

        # Get recent activity
        recent_activity = db.execute(
            text("""
                SELECT DATE(timestamp) as date, COUNT(*) as queries
                FROM chat_history
                WHERE company_number = :company_number AND user_id = :user_id
                AND timestamp >= :since
                GROUP BY DATE(timestamp)
                ORDER BY DATE(timestamp) DESC
                LIMIT 7
            """),
            {
                "company_number": company_number,
                "user_id": user_id,
                "since": datetime.utcnow() - timedelta(days=7)
            }
        )

        stats = {
            "query_types": [dict(row) for row in query_stats.mappings()],
            "recent_activity": [dict(row) for row in recent_activity.mappings()],
            "generated_at": datetime.utcnow()
        }

        return stats

    except Exception as e:
        logger.error(f"Stats retrieval error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error retrieving stats: {str(e)}")

@router.post("/feedback")
async def submit_feedback(
    request: FeedbackRequest,
    company_number: str = Depends(get_company_number),
    user_id: str = Depends(get_user_id),
    db: Session = Depends(get_db)
):
    """Submit user feedback on query results"""
    try:
        # Insert feedback
        db.execute(
            text("""
                INSERT INTO query_feedback (query_id, company_number, user_id, rating, feedback_text, helpful, created_timestamp)
                VALUES (:query_id, :company_number, :user_id, :rating, :feedback_text, :helpful, :timestamp)
                ON CONFLICT (query_id, user_id) DO UPDATE SET
                rating = EXCLUDED.rating,
                feedback_text = EXCLUDED.feedback_text,
                helpful = EXCLUDED.helpful,
                created_timestamp = EXCLUDED.created_timestamp
            """),
            {
                "query_id": request.query_id,
                "company_number": company_number,
                "user_id": user_id,
                "rating": request.rating,
                "feedback_text": request.feedback,
                "helpful": request.helpful,
                "timestamp": datetime.utcnow()
            }
        )
        db.commit()

        return {"success": True, "message": "Feedback submitted successfully"}

    except Exception as e:
        logger.error(f"Feedback submission error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error submitting feedback: {str(e)}")

@router.get("/suggestions")
async def get_query_suggestions(
    q: str = "",
    limit: int = 5,
    company_number: str = Depends(get_company_number),
    user_id: str = Depends(get_user_id)
):
    """Get query suggestions based on partial input"""
    try:
        if not q or len(q) < 2:
            return []

        suggestions = [
            "What is the total insured value by state?",
            "Show me properties with high earthquake risk",
            "List all buildings built after year 2000",
            "Properties in high flood zones",
            "Average TIV by construction type",
            "Map of all property locations",
            "Buildings without sprinkler systems",
            "Revenue distribution by business unit",
            "Properties with basement flood risk",
            "Construction quality analysis by region"
        ]

        # Filter suggestions based on query
        filtered_suggestions = [
            s for s in suggestions
            if q.lower() in s.lower()
        ]

        return filtered_suggestions[:limit]

    except Exception as e:
        logger.error(f"Suggestions error: {str(e)}")
        return []

@router.get("/schema")
async def get_schema():
    """Get database schema information for autocomplete"""
    try:
        # This would typically come from your schema.json file
        # For brevity, returning a simplified version
        schema_info = [
            {"name": "marsh_location_id", "type": "bigint", "description": "Unique Marsh identifier for the property location."},
            {"name": "company_number", "type": "string", "description": "Unique identifier for the company owning the property."},
            {"name": "location_name", "type": "string", "description": "Name or description of the location."},
            {"name": "address", "type": "string", "description": "Street address of the property."},
            {"name": "city", "type": "string", "description": "City where the property is located."},
            {"name": "state", "type": "string", "description": "State or province of the property."},
            {"name": "derived_country", "type": "string", "description": "Standardized 2-letter country code."},
            {"name": "latitude", "type": "string", "description": "Geographic latitude of the property."},
            {"name": "longitude", "type": "string", "description": "Geographic longitude of the property."},
            {"name": "derived_total_insured_value", "type": "numeric", "description": "Total insured value in USD."},
        ]

        return schema_info

    except Exception as e:
        logger.error(f"Schema retrieval error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error retrieving schema: {str(e)}")