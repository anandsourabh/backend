from pydantic import BaseModel
from typing import List, Dict, Optional, Any
from datetime import datetime

class QueryRequest(BaseModel):
    question: str
    visualization_type: Optional[str] = None

class QueryResponse(BaseModel):
    query_id: str
    question: str
    sql_query: Optional[str] = None
    explanation: str
    summary: Optional[str] = None
    data: Optional[List[Dict[str, Any]]] = None
    visualization: Optional[Dict[str, str]] = None
    timestamp: datetime
    response_type: str

class ChatHistory(BaseModel):
    query_id: str
    question: str
    sql_query: Optional[str] = None
    response_type: str
    timestamp: datetime

class BookmarkRequest(BaseModel):
    query_id: str
    question: str

class FeedbackRequest(BaseModel):
    query_id: str
    rating: int
    feedback: Optional[str] = ""
    helpful: bool = True

class QueryClassification(BaseModel):
    category: str
    is_safe: bool
    confidence: float
    reasoning: str