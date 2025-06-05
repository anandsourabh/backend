# app/models/schemas.py

from pydantic import BaseModel
from typing import List, Dict, Optional, Any
from datetime import datetime

# ==========================================
# EXISTING SCHEMAS (Original Functionality)
# ==========================================

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

# ==========================================
# NEW DOCUMENT MANAGEMENT SCHEMAS
# ==========================================

class DocumentUploadResponse(BaseModel):
    """Response schema for document upload endpoint"""
    doc_id: str
    filename: str
    chunk_count: int
    document_type: str
    status: str

class DocumentSearchRequest(BaseModel):
    """Request schema for document search endpoint"""
    query: str
    top_k: int = 5
    similarity_threshold: float = 0.7

class DocumentSearchResult(BaseModel):
    """Individual search result item"""
    score: float
    metadata: Dict[str, Any]
    chunk_text: str

class DocumentSearchResponse(BaseModel):
    """Complete response for document search endpoint"""
    query: str
    results: List[DocumentSearchResult]
    total_found: int

class DocumentListResponse(BaseModel):
    """Response schema for listing documents"""
    doc_id: str
    filename: str
    company_number: Optional[str]
    document_type: str
    user_id: str
    chunk_count: int
    file_size: int
    upload_timestamp: Optional[str]

class DocumentDeleteResponse(BaseModel):
    """Response schema for document deletion"""
    success: bool
    message: str

class DocumentTypeStats(BaseModel):
    """Statistics for a specific document type"""
    document_count: int
    chunk_count: int
    total_size_bytes: int

class DocumentTotalStats(BaseModel):
    """Total statistics across all document types"""
    total_documents: int
    total_chunks: int
    total_size_bytes: int
    total_size_mb: float

class DocumentStatsResponse(BaseModel):
    """Complete response for document statistics endpoint"""
    by_type: Dict[str, DocumentTypeStats]
    totals: DocumentTotalStats

# ==========================================
# ERROR AND UTILITY SCHEMAS
# ==========================================

class ErrorResponse(BaseModel):
    """Standard error response schema"""
    detail: str
    error_code: Optional[str] = None
    timestamp: Optional[datetime] = None

class SuccessResponse(BaseModel):
    """Standard success response schema"""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None

# ==========================================
# INTERNAL TYPE HINT SCHEMAS
# ==========================================

class DocumentChunkMetadata(BaseModel):
    """Metadata for document chunks (internal use)"""
    doc_id: str
    chunk_index: int
    company_number: Optional[str]
    document_type: str
    token_count: int

class VectorSearchConfig(BaseModel):
    """Configuration for vector search operations"""
    similarity_threshold: float = 0.7
    max_results: int = 5
    include_metadata: bool = True
    company_filter: Optional[str] = None

class DocumentProcessingStatus(BaseModel):
    """Status of document processing operations"""
    doc_id: str
    status: str  # "processing", "completed", "failed"
    progress: float  # 0.0 to 1.0
    message: Optional[str] = None
    chunks_processed: Optional[int] = None
    total_chunks: Optional[int] = None

# ==========================================
# VALIDATION SCHEMAS
# ==========================================

class DocumentUploadValidation(BaseModel):
    """Validation schema for document uploads"""
    filename: str
    file_size: int
    content_type: str
    document_type: str
    company_number: Optional[str] = None

class VectorSearchValidation(BaseModel):
    """Validation schema for vector search requests"""
    query: str
    company_number: Optional[str] = None
    user_id: str
    search_config: VectorSearchConfig

# ==========================================
# CONFIGURATION SCHEMAS
# ==========================================

class DocumentServiceConfig(BaseModel):
    """Configuration for the document service"""
    max_file_size_mb: int = 10
    chunk_size: int = 1000
    chunk_overlap: int = 200
    supported_file_types: List[str] = ["pdf", "txt", "md"]
    vector_store_path: str = "data/vector_store"
    embedding_model: str = "text-embedding-ada-002"

class APIHealthResponse(BaseModel):
    """Health check response schema"""
    status: str
    timestamp: datetime
    version: Optional[str] = None
    services: Optional[Dict[str, str]] = None