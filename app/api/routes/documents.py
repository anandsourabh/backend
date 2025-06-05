# app/api/routes/documents.py

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, Query
from sqlalchemy.orm import Session
from typing import List, Dict, Optional, Any

from app.models.schemas import DocumentUploadResponse, DocumentSearchRequest, DocumentListResponse
from app.services.document_service import DocumentService
from app.core.dependencies import get_db, get_company_number, get_user_id
from app.utils.logging import logger

router = APIRouter()

@router.post("/documents/upload", response_model=DocumentUploadResponse)
async def upload_document(
    file: UploadFile = File(...),
    document_type: str = Form("general", description="Type: 'company_specific' or 'general'"),
    company_number: Optional[str] = Depends(get_company_number),
    user_id: str = Depends(get_user_id),
    db: Session = Depends(get_db)
):
    """
    Upload a PDF or text document for vector search.
    
    - **file**: PDF or text file to upload (max 10MB)
    - **document_type**: 'company_specific' (tied to company_number) or 'general' (available to all)
    """
    try:
        # Validate document type
        if document_type not in ["company_specific", "general"]:
            raise HTTPException(
                status_code=400, 
                detail="document_type must be 'company_specific' or 'general'"
            )
        
        # For company-specific documents, ensure company_number is provided
        if document_type == "company_specific" and not company_number:
            raise HTTPException(
                status_code=400,
                detail="company_number is required for company_specific documents"
            )
        
        # Set company_number to None for general documents
        doc_company_number = company_number if document_type == "company_specific" else None
        
        document_service = DocumentService()
        result = await document_service.upload_document(
            file=file,
            company_number=doc_company_number,
            document_type=document_type,
            user_id=user_id,
            db=db
        )
        
        return DocumentUploadResponse(**result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Document upload error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error uploading document: {str(e)}")

@router.get("/documents", response_model=List[DocumentListResponse])
async def list_documents(
    company_number: Optional[str] = Depends(get_company_number),
    user_id: str = Depends(get_user_id),
    db: Session = Depends(get_db),
    show_all: bool = Query(False, description="Show all documents (admin only)")
):
    """
    List uploaded documents.
    
    Returns company-specific documents for the user's company and all general documents.
    """
    try:
        document_service = DocumentService()
        
        # Filter by user unless show_all is requested (you might want to add admin check here)
        filter_user_id = None if show_all else user_id
        
        documents = document_service.list_documents(
            company_number=company_number,
            user_id=filter_user_id,
            db=db
        )
        
        return [DocumentListResponse(**doc) for doc in documents]
        
    except Exception as e:
        logger.error(f"Document listing error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error listing documents: {str(e)}")

@router.delete("/documents/{doc_id}")
async def delete_document(
    doc_id: str,
    user_id: str = Depends(get_user_id),
    db: Session = Depends(get_db)
):
    """
    Delete a document and its associated vectors.
    
    Note: Users can only delete their own documents unless they have admin privileges.
    """
    try:
        # Check if user owns the document (you might want to add admin override)
        from sqlalchemy import text
        result = db.execute(
            text("SELECT user_id FROM document_metadata WHERE doc_id = :doc_id"),
            {"doc_id": doc_id}
        )
        row = result.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail="Document not found")
        
        if row[0] != user_id:
            raise HTTPException(status_code=403, detail="Permission denied")
        
        document_service = DocumentService()
        success = document_service.delete_document(doc_id, db)
        
        if not success:
            raise HTTPException(status_code=404, detail="Document not found")
        
        return {"success": True, "message": "Document deleted successfully"}
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Document deletion error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error deleting document: {str(e)}")

@router.post("/documents/search")
async def search_documents(
    request: DocumentSearchRequest,
    company_number: Optional[str] = Depends(get_company_number),
    user_id: str = Depends(get_user_id)
):
    """
    Search documents using vector similarity.
    
    Returns relevant document chunks based on the search query.
    """
    try:
        document_service = DocumentService()
        
        results = await document_service.search_documents(
            query=request.query,
            company_number=company_number,
            top_k=request.top_k,
            similarity_threshold=request.similarity_threshold
        )
        
        return {
            "query": request.query,
            "results": results,
            "total_found": len(results)
        }
        
    except Exception as e:
        logger.error(f"Document search error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error searching documents: {str(e)}")

@router.get("/documents/stats")
async def get_document_stats(
    company_number: Optional[str] = Depends(get_company_number),
    user_id: str = Depends(get_user_id),
    db: Session = Depends(get_db)
):
    """
    Get statistics about uploaded documents.
    """
    try:
        from sqlalchemy import text
        
        # Get stats for user's company
        result = db.execute(
            text("""
                SELECT 
                    document_type,
                    COUNT(*) as doc_count,
                    SUM(chunk_count) as total_chunks,
                    SUM(file_size) as total_size
                FROM document_metadata
                WHERE (company_number = :company_number OR document_type = 'general')
                GROUP BY document_type
            """),
            {"company_number": company_number}
        )
        
        stats = {}
        total_docs = 0
        total_chunks = 0
        total_size = 0
        
        for row in result.mappings():
            doc_type = row["document_type"]
            stats[doc_type] = {
                "document_count": row["doc_count"],
                "chunk_count": row["total_chunks"],
                "total_size_bytes": row["total_size"]
            }
            total_docs += row["doc_count"]
            total_chunks += row["total_chunks"] or 0
            total_size += row["total_size"] or 0
        
        return {
            "by_type": stats,
            "totals": {
                "total_documents": total_docs,
                "total_chunks": total_chunks,
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2)
            }
        }
        
    except Exception as e:
        logger.error(f"Document stats error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting document stats: {str(e)}")