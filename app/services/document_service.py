# app/services/document_service.py

import os
import uuid
import json
import pickle
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime
import PyPDF2
import faiss
import numpy as np
from pathlib import Path
from sqlalchemy.orm import Session
from sqlalchemy import text
from fastapi import HTTPException, UploadFile
import tiktoken

from app.services.openai_service import OpenAIService
from app.utils.logging import logger
from app.core.database import engine

class DocumentChunk:
    """Represents a chunk of document text with metadata"""
    def __init__(self, text: str, metadata: Dict[str, Any]):
        self.text = text
        self.metadata = metadata
        self.embedding: Optional[np.ndarray] = None

class DocumentService:
    """Service for handling document upload, processing, and vector search"""
    
    def __init__(self):
        self.openai_service = OpenAIService()
        self.vector_store_path = "data/vector_store"
        self.chunk_size = 1000
        self.chunk_overlap = 200
        self.max_file_size = 50 * 1024 * 1024  # 10MB
        
        # Initialize storage directories
        self._ensure_directories()
        
        # Initialize tokenizer for text chunking
        self.tokenizer = tiktoken.get_encoding("cl100k_base")
        
    def _ensure_directories(self):
        """Ensure required directories exist"""
        Path(self.vector_store_path).mkdir(parents=True, exist_ok=True)
        Path("data/uploaded_files").mkdir(parents=True, exist_ok=True)
        
    async def upload_document(
        self, 
        file: UploadFile, 
        company_number: Optional[str] = None,
        document_type: str = "general",
        user_id: str = "",
        db: Session = None
    ) -> Dict[str, Any]:
        """Upload and process a document"""
        try:
            # Validate file
            if file.size > self.max_file_size:
                raise HTTPException(status_code=400, detail="File too large")
            
            # Generate unique document ID
            doc_id = str(uuid.uuid4())
            
            # Read file content
            content = await file.read()
            
            # Extract text based on file type
            if file.filename.lower().endswith('.pdf'):
                text_content = self._extract_pdf_text(content)
            elif file.filename.lower().endswith(('.txt', '.md')):
                text_content = content.decode('utf-8')
            else:
                raise HTTPException(status_code=400, detail="Unsupported file type")
            
            if not text_content.strip():
                raise HTTPException(status_code=400, detail="No text content found in file")
            
            # Chunk the text
            chunks = self._chunk_text(text_content, doc_id, company_number, document_type)
            
            # Generate embeddings for chunks
            embeddings = await self._generate_embeddings([chunk.text for chunk in chunks])
            
            # Assign embeddings to chunks
            for chunk, embedding in zip(chunks, embeddings):
                chunk.embedding = embedding
            
            # Store in vector database
            vector_ids = self._store_in_vector_db(chunks)
            
            # Save document metadata to database
            doc_metadata = {
                "doc_id": doc_id,
                "filename": file.filename,
                "company_number": company_number,
                "document_type": document_type,
                "user_id": user_id,
                "chunk_count": len(chunks),
                "vector_ids": vector_ids,
                "file_size": file.size,
                "upload_timestamp": datetime.utcnow()
            }
            
            self._save_document_metadata(db, doc_metadata)
            
            logger.info(f"Document {doc_id} uploaded successfully with {len(chunks)} chunks")
            
            return {
                "doc_id": doc_id,
                "filename": file.filename,
                "chunk_count": len(chunks),
                "document_type": document_type,
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error uploading document: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error processing document: {str(e)}")
    
    def _extract_pdf_text(self, content: bytes) -> str:
        """Extract text from PDF content"""
        try:
            from io import BytesIO
            pdf_reader = PyPDF2.PdfReader(BytesIO(content))
            text = ""
            for page in pdf_reader.pages:
                text += page.extract_text() + "\n"
            return text
        except Exception as e:
            logger.error(f"Error extracting PDF text: {str(e)}")
            raise HTTPException(status_code=400, detail="Error reading PDF file")
    
    def _chunk_text(
        self, 
        text: str, 
        doc_id: str, 
        company_number: Optional[str], 
        document_type: str
    ) -> List[DocumentChunk]:
        """Split text into chunks with overlap"""
        chunks = []
        
        # Tokenize the text
        tokens = self.tokenizer.encode(text)
        
        # Split into chunks
        for i in range(0, len(tokens), self.chunk_size - self.chunk_overlap):
            chunk_tokens = tokens[i:i + self.chunk_size]
            chunk_text = self.tokenizer.decode(chunk_tokens)
            
            # Create metadata for the chunk
            metadata = {
                "doc_id": doc_id,
                "chunk_index": len(chunks),
                "company_number": company_number,
                "document_type": document_type,
                "token_count": len(chunk_tokens)
            }
            
            chunks.append(DocumentChunk(chunk_text, metadata))
        
        return chunks
    
    async def _generate_embeddings(self, texts: List[str]) -> List[np.ndarray]:
        """Generate embeddings for text chunks using OpenAI"""
        embeddings = []
        
        # Process in batches to avoid rate limits
        batch_size = 20
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            
            try:
                # Use OpenAI embeddings
                import openai
                response = openai.Embedding.create(
                    input=batch,
                    model="text-embedding-ada-002",
                    deployment_id="mmc-tech-text-embedding-ada-002"
                )
                
                batch_embeddings = [np.array(item['embedding'], dtype=np.float32) 
                                  for item in response['data']]
                logger.info("Complete generating embedding from Open AI")
                embeddings.extend(batch_embeddings)
                
            except Exception as e:
                logger.error(f"Error generating embeddings: {str(e)}")
                raise HTTPException(status_code=500, detail="Error generating embeddings")
        
        return embeddings
    
    def _store_in_vector_db(self, chunks: List[DocumentChunk]) -> List[int]:
        """Store chunks in FAISS vector database"""
        try:
            # Load existing index or create new one
            index_path = os.path.join(self.vector_store_path, "faiss.index")
            metadata_path = os.path.join(self.vector_store_path, "metadata.pkl")
            
            if os.path.exists(index_path):
                index = faiss.read_index(index_path)
                with open(metadata_path, 'rb') as f:
                    metadata_list = pickle.load(f)
            else:
                # Create new index (assuming 1536 dimensions for OpenAI embeddings)
                index = faiss.IndexFlatIP(1536)  # Inner product similarity
                metadata_list = []
            
            # Prepare embeddings and metadata
            embeddings = np.array([chunk.embedding for chunk in chunks])
            
            # Normalize embeddings for cosine similarity
            faiss.normalize_L2(embeddings)
            
            # Add to index
            start_id = index.ntotal
            index.add(embeddings)
            
            # Store metadata
            for i, chunk in enumerate(chunks):
                metadata_list.append({
                    "vector_id": start_id + i,
                    **chunk.metadata
                })
            
            # Save index and metadata
            faiss.write_index(index, index_path)
            with open(metadata_path, 'wb') as f:
                pickle.dump(metadata_list, f)
            
            vector_ids = list(range(start_id, start_id + len(chunks)))
            
            logger.info(f"Stored {len(chunks)} chunks in vector database")
            return vector_ids
            
        except Exception as e:
            logger.error(f"Error storing in vector database: {str(e)}")
            raise HTTPException(status_code=500, detail="Error storing in vector database")
    
    def _save_document_metadata(self, db: Session, metadata: Dict[str, Any]):
        """Save document metadata to database"""
        try:
            db.execute(
                text("""
                    INSERT INTO document_metadata 
                    (doc_id, filename, company_number, document_type, user_id, 
                     chunk_count, vector_ids, file_size, upload_timestamp)
                    VALUES (:doc_id, :filename, :company_number, :document_type, :user_id,
                           :chunk_count, :vector_ids, :file_size, :upload_timestamp)
                """),
                {
                    **metadata,
                    "vector_ids": json.dumps(metadata["vector_ids"])
                }
            )
            db.commit()
            
        except Exception as e:
            logger.error(f"Error saving document metadata: {str(e)}")
            raise HTTPException(status_code=500, detail="Error saving document metadata")
    
    async def search_documents(
        self, 
        query: str, 
        company_number: Optional[str] = None,
        top_k: int = 5,
        similarity_threshold: float = 0.7
    ) -> List[Dict[str, Any]]:
        """Search for relevant document chunks"""
        try:
            # Generate query embedding
            query_embeddings = await self._generate_embeddings([query])
            query_embedding = query_embeddings[0]
            
            # Load index and metadata
            index_path = os.path.join(self.vector_store_path, "faiss.index")
            metadata_path = os.path.join(self.vector_store_path, "metadata.pkl")
            
            if not os.path.exists(index_path):
                logger.info("No vector index found")
                return []
            
            index = faiss.read_index(index_path)
            with open(metadata_path, 'rb') as f:
                metadata_list = pickle.load(f)
            
            # Normalize query embedding
            query_embedding = query_embedding.reshape(1, -1)
            faiss.normalize_L2(query_embedding)
            
            # Search
            scores, indices = index.search(query_embedding, min(top_k * 3, index.ntotal))
            
            # Filter results
            results = []
            for score, idx in zip(scores[0], indices[0]):
                if idx == -1 or score < similarity_threshold:
                    continue
                
                metadata = metadata_list[idx]
                
                # Filter by company if specified
                if company_number:
                    # Include general documents and company-specific documents
                    if (metadata.get("company_number") != company_number and 
                        metadata.get("document_type") != "general"):
                        continue
                
                # Reconstruct chunk text (this is simplified - in production you might store chunks separately)
                results.append({
                    "score": float(score),
                    "metadata": metadata,
                    "chunk_text": f"Document chunk from {metadata.get('doc_id', 'unknown')}"
                })
                
                if len(results) >= top_k:
                    break
            
            logger.info(f"Found {len(results)} relevant document chunks")
            return results
            
        except Exception as e:
            logger.error(f"Error searching documents: {str(e)}")
            return []
    
    def delete_document(self, doc_id: str, db: Session) -> bool:
        """Delete a document and its vectors"""
        try:
            # Get document metadata
            result = db.execute(
                text("SELECT vector_ids FROM document_metadata WHERE doc_id = :doc_id"),
                {"doc_id": doc_id}
            )
            row = result.fetchone()
            
            if not row:
                return False
            
            # Note: FAISS doesn't support efficient deletion
            # In production, you might want to use a different vector DB like Pinecone or Weaviate
            # For now, we'll just mark as deleted in metadata
            
            db.execute(
                text("DELETE FROM document_metadata WHERE doc_id = :doc_id"),
                {"doc_id": doc_id}
            )
            db.commit()
            
            logger.info(f"Document {doc_id} deleted")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting document: {str(e)}")
            return False
    
    def list_documents(
        self, 
        company_number: Optional[str] = None,
        user_id: Optional[str] = None,
        db: Session = None
    ) -> List[Dict[str, Any]]:
        """List documents with optional filtering"""
        try:
            query = """
                SELECT doc_id, filename, company_number, document_type, 
                       user_id, chunk_count, file_size, upload_timestamp
                FROM document_metadata
                WHERE 1=1
            """
            params = {}
            
            if company_number:
                query += " AND (company_number = :company_number OR document_type = 'general')"
                params["company_number"] = company_number
            
            if user_id:
                query += " AND user_id = :user_id"
                params["user_id"] = user_id
            
            query += " ORDER BY upload_timestamp DESC"
            
            result = db.execute(text(query), params)
            
            documents = []
            for row in result.mappings():
                doc = dict(row)
                doc["upload_timestamp"] = doc["upload_timestamp"].isoformat() if doc["upload_timestamp"] else None
                documents.append(doc)
            
            return documents
            
        except Exception as e:
            logger.error(f"Error listing documents: {str(e)}")
            return []

    async def augment_response_with_context(
        self, 
        question: str, 
        company_number: Optional[str] = None,
        max_context_chunks: int = 3
    ) -> str:
        """Search for relevant context and return formatted context string"""
        try:
            # Search for relevant documents
            relevant_chunks = await self.search_documents(
                query=question,
                company_number=company_number,
                top_k=max_context_chunks,
                similarity_threshold=0.6
            )
            
            if not relevant_chunks:
                return ""
            
            # Format context
            context_parts = []
            for i, chunk in enumerate(relevant_chunks):
                context_parts.append(
                    f"**Document Context {i+1}** (Score: {chunk['score']:.2f}):\n"
                    f"{chunk['chunk_text']}\n"
                )
            
            context_string = (
                "📚 **Relevant Information from Company Documents:**\n\n" +
                "\n".join(context_parts) +
                "\n---\n"
            )
            
            return context_string
            
        except Exception as e:
            logger.error(f"Error augmenting response with context: {str(e)}")
            return ""