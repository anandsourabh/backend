from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from app.config.settings import settings
from app.utils.logging import logger

# Database setup with connection pooling
engine = create_engine(
    settings.database_url,
    poolclass=QueuePool,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_max_overflow,
    pool_timeout=settings.db_pool_timeout
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def create_tables():
    """Create necessary database tables"""
    with SessionLocal() as session:
        # Create chat history table
        session.execute(
            text("""
                CREATE TABLE IF NOT EXISTS chat_history (
                    query_id VARCHAR PRIMARY KEY,
                    question TEXT NOT NULL,
                    sql_query TEXT,
                    response_type VARCHAR NOT NULL,
                    company_number VARCHAR NOT NULL,
                    user_id VARCHAR NOT NULL,
                    timestamp TIMESTAMP NOT NULL
                )
            """)
        )

        # Create bookmarks table
        session.execute(
            text("""
                CREATE TABLE IF NOT EXISTS bookmarked_queries (
                    id SERIAL PRIMARY KEY,
                    query_id VARCHAR NOT NULL,
                    question TEXT NOT NULL,
                    company_number VARCHAR NOT NULL,
                    user_id VARCHAR NOT NULL,
                    created_timestamp TIMESTAMP NOT NULL,
                    UNIQUE(query_id, user_id)
                )
            """)
        )

        # Create feedback table
        session.execute(
            text("""
                CREATE TABLE IF NOT EXISTS query_feedback (
                    id SERIAL PRIMARY KEY,
                    query_id VARCHAR NOT NULL,
                    company_number VARCHAR NOT NULL,
                    user_id VARCHAR NOT NULL,
                    rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
                    feedback_text TEXT,
                    helpful BOOLEAN DEFAULT TRUE,
                    created_timestamp TIMESTAMP NOT NULL,
                    UNIQUE(query_id, user_id)
                )
            """)
        )

        # Create indexes
        session.execute(
            text("""
                CREATE INDEX IF NOT EXISTS idx_chat_history_user
                ON chat_history(company_number, user_id, timestamp DESC)
            """)
        )

        session.execute(
            text("""
                CREATE INDEX IF NOT EXISTS idx_bookmarks_user
                ON bookmarked_queries(company_number, user_id, created_timestamp DESC)
            """)
        )

        # Create document metadata table for vector search
        session.execute(
                text("""
                    CREATE TABLE IF NOT EXISTS document_metadata (
                        id SERIAL PRIMARY KEY,
                        doc_id VARCHAR UNIQUE NOT NULL,
                        filename VARCHAR NOT NULL,
                        company_number VARCHAR,
                        document_type VARCHAR NOT NULL CHECK (document_type IN ('company_specific', 'general')),
                        user_id VARCHAR NOT NULL,
                        chunk_count INTEGER NOT NULL,
                        vector_ids JSONB NOT NULL,
                        file_size BIGINT NOT NULL,
                        upload_timestamp TIMESTAMP NOT NULL,
                        created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            )
            
        # Create indexes for document metadata
        session.execute(
                text("""
                    CREATE INDEX IF NOT EXISTS idx_documents_user
                    ON document_metadata(company_number, user_id, upload_timestamp DESC)
                """)
            )

        session.execute(
                text("""
                    CREATE INDEX IF NOT EXISTS idx_documents_type
                    ON document_metadata(document_type, upload_timestamp DESC)
                """)
            )
            
        session.execute(
                text("""
                    CREATE INDEX IF NOT EXISTS idx_documents_doc_id
                    ON document_metadata(doc_id)
                """)
            )    

        session.commit()
        logger.info("Database tables created successfully")
