from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.database import create_tables
from app.api.routes import query, history, bookmarks, stats, documents
from app.config.settings import settings
from app.utils.logging import logger

def create_app() -> FastAPI:
    """Create and configure FastAPI application"""
    
    app = FastAPI(title="Blue[i] Property Gen BI Backend")

    # CORS configuration
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include routers
    app.include_router(query.router, prefix="/api", tags=["query"])
    app.include_router(history.router, prefix="/api", tags=["history"])
    app.include_router(bookmarks.router, prefix="/api", tags=["bookmarks"])
    app.include_router(stats.router, prefix="/api", tags=["stats"])
    app.include_router(documents.router, prefix="/api", tags=["documents"])

    @app.on_event("startup")
    async def startup_event():
        """Initialize database on startup"""
        create_tables()
        logger.info("Application started successfully")

    @app.get("/health")
    async def health_check():
        """Health check endpoint"""
        return {"status": "healthy"}

    return app

app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)