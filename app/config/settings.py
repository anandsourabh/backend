from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Database settings
    database_url: str = "postgresql://SVC-DEV-EXP-INT-APP:E1$++iSP$hym&R!@ip-10-237-133-81.ec2.internal:5635/exposures"
    
    # OpenAI settings
    openai_api_key: str = "Cg5Gvh5qnhAmV9qGwhJsUJLNlAKZW4bL29rz9FiwRpvGju06"
    openai_api_base: str = "https://stg1.mmc-dallas-int-non-prod-ingress.mgti.mmc.com/coreapi/openai/v1"
    openai_api_version: str = "2015-05-15"
    openai_api_type: str = "azure"
    openai_engine: str = "mmc-tech-gpt-4o-mini-128k-2024-07-18"
    
    # Schema settings
    schema_file_path: str = "schema.json"
    
    # CORS settings
    cors_origins: list = ["http://localhost:4200"]
    
    # Database pool settings
    db_pool_size: int = 20
    db_max_overflow: int = 10
    db_pool_timeout: int = 30
    
    # OpenAI retry settings
    openai_max_retries: int = 3
    openai_retry_delay: float = 1.0
    
    class Config:
        env_file = ".env"

settings = Settings()