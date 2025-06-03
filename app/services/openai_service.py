import time
import openai
from fastapi import HTTPException
from app.config.settings import settings
from app.utils.logging import logger

class OpenAIService:
    def __init__(self):
        openai.api_type = settings.openai_api_type
        openai.api_base = settings.openai_api_base
        openai.api_version = settings.openai_api_version
        openai.api_key = settings.openai_api_key

    def call_with_retry(self, prompt: str, max_retries: int = None, delay: float = None) -> str:
        """Generic OpenAI call with retry logic"""
        max_retries = max_retries or settings.openai_max_retries
        delay = delay or settings.openai_retry_delay

        for attempt in range(max_retries):
            try:
                logger.info(f"OpenAI call attempt {attempt + 1}/{max_retries}")

                response = openai.ChatCompletion.create(
                    engine=settings.openai_engine,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.3,
                )

                return response.choices[0].message.content.strip()

            except Exception as e:
                logger.warning(f"OpenAI call attempt {attempt + 1} failed: {str(e)}")

                if attempt == max_retries - 1:
                    logger.error(f"All {max_retries} OpenAI call attempts failed")
                    raise HTTPException(
                        status_code=502,
                        detail=f"OpenAI service unavailable after {max_retries} attempts: {str(e)}"
                    )

                # Exponential backoff
                time.sleep(delay * (2 ** attempt))

        raise HTTPException(status_code=502, detail="OpenAI service unavailable")