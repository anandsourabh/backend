import logging
import sys
from typing import Optional

def setup_logging(level: str = "INFO") -> logging.Logger:
    """Setup application logging configuration"""
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('app.log')
        ]
    )
    
    logger = logging.getLogger(__name__)
    return logger

logger = setup_logging()