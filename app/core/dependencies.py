
from fastapi import Depends, Header
from sqlalchemy.orm import Session
from app.core.database import SessionLocal
from app.utils.validators import validate_company_number, validate_user_id

def get_db():
    """Database dependency"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_company_number(company_number: str = Header(...)) -> str:
    """Company number dependency with validation"""
    return validate_company_number(company_number)

def get_user_id(user_id: str = Header(...)) -> str:
    """User ID dependency with validation"""
    return validate_user_id(user_id)