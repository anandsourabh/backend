import re
from fastapi import HTTPException

def validate_company_number(company_number: str) -> str:
    """Validate company number format"""
    if not re.match(r"^[a-zA-Z0-9_-]+$", company_number):
        raise HTTPException(status_code=400, detail="Invalid company_number format")
    return company_number

def validate_user_id(user_id: str) -> str:
    """Validate user ID format"""
    if not re.match(r"^[a-zA-Z0-9_-]+$", user_id):
        raise HTTPException(status_code=400, detail="Invalid user_id format")
    return user_id

def validate_query_safety(question: str) -> bool:
    """Check if query contains unsafe operations"""
    unsafe_keywords = ["drop", "delete", "union", ";", "--", "insert", "update", "create"]
    return not any(keyword in question.lower() for keyword in unsafe_keywords)