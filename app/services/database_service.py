import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.orm import Session
from fastapi import HTTPException
from app.core.database import engine
from app.utils.logging import logger
from typing import List, Dict

class DatabaseService:
    @staticmethod
    def execute_query(sql_query: str, company_number: str) -> pd.DataFrame:
        """Execute SQL query in read-only transaction and return results as DataFrame"""
        try:
            with engine.connect() as connection:
                with connection.begin() as tx:
                    connection.execute(text("SET TRANSACTION READ ONLY"))
                    result = connection.execute(
                        text(sql_query), {"company_number": company_number}
                    )
                    data = [dict(row) for row in result.mappings()]
                    tx.rollback()  # Ensure no changes are committed
                    return pd.DataFrame(data)

        except Exception as e:
            logger.error(f"Query execution error: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Error executing query: {str(e)}"
            )

    @staticmethod
    def get_company_data(company_number: str) -> pd.DataFrame:
        """Get all data for a company for insights generation"""
        try:
            sql_query = "SELECT * FROM ux_all_info_consolidated WHERE company_number = :company_number LIMIT 5000"

            with engine.connect() as connection:
                with connection.begin() as tx:
                    connection.execute(text("SET TRANSACTION READ ONLY"))
                    result = connection.execute(
                        text(sql_query), {"company_number": company_number}
                    )
                    data = [dict(row) for row in result.mappings()]
                    tx.rollback()
                    return pd.DataFrame(data)

        except Exception as e:
            logger.error(f"Company data retrieval error: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Error retrieving company data: {str(e)}"
            )

    @staticmethod
    def save_chat_history(db: Session, query_id: str, question: str, sql_query: str,
                          response_type: str, company_number: str, user_id: str):
        """Save query to chat history"""
        try:
            db.execute(
                text("""
                    INSERT INTO chat_history (query_id, question, sql_query, response_type,
                                            company_number, user_id, timestamp)
                    VALUES (:query_id, :question, :sql_query, :response_type,
                           :company_number, :user_id, :timestamp)
                """),
                {
                    "query_id": query_id,
                    "question": question,
                    "sql_query": sql_query,
                    "response_type": response_type,
                    "company_number": company_number,
                    "user_id": user_id,
                    "timestamp": datetime.utcnow()
                },
            )
            db.commit()

        except Exception as e:
            logger.error(f"Chat history save error: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Error saving chat history: {str(e)}"
            )

    @staticmethod
    def get_chat_history(db: Session, company_number: str, user_id: str) -> List[Dict]:
        """Retrieve chat history for specific user and company"""
        try:
            result = db.execute(
                text("""SELECT query_id, question, sql_query, response_type, timestamp
                       FROM chat_history
                       WHERE company_number = :company_number AND user_id = :user_id
                       ORDER BY timestamp DESC"""),
                {"company_number": company_number, "user_id": user_id}
            )
            return [dict(row) for row in result.mappings()]

        except Exception as e:
            logger.error(f"Chat history retrieval error: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Error retrieving history: {str(e)}"
            )
