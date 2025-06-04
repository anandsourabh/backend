import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.orm import Session
from fastapi import HTTPException
from app.core.database import engine
from app.utils.logging import logger
from app.utils.currency_utils import CurrencyFormatter
from typing import List, Dict, Optional

class DatabaseService:
    # Define monetary columns that need currency formatting
    MONETARY_COLUMNS = {
        'derived_total_insured_value', 'total_insured_value', 'derived_local_total_insured_value',
        'modelable_tiv', 'building_values', 'derived_building_values', 'content_values',
        'derived_content_values', 'local_content_values', 'total_content_values',
        'derived_total_content_values', 'business_income', 'derived_business_income',
        'business_interrupt_val', 'derived_business_interrupt_val', 'derived_business_interrupt_val_12mo',
        'revenue', 'derived_revenue', 'property_damage', 'derived_property_damage',
        'rental_income', 'derived_rental_income', 'invtry_or_stock_val_ctnt2',
        'derived_invtry_or_stock_val_ctnt2', 'local_invtry_or_stock_val_ctnt2',
        'machinery_equipment_ctnt3', 'derived_machinery_equipment_ctnt3',
        # Add computed/aggregated column patterns
        'total_tiv', 'sum_tiv', 'avg_tiv', 'average_tiv', 'total_value', 'sum_value',
        'total_insured', 'sum_insured', 'avg_value', 'average_value', 'total_revenue',
        'sum_revenue', 'avg_revenue', 'average_revenue', 'total_income', 'sum_income',
        'avg_income', 'average_income'
    }

    @staticmethod
    def get_currency_symbol(company_number: str) -> str:
        """Get currency symbol for the company"""
        try:
            with engine.connect() as connection:
                result = connection.execute(
                    text("""
                        SELECT SPLIT_PART(SUBSTRING(column_preferences FROM '"key":"([^"]+)"'), '-', 1) AS currency_code
                        FROM ux_app_preference
                        WHERE company_number = :company_number
                        LIMIT 1
                    """),
                    {"company_number": company_number}
                )
                row = result.fetchone()
                if row and row[0]:
                    currency_code = row[0].strip().upper()
                    logger.debug(f"Retrieved currency code '{currency_code}' for company {company_number}")
                    
                    # Convert currency code to symbol using CurrencyFormatter
                    symbol = CurrencyFormatter.get_currency_symbol(currency_code)
                    logger.debug(f"Converted currency code '{currency_code}' to symbol '{symbol}'")
                    return symbol
                else:
                    logger.debug(f"No currency preference found for company {company_number}, using default USD")
                    return "$"  # Default currency symbol
        except Exception as e:
            logger.error(f"Error retrieving currency symbol for company {company_number}: {str(e)}")
            return "$"  # Default fallback

    @staticmethod
    def format_currency_value(value, currency_symbol: str) -> str:
        """Format a numeric value with currency symbol"""
        return CurrencyFormatter.format_currency(value, currency_symbol)

    @staticmethod
    def apply_currency_formatting(df: pd.DataFrame, company_number: str) -> pd.DataFrame:
        """Apply currency formatting to monetary columns in DataFrame"""
        if df.empty:
            return df
        
        # Get currency symbol for the company
        currency_symbol = DatabaseService.get_currency_symbol(company_number)
        logger.debug(f"Applying currency formatting with symbol '{currency_symbol}' to {len(df.columns)} columns")
        
        # Create a copy to avoid modifying original
        formatted_df = df.copy()
        
        # Apply formatting to monetary columns that exist in the DataFrame
        monetary_columns_found = []
        for column in formatted_df.columns:
            if DatabaseService._is_monetary_column(column):
                monetary_columns_found.append(column)
                logger.debug(f"Formatting monetary column '{column}'")
                
                # Log sample values before formatting
                if len(formatted_df) > 0:
                    sample_before = formatted_df[column].iloc[0]
                    logger.debug(f"Sample value before formatting: {sample_before} (type: {type(sample_before)})")
                
                formatted_df[column] = formatted_df[column].apply(
                    lambda x: CurrencyFormatter.format_currency(x, currency_symbol)
                )
                
                # Log sample values after formatting
                if len(formatted_df) > 0:
                    sample_after = formatted_df[column].iloc[0]
                    logger.debug(f"Sample value after formatting: {sample_after}")
        
        logger.debug(f"Currency formatting applied to {len(monetary_columns_found)} monetary columns: {monetary_columns_found}")
        return formatted_df
    
    @staticmethod
    def _is_monetary_column(column_name: str) -> bool:
        """Check if a column should be treated as monetary (includes dynamic detection)"""
        column_lower = column_name.lower()
        
        # Check against known monetary columns
        if column_name in DatabaseService.MONETARY_COLUMNS:
            return True
        
        # Dynamic detection for computed columns
        monetary_patterns = [
            'tiv', 'value', 'insured', 'revenue', 'income', 'cost', 'amount',
            'price', 'payment', 'premium', 'limit', 'deductible', 'loss',
            'damage', 'content', 'building', 'business', 'rental', 'property'
        ]
        
        # Check if column name contains monetary patterns
        for pattern in monetary_patterns:
            if pattern in column_lower:
                # Additional checks to avoid false positives
                if not any(exclude in column_lower for exclude in ['id', 'code', 'type', 'name', 'description', 'flag']):
                    return True
        
        return False

    @staticmethod
    def execute_query(sql_query: str, company_number: str) -> pd.DataFrame:
        """Execute SQL query in read-only transaction and return results as DataFrame with currency formatting"""
        try:
            with engine.connect() as connection:
                with connection.begin() as tx:
                    connection.execute(text("SET TRANSACTION READ ONLY"))
                    result = connection.execute(
                        text(sql_query), {"company_number": company_number}
                    )
                    data = [dict(row) for row in result.mappings()]
                    tx.rollback()  # Ensure no changes are committed
                    
                    df = pd.DataFrame(data)
                    
                    # Apply currency formatting if DataFrame is not empty
                    if not df.empty:
                        logger.info(f"Query returned {len(df)} rows with columns: {list(df.columns)}")
                        df = DatabaseService.apply_currency_formatting(df, company_number)
                    else:
                        logger.info("Query returned no data")
                    
                    return df

        except Exception as e:
            logger.error(f"Query execution error: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Error executing query: {str(e)}"
            )

    @staticmethod
    def get_company_data(company_number: str) -> pd.DataFrame:
        """Get all data for a company for insights generation with currency formatting"""
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
                    
                    df = pd.DataFrame(data)
                    
                    # Apply currency formatting if DataFrame is not empty
                    if not df.empty:
                        df = DatabaseService.apply_currency_formatting(df, company_number)
                    
                    return df

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