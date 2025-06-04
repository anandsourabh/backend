import uuid
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Dict, Any

from app.models.schemas import QueryRequest, QueryResponse
from app.services.query_processor import QueryProcessor
from app.services.database_service import DatabaseService
from app.services.visualization_rec_service import VisualizationService
from app.services.query_analyzer import QueryAnalyzer
from app.services.portfolio_dashboard_service import PortfolioDashboardService
from app.core.dependencies import get_db, get_company_number, get_user_id
from app.utils.logging import logger

router = APIRouter()

@router.post("/query", response_model=QueryResponse)
async def process_query(
    request: QueryRequest,
    company_number: str = Depends(get_company_number),
    user_id: str = Depends(get_user_id),
    db: Session = Depends(get_db),
):
    """Process text-to-SQL query with enhanced context handling and currency formatting"""
    try:
        query_processor = QueryProcessor()
        database_service = DatabaseService()
        visualization_service = VisualizationService()
        
        # Classify the question
        classification = query_processor.classify_question(request.question)

        if not classification.is_safe:
            raise HTTPException(
                status_code=400,
                detail="Question contains potentially harmful content and cannot be processed"
            )

        query_id = str(uuid.uuid4())
        response_type = classification.category

        logger.info(f"Question classified as: {response_type} with confidence: {classification.confidence}")

        if classification.category == "sql_convertible":
            return await _handle_sql_convertible(
                request, query_id, query_processor, database_service, visualization_service,
                company_number, user_id, db
            )
        elif classification.category == "property_risk_insurance":
            return await _handle_property_risk_insurance(
                request, query_id, query_processor, database_service, 
                company_number, user_id, db
            )
        elif classification.category == "data_insights":
            return await _handle_data_insights(
                request, query_id, query_processor, database_service, 
                company_number, user_id, db
            )
        elif classification.category == "portfolio_dashboard":
            return await _handle_portfolio_dashboard(
            request, query_id, database_service, company_number, user_id, db
            )
        else:
            return _handle_unrelated(request, query_id, database_service, company_number, user_id, db)

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Query processing error: {str(e)}")
        return _handle_processing_error(request, str(e))

async def _handle_sql_convertible(
    request, query_id, query_processor, database_service, visualization_service,
    company_number, user_id, db
):
    """Handle SQL convertible questions with currency formatting"""
    sql_query = query_processor.generate_sql(request.question, company_number)
    
    if not sql_query:
        explanation = _build_query_generation_failed_explanation(request.question)
        return QueryResponse(
            query_id=query_id,
            question=request.question,
            explanation=explanation,
            summary="I need help understanding your question - let's try a different approach",
            timestamp=datetime.utcnow(),
            response_type="query_generation_failed",
        )

    # Execute the query (currency formatting is now handled in DatabaseService)
    df = database_service.execute_query(sql_query, company_number)

    if df is None or df.empty:
        return _handle_no_data_response(
            request, query_id, sql_query, company_number, database_service, user_id, db
        )

    # Success path
    explanation, summary = query_processor.generate_explanation(request.question, sql_query)
    
    # Add currency context to explanation if monetary columns are involved
    explanation = _enhance_explanation_with_currency_context(
        explanation, df.columns.tolist(), company_number, database_service
    )    

    # Save to chat history
    database_service.save_chat_history(
        db, query_id, request.question, sql_query, "sql_convertible",
        company_number, user_id
    )

    visualization = visualization_service.recommend(sql_query, df)
    visualization_formatted = _parse_visualization_response(visualization)
    
    # ENHANCED: Handle single-value results
    if visualization == "None" and len(df) == 1 and len(df.columns) == 1:
        # Generate human-readable summary for single values
        value = df.iloc[0, 0]
        column_name = df.columns[0]
        
        # Create contextual summary based on the query
        human_readable_summary = _generate_single_value_summary(
            request.question,sql_query, column_name, value, company_number, database_service
        )
        
        query_response_data = {
            'query_id': query_id,
            'question': request.question,
            'sql_query': sql_query,
            'explanation': explanation,
            'summary': human_readable_summary,
            'visualization': None,  # Explicitly set to None
            'data': df.to_dict('records') if not df.empty else [],
            'timestamp': datetime.utcnow(),
            'response_type': "sql_convertible_single_value",  # New response type
        }
    elif visualization_formatted and "None" not in str(visualization_formatted.get("Chart Type", "")):
        # Regular visualization response
        query_response_data = {
            'query_id': query_id,
            'question': request.question,
            'sql_query': sql_query,
            'explanation': explanation,
            'summary': summary,
            'visualization': visualization_formatted,
            'data': df.to_dict('records') if not df.empty else [],
            'timestamp': datetime.utcnow(),
            'response_type': "sql_convertible",
        }
    else:
        # No visualization but multi-row data
        query_response_data = {
            'query_id': query_id,
            'question': request.question,
            'sql_query': sql_query,
            'explanation': explanation,
            'summary': summary,
            'visualization': None,
            'data': df.to_dict('records') if not df.empty else [],
            'timestamp': datetime.utcnow(),
            'response_type': "sql_convertible",
        }
    
    return QueryResponse(**query_response_data)

def _generate_single_value_summary(
    question: str, sql_query: str, column_name: str, value: Any, 
    company_number: str, database_service: DatabaseService
) -> str:
    """Generate human-readable summary for single value results"""
    
    # Detect the type of aggregation from SQL
    sql_upper = sql_query.upper()
    
    # Format the value if it's already currency-formatted
    formatted_value = str(value)
    
    # Detect query type and create appropriate summary
    if 'COUNT' in sql_upper:
        if 'location' in question.lower() or 'properties' in question.lower():
            return f"The total number of locations is {formatted_value}"
        elif 'building' in question.lower():
            return f"The total number of buildings is {formatted_value}"
        else:
            return f"The count is {formatted_value}"
    
    elif 'SUM' in sql_upper:
        if 'tiv' in question.lower() or 'insured value' in question.lower():
            return f"The total insured value is {formatted_value}"
        elif 'revenue' in question.lower():
            return f"The total revenue is {formatted_value}"
        elif 'business' in question.lower() and 'interrupt' in question.lower():
            return f"The total business interruption value is {formatted_value}"
        else:
            return f"The total is {formatted_value}"
    
    elif 'AVG' in sql_upper or 'AVERAGE' in sql_upper:
        if 'tiv' in question.lower() or 'insured value' in question.lower():
            return f"The average insured value is {formatted_value}"
        elif 'revenue' in question.lower():
            return f"The average revenue is {formatted_value}"
        else:
            return f"The average is {formatted_value}"
    
    elif 'MAX' in sql_upper:
        if 'tiv' in question.lower() or 'insured value' in question.lower():
            return f"The maximum insured value is {formatted_value}"
        else:
            return f"The maximum value is {formatted_value}"
    
    elif 'MIN' in sql_upper:
        if 'tiv' in question.lower() or 'insured value' in question.lower():
            return f"The minimum insured value is {formatted_value}"
        else:
            return f"The minimum value is {formatted_value}"
    
    else:
        # Generic response
        return f"The result is {formatted_value}"

def _parse_visualization_response(response: str) -> Dict[str, str]:
        """Parse the visualization response into a dictionary"""
        response_dict = {}
        for line in response.split("\n"):
            if line.strip() and ":" in line:
                key, value = line.split(":", 1)
                response_dict[key.strip()] = value.strip()
        return response_dict 
    
def _enhance_explanation_with_currency_context(
    explanation: str, columns: list, company_number: str, database_service: DatabaseService
) -> str:
    """Add currency context to explanation if monetary columns are present"""
    monetary_cols_in_result = [col for col in columns if col in database_service.MONETARY_COLUMNS]
    
    if monetary_cols_in_result:
        currency_symbol = database_service.get_currency_symbol(company_number)
        currency_note = f"\n\n💰 **Currency Information:** All monetary values are displayed in {currency_symbol} format with proper formatting."
        explanation += currency_note
    
    return explanation

def _build_query_generation_failed_explanation(question: str) -> str:
    """Build explanation for failed query generation"""
    return f"""I understand you're asking about: "{question}"

    However, I had trouble converting your question into a database query. This could be because:

    • **Your question might be asking about data we don't have** - Try asking "What data do we have available?"
    • **The question needs to be more specific** - Try including specific column names or data types
    • **Technical terms might need clarification** - Use simpler language or business terms

    **Here are some example queries that work well:**
    • "What is the total insured value by state?"
    • "Show me properties with high earthquake risk"
    • "List all buildings built after 2000"
    • "Properties in California"

    Would you like to try rephrasing your question?"""

def _handle_no_data_response(request, query_id, sql_query, company_number, database_service, user_id, db):
    """Handle case when query returns no data"""
    context = QueryAnalyzer.analyze_no_data_context(request.question, sql_query, company_number)
    
    explanation = f"""I successfully understood and executed your query: "{request.question}"

    **Query Results:** No data found matching your specific criteria.

    **💡 Suggestions to get results:**
    """
    for suggestion in context["suggestions"]:
        explanation += f"• {suggestion}\n"

    explanation += f"""

    **🔍 Try these alternative queries:**
    """
    for alt_query in context["alternative_queries"]:
        explanation += f'• "{alt_query}"\n'

    # Save the attempt to chat history for learning
    database_service.save_chat_history(
        db, query_id, request.question, sql_query, "no_data_found",
        company_number, user_id
    )

    return QueryResponse(
        query_id=query_id,
        question=request.question,
        sql_query=sql_query,
        explanation=explanation,
        summary="Your query was valid but returned no results - here are some ways to get data",
        data=[],
        timestamp=datetime.utcnow(),
        response_type="no_data_found",
    )

async def _handle_property_risk_insurance(request, query_id, query_processor, database_service, company_number, user_id, db):
    """Handle property risk and insurance related questions"""
    explanation = query_processor.generate_contextual_response(request.question)

    database_service.save_chat_history(
        db, query_id, request.question, None, "property_risk_insurance",
        company_number, user_id
    )

    return QueryResponse(
        query_id=query_id,
        question=request.question,
        explanation=explanation,
        summary=explanation,
        timestamp=datetime.utcnow(),
        response_type="property_risk_insurance",
    )

async def _handle_data_insights(request, query_id, query_processor, database_service, company_number, user_id, db):
    """Handle data insights questions with currency formatting"""
    company_data = database_service.get_company_data(company_number)

    if company_data.empty:
        explanation = "No data available for your company to generate insights."
    else:
        explanation = query_processor.generate_data_insights(request.question, company_data)
        
        # Add currency context for insights
        currency_symbol = database_service.get_currency_symbol(company_number)
        explanation += f"\n\n💰 **Note:** All monetary values in the analysis are displayed in {currency_symbol} format."

    database_service.save_chat_history(
        db, query_id, request.question, None, "data_insights",
        company_number, user_id
    )

    return QueryResponse(
        query_id=query_id,
        question=request.question,
        explanation=explanation,
        summary=explanation,
        timestamp=datetime.utcnow(),
        response_type="data_insights",
    )

async def _handle_portfolio_dashboard(request, query_id, database_service, company_number, user_id, db):
    """Handle portfolio dashboard requests"""
    try:
        dashboard_service = PortfolioDashboardService()
        dashboard_data = dashboard_service.generate_portfolio_dashboard(company_number)
        
        explanation = "Here's your comprehensive portfolio overview dashboard with key metrics, geographic distribution, risk analysis, and data quality indicators."
        
        database_service.save_chat_history(
            db, query_id, request.question, None, "portfolio_dashboard",
            company_number, user_id
        )
        
        return QueryResponse(
            query_id=query_id,
            question=request.question,
            explanation=explanation,
            summary="Portfolio Overview Dashboard",
            data=[dashboard_data],  # Wrap in list for consistency
            timestamp=datetime.utcnow(),
            response_type="portfolio_dashboard",
        )
        
    except Exception as e:
        logger.error(f"Portfolio dashboard generation error: {str(e)}")
        return _handle_processing_error(request, str(e))

def _handle_unrelated(request, query_id, database_service, company_number, user_id, db):
    """Handle unrelated questions"""
    database_service.save_chat_history(
        db, query_id, request.question, None, "unrelated",
        company_number, user_id
    )

    explanation = """I'm designed to help with property risk and insurance data queries.

    **I can help you with:**
    • SQL queries about your property data
    • Property risk management questions
    • Insurance industry insights
    • Data analysis and visualization

    **Try asking questions like:**
    • "What is the total insured value by state?"
    • "Show me properties with earthquake risk"
    • "What does COPE stand for in property insurance?"
    • "Analyze our property portfolio by construction type"

    Please ask a question related to property data, risk management, or insurance topics."""

    return QueryResponse(
        query_id=query_id,
        question=request.question,
        explanation=explanation,
        summary="I can help with property data and insurance questions - try asking something related to those topics",
        timestamp=datetime.utcnow(),
        response_type="unrelated",
    )

def _handle_processing_error(request, error_str):
    """Handle processing errors"""
    explanation = f"""I encountered an unexpected issue while processing your question: "{request.question}"

    **What happened:** {error_str}

    **What you can try:**
    • Refresh the page and try again
    • Simplify your question and try again
    • Check if you're asking about data that exists in our system
    • Contact support if this problem continues

    **Example queries that usually work:**
    • "Show me all properties"
    • "What data do we have?"
    • "List properties by state"

    Would you like to try a simpler question first?"""

    return QueryResponse(
        query_id=str(uuid.uuid4()),
        question=request.question,
        explanation=explanation,
        summary="Technical issue occurred - here's how to recover",
        timestamp=datetime.utcnow(),
        response_type="processing_error",
    )