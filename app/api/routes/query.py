import uuid
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.models.schemas import QueryRequest, QueryResponse
from app.services.query_processor import QueryProcessor
from app.services.database_service import DatabaseService
from app.services.visualization_service import VisualizationService
from app.services.query_analyzer import QueryAnalyzer
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
    """Process text-to-SQL query with enhanced context handling"""
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
                request, query_id, query_processor, database_service, 
                visualization_service, company_number, user_id, db
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
        else:
            return _handle_unrelated(request, query_id, database_service, company_number, user_id, db)

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Query processing error: {str(e)}")
        return _handle_processing_error(request, str(e))

async def _handle_sql_convertible(
    request, query_id, query_processor, database_service, 
    visualization_service, company_number, user_id, db
):
    """Handle SQL convertible questions"""
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

    # Execute the query
    df = database_service.execute_query(sql_query, company_number)

    if df is None or df.empty:
        return _handle_no_data_response(
            request, query_id, sql_query, company_number, database_service, user_id, db
        )

    # Success path
    explanation, summary = query_processor.generate_explanation(request.question, sql_query)
    
    visualization = None
    if visualization_service.should_show_visualization(df, sql_query):
        viz_result = visualization_service.suggest_visualization(
            df, request.question, request.visualization_type, sql_query
        )
        if viz_result:
            visualization = viz_result

    # Save to chat history
    database_service.save_chat_history(
        db, query_id, request.question, sql_query, "sql_convertible",
        company_number, user_id
    )

    query_response_data = {
        'query_id': query_id,
        'question': request.question,
        'sql_query': sql_query,
        'explanation': explanation,
        'summary': summary,
        'data': df.to_dict('records') if not df.empty else [],
        'timestamp': datetime.utcnow(),
        'response_type': "sql_convertible",
    }

    if visualization:
        query_response_data['visualization'] = visualization

    return QueryResponse(**query_response_data)

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
    """Handle data insights questions"""
    company_data = database_service.get_company_data(company_number)

    if company_data.empty:
        explanation = "No data available for your company to generate insights."
    else:
        explanation = query_processor.generate_data_insights(request.question, company_data)

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