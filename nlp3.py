
import os

import re

import logging

import time

from fastapi import FastAPI, HTTPException, Depends, Header

from fastapi.middleware.cors import CORSMiddleware

from sqlalchemy import create_engine, text

from sqlalchemy.orm import sessionmaker

from sqlalchemy.pool import QueuePool

from pydantic import BaseModel

 

from typing import List, Dict, Optional, Any

import datetime

from datetime import datetime, timedelta

import openai

import plotly.express as px

import plotly.io as pio

import pandas as pd

import json

import uuid

 

# Configuration

DATABASE_URL = "postgresql://host:port:5635/exposures"

 

api_config = {

    "API_KEY": key

    "API_BASE": url,

    "API_VERSION": "2015-05-15",

    "API_TYPE": "azure"

}

# Configuration for schema file path

SCHEMA_FILE_PATH = "schema.json"  # Update with actual path or use environment variable

 

# Logging setup

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

 

# FastAPI app

app = FastAPI(title="Text-to-SQL Backend")

 

# CORS configuration for Angular UI

app.add_middleware(

    CORSMiddleware,

    allow_origins=[http://localhost:4200],  # Update with your Angular UI URL

    allow_credentials=True,

    allow_methods=["*"],

    allow_headers=["*"],

)

 

# Database setup with connection pooling

engine = create_engine(

    DATABASE_URL, poolclass=QueuePool, pool_size=20, max_overflow=10, pool_timeout=30

)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

 

 

# Pydantic Models

class QueryRequest(BaseModel):

    question: str

    visualization_type: Optional[str] = None

 

 

class QueryResponse(BaseModel):

    query_id: str

    question: str

    sql_query: Optional[str] = None

    explanation: str

    summary: Optional[str] = None  # Added summary field

    data: Optional[List[Dict[str, Any]]] = None

    visualization: Optional[Dict[str, str]] = None

    timestamp: datetime

    response_type: str

 

 

class ChatHistory(BaseModel):

    query_id: str

    question: str

    sql_query: Optional[str] = None

    response_type: str

    timestamp: datetime

 

 

# Database Dependency

def get_db():

    db = SessionLocal()

    try:

        yield db

    finally:

        db.close()

 

 

# Generic OpenAI Service with Retry Logic

class OpenAIService:

    @staticmethod

    def call_openai_with_retry(prompt: str, max_retries: int = 3, delay: float = 1.0) -> str:

        """Generic OpenAI call with retry logic"""

 

        openai.api_type = api_config["API_TYPE"]

        openai.api_base = api_config["API_BASE"]

        openai.api_version = api_config["API_VERSION"]

        openai.api_key = api_config["API_KEY"]

 

        for attempt in range(max_retries):

            try:

                logger.info(f"OpenAI call attempt {attempt + 1}/{max_retries}")

 

                response = openai.ChatCompletion.create(

                    engine="mmc-tech-gpt-4o-mini-128k-2024-07-18",

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

 

 

# Enhanced Query Processing Service

class QueryProcessor:

    @staticmethod

    def classify_question(question: str) -> Dict[str, any]:

        """Classify the question and determine how to handle it"""

        try:

            # Load schema from file

            if not os.path.exists(SCHEMA_FILE_PATH):

                raise HTTPException(

                    status_code=500,

                    detail=f"Schema file not found at {SCHEMA_FILE_PATH}"

                )

 

            with open(SCHEMA_FILE_PATH, 'r') as file:

                try:

                    schema = json.load(file)

                except json.JSONDecodeError as e:

                    logger.error(f"Invalid JSON in schema file: {str(e)}")

                    raise HTTPException(

                        status_code=500,

                        detail=f"Failed to parse schema file: Invalid JSON format"

                    )

               

            prompt = f"""

 

            Schema: {json.dumps(schema, indent=2)}

 

            Analyze the following question and classify it into one of these categories:

            - ***If the question only contains words such 'analysis', 'analyze' and contains column names in {question} that are in the Schema, strictly return 'data_insights'.

            - For all others, follow the below rules.

 

            1. "sql_convertible" - Can be converted into a SQL query for the database table `ux_all_info_consolidated`. If the {question} contains columns in the `ux_all_info_consolidated` table, consider it as a SQL Query.

            Examples:

            - "What is the total insured value for company X?"

            - "List all properties in California."

            - "How many buildings are owned by company Y?"

            - "Show me the locations grouped by the year of construction as a bar chart. For year of contruction use the folowing groups - 1900 to 1930, 1930 to 1970, 1970 to 2000, 2000 to 2025"

            - "Show me the count of locations for this company?"

            - "Plot locations on a map"

 

            2. "property_risk_insurance" - Related to property risk management, insurance concepts, or industry knowledge. Topics include, but is not limited to, exposure management, property risk management, property risk engineering, risk assessment, insurance policies, claims processes, regulatory requirements, limits, industry trends, and best practices in risk management.

            Examples:

            - "What are the best practices for risk assessment?"

            - "How does CAT modeling work?"

            - "What does COPE stand for?"

            - "What does Risk to Capital mean in Propety Insurance"?

            - "What is a Schedule of Value file"?

            - "What is the importance of Data Quality in Property Risk Management?"

 

            3. "data_insights" - Asking for insights, analysis, or patterns from company data.

            Examples:

            - "What trends can be observed in the total insured values over the last five years?"

            - "Analyze the risk factors for properties in flood zones."

 

            4. "unrelated" - Not related to any of the above categories.

 

            Additionally, determine if the question contains any potentially harmful SQL operations like DROP, DELETE, UNION, semicolons, or SQL comments.

 

            Question: {question}

 

            Respond in JSON format:

            {{

                "category": "<category>",

                "is_safe": true/false,

                "confidence": 0.0-1.0,

                "reasoning": "Brief explanation"

            }}

 

            Clean the response object to just contains the JSON object and no leading and trailing content.

            """

 

            response = OpenAIService.call_openai_with_retry(prompt)

            try:

                # Try to parse as JSON

                result = json.loads(response)

                return result

            except json.JSONDecodeError:

                # Fallback parsing if JSON is malformed

                logger.warning("Failed to parse JSON response, using fallback parsing")

 

                category = "unrelated"

                is_safe = True

 

                if "sql_convertible" in response.lower():

                    category = "sql_convertible"

                elif "property_risk_insurance" in response.lower():

                    category = "property_risk_insurance"

                elif "data_insights" in response.lower():

                    category = "data_insights"

 

                # Check for unsafe operations

                unsafe_keywords = ["drop", "delete", "union", ";", "--"]

                if any(keyword in question.lower() for keyword in unsafe_keywords):

                    is_safe = False

 

                return {

                    "category": category,

                    "is_safe": is_safe,

                    "confidence": 0.7,

                    "reasoning": "Fallback parsing used"

                }

 

        except Exception as e:

            logger.error(f"Question classification error: {str(e)}")

            return {

                "category": "unrelated",

                "is_safe": True,

                "confidence": 0.0,

                "reasoning": f"Classification failed: {str(e)}"

            }

 

    @staticmethod

    def generate_contextual_response(question: str) -> str:

        """Generate contextual response for property risk management and insurance questions"""

        try:

            prompt = f"""

            You are an expert in property risk management and insurance concepts. Your task is to provide clear, accurate, and informative answers to any questions related to these topics. This includes, but is not limited to, exposure management, property risk management, property risk engineering, risk assessment, insurance policies, claims processes, regulatory requirements, limits, industry trends, and best practices in risk management.

            - Please ensure your responses are detailed and relevant, drawing on your extensive knowledge of the insurance industry and property risk management principles.

            - Produce a contextual response that provides practical insights and industry best practices.

            - Cover topics such as Catastrophe Modeling, Natural Hazards, All Other Perils (AOP) Models, Excess Probability Curves (EP Curves), and property characteristics like Construction, Occupancy, Purpose, and Exposure (COPE) attributes but not just limited to them.

            Your response should be informative, concise, and relevant to the needs of professionals in the field.

 

            Question: {question}

 

            Provide a comprehensive but concise response that would be helpful for risk management professionals.

            """

 

            return OpenAIService.call_openai_with_retry(prompt)

 

        except Exception as e:

            logger.error(f"Contextual response generation error: {str(e)}")

            return f"I apologize, but I'm unable to generate a response at this time due to a technical issue: {str(e)}"

 

    @staticmethod

    def generate_data_insights(question: str, company_data: pd.DataFrame) -> str:

        """Generate insights from company data"""

        try:

            # Sample the data if it's too large

            if len(company_data) > 20:

                sample_data = company_data.head(20).to_string()

                data_summary = f"Data sample (showing 20 of {len(company_data)} records):\n{sample_data}"

            else:

                data_summary = f"Complete dataset ({len(company_data)} records):\n{company_data.to_string()}"

 

            prompt = f"""

            As a data analyst expert in property risk and insurance, analyze the following company data and provide insights

            based on this question: {question}

 

            {data_summary}

 

            Provide actionable insights, trends, patterns, and recommendations based on the data. Focus on:

            - Key risk indicators

            - Geographic or sector concentrations

            - Value distributions

            - Potential risk management opportunities

            - Data Quality Issues

            - Search the internet and provide references to Marsh & McLennan Property Insurance and Consulting practice to provide relevant services.

            - Be categorical and detailed. Analyze break up the Marsh & McLennan service offerings under each of the sections.

          

            Do not include training and other kind of recommendations.

            Focus on the impact of risk engineering and readiness assessment.

            Content can be referred from https://www.marsh.com/en/services/risk-consulting/expertise/property-consulting.html

 

            Format your response in a clear, professional manner suitable for risk management decision-making.

            """

 

            return OpenAIService.call_openai_with_retry(prompt)

 

        except Exception as e:

            logger.error(f"Data insights generation error: {str(e)}")

            return f"I apologize, but I'm unable to generate insights at this time due to a technical issue: {str(e)}"

 

    @staticmethod

    def generate_sql(question: str, company_number: str) -> tuple:

        try:

            # Load schema from file

            if not os.path.exists(SCHEMA_FILE_PATH):

                raise HTTPException(

                    status_code=500,

                    detail=f"Schema file not found at {SCHEMA_FILE_PATH}"

                )

 

            with open(SCHEMA_FILE_PATH, 'r') as file:

                try:

                    schema = json.load(file)

                except json.JSONDecodeError as e:

                    logger.error(f"Invalid JSON in schema file: {str(e)}")

                    raise HTTPException(

                        status_code=500,

                        detail=f"Failed to parse schema file: Invalid JSON format"

                    )

 

            # Validate schema structure

            if not isinstance(schema, dict) or "table_name" not in schema or "columns" not in schema:

                raise HTTPException(

                    status_code=500,

                    detail="Schema file has invalid structure"

                )

 

            prompt = f"""

            You are a highly skilled database assistant.

            Your task is to convert the following natural language question into a valid SQL query for the PostgreSQL materialized view named `ux_all_info_consolidated`, which consolidates data from all relevant tables.

            The table has a column company_number. Always include WHERE company_number = {company_number}.

 

            Schema: {json.dumps(schema, indent=2)}

 

            Guidelines:

            1. Ensure all attribute searches are case-insensitive using LOWER() or ILIKE.

            2. Double-check the SQL query for common mistakes, including:

            - Using NOT IN with NULL values (use NOT EXISTS or COALESCE).

            - Using UNION when UNION ALL is more efficient.

            - Using BETWEEN for exclusive ranges (use >= and < instead).

            - Data type mismatches in predicates (cast appropriately).

            - Properly quoting identifiers with double quotes.

            - Using the correct number of arguments for functions.

            - Casting to the correct data type (e.g., NUMERIC for monetary values).

            - Missing LIMIT clause when returning raw records (add LIMIT if needed).

            3. Rewrite the query if any mistakes are detected; otherwise, reproduce the original query.

            4. Ensure the SQL query is syntactically correct and optimized for performance.

            5. Do not include additional text, comments, or symbols in the response—return only the SQL query.

            6. Use `derived_country` column for country-related queries and convert full names to 2-letter country codes.

            7. For `year_built`, expect the format MM/DD/YY and cast to DATE if needed.

            8. Handle column name variations and add underscores where needed.

            9. For invalid columns, use '0' (as a string) or NULL.

            10. If 'distribution' is mentioned, use GROUP BY for the specified column.

            11. Since all data is in `ux_all_info_consolidated`, avoid joins unless explicitly required.

            21. Strictly follow this rule - ****If the user question {question} contains terms like 'all locations', 'list', 'data is available' and does not have a filter criteria, the SQL query must contain all columns in the query with a limit of 100 rows****

            17. Strictly follow this rule - ****If the user question {question} contains terms like ''all locations', 'can you list', 'list', 'give the details','show' and also has a filter cteria, the SQL query must contain Marsh Location Id, Address, State, Country, Total Insured Value, Latitude, Longitude and long with the additional data user is requesting in the query. Strictly do not add a LIMIT clause.****

            12. For monetary columns 'business_interrupt_val', ensure proper casting to NUMERIC and handle non-numeric characters using REGEXP_REPLACE.

            13. When user asks for the tiv or total insured value or total_insured_value always refer to the derived_total_insured_value column.Don't ever refer to total_insured_value colunm in any case

            14. Strictly refuse to execute queries for company_number other than the one passed in the request header and is different from {company_number}

            15. ****If the user question {question} contains terms like 'average', 'group', 'aggregate', 'count', the SQL query must always group on the requested column to form a valid query. Don't ever include any other columns.****

            16. Use case-insensitive searches with `ILIKE` or `LOWER()` for string columns (e.g., `state`, `construction`).

            18. Reject queries with:

                 - DML (INSERT, UPDATE, DELETE) or DDL (DROP, CREATE) operations.

                - Executable functions (e.g., `EXECUTE`).

                - References to company numbers other than '{company_number}'.

                - Harmful SQL (e.g., `DROP`, `UNION`, semicolon, `OR 1=1`).

            19. Optimize queries by:

                - Using `NOT EXISTS` or `COALESCE` instead of `NOT IN` for NULL values.

                - Preferring `UNION ALL` over `UNION` for efficiency.

                - Using `>=` and `<` instead of `BETWEEN` for exclusive ranges.

                - Ensuring proper type casting (e.g., `NUMERIC` for monetary values).

            20. The user can request details on locations that fall within high peril zones. These peril zones correspond to hazards that start with nathan_ in the schema.

            - Hazard Definitions: Below are the hazards, their range values, and the corresponding high-risk values. Include these high-risk values in an IN clause in the SQL query.

            - Strictly, if the question {question} is ambigous, and you find too many matching columns for the requested hazard, do not assume the columns and always ask the user to provide more details.

            - Non-Impacted Locations: Locations with a hazard score of -1 are not impacted by any perils.

            - Query Requirements: The SQL query should return the following columns: Marsh Location Id, Address, State, Country, Total Insured Value, Latitude, Longitude, Hazard Score, and the corresponding nathan_ column for each peril.

           

            Hazard Zones:

            nathan_earthquake_hazardzone: Range -1 to 4; High-risk values: 3, 4

            nathan_hail_hazardzone: Range -1 to 6; High-risk values: 4, 5, 6

            Hail (Marsh 1 Inch): Range -1 to 4; High-risk values: 0, 1

            Hail (Marsh 2 Inch): Range -1 to 4; High-risk values: 0, 1

            Hail (Marsh 3 Inch): Range -1 to 4; High-risk values: 0, 1

            nathan_volcano_hazardzone: Range -1 to 3; High-risk values: 2, 3

            nathan_tsunami_hazardzone: Range -1, 100, 500, 1000; High-risk values: 100, 500

            nathan_hurricane_hazardzone: Range -1 to 5; High-risk values: 4, 5

            nathan_extra_tropical_storm_hazardzone: Range -1 to 4; High-risk values: 3, 4

            nathan_tornado_hazardzone: Range -1 to 4; High-risk values: 3, 4

            nathan_lightning_hazardzone: Range -1 to 6; High-risk values: 4, 5, 6

            nathan_wildfire_hazardzone: Range -1 to 4; High-risk values: 3, 4

            nathan_river_flood_hazardzone: Range -1, 50, 100, 500; High-risk values: 50, 100

            nathan_flash_flood_hazardzone: Range -1 to 6; High-risk values: 5, 6

            nathan_storm_surge_tornado_hazardzone: Range -1, 100, 500, 1000; High-risk values: 100, 500

            ****Expert Guidance: When generating the SQL query, act as an expert in risk assessment and hazard analysis. For each peril, define what constitutes a high-risk location, the potential consequences of being in such an area, and any relevant factors that contribute to the risk level.****

 

            Example Queries:

            - Question: "What is the TIV for properties in California?"

            Query: `SELECT SUM(derived_total_insured_value) AS total_tiv FROM ux_all_info_consolidated WHERE company_number = '{company_number}' AND state ILIKE 'california'`

            - Question: "List locations with high earthquake risk."

            Query: `SELECT marsh_location_id, location_name, address, state, derived_country, postal_code FROM ux_all_info_consolidated WHERE company_number = '{company_number}' AND nathan_earthquake_hazardzone IN ('2', '3', '4')`

            - Question: "Show TIV by construction type."

            Query: `SELECT construction, SUM(derived_total_insured_value) AS total_tiv FROM ux_all_info_consolidated WHERE company_number = '{company_number}' GROUP BY construction_cd_scheme`

            Here is the question: {question}

            - Question: "Can you list down all the countries having latitude between -90 and 90"

            Query: `SELECT derived_country, COUNT(*) AS country_count FROM ux_all_info_consolidated WHERE company_number = '{company_number}' AND latitude IS NOT NULL AND latitude ~ '^-?\\d+(\\.\\d+)?$' AND CAST(latitude AS DOUBLE PRECISION) >= -90 AND CAST(latitude AS DOUBLE PRECISION) < 90 GROUP BY derived_country ORDER BY country_count DESC`;

            - Question: "Can you identify the country with invalid occupancy code scheme"

            Query: `SELECT derived_country, COUNT(*) AS invalid_count FROM ux_all_info_consolidated WHERE occup_code_scheme IS NULL OR occup_code_scheme = '0' GROUP BY derived_country ORDER BY invalid_count DESC`;

            - Question 'Can you show total insured value by country and year'

            Query: `

            SELECT derived_country, year_built AS year, SUM(derived_total_insured_value) AS total_tiv

            FROM ux_all_info_consolidated

            WHERE company_number = '{company_number}'

            GROUP BY derived_country, year

            ORDER BY derived_country, year`

            - Question 'average tiv by const_cd_scheme'

            Query: `

            SELECT const_cd_scheme, AVG(derived_total_insured_value) AS average_tiv

            FROM ux_all_info_consolidated

            WHERE company_number = '{company_number}'

            GROUP BY const_cd_scheme

            `

            22. Return only the SQL query. Ensure it:

            - Avoids any DML (INSERT, UPDATE, DELETE) or DDL (DROP, CREATE) operations

            - Excludes executable functions

            If the question cannot be converted safely, return an empty string.

            """

 

            response = OpenAIService.call_openai_with_retry(prompt)

            sql_query = response.strip()

            sql_query = re.sub(r'^```sql\s*|\s*```$', '', sql_query, flags=re.MULTILINE).strip()

 

            if not sql_query:

                logger.error("Generated SQL query is empty.")

                return ""  # Return empty strings to avoid unpacking errors

 

            logger.info(f"Generated SQL: {sql_query}")

            return sql_query

 

        except Exception as e:

            logger.error(f"SQL generation error: {str(e)}")

            return ""  # Return empty strings to avoid unpacking errors

 

 

    @staticmethod

    def generate_explanation(question: str, sql_query: str) -> tuple:

        """Generate explanation and summary for the SQL query"""

        try:

            # Generate detailed explanation

            explanation_prompt = f"""

            You are a SQL expert. Given the following SQL query, explain it in simple terms for a non-technical user.

            - Break it down step by step.

            - Describe what the query does.

            - Mention filtering conditions, aggregations, and joins if any.

 

            Question: {question}

            SQL Query: {sql_query}

            Provide a detailed and clear explanation.

            """

 

            explanation = OpenAIService.call_openai_with_retry(explanation_prompt)

 

            # Generate summary

            summary_prompt = f"""

            Based on the following question and SQL query, provide a brief 2-3 line summary that explains the AI approach used to answer this question. Focus on what the AI analyzed and how it processed the request.

 

            Question: {question}

            SQL Query: {sql_query}

           

            Provide only a concise 2-3 line summary of the AI approach.

            """

 

            summary = OpenAIService.call_openai_with_retry(summary_prompt)

 

            return explanation, summary

 

        except Exception as e:

            logger.error(f"Explanation generation error: {str(e)}")

            return f"Unable to generate explanation: {str(e)}", f"Unable to generate summary: {str(e)}"

 

class QueryAnalyzer:

    """Helper class to analyze queries and provide contextual responses"""

   

    @staticmethod

    def analyze_no_data_context(question: str, sql_query: str, company_number: str) -> dict:

        """Analyze why a query returned no data and provide contextual help"""

       

        # Extract key components from the query for analysis

        question_lower = question.lower()

        sql_lower = sql_query.lower()

       

        context = {

            "reason": "general",

            "suggestions": [],

            "alternative_queries": [],

            "is_first_attempt": True

        }

       

        # Check for common patterns that might cause no results

        if any(word in question_lower for word in ['california', 'texas', 'florida', 'new york']):

            if 'state' in sql_lower and any(state in sql_lower for state in ['california', 'texas', 'florida']):

                context["reason"] = "specific_location"

                context["suggestions"] = [

                    "Try using state abbreviations (CA, TX, FL, NY)",

                    "Check if the location data uses full state names or abbreviations",

                    "Try a broader search like 'properties in the US'"

                ]

                context["alternative_queries"] = [

                    "What states do we have data for?",

                    "Show me all available locations",

                    "What is the count of properties by state?"

                ]

       

        elif any(word in question_lower for word in ['earthquake', 'flood', 'hurricane', 'tornado']):

            context["reason"] = "risk_criteria"

            context["suggestions"] = [

                "Try using different risk level terms (high, medium, low)",

                "Check what hazard zones are available in our data",

                "Ask about specific risk ratings or scores"

            ]

            context["alternative_queries"] = [

                "What earthquake hazard zones do we have data for?",

                "Show me properties with any natural hazard risk",

                "What are the available risk categories?"

            ]

       

        elif any(word in question_lower for word in ['built after', 'construction year', 'year built']):

            context["reason"] = "date_criteria"

            context["suggestions"] = [

                "Try different year ranges or broader date criteria",

                "Check what construction years are available in our data",

                "Use 'built between' for year ranges"

            ]

            context["alternative_queries"] = [

                "What is the range of construction years in our data?",

                "Show me properties built in the last 20 years",

                "What is the average construction year of our properties?"

            ]

       

        elif any(word in question_lower for word in ['total insured value', 'tiv', 'value above', 'value over']):

            context["reason"] = "value_criteria"

            context["suggestions"] = [

                "Try lower value thresholds",

                "Ask about value ranges instead of minimums",

                "Check what the typical value ranges are in our data"

            ]

            context["alternative_queries"] = [

                "What is the range of insured values in our data?",

                "Show me the distribution of property values",

                "What is the average total insured value?"

            ]

       

        else:

            context["suggestions"] = [

                "Try using broader search criteria",

                "Check spelling of location names or property details",

                "Ask about what data is available first",

                "Use partial matches instead of exact terms"

            ]

            context["alternative_queries"] = [

                "What data do we have available?",

                "Show me a sample of our properties",

                "What locations do we have data for?"

            ]

       

        return context

 

# Visualization Service

class VisualizationGenerator:

    @staticmethod

    def should_show_visualization(df: pd.DataFrame, sql_query: str) -> bool:

        """Determine if visualization should be shown"""

        # Conditions to HIDE visualization

        if len(df) == 1 and len(df.columns) == 1:  # Single-value results

            return False

        if any(keyword in sql_query.upper() for keyword in

               ["SUM(", "AVG(", "COUNT(", "MAX(", "MIN(", "DISTINCT"]):

            return len(df) > 1  # Only show for multi-row aggregations

        return True

 

    @staticmethod

    def suggest_visualization(

        data: pd.DataFrame, question: str, visualization_type: Optional[str], sql_query: Optional[str] = None

    ) -> Dict:

        """Suggest and generate Plotly visualization"""

        try:

            if visualization_type:

                chart_type = visualization_type

            else:

                prompt = f"""

                You are a great data scientist who can analyze data and suggest the best visualization type.

                Based on the following question, SQL query and sample data, suggest the best visualization type (bar, pie, line, scatter, area, histogram, heatmap, map etc.) and the appropriate columns for plotting.

                If user explicitly mention in my input line chart or pie chart or bar chart, then try to suggest only that visualization which I have mentioned.

                If there are two categorical columns (e.g., 'country' and 'construction'), suggest a **stacked bar chart**

                If time series data is detected, suggest an **area chart**.

                                If the {sql_query} contains "SUM(", "AVG(", "COUNT(", "MAX(", "MIN(", "DISTINCT" do not suggest a chart type and return None.

                If {data} contains only 1 row of data, do not suggest a chart type and return None.

                Always suggest one word for the chart type, an an example bar,pie,stackedbar,line,map,scatterplot,areachart,histogram,heatmap etc.

                If the user mentions terms as latitude, longitude or its abbreviations, strictly suggest ***map*** as chart type.

                If the user uses words as plot, map and other ways of depicting geo data, strictly suggest ***map*** as chart type.

                *** If the number of columns is big, and the user has asked for a map or plot locations, then strictly return map.***

                *** If the number of columns is big, do not suggest a visualization and return None.***

                As your role as a data scientist select the bext x and Y axes values.

 

                Data: {data}

                Question: {question}

                SQL Query: {sql_query}

                Return only the visualization type.

                Respond in this format:

                Chart Type: <chart_type>

                X-axis: <column>

                Y-axis: <column>

                Color: <stacking_column> (only for stacked bar chart)

                """

 

                response = OpenAIService.call_openai_with_retry(prompt)

                return response

 

        except Exception as e:

            logger.error(f"Visualization generation error: {str(e)}")

            raise HTTPException(

                status_code=500, detail=f"Error generating visualization: {str(e)}"

            )

 

 

# Enhanced Database Service

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

    def save_chat_history(db, query_id: str, question: str, sql_query: Optional[str],

                          response_type: str, company_number: str, user_id: str):

        """Save query to chat history with user_id and company_number"""

        try:

            db.execute(

                text(

                    """

                    INSERT INTO chat_history (query_id, question, sql_query, response_type,

                                            company_number, user_id, timestamp)

                    VALUES (:query_id, :question, :sql_query, :response_type,

                           :company_number, :user_id, :timestamp)

                """

                ),

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

    def get_chat_history(db, company_number: str, user_id: str) -> List[Dict]:

        """Retrieve chat history for specific user and company"""

        try:

            result = db.execute(

                text(

                    """SELECT query_id, question, sql_query, response_type, timestamp

                       FROM chat_history

                       WHERE company_number = :company_number AND user_id = :user_id

                       ORDER BY timestamp DESC"""

                ),

                {"company_number": company_number, "user_id": user_id}

            )

            return [dict(row) for row in result.mappings()]

 

        except Exception as e:

            logger.error(f"Chat history retrieval error: {str(e)}")

            raise HTTPException(

                status_code=500, detail=f"Error retrieving history: {str(e)}"

            )

 

 

# Enhanced API Endpoints

@app.post("/api/query", response_model=QueryResponse)

async def process_query(

        request: QueryRequest,

        company_number: str = Header(...),

        user_id: str = Header(...),

        db: SessionLocal = Depends(get_db),

):

    """Process text-to-SQL query with enhanced context handling"""

    try:

        # Sanitize inputs

        if not re.match(r"^[a-zA-Z0-9_-]+$", company_number):

            raise HTTPException(status_code=400, detail="Invalid company_number format")

 

        if not re.match(r"^[a-zA-Z0-9_-]+$", user_id):

            raise HTTPException(status_code=400, detail="Invalid user_id format")

 

        # Classify the question

        classification = QueryProcessor.classify_question(request.question)

 

        if not classification["is_safe"]:

            raise HTTPException(

                status_code=400,

                detail="Question contains potentially harmful content and cannot be processed"

            )

 

        query_id = str(uuid.uuid4())

        response_type = classification["category"]

 

        logger.info(f"Question classified as: {response_type} with confidence: {classification['confidence']}")

 

        if classification["category"] == "sql_convertible":

            # Handle SQL convertible questions

            sql_query = QueryProcessor.generate_sql(request.question, company_number)

           

            if not sql_query:

                # ENHANCED: Better handling for query generation failure

                explanation = f"""I understand you're asking about: "{request.question}"

 

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

 

                return QueryResponse(

                    query_id=query_id,

                    question=request.question,

                    explanation=explanation,

                    summary="I need help understanding your question - let's try a different approach",

                    timestamp=datetime.utcnow(),

                    response_type="query_generation_failed",

                )

          

            # Execute the query

            df = DatabaseService.execute_query(sql_query, company_number)

 

            # ENHANCED: Smart handling of no data scenarios

            if df is None or df.empty:

                # Analyze the context of why no data was found

                context = QueryAnalyzer.analyze_no_data_context(request.question, sql_query, company_number)

               

                # Create contextual explanation based on analysis

                explanation = f"""I successfully understood and executed your query: "{request.question}"

 

                **Query Results:** No data found matching your specific criteria.

 

                **Possible reasons:**

                """

                               

                if context["reason"] == "specific_location":

                                    explanation += """• The location you specified might not exist in our database

                • Location names might be stored differently (full names vs abbreviations)

                • The geographic area might not have any properties in our system"""

                               

                elif context["reason"] == "risk_criteria":

                                    explanation += """• No properties meet your specific risk criteria

                • Risk categories might be defined differently in our system

                • The hazard type might not apply to properties in our database"""

                               

                elif context["reason"] == "date_criteria":

                                    explanation += """• No properties were built in the specified time period

                • Construction dates might be stored in a different format

                • The date range might be outside our data coverage"""

                               

                elif context["reason"] == "value_criteria":

                                    explanation += """• No properties meet your value threshold

                • Values might be stored in different units or currency

                • The value range might be outside our typical property values"""

                               

                else:

                                    explanation += """• Your search criteria might be too specific

                • There might be typos in location names or other details

                • The data you're looking for might not be available"""

 

                explanation += f"""

 

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

                DatabaseService.save_chat_history(

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

 

            # SUCCESS PATH: Query returned data

            explanation, summary = QueryProcessor.generate_explanation(request.question, sql_query)

            visualization = (

                VisualizationGenerator.suggest_visualization(

                    df,

                    request.question,

                    request.visualization_type,

                    sql_query

                )

                if VisualizationGenerator.should_show_visualization(df, sql_query)

                else None

            )

            print(visualization)

 

            response_dict = {}

 

        # Check if visualization is not None before processing

            if visualization and visualization != "None":

                for line in visualization.split("\n"):

                    if line.strip():

                        key, value = line.split(":", 1)

                        response_dict[key.strip()] = value.strip()

 

            # Save to chat history

            DatabaseService.save_chat_history(

                db, query_id, request.question, sql_query, response_type,

                company_number, user_id

            )

 

            # Prepare the QueryResponse without the visualization field if it's None

            query_response_data = {

                'query_id': query_id,

                'question': request.question,

                'sql_query': sql_query,

                'explanation': explanation,

                'summary': summary,

                'data': df.to_dict('records') if not df.empty else [],

                'timestamp': datetime.utcnow(),

                'response_type': response_type,

            }

 

            # Only add visualization if it exists

            if visualization:

                query_response_data['visualization'] = response_dict

            return QueryResponse(**query_response_data)

 

        elif classification["category"] == "property_risk_insurance":

            # Handle property risk and insurance related questions

            explanation = QueryProcessor.generate_contextual_response(request.question)

 

            DatabaseService.save_chat_history(

                db, query_id, request.question, None, response_type,

                company_number, user_id

            )

 

            return QueryResponse(

                query_id=query_id,

                question=request.question,

                explanation=explanation,

                summary=explanation,

                timestamp=datetime.utcnow(),

                response_type=response_type,

            )

 

        elif classification["category"] == "data_insights":

            # Handle data insights questions

            company_data = DatabaseService.get_company_data(company_number)

 

            if company_data.empty:

                explanation = "No data available for your company to generate insights."

            else:

                explanation = QueryProcessor.generate_data_insights(request.question, company_data)

 

            DatabaseService.save_chat_history(

                db, query_id, request.question, None, response_type,

                company_number, user_id

            )

 

            return QueryResponse(

                query_id=query_id,

                question=request.question,

                explanation=explanation,

                summary=explanation,

                timestamp=datetime.utcnow(),

                response_type=response_type,

            )

 

        else:

            # Handle unrelated questions

            DatabaseService.save_chat_history(

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

 

    except HTTPException as e:

        raise e

    except Exception as e:

        logger.error(f"Query processing error: {str(e)}")

       

        # ENHANCED: Better error handling with recovery suggestions

        explanation = f"""I encountered an unexpected issue while processing your question: "{request.question}"

 

        **What happened:** {str(e)}

 

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

 

@app.get("/api/history", response_model=List[ChatHistory])

async def get_history(

        company_number: str = Header(...),

        user_id: str = Header(...),

        db: SessionLocal = Depends(get_db)

):

    """Retrieve chat history for specific user and company"""

    try:

        # Sanitize inputs

        if not re.match(r"^[a-zA-Z0-9_-]+$", company_number):

            raise HTTPException(status_code=400, detail="Invalid company_number format")

 

        if not re.match(r"^[a-zA-Z0-9_-]+$", user_id):

            raise HTTPException(status_code=400, detail="Invalid user_id format")

 

        history = DatabaseService.get_chat_history(db, company_number, user_id)

        return [ChatHistory(**item) for item in history]

 

    except HTTPException as e:

        raise e

    except Exception as e:

        logger.error(f"History retrieval error: {str(e)}")

        raise HTTPException(

            status_code=500, detail=f"Error retrieving history: {str(e)}"

        )

 

 

@app.get("/api/schema", response_model=List[Dict[str, str]])

async def get_schema():

    """Get database schema information for autocomplete"""

    try:

        schema_info = [

                        {"name":"marsh_location_id","type":"bigint","description":"Unique Marsh identifier for the property location."},

                        {"name":"company_number","type":"string","description":"Unique identifier for the company owning the property."},

                        {"name":"location_identifier","type":"string","description":"Unique identifier for the specific location."},

                        {"name":"location_name","type":"string","description":"Name or description of the location."},

                        {"name":"address","type":"string","description":"Street address of the property."},

                        {"name":"address_3","type":"string","description":"Additional address information."},

                        {"name":"city","type":"string","description":"City where the property is located."},

                        {"name":"state","type":"string","description":"State or province of the property."},

                        {"name":"postal_code","type":"string","description":"ZIP or postal code."},

                        {"name":"county","type":"string","description":"County where the property is located."},

                        {"name":"country","type":"string","description":"Country name (may be non-standardized)."},

                        {"name":"derived_country","type":"string","description":"Standardized 2-letter country code (e.g., 'US'). Use for country queries."},

                        {"name":"country_code","type":"string","description":"Alternative country code."},

                        {"name":"latitude","type":"string","description":"Geographic latitude of the property."},

                        {"name":"longitude","type":"string","description":"Geographic longitude of the property."},

                        {"name":"recommended_latitude","type":"string","description":"Recommended latitude from geocoding."},

                        {"name":"recommended_longitude","type":"string","description":"Recommended longitude from geocoding."},

                        {"name":"recommended_state","type":"string","description":"Recommended state from geocoding."},

                        {"name":"recommended_country","type":"string","description":"Recommended country from geocoding."},

                        {"name":"add_or_intl_address","type":"string","description":"Additional or international address details."},

                        {"name":"recommended_geocoder","type":"string","description":"Geocoder used for location validation."},

                        {"name":"construction","type":"string","description":"Construction type (e.g., frame, masonry). Primary COPE attribute."},

                        {"name":"construction_description","type":"string","description":"Detailed description of construction type."},

                        {"name":"const_descrip_schdl_val","type":"string","description":"Construction description from schedule of values."},

                        {"name":"const_cd_scheme","type":"string","description":"Construction code scheme used."},

                        {"name":"construction_quality","type":"string","description":"Quality of construction (e.g., high, low)."},

                        {"name":"construction_quality_wind","type":"string","description":"Construction quality for wind resistance."},

                        {"name":"occupancy","type":"string","description":"Occupancy type (e.g., office, residential). Primary COPE attribute."},

                        {"name":"occupancy_description","type":"string","description":"Detailed description of occupancy type."},

                        {"name":"occup_descrip_schdl_val","type":"string","description":"Occupancy description from schedule of values."},

                        {"name":"occup_code_scheme","type":"string","description":"Occupancy code scheme used."},

                        {"name":"year_built","type":"date (string, MM/DD/YYYY)","description":"Year the property was built. Cast to DATE for comparisons."},

                        {"name":"year_upgrade","type":"date (string, MM/DD/YYYY)","description":"Year of major upgrades. Cast to DATE for comparisons."},

                        {"name":"number_of_stories","type":"string","description":"Number of floors in the building."},

                        {"name":"floors_occupied","type":"string","description":"Number of floors occupied by the company."},

                        {"name":"number_of_buildings","type":"string","description":"Total number of buildings at the location."},

                        {"name":"sprinklered_y_n","type":"string","description":"Indicates if the property has sprinklers ('Y' or 'N')."},

                        {"name":"sprinkler_type","type":"string","description":"Type of sprinkler system."},

                        {"name":"sprinkler_leakage","type":"string","description":"Presence of sprinkler leakage issues."},

                        {"name":"sprinkler_leakage_susceptibility","type":"string","description":"Susceptibility to sprinkler leakage."},

                        {"name":"flood_protection","type":"string","description":"Flood protection measures (e.g., barriers, elevation)."},

                        {"name":"earthquake_equipment_bracing","type":"string","description":"Presence of equipment bracing for earthquake resistance."},

                        {"name":"structural_upgrade","type":"string","description":"Details of structural upgrades for risk mitigation."},

                        {"name":"base_isolation","type":"string","description":"Presence of base isolation for seismic protection."},

                        {"name":"roof_equipment_bracing","type":"string","description":"Bracing for rooftop equipment against wind or seismic risks."},

                        {"name":"outdoor_mach_eqp_bracing","type":"string","description":"Bracing for outdoor machinery or equipment."},

                        {"name":"total_insured_value","type":"string","description":"Original TIV, may require casting to numeric. Use derived_total_insured_value for accuracy."},

                        {"name":"modelable_tiv","type":"numeric (double precision)","description":"TIV suitable for risk modeling."},

                        {"name":"building_values","type":"string","description":"Value of the building structure, may require casting."},

                        {"name":"derived_building_values","type":"numeric (double precision)","description":"Calculated building value in USD."},

                        {"name":"local_building_values","type":"string","description":"Building value in local currency."},

                        {"name":"content_values","type":"string","description":"Value of contents, may require casting."},

                        {"name":"derived_content_values","type":"numeric (double precision)","description":"Calculated contents value in USD."},

                        {"name":"local_content_values","type":"string","description":"Contents value in local currency."},

                        {"name":"total_content_values","type":"string","description":"Total value of all contents."},

                        {"name":"derived_total_content_values","type":"numeric (double precision)","description":"Calculated total contents value in USD."},

                        {"name":"business_income","type":"string","description":"Business income value, may require casting."},

                        {"name":"derived_business_income","type":"numeric (double precision)","description":"Calculated business income value in USD."},

                        {"name":"local_business_income","type":"string","description":"Business income in local currency."},

                        {"name":"business_interrupt_val","type":"string","description":"Business interruption value, may require casting."},

                        {"name":"derived_business_interrupt_val","type":"numeric (double precision)","description":"Calculated business interruption value in USD."},

                        {"name":"local_business_interrupt_val","type":"string","description":"Business interruption value in local currency."},

                        {"name":"derived_business_interrupt_val_12mo","type":"numeric (double precision)","description":"Business interruption value over 12 months."},

                        {"name":"revenue","type":"string","description":"Annual revenue, may require casting."},

                        {"name":"derived_revenue","type":"numeric (double precision)","description":"Calculated annual revenue in USD."},

                        {"name":"local_revenue","type":"string","description":"Revenue in local currency."},

                        {"name":"property_damage","type":"string","description":"Property damage value, may require casting."},

                        {"name":"derived_property_damage","type":"numeric (double precision)","description":"Calculated property damage value in USD."},

                        {"name":"local_property_damage","type":"string","description":"Property damage in local currency."},

                        {"name":"rental_income","type":"string","description":"Rental income value, may require casting."},

                        {"name":"derived_rental_income","type":"numeric (double precision)","description":"Calculated rental income in USD."},

                        {"name":"local_rental_income","type":"string","description":"Rental income in local currency."},

                        {"name":"invtry_or_stock_val_ctnt2","type":"string","description":"Inventory or stock value, may require casting."},

                        {"name":"derived_invtry_or_stock_val_ctnt2","type":"numeric (double precision)","description":"Calculated inventory/stock value in USD."},

                        {"name":"local_invtry_or_stock_val_ctnt2","type":"string","description":"Inventory/stock value in local currency."},

                        {"name":"machinery_equipment_ctnt3","type":"string","description":"Machinery/equipment value, may require casting."},

                        {"name":"derived_machinery_equipment_ctnt3","type":"numeric (double precision)","description":"Calculated machinery/equipment value in USD."},

                        {"name":"local_machinery_equipment_ctnt3","type":"string","description":"Machinery/equipment value in local currency."},

                        {"name":"currency","type":"string","description":"Currency used for financial values."},

                        {"name":"nathan_earthquake_hazardzone","type":"string (text)","description":"Earthquake hazard zone (0-4). High risk: 2-4. 'UNKNOWN' = 0."},

                        {"name":"nathan_hurricane_hazardzone","type":"string (text)","description":"Hurricane hazard zone. 'UNKNOWN' = 0."},

                        {"name":"nathan_tornado_hazardzone","type":"string (text)","description":"Tornado hazard zone. 'UNKNOWN' = 0."},

                        {"name":"nathan_wildfire_hazardzone","type":"string (text)","description":"Wildfire hazard zone. 'UNKNOWN' = 0."},

                        {"name":"nathan_flash_flood_hazardzone","type":"string (text)","description":"Flash flood hazard zone. 'UNKNOWN' = 0."},

                        {"name":"nathan_river_flood_hazardzone","type":"string (text)","description":"River flood hazard zone. 'UNKNOWN' = 0."},

                        {"name":"nathan_storm_surge_tornado_hazardzone","type":"string (text)","description":"Storm surge/tornado hazard zone. 'UNKNOWN' = 0."},

                        {"name":"nathan_hail_hazardzone","type":"string (text)","description":"Hail hazard zone. 'UNKNOWN' = 0."},

                        {"name":"nathan_lightning_hazardzone","type":"string (text)","description":"Lightning hazard zone. 'UNKNOWN' = 0."},

                        {"name":"nathan_tsunami_hazardzone","type":"string (text)","description":"Tsunami hazard zone. 'UNKNOWN' = 0."},

                        {"name":"nathan_volcano_hazardzone","type":"string (text)","description":"Volcano hazard zone. 'UNKNOWN' = 0."},

                        {"name":"nathan_extra_tropical_storm_hazardzone","type":"string (text)","description":"Extra-tropical storm hazard zone. 'UNKNOWN' = 0."},

                        {"name":"fema_flood_zone","type":"string","description":"FEMA flood zone designation."},

                        {"name":"custom_flood_zone_code","type":"string","description":"Custom flood zone code."},

                        {"name":"atc_earthquake","type":"string (text)","description":"ATC earthquake risk classification."},

                        {"name":"california_earthquake","type":"string (text)","description":"California-specific earthquake risk."},

                        {"name":"us_hurricane_wind_tier_wind","type":"string (text)","description":"US hurricane wind tier."},

                        {"name":"zurich_earthquake","type":"string (text)","description":"Zurich earthquake risk classification."},

                        {"name":"zurich_wind","type":"string (text)","description":"Zurich wind risk classification."},

                        {"name":"wildfire_air_aal","type":"numeric (double precision)","description":"Annual average loss for wildfire (AIR model)."},

                        {"name":"wildfire_rms_aal","type":"numeric (double precision)","description":"Annual average loss for wildfire (RMS model)."},

                        {"name":"flood_air_aal","type":"numeric (double precision)","description":"Annual average loss for flood (AIR model)."},

                        {"name":"flood_rms_aal","type":"numeric (double precision)","description":"Annual average loss for flood (RMS model)."},

                        {"name":"earthquake_air_aal","type":"numeric (double precision)","description":"Annual average loss for earthquake (AIR model)."},

                        {"name":"earthquake_rms_aal","type":"numeric (double precision)","description":"Annual average loss for earthquake (RMS model)."},

                        {"name":"severe_convective_storm_air_aal","type":"numeric (double precision)","description":"Annual average loss for severe convective storm (AIR model)."},

                        {"name":"severe_convective_storm_rms_aal","type":"numeric (double precision)","description":"Annual average loss for severe convective storm (RMS model)."},

                        {"name":"tropical_cyclone_air_aal","type":"numeric (double precision)","description":"Annual average loss for tropical cyclone (AIR model)."},

                        {"name":"tropical_cyclone_rms_aal","type":"numeric (double precision)","description":"Annual average loss for tropical cyclone (RMS model)."},

                        {"name":"normal_loss_estimate","type":"string","description":"Estimated normal loss."},

                        {"name":"probable_max_loss","type":"string","description":"Probable maximum loss (PML)."},

                        {"name":"max_foreseeable_loss","type":"string","description":"Maximum foreseeable loss."},

                        {"name":"catastrophic_loss_event","type":"string","description":"Details of catastrophic loss events."},

                        {"name":"machinery_normal_loss_est","type":"string","description":"Normal loss estimate for machinery."},

                        {"name":"machinery_probable_max_loss","type":"string","description":"Probable maximum loss for machinery."},

                        {"name":"machinery_max_foreseeable_loss","type":"string","description":"Maximum foreseeable loss for machinery."},

                        {"name":"floor_area","type":"string","description":"Floor area of the building."},

                        {"name":"area_unit","type":"string","description":"Unit of measurement for floor area (e.g., sq ft)."},

                        {"name":"basement","type":"string","description":"Presence of a basement."},

                        {"name":"number_of_basement_levels","type":"string","description":"Number of basement levels."},

                        {"name":"basement_finish_type","type":"string","description":"Type of basement finish."},

                        {"name":"roof_age","type":"string","description":"Age of the roof."},

                        {"name":"roof_covering","type":"string","description":"Type of roof covering (e.g., shingles)."},

                        {"name":"roof_geometry","type":"string","description":"Shape of the roof (e.g., flat, gabled)."},

                        {"name":"roof_anchor","type":"string","description":"Presence of roof anchoring."},

                        {"name":"roof_sheathing_attachment","type":"string","description":"Method of roof sheathing attachment."},

                        {"name":"roof_parapet","type":"string","description":"Presence of a roof parapet."},

                        {"name":"rooftop_tanks","type":"string","description":"Presence of rooftop tanks."},

                        {"name":"cladding_type","type":"string","description":"Type of exterior cladding."},

                        {"name":"exterior_walls_or_cladding","type":"string","description":"Details of exterior walls or cladding."},

                        {"name":"opening_protection","type":"string","description":"Protection for openings (e.g., windows, doors)."},

                        {"name":"opening_protection_description","type":"string","description":"Detailed description of opening protection."},

                        {"name":"frame_foundation_connection","type":"string","description":"Connection between frame and foundation."},

                        {"name":"engineered_foundation","type":"string","description":"Presence of an engineered foundation."},

                        {"name":"slope","type":"string","description":"Slope of the property site."},

                        {"name":"building_elevation","type":"string","description":"Elevation of the building."},

                        {"name":"finished_floor_elevation","type":"string","description":"Elevation of the finished floor."},

                        {"name":"base_flood_elevation","type":"string","description":"Base flood elevation for flood risk."},

                        {"name":"custom_elevation","type":"string","description":"Custom elevation data."},

                        {"name":"first_floor_height","type":"string","description":"Height of the first floor."},

                        {"name":"floor_type","type":"string","description":"Type of flooring."},

                        {"name":"structure_condition","type":"string","description":"Overall condition of the structure."},

                        {"name":"ibhs_certified_structures","type":"string","description":"IBHS certification status."},

                        {"name":"unreinforced_masonry_retrofit","type":"string","description":"Retrofit status for unreinforced masonry."},

                        {"name":"unreinforced_masonry_chimney","type":"string","description":"Presence of unreinforced masonry chimney."},

                        {"name":"pounding","type":"string","description":"Risk of building pounding during earthquakes."},

                        {"name":"soft_story","type":"string","description":"Presence of soft story structures."},

                        {"name":"vertical_irregularity","type":"string","description":"Presence of vertical structural irregularities."},

                        {"name":"plan_irregularity","type":"string","description":"Presence of plan irregularities."},

                        {"name":"short_column","type":"string","description":"Presence of short columns."},

                        {"name":"cripple_walls","type":"string","description":"Presence of cripple walls."},

                        {"name":"frame_bolted","type":"string","description":"Bolted frame status."},

                        {"name":"purlin_anchoring","type":"string","description":"Purlin anchoring status."},

                        {"name":"equip_support_or_fatigue","type":"string","description":"Equipment support or fatigue status."},

                        {"name":"flashing_and_coping_quality","type":"string","description":"Quality of flashing and coping."},

                        {"name":"wet_floodproofing_code","type":"string","description":"Wet floodproofing code compliance."},

                        {"name":"service_equipment_protection","type":"string","description":"Protection for service equipment."},

                        {"name":"ground_level_mech_or_elec_eqpment","type":"string","description":"Presence of ground-level mechanical/electrical equipment."},

                        {"name":"business_interrupt_preparedness","type":"string","description":"Preparedness measures for business interruption."},

                        {"name":"derived_business_interrupt_preparedness","type":"numeric (double precision)","description":"Calculated preparedness score."},

                        {"name":"business_interrupt_redundancy","type":"string","description":"Redundancy measures for business interruption."},

                        {"name":"derived_business_interrupt_redundancy","type":"numeric (double precision)","description":"Calculated redundancy score."},

                        {"name":"business_interruption_dependency","type":"string","description":"Dependencies affecting business interruption."},

                        {"name":"derived_business_interruption_dependency","type":"numeric (double precision)","description":"Calculated dependency score."},

                        {"name":"business_interruption_value_basis","type":"string","description":"Basis for business interruption value."},

                        {"name":"business_interruption_indemnity_waiting_period","type":"string","description":"Waiting period for indemnity."},

                        {"name":"business_interruption_indemnity_period_limit","type":"string","description":"Limit of indemnity period."},

                        {"name":"business_interruption_value_12mo","type":"string","description":"12-month business interruption value."},

                        {"name":"ad_flag_value","type":"boolean","description":"Address data quality flag."},

                        {"name":"ad_flag_name","type":"string","description":"Name of address data quality flag."},

                        {"name":"ad_flag_msg","type":"string (text)","description":"Message for address data quality flag."},

                        {"name":"bh_flag_value","type":"boolean","description":"Building height data quality flag."},

                        {"name":"bh_flag_msg","type":"string (text)","description":"Message for building height flag."},

                        {"name":"ct_flag_value","type":"boolean","description":"Construction type data quality flag."},

                        {"name":"ct_flag_msg","type":"string (text)","description":"Message for construction type flag."},

                        {"name":"gc_flag_value_new","type":"boolean","description":"Geocoding data quality flag."},

                        {"name":"gc_rule_desc","type":"string (text)","description":"Geocoding rule description."},

                        {"name":"gc_rule_id","type":"string","description":"Geocoding rule ID."},

                        {"name":"fmt_flag_value","type":"boolean","description":"Format data quality flag."},

                        {"name":"fmt_rule_desc","type":"string (text)","description":"Format rule description."},

                        {"name":"fmt_rule_id","type":"string","description":"Format rule ID."},

                        {"name":"eng_flag_value","type":"boolean","description":"Engineering data quality flag."},

                        {"name":"eng_rule_desc","type":"string (text)","description":"Engineering rule description."},

                        {"name":"eng_rule_id","type":"string","description":"Engineering rule ID."},

                        {"name":"values_flag_value","type":"boolean","description":"Values data quality flag."},

                        {"name":"values_rule_desc","type":"string","description":"Values rule description."},

                        {"name":"values_rule_id","type":"string","description":"Values rule ID."},

                        {"name":"dq_flags_status","type":"string","description":"Overall data quality flags status."},

                        {"name":"created_by","type":"string","description":"User who created the record."},

                        {"name":"created_timestamp","type":"timestamp","description":"Timestamp when the record was created."},

                        {"name":"last_modified_by","type":"string","description":"User who last modified the record."},

                        {"name":"last_updated_timestamp","type":"timestamp","description":"Timestamp of last update."},

                        {"name":"comments","type":"string","description":"General comments about the record."},

                        {"name":"comments_description","type":"string","description":"Detailed comment description."},

                        {"name":"un_mapped_fields","type":"jsonb","description":"Additional fields stored as JSON."},

                        {"name":"location_summary_status","type":"string","description":"Summary status of the location."},

                        {"name":"business_unit","type":"string","description":"Business unit associated with the property."},

                        {"name":"owned_or_leased","type":"string","description":"Ownership status (owned or leased)."},

                        {"name":"security","type":"string","description":"Security measures at the property."},

                        {"name":"no_of_employees","type":"string","description":"Number of employees at the location."},

                        {"name":"percent_day_shift","type":"string","description":"Percentage of employees on day shift."},

                        {"name":"percent_evening_shift","type":"string","description":"Percentage of employees on evening shift."},

                        {"name":"percent_night_shift","type":"string","description":"Percentage of employees on night shift."},

                        {"name":"risk_accumulation_zone","type":"string","description":"Zone for risk accumulation analysis."},

                        {"name":"age","type":"string","description":"Age of the property."},

                        {"name":"site_hazard","type":"string","description":"Site-specific hazard details."},

                        {"name":"tree_density","type":"string","description":"Density of trees around the property."},

                        {"name":"wind_missile","type":"string","description":"Risk of wind-driven missiles."},

                        {"name":"envelope_opening","type":"string","description":"Details of envelope openings."},

                        {"name":"acquisition_date","type":"string","description":"Date of property acquisition."},

                        {"name":"is_acquisition_date_updated","type":"boolean","description":"Indicates if acquisition date was updated."}

                    ]

 

        return schema_info

 

    except Exception as e:

        logger.error(f"Schema retrieval error: {str(e)}")

        raise HTTPException(status_code=500, detail=f"Error retrieving schema: {str(e)}")

 

 

@app.get("/api/suggestions")

async def get_query_suggestions(

        q: str = "",

        limit: int = 5,

        company_number: str = Header(...),

        user_id: str = Header(...)

):

    """Get query suggestions based on partial input"""

    try:

        if not q or len(q) < 2:

            return []

 

        suggestions = [

            "What is the total insured value by state?",

            "Show me properties with high earthquake risk",

            "List all buildings built after year 2000",

            "Properties in high flood zones",

            "Average TIV by construction type",

            "Map of all property locations",

            "Provide a list Buildings without sprinkler systems",

            "Revenue distribution by business unit",

            "Properties with basement flood risk",

            "Construction quality analysis by region"

        ]

 

        # Filter suggestions based on query

        filtered_suggestions = [

            s for s in suggestions

            if q.lower() in s.lower()

        ]

 

        return filtered_suggestions[:limit]

 

    except Exception as e:

        logger.error(f"Suggestions error: {str(e)}")

        return []

 

 

@app.post("/api/bookmark")

async def bookmark_query(

        request: Dict[str, str],

        company_number: str = Header(...),

        user_id: str = Header(...),

        db: SessionLocal = Depends(get_db)

):

    """Bookmark a query for later use"""

    try:

        query_id = request.get("query_id")

        question = request.get("question")

 

        if not query_id or not question:

            raise HTTPException(status_code=400, detail="Missing query_id or question")

 

        # Insert bookmark

        db.execute(

            text("""

                INSERT INTO bookmarked_queries (query_id, question, company_number, user_id, created_timestamp)

                VALUES (:query_id, :question, :company_number, :user_id, :timestamp)

                ON CONFLICT (query_id, user_id) DO UPDATE SET

                question = EXCLUDED.question,

                created_timestamp = EXCLUDED.created_timestamp

            """),

            {

                "query_id": query_id,

                "question": question,

                "company_number": company_number,

                "user_id": user_id,

                "timestamp": datetime.utcnow()

            }

        )

        db.commit()

 

        return {"success": True, "message": "Query bookmarked successfully"}

 

    except Exception as e:

        logger.error(f"Bookmark error: {str(e)}")

        raise HTTPException(status_code=500, detail=f"Error bookmarking query: {str(e)}")

 

 

@app.get("/api/bookmarks")

async def get_bookmarks(

        company_number: str = Header(...),

        user_id: str = Header(...),

        db: SessionLocal = Depends(get_db)

):

    """Get user's bookmarked queries"""

    try:

        result = db.execute(

            text("""

                SELECT query_id, question, created_timestamp

                FROM bookmarked_queries

                WHERE company_number = :company_number AND user_id = :user_id

                ORDER BY created_timestamp DESC

                LIMIT 50

            """),

            {"company_number": company_number, "user_id": user_id}

        )

 

        bookmarks = [dict(row) for row in result.mappings()]

        return bookmarks

 

    except Exception as e:

        logger.error(f"Bookmarks retrieval error: {str(e)}")

        raise HTTPException(status_code=500, detail=f"Error retrieving bookmarks: {str(e)}")

 

 

@app.delete("/api/bookmark/{query_id}")

async def remove_bookmark(

        query_id: str,

        company_number: str = Header(...),

        user_id: str = Header(...),

        db: SessionLocal = Depends(get_db)

):

    """Remove a bookmarked query"""

    try:

        db.execute(

            text("""

                DELETE FROM bookmarked_queries

                WHERE query_id = :query_id AND company_number = :company_number AND user_id = :user_id

            """),

            {

                "query_id": query_id,

                "company_number": company_number,

                "user_id": user_id

            }

        )

        db.commit()

 

        return {"success": True, "message": "Bookmark removed successfully"}

 

    except Exception as e:

        logger.error(f"Bookmark removal error: {str(e)}")

        raise HTTPException(status_code=500, detail=f"Error removing bookmark: {str(e)}")

 

 

@app.get("/api/stats")

async def get_user_stats(

        company_number: str = Header(...),

        user_id: str = Header(...),

        db: SessionLocal = Depends(get_db)

):

    """Get user statistics and insights"""

    try:

        # Get query count by response type

        query_stats = db.execute(

            text("""

                SELECT response_type, COUNT(*) as count

                FROM chat_history

                WHERE company_number = :company_number AND user_id = :user_id

                AND timestamp >= :since

                GROUP BY response_type

            """),

            {

                "company_number": company_number,

                "user_id": user_id,

                "since": datetime.utcnow() - timedelta(days=30)

            }

        )

 

        # Get recent activity

        recent_activity = db.execute(

            text("""

                SELECT DATE(timestamp) as date, COUNT(*) as queries

                FROM chat_history

                WHERE company_number = :company_number AND user_id = :user_id

                AND timestamp >= :since

                GROUP BY DATE(timestamp)

                ORDER BY DATE(timestamp) DESC

                LIMIT 7

            """),

            {

                "company_number": company_number,

                "user_id": user_id,

                "since": datetime.utcnow() - timedelta(days=7)

            }

        )

 

        stats = {

            "query_types": [dict(row) for row in query_stats.mappings()],

            "recent_activity": [dict(row) for row in recent_activity.mappings()],

            "generated_at": datetime.utcnow()

        }

 

        return stats

 

    except Exception as e:

        logger.error(f"Stats retrieval error: {str(e)}")

        raise HTTPException(status_code=500, detail=f"Error retrieving stats: {str(e)}")

 

 

@app.post("/api/feedback")

async def submit_feedback(

        request: Dict[str, Any],

        company_number: str = Header(...),

        user_id: str = Header(...),

        db: SessionLocal = Depends(get_db)

):

    """Submit user feedback on query results"""

    try:

        query_id = request.get("query_id")

        rating = request.get("rating")  # 1-5 scale

        feedback_text = request.get("feedback", "")

        helpful = request.get("helpful", True)

 

        if not query_id or not rating:

            raise HTTPException(status_code=400, detail="Missing query_id or rating")

 

        # Insert feedback

        db.execute(

            text("""

                INSERT INTO query_feedback (query_id, company_number, user_id, rating, feedback_text, helpful, created_timestamp)

                VALUES (:query_id, :company_number, :user_id, :rating, :feedback_text, :helpful, :timestamp)

                ON CONFLICT (query_id, user_id) DO UPDATE SET

                rating = EXCLUDED.rating,

                feedback_text = EXCLUDED.feedback_text,

                helpful = EXCLUDED.helpful,

                created_timestamp = EXCLUDED.created_timestamp

            """),

            {

                "query_id": query_id,

                "company_number": company_number,

                "user_id": user_id,

                "rating": rating,

                "feedback_text": feedback_text,

                "helpful": helpful,

                "timestamp": datetime.utcnow()

            }

        )

        db.commit()

 

        return {"success": True, "message": "Feedback submitted successfully"}

 

    except Exception as e:

        logger.error(f"Feedback submission error: {str(e)}")

        raise HTTPException(status_code=500, detail=f"Error submitting feedback: {str(e)}")

 

 

# Database schema for chat history

@app.on_event("startup")

async def startup_event():

    with engine.connect() as connection:

        # Create chat history table

        connection.execute(

            text("""

                CREATE TABLE IF NOT EXISTS chat_history (

                    query_id VARCHAR PRIMARY KEY,

                    question TEXT NOT NULL,

                    sql_query TEXT,

                    response_type VARCHAR NOT NULL,

                    company_number VARCHAR NOT NULL,

                    user_id VARCHAR NOT NULL,

                    timestamp TIMESTAMP NOT NULL

                )

            """)

        )

 

        # Create bookmarks table

        connection.execute(

            text("""

                CREATE TABLE IF NOT EXISTS bookmarked_queries (

                    id SERIAL PRIMARY KEY,

                    query_id VARCHAR NOT NULL,

                    question TEXT NOT NULL,

                    company_number VARCHAR NOT NULL,

                    user_id VARCHAR NOT NULL,

                    created_timestamp TIMESTAMP NOT NULL,

                    UNIQUE(query_id, user_id)

                )

            """)

        )

 

        # Create feedback table

        connection.execute(

            text("""

                CREATE TABLE IF NOT EXISTS query_feedback (

                    id SERIAL PRIMARY KEY,

                    query_id VARCHAR NOT NULL,

                    company_number VARCHAR NOT NULL,

                    user_id VARCHAR NOT NULL,

                    rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),

                    feedback_text TEXT,

                    helpful BOOLEAN DEFAULT TRUE,

                    created_timestamp TIMESTAMP NOT NULL,

                    UNIQUE(query_id, user_id)

                )

            """)

        )

 

        # Create indexes for better performance

        connection.execute(

            text("""

                CREATE INDEX IF NOT EXISTS idx_chat_history_user

                ON chat_history(company_number, user_id, timestamp DESC)

            """)

        )

 

        connection.execute(

            text("""

                CREATE INDEX IF NOT EXISTS idx_bookmarks_user

                ON bookmarked_queries(company_number, user_id, created_timestamp DESC)

            """)

        )

 

        connection.commit()

 

 

if __name__ == "__main__":

    import uvicorn

 

    uvicorn.run(app, host="0.0.0.0", port=8000)
