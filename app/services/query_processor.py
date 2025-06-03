import os
import json
import re
from typing import Dict, Tuple
import pandas as pd
from fastapi import HTTPException
from app.services.openai_service import OpenAIService
from app.models.schemas import QueryClassification
from app.config.settings import settings
from app.utils.logging import logger

class QueryProcessor:
    def __init__(self):
        self.openai_service = OpenAIService()

    def classify_question(self, question: str) -> QueryClassification:
        """Classify the question and determine how to handle it"""
        try:
            # Load schema from file
            if not os.path.exists(settings.schema_file_path):
                raise HTTPException(
                    status_code=500,
                    detail=f"Schema file not found at {settings.schema_file_path}"
                )

            with open(settings.schema_file_path, 'r') as file:
                try:
                    schema = json.load(file)
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON in schema file: {str(e)}")
                    raise HTTPException(
                        status_code=500,
                        detail=f"Failed to parse schema file: Invalid JSON format"
                    )

            prompt = self._build_classification_prompt(question, schema)
            response = self.openai_service.call_with_retry(prompt)
            
            try:
                result = json.loads(response)
                return QueryClassification(**result)
            except json.JSONDecodeError:
                logger.warning("Failed to parse JSON response, using fallback parsing")
                return self._fallback_classification(question, response)

        except Exception as e:
            logger.error(f"Question classification error: {str(e)}")
            return QueryClassification(
                category="unrelated",
                is_safe=True,
                confidence=0.0,
                reasoning=f"Classification failed: {str(e)}"
            )

    def _build_classification_prompt(self, question: str, schema: Dict) -> str:
        """Build the classification prompt"""
        return f"""

        Schema: {json.dumps(schema, indent=2)}

        Analyze the following question and classify it into one of these categories:
        - If the question references column names found in the schema and contains terms like "count", "sum", "average", "list", "show", "group by", or "distribution", classify it as "sql_convertible".
        - If the question directly requests insights, trends, or analysis from data without specific reference to SQL operations or schema columns, classify it as "data_insights".
        - For all others, follow the below rules.

        1. "sql_convertible" - Can be converted into a SQL query for the database table `ux_all_info_consolidated`. If the question contains columns in the `ux_all_info_consolidated` table, consider it as a SQL Query.
        Examples:
        - "What is the total insured value for company X?"
        - "List all properties in California."
        - "How many buildings are owned by company Y?"
        - "Show me the distribution of the TIV by countries as a bar chart."
        - "Count the number of locations for this company?"
        - "Plot locations on a map."

        2. "property_risk_insurance" - Related to property risk management, insurance concepts, or industry knowledge. Topics include, but are not limited to, exposure management, property risk management, property risk engineering, risk assessment, insurance policies, claims processes, regulatory requirements, limits, industry trends, and best practices in risk management.
        Examples:
        - "What are the best practices for risk assessment?"
        - "How does CAT modeling work?"
        - "What does COPE stand for?"
        - "What does Risk to Capital mean in Property Insurance?"
        - "What is a Schedule of Value file?"
        - "What is the importance of Data Quality in Property Risk Management?"

        3. "data_insights" - Asking for insights, analysis, or patterns from company data.
        Examples:
        - "What trends can be observed in the total insured values over the last five years?"
        - "Analyze the risk factors for properties in flood zones."
        - "Show me the key data insights."
        - "Provide me a summary data of my SOV."

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

        Clean the response object to just contain the JSON object and no leading and trailing content.
        """

    def _fallback_classification(self, question: str, response: str) -> QueryClassification:
        """Fallback classification when JSON parsing fails"""
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

        return QueryClassification(
            category=category,
            is_safe=is_safe,
            confidence=0.7,
            reasoning="Fallback parsing used"
        )

    def generate_contextual_response(self, question: str) -> str:
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

            return self.openai_service.call_with_retry(prompt)

        except Exception as e:
            logger.error(f"Contextual response generation error: {str(e)}")
            return f"I apologize, but I'm unable to generate a response at this time due to a technical issue: {str(e)}"

    def generate_data_insights(self, question: str, company_data: pd.DataFrame) -> str:
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

            Provide actionable insights, trends, patterns, and recommendations based on the data.
            Format your response in a clear, professional manner suitable for risk management decision-making.
            """

            return self.openai_service.call_with_retry(prompt)

        except Exception as e:
            logger.error(f"Data insights generation error: {str(e)}")
            return f"I apologize, but I'm unable to generate insights at this time due to a technical issue: {str(e)}"

    def generate_sql(self, question: str, company_number: str) -> str:
        """Generate SQL query from natural language question"""
        try:
            # Load schema from file
            if not os.path.exists(settings.schema_file_path):
                raise HTTPException(
                    status_code=500,
                    detail=f"Schema file not found at {settings.schema_file_path}"
                )

            with open(settings.schema_file_path, 'r') as file:
                try:
                    schema = json.load(file)
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON in schema file: {str(e)}")
                    raise HTTPException(
                        status_code=500,
                        detail=f"Failed to parse schema file: Invalid JSON format"
                    )

            prompt = self._build_sql_generation_prompt(question, company_number, schema)
            response = self.openai_service.call_with_retry(prompt)
            
            sql_query = response.strip()
            sql_query = re.sub(r'^```sql\s*|\s*```$', '', sql_query, flags=re.MULTILINE).strip()

            if not sql_query:
                logger.error("Generated SQL query is empty.")
                return ""

            logger.info(f"Generated SQL: {sql_query}")
            return sql_query

        except Exception as e:
            logger.error(f"SQL generation error: {str(e)}")
            return ""

    def _build_sql_generation_prompt(self, question: str, company_number: str, schema: Dict) -> str:
        """Build the SQL generation prompt"""
        return f"""
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
            25. `yeat_built` values that are not known are defaulted to 12/31/99.
            26. For cladding type, replacing blanks with UNKNOWN
            8. Handle column name variations and add underscores where needed.
            9. For invalid columns, use '0' (as a string) or NULL.
            29. Analyse the user intent in detail and add a GROUP BY only when you have identified that the user intent is to view aggregated data.
            11. Since all data is in `ux_all_info_consolidated`, avoid joins unless explicitly required.
            12. For monetary columns 'business_interrupt_val', ensure proper casting to NUMERIC and handle non-numeric characters using REGEXP_REPLACE.
            13. When user asks for the tiv or total insured value or total_insured_value always refer to the derived_total_insured_value column.Don't ever refer to total_insured_value colunm in any case
            14. Strictly refuse to execute queries for company_number other than the one passed in the request header and is different from {company_number}
            15. ****If the user question {question} contains terms like 'average', 'group', 'aggregate', 'count', the SQL query must always group on the requested column to form a valid query. Don't ever include any other columns.****
            16. Use case-insensitive searches with `ILIKE` or `LOWER()` for string columns (e.g., `state`, `construction`).
            26. If fema_flood_zone is empty, replace with UNKNOWN.
            27. If user asks for flood zone, include fema and nathan fields.
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
            - Question 'I want to group my locations in the derived tiv ranges of 0 to 1000000, 1000000 to 5000000 and more than 5000000 where company_number= {company_number} further group by state'
            Query: `
            SELECT state, 
            CASE 
                WHEN derived_total_insured_value >= '0' AND derived_total_insured_value < '1000000' THEN '0 to 1000000' 
                WHEN derived_total_insured_value >= '1000000' AND derived_total_insured_value < '5000000' THEN '1000000 to 5000000' 
                ELSE 'more than 5000000' 
            END AS tiv_range, 
            COUNT(*) 
            FROM ux_all_info_consolidated
            WHERE company_number = '{company_number}' 
            GROUP BY state, tiv_range;
            `
            - Question 'can you show how has business interruption value changed over time'
            Query: `
            SELECT year_built, 
            SUM(CAST(NULLIF(REGEXP_REPLACE(COALESCE(wf_dq_summary.business_interrupt_val, '0'), '[^\d]', '', 'g'), '') AS NUMERIC)) AS total_bi_value 
            FROM ux_all_info_consolidated
            WHERE company_number = '{company_number}' 
            GROUP BY year_built 
            ORDER BY year_built;
            - Question 'Show me the nathan_flash_flood_hazardzone, nathan_river_flood_hazardzone and fema_flood_zone for my sov'
            Query: `
            SELECT marsh_location_id, nathan_flash_flood_hazardzone, nathan_river_flood_hazardzone,COALESCE(NULLIF(fema_flood_zone, ''), 'UNKNOWN') AS fema_flood_zone
            FROM ux_all_info_consolidated
            WHERE company_number = '{company_number}' 
            - Question 'Plot locations on a map'
            Query: `
            SELECT marsh_location_id, latitude, longitude
            FROM ux_all_info_consolidated
            WHERE company_number = '{company_number}' 
            `
            
            22. Return only the SQL query. Ensure it:
            - Avoids any DML (INSERT, UPDATE, DELETE) or DDL (DROP, CREATE) operations
            - Excludes executable functions
            If the question cannot be converted safely, return an empty string.
        """

    def generate_explanation(self, question: str, sql_query: str) -> Tuple[str, str]:
        """Generate explanation and summary for the SQL query"""
        try:
            explanation_prompt = f"""
            You are a SQL expert. Given the following SQL query, explain it in simple terms for a non-technical user.
            - Break it down step by step.
            - Describe what the query does.
            - Mention filtering conditions, aggregations, and joins if any.

            Question: {question}
            SQL Query: {sql_query}
            Provide a detailed and clear explanation.
            """
            explanation = self.openai_service.call_with_retry(explanation_prompt)

            summary_prompt = f"""
            Based on the following question and SQL query, provide a brief 2-3 line summary that explains the AI approach used to answer this question. Focus on what the AI analyzed and how it processed the request.

            Question: {question}
            SQL Query: {sql_query}
            
            Provide only a concise 2-3 line summary of the AI approach.
            """

            summary = self.openai_service.call_with_retry(summary_prompt)

            return explanation, summary

        except Exception as e:
            logger.error(f"Explanation generation error: {str(e)}")
            return f"Unable to generate explanation: {str(e)}", f"Unable to generate summary: {str(e)}"