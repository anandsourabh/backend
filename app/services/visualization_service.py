import pandas as pd
from typing import Dict, Optional
from app.services.openai_service import OpenAIService
from app.utils.logging import logger

class VisualizationService:
    def __init__(self):
        self.openai_service = OpenAIService()

    def should_show_visualization(self, df: pd.DataFrame, sql_query: str) -> bool:
        """Determine if visualization should be shown"""
        # Conditions to HIDE visualization
        if len(df) == 1 and len(df.columns) == 1:  # Single-value results
            return False
        if any(keyword in sql_query.upper() for keyword in
               ["SUM(", "AVG(", "COUNT(", "MAX(", "MIN(", "DISTINCT"]):
            return len(df) > 1  # Only show for multi-row aggregations
        return True

    def suggest_visualization(
        self, data: pd.DataFrame, question: str, visualization_type: Optional[str], sql_query: Optional[str] = None
    ) -> Optional[Dict]:
        """Suggest and generate Plotly visualization"""
        try:
            if visualization_type:
                chart_type = visualization_type
            else:
                prompt = f"""
                You are a data scientist analyzing data for visualization.
                Based on the question, SQL query and sample data, suggest the best visualization type.

                If the SQL query contains aggregation functions (SUM, AVG, COUNT, etc.) and returns only 1 row, return None.
                If the data contains only 1 row, return None.
                If the number of columns is large and not suitable for visualization, return None.

                For geographic data (latitude, longitude), strictly suggest 'map'.
                For two categorical columns, suggest 'stackedbar'.
                For time series data, suggest 'areachart'.

                Data: {data.head().to_string() if not data.empty else 'No data'}
                Question: {question}
                SQL Query: {sql_query}

                Return only the visualization type or None.
                Respond in this format:
                Chart Type: <chart_type>
                X-axis: <column>
                Y-axis: <column>
                Color: <stacking_column> (only for stacked bar chart)
                """

                response = self.openai_service.call_with_retry(prompt)
                
                if "None" in response or response.strip().lower() == "none":
                    return None
                    
                return self._parse_visualization_response(response)

        except Exception as e:
            logger.error(f"Visualization generation error: {str(e)}")
            return None

    def _parse_visualization_response(self, response: str) -> Dict[str, str]:
        """Parse the visualization response into a dictionary"""
        response_dict = {}
        for line in response.split("\n"):
            if line.strip() and ":" in line:
                key, value = line.split(":", 1)
                response_dict[key.strip()] = value.strip()
        return response_dict
