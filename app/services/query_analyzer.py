from typing import Dict

class QueryAnalyzer:
    """Helper class to analyze queries and provide contextual responses"""
   
    @staticmethod
    def analyze_no_data_context(question: str, sql_query: str, company_number: str) -> Dict:
        """Analyze why a query returned no data and provide contextual help"""
       
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
            if 'state' in sql_lower:
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