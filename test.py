# test_visualization.py
from app.services.visualization_recommender_service import VisualizationRecommenderService
import pandas as pd

class TestVisualization:
    def __init__(self):
        self.viz_service = VisualizationRecommenderService()
    
    def test_state_query(self):
        # Your problematic query data
        state_data = pd.DataFrame({
            'state': ['CA', 'TX', 'NY', 'FL', 'IL'],
            'total_tiv': [1500000.50, 2300000.75, 1800000.25, 1200000.00, 950000.80]
        })
        
        sql_query = """SELECT state, SUM(derived_total_insured_value) AS total_tiv
        FROM ux_all_info_consolidated
        WHERE company_number = 'CN101741403'
        GROUP BY state"""
        
        # Call the main recommend method
        result = self.viz_service.recommend(
            sql_query=sql_query,
            dataframe=state_data
        )
        
        print("Result:", result)
        return result
    
    def test_detailed_analysis(self):
        # Get detailed recommendation for debugging
        state_data = pd.DataFrame({
            'state': ['CA', 'TX', 'NY'],
            'total_tiv': [1500000.50, 2300000.75, 1800000.25]
        })
        
        detailed = self.viz_service.recommend(
            sql_query="SELECT state, SUM(amount) FROM data GROUP BY state",
            dataframe=state_data
        )
        
        print(f"Chart Type: {detailed}")
        return detailed

# Usage
if __name__ == "__main__":
    test = TestVisualization()
    test.test_state_query()
    test.test_detailed_analysis()