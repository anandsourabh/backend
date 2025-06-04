import re
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional


class VisualizationService:
    """
    A service that recommends visualization types based on SQL queries and dataframes.
    
    Chart selection rules:
    - Pie chart: For aggregations with <= 5 categories
    - Bar chart: For aggregations with > 5 categories or categorical comparisons
    - Line/Area: For time series data
    - Scatter: For correlations between numeric variables
    - Map: When latitude/longitude are detected
    
    Usage:
        service = VisualizationService()  # Default: pie chart for <= 5 categories
        recommendation = service.recommend(sql_query, dataframe)
        
        # Or customize the threshold:
        service = VisualizationService(pie_chart_threshold=3)  # Only use pie for <= 3 categories
    """
    
    def __init__(self, pie_chart_threshold=5):
        self.aggregation_functions = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'MEAN', 'MEDIAN', 'STDDEV', 'VARIANCE']
        self.time_keywords = ['date', 'time', 'timestamp', 'year', 'month', 'day', 'hour', 'created', 'updated']
        self.geo_keywords = ['latitude', 'longitude', 'lat', 'lon', 'lng', 'coord']
        self.pie_chart_threshold = pie_chart_threshold  # Use pie chart for <= N categories
    
    def recommend(self, sql_query: str, df: pd.DataFrame) -> str:
        """
        Main method to get visualization recommendation.
        
        Args:
            sql_query: SQL query string
            df: Pandas DataFrame containing the data
            
        Returns:
            String in format:
            Chart Type:<chart_type>
            X-axis:<value>
            Y-axis:<value>
            Color:<colors>
        """
        try:
            # Handle empty dataframe
            if df.empty:
                return "None"
            if len(df) == 1 and len(df.columns) == 1:
                return "None"
            
            # Analyze SQL and dataframe
            sql_info = self._analyze_sql(sql_query)
            df_info = self._analyze_dataframe(df)
            
            # Check for map visualization
            if sql_info['has_geo'] and self._can_create_map(df):
                return self._create_map_recommendation(df)
            
            # Determine best visualization
            chart_type, x_axis, y_axis, color = self._determine_visualization(
                sql_info, df_info, df
            )
            
            return f"Chart Type:{chart_type}\nX-axis:{x_axis}\nY-axis:{y_axis}\nColor:{color}"
            
        except Exception as e:
            # Return safe default on any error
            return "Chart Type:bar\nX-axis:category\nY-axis:value\nColor:None"
    
    def recommend_with_debug(self, sql_query: str, df: pd.DataFrame) -> Tuple[str, Dict]:
        """
        Get recommendation with debugging information.
        
        Returns:
            Tuple of (recommendation_string, debug_info_dict)
        """
        sql_info = self._analyze_sql(sql_query)
        df_info = self._analyze_dataframe(df)
        recommendation = self.recommend(sql_query, df)
        
        debug_info = {
            'dataframe_columns': list(df.columns),
            'numeric_columns': df_info['numeric_columns'],
            'categorical_columns': df_info['categorical_columns'],
            'has_aggregation': sql_info['has_aggregation'],
            'has_group_by': sql_info['has_group_by'],
            'group_by_columns': sql_info['group_by_columns'],
            'recommendation': recommendation
        }
        
        return recommendation, debug_info
    
    def _analyze_sql(self, sql_query: str) -> Dict:
        """Analyze SQL query structure."""
        sql_upper = sql_query.upper()
        
        # Extract key SQL components
        has_geo = any(geo in sql_upper for geo in ['LATITUDE', 'LONGITUDE', 'LAT', 'LON'])
        has_aggregation = any(func in sql_upper for func in self.aggregation_functions)
        has_group_by = 'GROUP BY' in sql_upper
        has_order_by = 'ORDER BY' in sql_upper
        is_select_all = bool(re.search(r'SELECT\s*\*', sql_upper))
        
        # Extract GROUP BY columns
        group_by_cols = []
        if has_group_by:
            match = re.search(r'GROUP BY\s+(.*?)(?:ORDER BY|HAVING|LIMIT|;|$)', 
                            sql_query, re.IGNORECASE | re.DOTALL)
            if match:
                cols_text = match.group(1).strip()
                group_by_cols = [col.strip().split('.')[-1].strip('"\'') 
                               for col in cols_text.split(',')]
        
        return {
            'has_geo': has_geo,
            'has_aggregation': has_aggregation,
            'has_group_by': has_group_by,
            'has_order_by': has_order_by,
            'is_select_all': is_select_all,
            'group_by_columns': group_by_cols
        }
    
    def _analyze_dataframe(self, df: pd.DataFrame) -> Dict:
        """Analyze dataframe characteristics."""
        numeric_cols = []
        categorical_cols = []
        datetime_cols = []
        unique_counts = {}
        
        for col in df.columns:
            if df[col].isna().all():
                continue
            
            unique_counts[col] = df[col].nunique()
            
            if pd.api.types.is_numeric_dtype(df[col]):
                numeric_cols.append(col)
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                datetime_cols.append(col)
            elif self._is_datetime_column(df[col], col):
                datetime_cols.append(col)
            else:
                categorical_cols.append(col)
        
        return {
            'numeric_columns': numeric_cols,
            'categorical_columns': categorical_cols,
            'datetime_columns': datetime_cols,
            'unique_counts': unique_counts,
            'row_count': len(df),
            'has_time_series': len(datetime_cols) > 0
        }
    
    def _is_datetime_column(self, series: pd.Series, col_name: str) -> bool:
        """Check if column contains datetime data."""
        if any(keyword in col_name.lower() for keyword in self.time_keywords):
            try:
                pd.to_datetime(series.dropna().head(10))
                return True
            except:
                pass
        return False
    
    def _can_create_map(self, df: pd.DataFrame) -> bool:
        """Check if dataframe has latitude and longitude columns."""
        cols_lower = [col.lower() for col in df.columns]
        has_lat = any('lat' in col for col in cols_lower)
        has_lon = any('lon' in col or 'lng' in col for col in cols_lower)
        return has_lat and has_lon
    
    def _determine_visualization(self, sql_info: Dict, df_info: Dict, 
                               df: pd.DataFrame) -> Tuple[str, str, str, str]:
        """Determine the best visualization type."""
        numeric_cols = df_info['numeric_columns']
        categorical_cols = df_info['categorical_columns']
        datetime_cols = df_info['datetime_columns']
        unique_counts = df_info['unique_counts']
        
        # Case 1: Aggregation with GROUP BY
        if sql_info['has_aggregation'] and sql_info['has_group_by']:
            # Find group column - this will be X-axis
            group_col = self._find_group_column(sql_info['group_by_columns'], df.columns)
            if not group_col and categorical_cols:
                group_col = categorical_cols[0]
            elif not group_col:
                group_col = list(df.columns)[0]
            
            # Find aggregated column - this will be Y-axis
            # For aggregations, the numeric column that's NOT in group_by is likely the result
            agg_col = None
            for col in df.columns:
                if col in numeric_cols and col not in sql_info['group_by_columns']:
                    agg_col = col
                    break
            
            # If not found, look for columns with aggregation keywords
            if not agg_col:
                agg_keywords = ['sum', 'count', 'avg', 'average', 'total', 'min', 'max', 'mean']
                for col in df.columns:
                    if any(keyword in col.lower() for keyword in agg_keywords):
                        agg_col = col
                        break
            
            # Final fallback
            if not agg_col and numeric_cols:
                agg_col = numeric_cols[0]
            elif not agg_col:
                # Use the second column if first is the group column
                remaining_cols = [c for c in df.columns if c != group_col]
                agg_col = remaining_cols[0] if remaining_cols else 'value'
            
            # Decide between pie and bar based on unique count
            if unique_counts.get(group_col, 0) <= self.pie_chart_threshold:
                return 'pie', group_col, agg_col, 'category'
            else:
                return 'bar', group_col, agg_col, 'None'
        
        # Case 2: Time series
        if df_info['has_time_series'] and datetime_cols and numeric_cols:
            chart_type = 'area' if df_info['row_count'] > 50 else 'line'
            return chart_type, datetime_cols[0], numeric_cols[0], 'None'
        
        # Case 3: Two numeric columns (scatter)
        if len(numeric_cols) >= 2 and not sql_info['has_aggregation']:
            if df_info['row_count'] < 1000:
                return 'scatter', numeric_cols[0], numeric_cols[1], 'None'
        
        # Case 4: Categorical + numeric
        if categorical_cols and numeric_cols:
            cat_col = self._select_best_categorical(categorical_cols, unique_counts)
            
            # Pie for limited categories
            if unique_counts.get(cat_col, 0) <= self.pie_chart_threshold and len(numeric_cols) == 1:
                return 'pie', cat_col, numeric_cols[0], 'category'
            
            # Stacked bar for multiple numeric
            if len(numeric_cols) > 1:
                return 'stacked_bar', cat_col, numeric_cols[0], numeric_cols[1]
            
            return 'bar', cat_col, numeric_cols[0], 'None'
        
        # Case 5: Only numeric
        if numeric_cols:
            if len(numeric_cols) == 1:
                return 'bar', 'index', numeric_cols[0], 'None'
            else:
                return 'scatter', numeric_cols[0], numeric_cols[1], 'None'
        
        # Default - use actual column names from dataframe
        cols = list(df.columns)
        x_col = cols[0] if cols else 'index'
        y_col = cols[1] if len(cols) > 1 else cols[0] if cols else 'value'
        return 'bar', x_col, y_col, 'None'
    
    def _find_group_column(self, group_cols: List[str], df_columns: List[str]) -> Optional[str]:
        """Find group by column in dataframe columns."""
        df_cols_lower = {col.lower(): col for col in df_columns}
        
        for gc in group_cols:
            # Direct match
            if gc in df_columns:
                return gc
            # Case-insensitive match
            if gc.lower() in df_cols_lower:
                return df_cols_lower[gc.lower()]
        
        return None
    
    def _select_best_categorical(self, cat_cols: List[str], 
                                unique_counts: Dict[str, int]) -> str:
        """Select best categorical column for visualization."""
        best_col = cat_cols[0]
        best_count = unique_counts.get(best_col, float('inf'))
        
        for col in cat_cols[1:]:
            count = unique_counts.get(col, float('inf'))
            if 2 <= count <= 20 and (count < best_count or best_count > 20):
                best_col = col
                best_count = count
        
        return best_col
    
    def _create_map_recommendation(self, df: pd.DataFrame) -> str:
        """Create recommendation for map visualization."""
        lat_col = lon_col = None
        
        for col in df.columns:
            col_lower = col.lower()
            if not lat_col and 'lat' in col_lower:
                lat_col = col
            elif not lon_col and ('lon' in col_lower or 'lng' in col_lower):
                lon_col = col
        
        # Find value column for coloring
        color_col = 'None'
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if numeric_cols:
            color_candidates = [c for c in numeric_cols if c not in [lat_col, lon_col]]
            if color_candidates:
                color_col = color_candidates[0]
        
        return f"Chart Type:map\nX-axis:{lon_col or 'longitude'}\nY-axis:{lat_col or 'latitude'}\nColor:{color_col}"


# Simple usage example
if __name__ == "__main__":
    # Initialize service
    service = VisualizationService()
    
    # Example 1: Aggregation query (4 categories = pie chart)
    df1 = pd.DataFrame({
        'category': ['A', 'B', 'C', 'D'],
        'total_sales': [150, 200, 175, 225]
    })
    query1 = "SELECT category, SUM(sales) as total_sales FROM data GROUP BY category"
    
    result1 = service.recommend(query1, df1)
    print("Example 1 - Aggregation (4 categories):")
    print(result1)
    print()
    
    # Example 1b: Aggregation with more categories (6 categories = bar chart)
    df1b = pd.DataFrame({
        'category': ['A', 'B', 'C', 'D', 'E', 'F'],
        'total_sales': [150, 200, 175, 225, 190, 210]
    })
    query1b = "SELECT category, SUM(sales) as total_sales FROM data GROUP BY category"
    
    result1b = service.recommend(query1b, df1b)
    print("Example 1b - Aggregation (6 categories):")
    print(result1b)
    print()
    
    # Test proper column mapping for bar chart
    df_test = pd.DataFrame({
        'product_name': ['Laptop', 'Phone', 'Tablet', 'Watch', 'Headphones', 'Speaker'],
        'revenue_total': [45000, 62000, 28000, 15000, 8000, 12000]
    })
    query_test = "SELECT product_name, SUM(revenue) as revenue_total FROM sales GROUP BY product_name"
    
    result_test = service.recommend(query_test, df_test)
    print("Column Mapping Test - Bar Chart (6 products):")
    print(result_test)
    print("✓ X-axis should be 'product_name'")
    print("✓ Y-axis should be 'revenue_total'")
    print()
    
    # Example 2: Time series
    df2 = pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=10),
        'value': [100, 110, 105, 115, 120, 118, 125, 130, 128, 135]
    })
    query2 = "SELECT date, value FROM metrics ORDER BY date"
    
    result2 = service.recommend(query2, df2)
    print("Example 2 - Time Series:")
    print(result2)
    print()
    
    # Example 3: Geographic data
    df3 = pd.DataFrame({
        'city': ['NYC', 'LA', 'Chicago'],
        'latitude': [40.7128, 34.0522, 41.8781],
        'longitude': [-74.0060, -118.2437, -87.6298],
        'population': [8.3, 3.9, 2.7]
    })
    query3 = "SELECT city, latitude, longitude, population FROM cities"
    
    result3 = service.recommend(query3, df3)
    print("Example 3 - Geographic:")
    print(result3)
    
    # Example showing pie chart threshold
    print("\n" + "="*50)
    print("Demonstrating pie chart threshold:")
    
    # Create service with custom threshold
    custom_service = VisualizationService(pie_chart_threshold=3)
    
    # 4 categories with default service (threshold=5) -> pie chart
    print("\nDefault service (threshold=5), 4 categories:")
    print(service.recommend(query1, df1).split('\n')[0])  # Just show chart type
    
    # 4 categories with custom service (threshold=3) -> bar chart  
    print("\nCustom service (threshold=3), 4 categories:")
    print(custom_service.recommend(query1, df1).split('\n')[0])  # Just show chart type
    
    # Debug example to check column mapping
    print("\n" + "="*50)
    print("Debug Column Mapping:")
    
    df_debug = pd.DataFrame({
        'department': ['Sales', 'Marketing', 'IT', 'HR', 'Finance', 'Operations'],
        'total_budget': [500000, 300000, 400000, 200000, 350000, 280000]
    })
    query_debug = "SELECT department, SUM(budget) as total_budget FROM budgets GROUP BY department"
    
    rec, debug = service.recommend_with_debug(query_debug, df_debug)
    print(f"\nQuery: {query_debug}")
    print(f"DataFrame columns: {debug['dataframe_columns']}")
    print(f"Detected numeric columns: {debug['numeric_columns']}")
    print(f"Detected categorical columns: {debug['categorical_columns']}")
    print(f"\nRecommendation:\n{rec}")
    print("\n✓ Verify Y-axis matches the aggregated column name from your DataFrame")