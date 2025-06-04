# app/services/visualization_recommender_service.py

import pandas as pd
import numpy as np
import re
import logging
from typing import Dict, List, Tuple, Optional, Any, Union
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum

# Configure logging
logger = logging.getLogger(__name__)

class ChartType(Enum):
    """Enumeration of supported chart types."""
    LINE = "line"
    BAR = "bar"
    STACKED_BAR = "stacked_bar"
    PIE = "pie"
    SCATTER = "scatter"
    HISTOGRAM = "histogram"
    BOX = "box"
    HEATMAP = "heatmap"
    AREA = "area"
    STACKED_AREA = "stacked_area"
    MAP = "map"
    TABLE = "table"
    NONE = "none"

@dataclass
class ColumnInfo:
    """Information about a DataFrame column."""
    dtype: str
    unique_count: int
    null_count: int
    is_numeric: bool
    is_datetime: bool
    is_categorical: bool
    is_geographic: bool
    sample_values: List[Any] = field(default_factory=list)

@dataclass
class DataFrameAnalysis:
    """Results of DataFrame analysis."""
    num_rows: int
    num_cols: int
    column_info: Dict[str, ColumnInfo]
    numeric_columns: List[str] = field(default_factory=list)
    categorical_columns: List[str] = field(default_factory=list)
    datetime_columns: List[str] = field(default_factory=list)
    text_columns: List[str] = field(default_factory=list)
    geographic_columns: List[str] = field(default_factory=list)
    latitude_columns: List[str] = field(default_factory=list)
    longitude_columns: List[str] = field(default_factory=list)
    has_missing_values: bool = False
    potential_x_columns: List[str] = field(default_factory=list)
    potential_y_columns: List[str] = field(default_factory=list)
    has_multiple_numeric_series: bool = False
    has_multiple_categories: bool = False

@dataclass
class SQLAnalysis:
    """Results of SQL query analysis."""
    has_aggregation: bool = False
    has_grouping: bool = False
    has_time_dimension: bool = False
    has_joins: bool = False
    has_where_clause: bool = False
    has_geographic_dimension: bool = False
    aggregation_functions: List[str] = field(default_factory=list)
    group_by_columns: List[str] = field(default_factory=list)
    select_columns: List[str] = field(default_factory=list)
    time_columns: List[str] = field(default_factory=list)
    geographic_columns: List[str] = field(default_factory=list)
    where_conditions: List[str] = field(default_factory=list)
    query_type: str = "simple"

@dataclass
class VisualizationRecommendation:
    """Visualization recommendation result."""
    chart_type: str
    x_axis: str
    y_axis: str
    title: str
    confidence: float
    alternatives: List[str]
    reasoning: str
    sql_analysis: Optional[SQLAnalysis] = None
    dataframe_analysis: Optional[DataFrameAnalysis] = None

class VisualizationRecommenderService:
    """
    Service class for recommending chart types and axes based on SQL queries and DataFrames.
    
    This service can be directly integrated into applications without REST endpoints.
    
    Usage:
        service = VisualizationRecommenderService()
        recommendation = service.recommend(sql_query="SELECT...", dataframe=df)
    """
    
    # Configuration constants
    MAX_PIE_CATEGORIES = 5
    MAX_BAR_CATEGORIES = 10
    MAX_CHART_CATEGORIES = 20
    MAX_TABLE_COLUMNS = 10
    MIN_HISTOGRAM_ROWS = 10
    MIN_VISUALIZATION_ROWS = 2
    MIN_VISUALIZATION_COLS = 2
    MAX_ALTERNATIVE_SUGGESTIONS = 2
    MAX_AXIS_LABEL_COLUMNS = 3
    DATETIME_DETECTION_THRESHOLD = 0.7
    LATITUDE_MIN, LATITUDE_MAX = -90, 90
    LONGITUDE_MIN, LONGITUDE_MAX = -180, 180
    
    def __init__(self):
        """Initialize the service with keyword sets and configurations."""
        self._setup_keywords()
        logger.info("VisualizationRecommenderService initialized")
    
    def _setup_keywords(self) -> None:
        """Setup keyword sets for SQL analysis."""
        self.aggregation_keywords = frozenset({
            'COUNT', 'SUM', 'AVG', 'AVERAGE', 'MIN', 'MAX', 'MEDIAN', 
            'STDDEV', 'VARIANCE', 'TOTAL'
        })
        
        self.time_keywords = frozenset({
            'DATE', 'TIME', 'YEAR', 'MONTH', 'DAY', 'WEEK', 'QUARTER',
            'DATETIME', 'TIMESTAMP', 'CREATED_AT', 'UPDATED_AT'
        })
        
        self.grouping_keywords = frozenset({
            'GROUP BY', 'PARTITION BY', 'DISTINCT', 'CATEGORY', 'TYPE',
            'STATUS', 'REGION', 'DEPARTMENT'
        })
        
        self.geo_keywords = frozenset({
            'LATITUDE','LONGITUDE'
        })
        
        self.latitude_keywords = frozenset(['LATITUDE', 'LAT', 'Y'])
        self.longitude_keywords = frozenset(['LONGITUDE', 'LON', 'LNG', 'X'])
    
    def recommend(self, 
                  sql_query: Optional[str] = None, 
                  dataframe: Optional[pd.DataFrame] = None,
                  **kwargs) -> str:
        """
        Main method to get visualization recommendations as formatted string.
        
        Args:
            sql_query: Optional SQL query string to analyze
            dataframe: Optional pandas DataFrame to analyze
            **kwargs: Additional configuration options
            
        Returns:
            Formatted string with chart type, axes, and color information
            
        Raises:
            ValueError: If neither sql_query nor dataframe is provided
            Exception: If analysis fails
        """
        try:
            if dataframe is None and sql_query is None:
                raise ValueError("Either sql_query or dataframe must be provided")
            
            logger.debug("Starting recommendation analysis")
            
            # Analyze inputs
            sql_analysis = self._analyze_sql_query(sql_query) if sql_query else None
            df_analysis = self._analyze_dataframe(dataframe) if dataframe is not None else None
            
            # Create mock analysis if needed
            if not df_analysis and sql_analysis:
                df_analysis = self._create_mock_dataframe_analysis(sql_analysis)
            
            # Generate recommendation
            chart_type = self._recommend_chart_type(sql_analysis, df_analysis)
            x_axis, y_axis = self._recommend_axes(sql_analysis, df_analysis, chart_type)
            color = self._recommend_color(sql_analysis, df_analysis, chart_type)
            
            logger.info(f"Generated recommendation: {chart_type}")
            return self._parse_visualization_response(self._format_recommendation_string(chart_type, x_axis, y_axis, color))
            
        except Exception as e:
            logger.error(f"Error in recommend: {str(e)}")
            raise
    
    def _analyze_sql_query(self, sql_query: str) -> SQLAnalysis:
        """Analyze SQL query to extract structural insights."""
        try:
            if not sql_query or not isinstance(sql_query, str):
                return SQLAnalysis()
                
            sql_upper = sql_query.upper().strip()
            if not sql_upper:
                return SQLAnalysis()
            
            analysis = SQLAnalysis()
            
            # Check for aggregation functions
            analysis.aggregation_functions = [
                func for func in self.aggregation_keywords if func in sql_upper
            ]
            analysis.has_aggregation = bool(analysis.aggregation_functions)
            
            # Check for GROUP BY
            if 'GROUP BY' in sql_upper:
                analysis.has_grouping = True
                analysis.group_by_columns = self._extract_group_by_columns(sql_upper)
            
            # Check for time-related columns in SELECT clause only
            analysis.time_columns = self._extract_time_columns(sql_upper, analysis.select_columns)
            analysis.has_time_dimension = bool(analysis.time_columns)
            
            # Check for WHERE clauses
            if 'WHERE' in sql_upper:
                analysis.has_where_clause = True
                analysis.where_conditions = self._extract_where_conditions(sql_upper)
            
            # Check for geographic columns in SELECT clause only
            analysis.geographic_columns = self._extract_geographic_columns(sql_upper, analysis.select_columns)
            analysis.has_geographic_dimension = bool(analysis.geographic_columns)
            
            # Check for JOINs
            analysis.has_joins = 'JOIN' in sql_upper
            
            # Extract SELECT columns
            analysis.select_columns = self._extract_select_columns(sql_upper)
            
            # Determine query type
            analysis.query_type = self._determine_query_type(analysis)
            
            return analysis
            
        except Exception as e:
            logger.warning(f"Error analyzing SQL query: {str(e)}")
            return SQLAnalysis()
    
    def _analyze_dataframe(self, dataframe: pd.DataFrame) -> DataFrameAnalysis:
        """Analyze DataFrame structure and content."""
        try:
            if dataframe is None or not isinstance(dataframe, pd.DataFrame):
                raise ValueError('Invalid DataFrame')
                
            if dataframe.empty:
                raise ValueError('Empty DataFrame')
            
            analysis = DataFrameAnalysis(
                num_rows=len(dataframe),
                num_cols=len(dataframe.columns),
                column_info={},
                has_missing_values=dataframe.isnull().any().any()
            )
            
            # Analyze each column
            for col in dataframe.columns:
                try:
                    col_info = self._analyze_column(col, dataframe[col])
                    analysis.column_info[col] = col_info
                    self._categorize_column(col, col_info, analysis)
                except Exception as e:
                    logger.warning(f"Failed to analyze column {col}: {str(e)}")
                    continue
            
            # Set derived properties
            analysis.has_multiple_numeric_series = len(analysis.numeric_columns) > 1
            analysis.has_multiple_categories = len(analysis.categorical_columns) > 1
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing DataFrame: {str(e)}")
            raise
    
    def _analyze_column(self, col_name: str, series: pd.Series) -> ColumnInfo:
        """Analyze individual column characteristics."""
        is_numeric = pd.api.types.is_numeric_dtype(series)
        is_datetime = pd.api.types.is_datetime64_any_dtype(series)
        unique_count = series.nunique()
        
        # Improve categorical detection
        is_categorical = False
        if not is_numeric and not is_datetime:
            # String/object columns with reasonable unique counts are categorical
            is_categorical = unique_count < self.MAX_CHART_CATEGORIES
        elif is_numeric and unique_count < self.MAX_PIE_CATEGORIES:
            # Small number of unique numeric values can be categorical
            is_categorical = True
        else:
            # Use pandas built-in categorical detection
            is_categorical = pd.api.types.is_categorical_dtype(series)
        
        return ColumnInfo(
            dtype=str(series.dtype),
            unique_count=unique_count,
            null_count=series.isnull().sum(),
            is_numeric=is_numeric,
            is_datetime=is_datetime,
            is_categorical=is_categorical,
            is_geographic=self._is_geographic_column(col_name, series),
            sample_values=self._get_safe_sample_values(series)
        )
    
    def _get_safe_sample_values(self, series: pd.Series) -> List[Any]:
        """Safely extract sample values from series."""
        try:
            return series.dropna().head(3).tolist()
        except Exception:
            return []
    
    def _categorize_column(self, col_name: str, col_info: ColumnInfo, analysis: DataFrameAnalysis) -> None:
        """Categorize column into appropriate lists."""
        if col_info.is_geographic:
            analysis.geographic_columns.append(col_name)
            if self._is_latitude_column_name(col_name):
                analysis.latitude_columns.append(col_name)
            elif self._is_longitude_column_name(col_name):
                analysis.longitude_columns.append(col_name)
        elif col_info.is_datetime:
            analysis.datetime_columns.append(col_name)
            analysis.potential_x_columns.append(col_name)
        elif col_info.is_numeric:
            analysis.numeric_columns.append(col_name)
            analysis.potential_y_columns.append(col_name)
            if col_info.unique_count < self.MAX_CHART_CATEGORIES:
                analysis.potential_x_columns.append(col_name)
        elif col_info.is_categorical:
            analysis.categorical_columns.append(col_name)
            analysis.potential_x_columns.append(col_name)
        else:
            analysis.text_columns.append(col_name)
    
    def _is_geographic_column(self, col_name: str, series: pd.Series) -> bool:
        """Check if column contains geographic data."""
        col_upper = col_name.upper()
        
        # Check column name
        if any(geo_kw in col_upper for geo_kw in self.geo_keywords):
            return True
        
        # Check value ranges for numeric columns
        if pd.api.types.is_numeric_dtype(series):
            try:
                non_null = series.dropna()
                if len(non_null) > 0:
                    min_val, max_val = float(non_null.min()), float(non_null.max())
                    # Check latitude or longitude ranges
                    if (self.LATITUDE_MIN <= min_val <= max_val <= self.LATITUDE_MAX or
                        self.LONGITUDE_MIN <= min_val <= max_val <= self.LONGITUDE_MAX):
                        return True
            except (ValueError, TypeError):
                pass
        
        return False
    
    def _looks_like_parts_of_whole(self, df_analysis: DataFrameAnalysis, cat_col: str) -> bool:
        """Check if the data looks like parts of a whole (good for pie charts)."""
        if not df_analysis.numeric_columns:
            return False
        
        numeric_col = df_analysis.numeric_columns[0]
        col_info = df_analysis.column_info[numeric_col]
        
        # Look for indicators that this represents parts of a whole:
        # 1. Column names that suggest percentages or parts
        numeric_col_lower = numeric_col.lower()
        cat_col_lower = cat_col.lower()
        
        part_indicators = ['percent', 'percentage', 'share', 'portion', 'ratio', 'proportion']
        if any(indicator in numeric_col_lower for indicator in part_indicators):
            return True
        
        # 2. Category names that suggest parts of a whole
        category_indicators = ['type', 'category', 'segment', 'group', 'class']
        if any(indicator in cat_col_lower for indicator in category_indicators):
            return True
        
        # 3. Values that look like percentages (0-100 range)
        sample_values = col_info.sample_values
        if sample_values:
            # Check if values are in 0-100 range (percentage-like)
            try:
                numeric_values = [float(v) for v in sample_values if isinstance(v, (int, float))]
                if numeric_values and all(0 <= v <= 100 for v in numeric_values):
                    return True
            except (ValueError, TypeError):
                pass
        
        # 4. Geographic or measurement data is usually NOT parts of a whole
        if cat_col_lower in ['state', 'country', 'region', 'city']:
            return False
        
        # 5. Large absolute values are usually not parts of a whole
        if sample_values:
            try:
                numeric_values = [float(v) for v in sample_values if isinstance(v, (int, float))]
                if numeric_values and any(v > 1000 for v in numeric_values):
                    return False  # Large values like TIV amounts are not parts of a whole
            except (ValueError, TypeError):
                pass
        
        # Default: for very small datasets with unclear context, lean towards not pie
        return False
    
    def _is_latitude_column_name(self, col_name: str) -> bool:
        """Check if column name indicates latitude."""
        return any(kw in col_name.upper() for kw in self.latitude_keywords)
    
    def _is_longitude_column_name(self, col_name: str) -> bool:
        """Check if column name indicates longitude."""
        return any(kw in col_name.upper() for kw in self.longitude_keywords)
    
    def _recommend_chart_type(self, sql_analysis: Optional[SQLAnalysis], 
                             df_analysis: Optional[DataFrameAnalysis]) -> str:
        """Recommend the most appropriate chart type."""
        if not df_analysis:
            return ChartType.NONE.value
        
        num_rows = df_analysis.num_rows
        num_cols = df_analysis.num_cols
        
        # Edge cases
        if num_rows == 0:
            return ChartType.NONE.value
        if num_rows == 1:
            return ChartType.NONE.value if num_cols <= 5 else ChartType.TABLE.value
        if num_cols < self.MIN_VISUALIZATION_COLS:
            return ChartType.NONE.value
        
        # Geographic visualization
        if self._should_recommend_map(sql_analysis, df_analysis):
            return ChartType.MAP.value
        
        # Time series visualization
        if self._has_time_dimension(sql_analysis, df_analysis):
            return ChartType.LINE.value if df_analysis.numeric_columns else ChartType.BAR.value
        
        # Multiple numeric series
        if df_analysis.has_multiple_numeric_series and df_analysis.categorical_columns:
            if df_analysis.datetime_columns:
                return ChartType.STACKED_AREA.value
            else:
                return ChartType.STACKED_BAR.value
        
        # Correlation analysis
        if len(df_analysis.numeric_columns) >= 2:
            if num_cols == 2:
                return ChartType.SCATTER.value
            elif num_cols > 2:
                return ChartType.HEATMAP.value
        
        # Distribution analysis
        if (len(df_analysis.numeric_columns) == 1 and 
            len(df_analysis.categorical_columns) == 0):
            return ChartType.HISTOGRAM.value if num_rows >= self.MIN_HISTOGRAM_ROWS else ChartType.NONE.value
        
        # Categorical vs numeric
        if len(df_analysis.categorical_columns) == 1 and len(df_analysis.numeric_columns) == 1:
            cat_col = df_analysis.categorical_columns[0]
            unique_count = df_analysis.column_info[cat_col].unique_count
            
            if unique_count <= self.MAX_PIE_CATEGORIES:
                return ChartType.PIE.value
            elif unique_count <= self.MAX_CHART_CATEGORIES:
                return ChartType.BAR.value
            else:
                return ChartType.TABLE.value
        
        # Complex data structures
        if self._should_be_table(df_analysis):
            return ChartType.TABLE.value
        
        # Default fallback
        if df_analysis.numeric_columns and (df_analysis.categorical_columns or df_analysis.datetime_columns):
            return ChartType.BAR.value
        
        return ChartType.TABLE.value
    
    def _should_recommend_map(self, sql_analysis: Optional[SQLAnalysis], 
                             df_analysis: DataFrameAnalysis) -> bool:
        """Check if map visualization should be recommended."""
        # Priority 1: Must have actual coordinate pairs (lat/lng)
        has_coordinates = (len(df_analysis.latitude_columns) > 0 and 
                          len(df_analysis.longitude_columns) > 0)
        if has_coordinates:
            return True
        
        # Priority 2: Geographic columns that are clearly mappable (addresses, cities, etc.)
        # but NOT state/country/region which are better as categorical
        mappable_geo_columns = []
        for col in df_analysis.geographic_columns:
            col_upper = col.upper()
            # Exclude state/country/region from mappable geographic columns
            if not any(excluded in col_upper for excluded in ['STATE', 'COUNTRY', 'REGION']):
                mappable_geo_columns.append(col)
        
        has_mappable_geo_with_values = (len(mappable_geo_columns) > 0 and 
                                       len(df_analysis.numeric_columns) > 0)
        
        # Only recommend map if we have clear mappable geographic data
        return has_mappable_geo_with_values
    
    def _has_time_dimension(self, sql_analysis: Optional[SQLAnalysis], 
                           df_analysis: DataFrameAnalysis) -> bool:
        """Check if data has time dimension."""
        return ((sql_analysis and sql_analysis.has_time_dimension) or 
                len(df_analysis.datetime_columns) > 0)
    
    def _should_be_table(self, df_analysis: DataFrameAnalysis) -> bool:
        """Check if data should be displayed as table."""
        return (len(df_analysis.categorical_columns) > 2 and len(df_analysis.numeric_columns) == 0) or \
               (len(df_analysis.text_columns) > 0 and len(df_analysis.numeric_columns) == 0) or \
               (df_analysis.num_cols > self.MAX_TABLE_COLUMNS)
    
    def _recommend_axes(self, sql_analysis: Optional[SQLAnalysis], 
                       df_analysis: Optional[DataFrameAnalysis], 
                       chart_type: str) -> Tuple[str, str]:
        """Recommend X and Y axes based on chart type."""
        if chart_type == ChartType.NONE.value:
            return 'N/A', 'N/A'
        if chart_type == ChartType.TABLE.value:
            return 'Columns', 'Rows'
        if not df_analysis:
            return 'Index', 'Value'
        
        # Extract column lists
        datetime_cols = df_analysis.datetime_columns
        numeric_cols = df_analysis.numeric_columns
        categorical_cols = df_analysis.categorical_columns
        lat_cols = df_analysis.latitude_columns
        lng_cols = df_analysis.longitude_columns
        
        # Chart-specific axis selection
        if chart_type == ChartType.MAP.value:
            if lat_cols and lng_cols:
                return lng_cols[0], lat_cols[0]
            elif df_analysis.geographic_columns:
                geo_col = df_analysis.geographic_columns[0]
                value_col = numeric_cols[0] if numeric_cols else 'Count'
                return geo_col, value_col
            else:
                return 'Longitude', 'Latitude'
        
        elif chart_type in [ChartType.LINE.value, ChartType.AREA.value]:
            x_axis = datetime_cols[0] if datetime_cols else (categorical_cols[0] if categorical_cols else None)
            y_axis = numeric_cols[0] if numeric_cols else None
        
        elif chart_type in [ChartType.BAR.value, ChartType.STACKED_BAR.value]:
            x_axis = categorical_cols[0] if categorical_cols else (datetime_cols[0] if datetime_cols else None)
            if chart_type == ChartType.STACKED_BAR.value and len(numeric_cols) > 1:
                y_axis = self._format_multiple_columns(numeric_cols)
            else:
                y_axis = numeric_cols[0] if numeric_cols else None
        
        elif chart_type == ChartType.STACKED_AREA.value:
            x_axis = datetime_cols[0] if datetime_cols else (categorical_cols[0] if categorical_cols else None)
            y_axis = self._format_multiple_columns(numeric_cols) if len(numeric_cols) > 1 else (numeric_cols[0] if numeric_cols else None)
        
        elif chart_type == ChartType.SCATTER.value:
            if len(numeric_cols) >= 2:
                return numeric_cols[0], numeric_cols[1]
            else:
                x_axis, y_axis = None, None
        
        elif chart_type == ChartType.PIE.value:
            x_axis = categorical_cols[0] if categorical_cols else None
            y_axis = numeric_cols[0] if numeric_cols else None
        
        elif chart_type == ChartType.HISTOGRAM.value:
            x_axis = numeric_cols[0] if numeric_cols else None
            y_axis = 'Frequency'
        
        elif chart_type == ChartType.HEATMAP.value:
            return 'Variables', 'Variables'
        
        else:
            x_axis = categorical_cols[0] if categorical_cols else (datetime_cols[0] if datetime_cols else None)
            y_axis = numeric_cols[0] if numeric_cols else None
        
        return x_axis or 'Index', y_axis or 'Value'
    
    def _recommend_color(self, sql_analysis: Optional[SQLAnalysis], 
                        df_analysis: Optional[DataFrameAnalysis], 
                        chart_type: str) -> str:
        """Recommend color scheme for the chart."""
        # For stacked charts, recommend multiple colors
        if chart_type in [ChartType.STACKED_BAR.value, ChartType.STACKED_AREA.value]:
            if df_analysis and len(df_analysis.numeric_columns) > 1:
                num_series = len(df_analysis.numeric_columns)
                return self._get_stacked_colors(num_series)
            else:
                return self._get_stacked_colors(3)  # Default 3 colors
        
        # For pie charts, use multiple colors for segments
        elif chart_type == ChartType.PIE.value:
            if df_analysis and df_analysis.categorical_columns:
                cat_col = df_analysis.categorical_columns[0]
                num_categories = df_analysis.column_info[cat_col].unique_count
                return self._get_pie_colors(num_categories)
            else:
                return self._get_pie_colors(5)  # Default 5 colors
        
        # For maps, use gradient or heat colors
        elif chart_type == ChartType.MAP.value:
            return "#1f77b4,#ff7f0e,#2ca02c,#d62728,#9467bd"  # Blue to red gradient
        
        # For scatter plots, single color or based on categories
        elif chart_type == ChartType.SCATTER.value:
            if df_analysis and len(df_analysis.categorical_columns) > 0:
                return "#1f77b4,#ff7f0e,#2ca02c,#d62728"  # Multi-color for categories
            else:
                return "#1f77b4"  # Single blue
        
        # For heatmaps, use gradient
        elif chart_type == ChartType.HEATMAP.value:
            return "#440154,#31688e,#35b779,#fde725"  # Viridis-like gradient
        
        # Default single color for other chart types
        else:
            return "#1f77b4"  # Default blue
    
    def _get_stacked_colors(self, num_series: int) -> str:
        """Get color palette for stacked charts."""
        # Professional color palette for stacked charts
        colors = [
            "#1f77b4",  # Blue
            "#ff7f0e",  # Orange
            "#2ca02c",  # Green
            "#d62728",  # Red
            "#9467bd",  # Purple
            "#8c564b",  # Brown
            "#e377c2",  # Pink
            "#7f7f7f",  # Gray
            "#bcbd22",  # Olive
            "#17becf"   # Cyan
        ]
        
        # Return appropriate number of colors
        selected_colors = colors[:min(num_series, len(colors))]
        
        # If we need more colors than available, cycle through
        while len(selected_colors) < num_series:
            selected_colors.extend(colors[:min(num_series - len(selected_colors), len(colors))])
        
        return ",".join(selected_colors)
    
    def _get_pie_colors(self, num_categories: int) -> str:
        """Get color palette for pie charts."""
        # Distinct colors for pie chart segments
        pie_colors = [
            "#1f77b4",  # Blue
            "#ff7f0e",  # Orange
            "#2ca02c",  # Green
            "#d62728",  # Red
            "#9467bd",  # Purple
            "#8c564b",  # Brown
            "#e377c2",  # Pink
            "#7f7f7f",  # Gray
            "#bcbd22",  # Olive
            "#17becf",  # Cyan
            "#aec7e8",  # Light blue
            "#ffbb78",  # Light orange
            "#98df8a",  # Light green
            "#ff9896",  # Light red
            "#c5b0d5"   # Light purple
        ]
        
        selected_colors = pie_colors[:min(num_categories, len(pie_colors))]
        return ",".join(selected_colors)
    
    def _format_recommendation_string(self, chart_type: str, x_axis: str, y_axis: str, color: str) -> str:
        """Format the recommendation as a string response."""
        return f"Chart Type: {chart_type}\nX-axis: {x_axis}\nY-axis: {y_axis}\nColor: {color}"
    
    def _generate_title(self, sql_analysis: Optional[SQLAnalysis], 
                       df_analysis: Optional[DataFrameAnalysis], 
                       chart_type: str, x_axis: str, y_axis: str) -> str:
        """Generate appropriate chart title."""
        if chart_type == ChartType.NONE.value:
            return "No Visualization Recommended"
        elif chart_type == ChartType.TABLE.value:
            return "Data Table View"
        elif chart_type == ChartType.MAP.value:
            if sql_analysis and sql_analysis.aggregation_functions:
                agg_func = sql_analysis.aggregation_functions[0].title()
                return f"Geographic Distribution of {agg_func}"
            return "Geographic Distribution"
        
        # Generate based on aggregation info
        if sql_analysis and sql_analysis.aggregation_functions and sql_analysis.group_by_columns:
            agg_func = sql_analysis.aggregation_functions[0].title()
            group_col = sql_analysis.group_by_columns[0]
            return f"{agg_func} by {group_col}"
        
        # Generate based on chart type
        title_map = {
            ChartType.STACKED_BAR.value: f"Stacked Comparison of {y_axis} by {x_axis}",
            ChartType.STACKED_AREA.value: f"Stacked Trend of {y_axis} Over {x_axis}",
            ChartType.LINE.value: f"{y_axis} Over {x_axis}",
            ChartType.BAR.value: f"{y_axis} by {x_axis}",
            ChartType.SCATTER.value: f"{y_axis} vs {x_axis}",
            ChartType.PIE.value: f"Distribution of {y_axis} by {x_axis}",
            ChartType.HISTOGRAM.value: f"Distribution of {x_axis}",
            ChartType.HEATMAP.value: "Correlation Matrix" if df_analysis and len(df_analysis.numeric_columns) > 2 else "Data Heatmap"
        }
        
        return title_map.get(chart_type, f"{chart_type.title()} Chart")
    
    def _calculate_confidence(self, sql_analysis: Optional[SQLAnalysis], 
                             df_analysis: Optional[DataFrameAnalysis], 
                             chart_type: str) -> float:
        """Calculate confidence score for recommendation."""
        if chart_type == ChartType.NONE.value:
            return 1.0
        elif chart_type == ChartType.TABLE.value:
            return 0.8
        
        confidence = 0.5
        
        # Boost confidence for clear patterns
        if chart_type == ChartType.MAP.value and df_analysis:
            if len(df_analysis.latitude_columns) > 0 and len(df_analysis.longitude_columns) > 0:
                confidence += 0.4
            elif sql_analysis and sql_analysis.has_geographic_dimension:
                confidence += 0.3
        
        if chart_type in [ChartType.STACKED_BAR.value, ChartType.STACKED_AREA.value] and df_analysis:
            if df_analysis.has_multiple_numeric_series:
                confidence += 0.3
        
        if sql_analysis:
            if sql_analysis.has_time_dimension and chart_type == ChartType.LINE.value:
                confidence += 0.3
            if sql_analysis.has_aggregation and sql_analysis.has_grouping:
                confidence += 0.2
        
        if df_analysis:
            if len(df_analysis.numeric_columns) > 0:
                confidence += 0.1
            if len(df_analysis.categorical_columns) > 0:
                confidence += 0.1
        
        return min(confidence, 1.0)
    
    def _suggest_alternatives(self, chart_type: str, 
                             sql_analysis: Optional[SQLAnalysis], 
                             df_analysis: Optional[DataFrameAnalysis]) -> List[str]:
        """Suggest alternative chart types."""
        alternatives_map = {
            ChartType.NONE.value: [ChartType.TABLE.value],
            ChartType.TABLE.value: [ChartType.BAR.value, ChartType.NONE.value],
            ChartType.MAP.value: [ChartType.SCATTER.value, ChartType.HEATMAP.value],
            ChartType.STACKED_BAR.value: [ChartType.BAR.value, ChartType.STACKED_AREA.value],
            ChartType.STACKED_AREA.value: [ChartType.AREA.value, ChartType.LINE.value],
            ChartType.BAR.value: [ChartType.STACKED_BAR.value, ChartType.PIE.value],
            ChartType.LINE.value: [ChartType.AREA.value, ChartType.STACKED_AREA.value],
            ChartType.SCATTER.value: [ChartType.LINE.value, ChartType.BAR.value],
            ChartType.PIE.value: [ChartType.BAR.value, ChartType.STACKED_BAR.value],
            ChartType.HISTOGRAM.value: [ChartType.BOX.value, ChartType.BAR.value],
            ChartType.HEATMAP.value: [ChartType.SCATTER.value, ChartType.BAR.value]
        }
        
        alternatives = alternatives_map.get(chart_type, [])
        return alternatives[:self.MAX_ALTERNATIVE_SUGGESTIONS]
    
    def _generate_reasoning(self, sql_analysis: Optional[SQLAnalysis], 
                           df_analysis: Optional[DataFrameAnalysis], 
                           chart_type: str) -> str:
        """Generate human-readable reasoning for recommendation."""
        reasons = []
        
        if chart_type == ChartType.NONE.value:
            if not df_analysis or df_analysis.num_rows == 0:
                reasons.append("No data available for visualization")
            elif df_analysis.num_rows == 1:
                reasons.append("Single record - insufficient data for meaningful chart")
            else:
                reasons.append("Data structure not suitable for standard visualizations")
        
        elif chart_type == ChartType.TABLE.value:
            if df_analysis and df_analysis.num_cols > self.MAX_TABLE_COLUMNS:
                reasons.append("Many columns detected - table format preserves data detail")
            elif df_analysis and len(df_analysis.text_columns) > 0:
                reasons.append("Text-heavy data - table format maintains readability")
            else:
                reasons.append("Complex data structure - table format recommended")
        
        elif chart_type == ChartType.MAP.value:
            reasons.append("Geographic data detected - map visualization shows spatial patterns")
        
        elif chart_type in [ChartType.STACKED_BAR.value, ChartType.STACKED_AREA.value]:
            if df_analysis and df_analysis.has_multiple_numeric_series:
                reasons.append("Multiple numeric series detected - stacked chart shows composition and comparison")
        
        elif sql_analysis and sql_analysis.has_time_dimension:
            reasons.append("Time dimension detected - line chart shows trends over time")
        
        if sql_analysis:
            if sql_analysis.has_aggregation and sql_analysis.has_grouping:
                reasons.append("Aggregated grouped data - bar chart effective for comparisons")
        
        if df_analysis:
            if len(df_analysis.numeric_columns) >= 2:
                reasons.append("Multiple numeric columns - scatter plot shows relationships")
            if len(df_analysis.categorical_columns) == 1:
                reasons.append("Single categorical variable - good for grouping/comparison")
        
        return "; ".join(reasons) if reasons else f"{chart_type.title()} chart recommended based on data structure"
    
    # Helper methods for SQL analysis
    def _extract_group_by_columns(self, sql_upper: str) -> List[str]:
        """Extract GROUP BY columns from SQL query."""
        try:
            match = re.search(r'GROUP BY\s+(.+?)(?:\s+ORDER BY|\s+HAVING|\s+LIMIT|$)', sql_upper)
            if match:
                return [col.strip() for col in match.group(1).split(',') if col.strip()]
            return []
        except Exception:
            return []
    
    def _extract_where_conditions(self, sql_upper: str) -> List[str]:
        """Extract WHERE conditions from SQL query."""
        try:
            match = re.search(r'WHERE\s+(.+?)(?:\s+GROUP BY|\s+ORDER BY|\s+HAVING|\s+LIMIT|$)', sql_upper)
            if match:
                return [match.group(1).strip()]
            return []
        except Exception:
            return []
    
    def _extract_select_columns(self, sql_upper: str) -> List[str]:
        """Extract SELECT columns from SQL query."""
        try:
            match = re.search(r'SELECT\s+(.+?)\s+FROM', sql_upper)
            if match and match.group(1).strip() != '*':
                select_part = match.group(1)
                columns = []
                
                # Split by comma and clean up each column
                for col in select_part.split(','):
                    col = col.strip()
                    
                    # Handle aliases (AS keyword or space-separated)
                    if ' AS ' in col:
                        # Extract alias after AS
                        alias = col.split(' AS ')[-1].strip()
                        columns.append(alias)
                    elif ' ' in col and not any(func in col for func in ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'CASE']):
                        # Handle space-separated alias (but not for functions)
                        parts = col.split()
                        if len(parts) >= 2:
                            # Last part is likely the alias
                            columns.append(parts[-1])
                        else:
                            columns.append(col)
                    else:
                        # Extract base column name from functions like COUNT(*), SUM(sales), etc.
                        if '(' in col and ')' in col:
                            # This is a function, check if it has an alias
                            if ' AS ' in col:
                                alias = col.split(' AS ')[-1].strip()
                                columns.append(alias)
                            else:
                                # Try to extract the function and create a meaningful name
                                func_match = re.match(r'(\w+)\s*\([^)]*\)', col.strip())
                                if func_match:
                                    func_name = func_match.group(1).lower()
                                    columns.append(func_name)
                                else:
                                    columns.append(col)
                        else:
                            columns.append(col)
                
                return [col for col in columns if col]  # Filter empty strings
            return []
        except Exception:
            return []
    
    def _extract_time_columns(self, sql_upper: str, select_columns: List[str]) -> List[str]:
        """Extract time-related columns from SELECT clause."""
        time_columns = []
        
        for col in select_columns:
            col_upper = col.upper()
            # Check if column name contains time-related keywords
            if any(time_kw in col_upper for time_kw in self.time_keywords):
                time_columns.append(col)
        
        return time_columns
    
    def _extract_geographic_columns(self, sql_upper: str, select_columns: List[str]) -> List[str]:
        """Extract geographic columns from SELECT clause."""
        geo_columns = []
        
        for col in select_columns:
            col_upper = col.upper()
            # Check if column name contains geographic keywords
            if any(geo_kw in col_upper for geo_kw in self.geo_keywords):
                geo_columns.append(col)
        
        return geo_columns
    
    def _parse_visualization_response(self, response: str) -> Dict[str, str]:
        """Parse the visualization response into a dictionary"""
        response_dict = {}
        for line in response.split("\n"):
            if line.strip() and ":" in line:
                key, value = line.split(":", 1)
                response_dict[key.strip()] = value.strip()
        return response_dict 
        
    def _determine_query_type(self, analysis: SQLAnalysis) -> str:
        """Determine query type based on analysis."""
        if analysis.has_geographic_dimension:
            return 'geographic'
        elif analysis.has_aggregation and analysis.has_grouping:
            return 'aggregated_grouped'
        elif analysis.has_aggregation:
            return 'aggregated'
        elif analysis.has_grouping:
            return 'grouped'
        elif analysis.has_time_dimension:
            return 'time_series'
        return 'simple'
    
    def _create_mock_dataframe_analysis(self, sql_analysis: SQLAnalysis) -> DataFrameAnalysis:
        """Create mock DataFrame analysis from SQL analysis."""
        return DataFrameAnalysis(
            num_rows=100,
            num_cols=len(sql_analysis.select_columns) or 2,
            column_info={},
            numeric_columns=['value'] if sql_analysis.has_aggregation else [],
            categorical_columns=sql_analysis.group_by_columns,
            datetime_columns=sql_analysis.time_columns,
            geographic_columns=sql_analysis.geographic_columns
        )


# Example usage and testing
def test_service():
    """Test the service with sample data."""
    service = VisualizationRecommenderService()
    
    # Test the FIXED state query - should now be BAR, not PIE
    print("=== TESTING STATE/TIV QUERY (Should be BAR, not PIE) ===")
    
    state_data = pd.DataFrame({
        'state': ['CA', 'TX', 'NY', 'FL', 'IL'],
        'total_tiv': [1500000.50, 2300000.75, 1800000.25, 1200000.00, 950000.80]
    })
    
    sql_query = """SELECT state, SUM(derived_total_insured_value) AS total_tiv
FROM ux_all_info_consolidated
WHERE company_number = 'CN101741403'
GROUP BY state"""
    
    print(f"Data: {len(state_data)} states with TIV values")
    print(f"State values: {state_data['state'].tolist()}")
    print(f"TIV values: {state_data['total_tiv'].tolist()}")
    print()
    
    # Get detailed analysis
    detailed = service.get_detailed_recommendation(
        sql_query=sql_query,
        dataframe=state_data
    )
    
    print("Analysis Results:")
    if detailed.dataframe_analysis and detailed.sql_analysis:
        print(f"  SQL query_type: {detailed.sql_analysis.query_type}")
        print(f"  SQL has_aggregation: {detailed.sql_analysis.has_aggregation}")
        print(f"  SQL has_grouping: {detailed.sql_analysis.has_grouping}")
        print(f"  categorical_columns: {detailed.dataframe_analysis.categorical_columns}")
        print(f"  numeric_columns: {detailed.dataframe_analysis.numeric_columns}")
        
        # Check parts of whole analysis
        if detailed.dataframe_analysis.categorical_columns:
            cat_col = detailed.dataframe_analysis.categorical_columns[0]
            looks_like_parts = service._looks_like_parts_of_whole(detailed.dataframe_analysis, cat_col)
            print(f"  looks_like_parts_of_whole: {looks_like_parts}")
    
    print(f"\nRecommendation:")
    print(f"  Chart Type: {detailed.chart_type}")
    print(f"  X-axis: {detailed.x_axis}")
    print(f"  Y-axis: {detailed.y_axis}")
    
    # Test the string output
    recommendation_string = service.recommend(
        sql_query=sql_query,
        dataframe=state_data
    )
    
    print(f"\nString Output:")
    print(recommendation_string)
    
    expected = "bar"
    actual = detailed.chart_type
    status = "✅ PASS" if actual == expected else "❌ FAIL"
    print(f"\n{status} - Expected: {expected}, Got: {actual}")
    print("\n" + "="*70 + "\n")
    
    # Test comparison cases to verify logic
    print("=== PIE vs BAR LOGIC TESTS ===")
    
    # Test 1: Should be PIE (small dataset, percentage-like)
    pie_data = pd.DataFrame({
        'category': ['Type A', 'Type B', 'Type C'],
        'percentage': [45.2, 32.8, 22.0]
    })
    
    pie_result = service.recommend(dataframe=pie_data)
    print(f"Percentage data (should be PIE): {pie_result}")
    
    # Test 2: Should be BAR (state data, large values)
    bar_data = pd.DataFrame({
        'state': ['CA', 'TX', 'NY'],
        'population': [39538223, 29145505, 20201249]
    })
    
    bar_result = service.recommend(dataframe=bar_data)
    print(f"State population (should be BAR): {bar_result}")
    
    # Test 3: Should be BAR (aggregated SQL context)
    agg_result = service.recommend(
        sql_query="SELECT region, COUNT(*) as count FROM sales GROUP BY region",
        dataframe=pd.DataFrame({
            'region': ['North', 'South', 'East'],
            'count': [25, 30, 18]
        })
    )
    print(f"Aggregated count (should be BAR): {agg_result}")
    
    # Test 4: Should be PIE (true parts of whole, no SQL context)
    true_pie_data = pd.DataFrame({
        'segment': ['Premium', 'Standard'],
        'share': [35.5, 64.5]
    })
    
    true_pie_result = service.recommend(dataframe=true_pie_data)
    print(f"Market share (should be PIE): {true_pie_result}")
    
    print("\n" + "="*70 + "\n")
    
    # Test edge cases
    print("=== EDGE CASE TESTS ===")
    
    # Test 5: Many categories (should be BAR or TABLE)
    many_categories = pd.DataFrame({
        'product': [f'Product_{i}' for i in range(8)],
        'sales': [100 + i*50 for i in range(8)]
    })
    
    many_result = service.recommend(dataframe=many_categories)
    print(f"Many categories (should be BAR): {many_result}")
    
    # Test 6: Geographic data should not be MAP anymore
    geo_test = pd.DataFrame({
        'country': ['USA', 'CAN', 'MEX'],
        'gdp': [21000000, 1700000, 1300000]
    })
    
    geo_result = service.recommend(dataframe=geo_test)
    print(f"Country GDP (should be BAR, not MAP): {geo_result}")


if __name__ == "__main__":
    test_service()


