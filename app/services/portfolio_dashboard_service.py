# app/services/portfolio_dashboard_service.py

import pandas as pd
from typing import Dict, List, Any
from sqlalchemy import text
from app.services.database_service import DatabaseService
from app.utils.logging import logger
from app.core.database import engine
from app.utils.currency_utils import CurrencyFormatter

class PortfolioDashboardService:
    """Service to generate portfolio overview dashboard data"""
    
    def __init__(self):
        self.database_service = DatabaseService()
    
    def generate_portfolio_dashboard(self, company_number: str) -> Dict[str, Any]:
        """Generate comprehensive portfolio dashboard data"""
        try:
            logger.info(f"Generating portfolio dashboard for company {company_number}")
            
            # Get currency symbol for the company
            currency_symbol = self.database_service.get_currency_symbol(company_number)
            
            dashboard_data = {
                "summary_metrics": self._get_summary_metrics(company_number, currency_symbol),
                "geographic_distribution": self._get_geographic_distribution(company_number, currency_symbol),
                "country_distribution": self._get_country_distribution(company_number, currency_symbol),
                "risk_analysis": self._get_risk_analysis(company_number, currency_symbol),
                "construction_breakdown": self._get_construction_breakdown(company_number, currency_symbol),
                "occupancy_breakdown": self._get_occupancy_breakdown(company_number, currency_symbol),
                "age_distribution": self._get_age_distribution(company_number, currency_symbol),
                "top_locations": self._get_top_locations(company_number, currency_symbol),
                "hazard_summary": self._get_hazard_summary(company_number),
                "business_unit_breakdown": self._get_business_unit_breakdown(company_number, currency_symbol),
                "data_quality_metrics": self._get_data_quality_metrics(company_number),
                "currency_symbol": currency_symbol
            }
            
            return dashboard_data
            
        except Exception as e:
            logger.error(f"Error generating portfolio dashboard: {str(e)}")
            raise
    
    def _get_summary_metrics(self, company_number: str, currency_symbol: str) -> Dict[str, Any]:
        """Get high-level portfolio summary metrics"""
        sql_query = """
        SELECT 
            COUNT(DISTINCT marsh_location_id) as total_locations,
            COUNT(DISTINCT CASE WHEN number_of_buildings IS NOT NULL THEN marsh_location_id END) as locations_with_buildings,
            SUM(derived_total_insured_value) as total_tiv,
            AVG(derived_total_insured_value) as avg_tiv,
            MAX(derived_total_insured_value) as max_tiv,
            SUM(derived_building_values) as total_building_value,
            SUM(derived_content_values) as total_content_value,
            SUM(derived_business_interrupt_val) as total_bi_value,
            COUNT(DISTINCT state) as unique_states,
            COUNT(DISTINCT derived_country) as unique_countries
        FROM ux_all_info_consolidated
        WHERE company_number = :company_number
        """
        
        df = self.database_service.execute_query_raw(sql_query, company_number)
        
        if df.empty:
            return {}
        
        metrics = df.to_dict('records')[0]
        
        # Apply currency formatting only to monetary fields for display
        currency_fields = ['total_tiv', 'avg_tiv', 'max_tiv', 'total_building_value', 'total_content_value', 'total_bi_value']
        
        for field in currency_fields:
            if field in metrics and pd.notna(metrics[field]):
                metrics[field] = CurrencyFormatter.format_currency(metrics[field], currency_symbol)
        
        # Ensure count fields are integers
        int_fields = ['total_locations', 'locations_with_buildings', 'total_buildings', 'unique_states', 'unique_countries']
        for field in int_fields:
            if field in metrics and pd.notna(metrics[field]):
                metrics[field] = int(metrics[field])
        
        return metrics
 
    def _get_country_distribution(self, company_number: str, currency_symbol: str) -> List[Dict[str, Any]]:
        """Get TIV and BIV 12 months distribution by country"""
        sql_query = """
        SELECT 
            derived_country AS country,
            COUNT(DISTINCT marsh_location_id) AS location_count,
            SUM(derived_total_insured_value) AS total_tiv,
            SUM(derived_business_interrupt_val_12mo) AS total_biv_12mo,
            AVG(derived_total_insured_value) AS avg_tiv,
            AVG(derived_business_interrupt_val_12mo) AS avg_biv_12mo,
            ROUND(
                CASE 
                    WHEN SUM(derived_total_insured_value) > 0 
                    THEN (SUM(derived_business_interrupt_val_12mo) / SUM(derived_total_insured_value)) * 100
                    ELSE 0 
                END::numeric, 2  -- Explicitly cast to numeric
            ) AS biv_to_tiv_ratio
        FROM 
            ux_all_info_consolidated
        WHERE 
            company_number = :company_number
            AND derived_country IS NOT NULL
        GROUP BY 
            derived_country
        ORDER BY 
            total_tiv DESC NULLS LAST
        LIMIT 15;

            """
            
        df = self.database_service.execute_query_raw(sql_query, company_number)
            
        if not df.empty:
            # Convert location_count to int
            df['location_count'] = df['location_count'].astype(int)
                
            # Format currency values for display
            currency_columns = ['total_tiv', 'total_biv_12mo', 'avg_tiv', 'avg_biv_12mo']
            for col in currency_columns:
                df[col] = df[col].apply(lambda x: CurrencyFormatter.format_currency(x, currency_symbol) if pd.notna(x) else currency_symbol + ' 0')
                
            # Format ratio as percentage
            df['biv_to_tiv_ratio'] = df['biv_to_tiv_ratio'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "0.0%")
            
        return df.to_dict('records') if not df.empty else []
   
    def _get_geographic_distribution(self, company_number: str, currency_symbol: str) -> List[Dict[str, Any]]:
        """Get geographic distribution data for map visualization"""
        sql_query = """
        SELECT 
            marsh_location_id,
            location_name,
            address,
            city,
            state,
            derived_country,
            latitude,
            longitude,
            derived_total_insured_value as tiv,
            nathan_earthquake_hazardzone as earthquake_zone,
            nathan_river_flood_hazardzone as flood_zone,
            CASE 
                WHEN nathan_earthquake_hazardzone IN ('2', '3', '4') OR
                     nathan_hurricane_hazardzone IN ('4', '5') OR
                     nathan_river_flood_hazardzone IN ('50', '100')
                THEN 'High'
                WHEN nathan_earthquake_hazardzone = '1' OR
                     nathan_hurricane_hazardzone IN ('2', '3') OR
                     nathan_river_flood_hazardzone = '500'
                THEN 'Medium'
                ELSE 'Low'
            END as overall_risk
        FROM ux_all_info_consolidated
        WHERE company_number = :company_number
            AND latitude IS NOT NULL 
            AND longitude IS NOT NULL
            AND latitude != ''
            AND longitude != ''
            AND latitude ~ '^-?[0-9]+\.?[0-9]*$'
            AND longitude ~ '^-?[0-9]+\.?[0-9]*$'
        LIMIT 5000
        """
        
        df = self.database_service.execute_query_raw(sql_query, company_number)
        
        if not df.empty:
            # Format TIV values for display
            df['tiv'] = df['tiv'].apply(lambda x: CurrencyFormatter.format_currency(x, currency_symbol) if pd.notna(x) else '')
        
        return df.to_dict('records') if not df.empty else []
    
    def _get_risk_analysis(self, company_number: str, currency_symbol: str) -> Dict[str, List[Dict[str, Any]]]:
        """Get risk analysis data for various hazards"""
        risk_data = {}
        
        # Earthquake risk distribution
        earthquake_sql = """
            SELECT 
                CASE 
                    WHEN nathan_earthquake_hazardzone IN ('3', '4') THEN 'High'
                    WHEN nathan_earthquake_hazardzone IN ('1', '2') THEN 'Medium'
                    WHEN nathan_earthquake_hazardzone IN ('0', '-1', 'UNKNOWN') THEN 'Low'
                    ELSE 'Unknown'
                END AS risk_level,
                COUNT(*) AS location_count,
                SUM(derived_total_insured_value) AS total_tiv
            FROM ux_all_info_consolidated
            WHERE company_number = :company_number
            GROUP BY 
                risk_level
        """
        
        df = self.database_service.execute_query_raw(earthquake_sql, company_number)
        if not df.empty:
            # Convert location_count to int and format TIV
            df['location_count'] = df['location_count'].astype(int)
            df['total_tiv'] = df['total_tiv'].apply(lambda x: CurrencyFormatter.format_currency(x, currency_symbol) if pd.notna(x) else '')
        risk_data['earthquake'] = df.to_dict('records') if not df.empty else []
        
        # Flood risk distribution
        flood_sql = """
        SELECT 
            CASE 
                WHEN nathan_river_flood_hazardzone IN ('50', '100') OR 
                     nathan_flash_flood_hazardzone IN ('5', '6') THEN 'High'
                WHEN nathan_river_flood_hazardzone = '500' OR 
                     nathan_flash_flood_hazardzone IN ('3', '4') THEN 'Medium'
                WHEN nathan_river_flood_hazardzone IN ('-1', 'UNKNOWN') AND 
                     nathan_flash_flood_hazardzone IN ('0', '1', '2', '-1', 'UNKNOWN') THEN 'Low'
                ELSE 'Unknown'
            END as risk_level,
            COUNT(*) as location_count,
            SUM(derived_total_insured_value) as total_tiv
        FROM ux_all_info_consolidated
        WHERE company_number = :company_number
        GROUP BY risk_level
        """
        
        df = self.database_service.execute_query_raw(flood_sql, company_number)
        if not df.empty:
            df['location_count'] = df['location_count'].astype(int)
            df['total_tiv'] = df['total_tiv'].apply(lambda x: CurrencyFormatter.format_currency(x, currency_symbol) if pd.notna(x) else '')
        risk_data['flood'] = df.to_dict('records') if not df.empty else []
        
        # Hurricane risk distribution
        hurricane_sql = """
        SELECT 
            CASE 
                WHEN nathan_hurricane_hazardzone IN ('4', '5') THEN 'High'
                WHEN nathan_hurricane_hazardzone IN ('2', '3') THEN 'Medium'
                WHEN nathan_hurricane_hazardzone IN ('0', '1', '-1', 'UNKNOWN') THEN 'Low'
                ELSE 'Unknown'
            END as risk_level,
            COUNT(*) as location_count,
            SUM(derived_total_insured_value) as total_tiv
        FROM ux_all_info_consolidated
        WHERE company_number = :company_number
        GROUP BY risk_level
        """
        
        df = self.database_service.execute_query_raw(hurricane_sql, company_number)
        if not df.empty:
            df['location_count'] = df['location_count'].astype(int)
            df['total_tiv'] = df['total_tiv'].apply(lambda x: CurrencyFormatter.format_currency(x, currency_symbol) if pd.notna(x) else '')
        risk_data['hurricane'] = df.to_dict('records') if not df.empty else []
        
        return risk_data
    
    def _get_construction_breakdown(self, company_number: str, currency_symbol: str) -> List[Dict[str, Any]]:
        """Get construction type breakdown"""
        sql_query = """
        SELECT 
            COALESCE(construction, 'Unknown') as construction_type,
            COUNT(*) as location_count,
            SUM(derived_total_insured_value) as total_tiv,
            AVG(derived_total_insured_value) as avg_tiv
        FROM ux_all_info_consolidated
        WHERE company_number = :company_number
        GROUP BY construction
        ORDER BY total_tiv DESC NULLS LAST
        LIMIT 10
        """
        
        df = self.database_service.execute_query_raw(sql_query, company_number)
        
        if not df.empty:
            df['location_count'] = df['location_count'].astype(int)
            # Keep total_tiv as numeric for chart, format avg_tiv for display
            df['avg_tiv'] = df['avg_tiv'].apply(lambda x: CurrencyFormatter.format_currency(x, currency_symbol) if pd.notna(x) else '')
        
        return df.to_dict('records') if not df.empty else []
    
    def _get_occupancy_breakdown(self, company_number: str, currency_symbol: str) -> List[Dict[str, Any]]:
        """Get occupancy type breakdown"""
        sql_query = """
        SELECT 
            COALESCE(occupancy, 'Unknown') as occupancy_type,
            COUNT(*) as location_count,
            SUM(derived_total_insured_value) as total_tiv,
            AVG(derived_total_insured_value) as avg_tiv
        FROM ux_all_info_consolidated
        WHERE company_number = :company_number
        GROUP BY occupancy
        ORDER BY total_tiv DESC NULLS LAST
        LIMIT 10
        """
        
        df = self.database_service.execute_query_raw(sql_query, company_number)
        
        if not df.empty:
            df['location_count'] = df['location_count'].astype(int)
            # Keep total_tiv as numeric for chart, format avg_tiv for display
            df['avg_tiv'] = df['avg_tiv'].apply(lambda x: CurrencyFormatter.format_currency(x, currency_symbol) if pd.notna(x) else '')
        
        return df.to_dict('records') if not df.empty else []
    
    def _get_age_distribution(self, company_number: str, currency_symbol: str) -> List[Dict[str, Any]]:
        """Get property age distribution"""
        sql_query = """
        SELECT 
            CASE 
                WHEN year_built::date >= '2020-01-01' THEN '0-5 years'
                WHEN year_built::date >= '2010-01-01' THEN '5-15 years'
                WHEN year_built::date >= '2000-01-01' THEN '15-25 years'
                WHEN year_built::date >= '1980-01-01' THEN '25-45 years'
                WHEN year_built::date < '1980-01-01' THEN '45+ years'
                ELSE 'Unknown'
            END as age_group,
            COUNT(*) as location_count,
            SUM(derived_total_insured_value) as total_tiv,
            AVG(derived_total_insured_value) as avg_tiv
        FROM ux_all_info_consolidated
        WHERE company_number = :company_number
            AND year_built IS NOT NULL
            AND year_built != '12/31/99'
        GROUP BY age_group
        ORDER BY age_group
        """
        
        df = self.database_service.execute_query_raw(sql_query, company_number)
        
        if not df.empty:
            df['location_count'] = df['location_count'].astype(int)
            # Keep total_tiv as numeric for chart, format avg_tiv for display
            df['avg_tiv'] = df['avg_tiv'].apply(lambda x: CurrencyFormatter.format_currency(x, currency_symbol) if pd.notna(x) else '')
        
        return df.to_dict('records') if not df.empty else []
    
    def _get_top_locations(self, company_number: str, currency_symbol: str) -> List[Dict[str, Any]]:
        """Get top locations by TIV"""
        sql_query = """
        SELECT 
            marsh_location_id,
            location_name,
            address,
            city,
            state,
            derived_country,
            derived_total_insured_value as tiv,
            construction,
            occupancy,
            year_built
        FROM ux_all_info_consolidated
        WHERE company_number = :company_number
        ORDER BY derived_total_insured_value DESC NULLS LAST
        LIMIT 20
        """
        
        df = self.database_service.execute_query_raw(sql_query, company_number)
        
        if not df.empty:
            # Format TIV for display
            df['tiv'] = df['tiv'].apply(lambda x: CurrencyFormatter.format_currency(x, currency_symbol) if pd.notna(x) else '')
        
        return df.to_dict('records') if not df.empty else []
    
    def _get_hazard_summary(self, company_number: str) -> Dict[str, int]:
        """Get count of locations in high-risk zones for each hazard"""
        sql_query = """
        SELECT 
            COALESCE(SUM(CASE WHEN nathan_earthquake_hazardzone IN ('3', '4') THEN 1 ELSE 0 END), 0) as high_earthquake_risk,
            COALESCE(SUM(CASE WHEN nathan_hurricane_hazardzone IN ('4', '5') THEN 1 ELSE 0 END), 0) as high_hurricane_risk,
            COALESCE(SUM(CASE WHEN nathan_tornado_hazardzone IN ('3', '4') THEN 1 ELSE 0 END), 0) as high_tornado_risk,
            COALESCE(SUM(CASE WHEN nathan_wildfire_hazardzone IN ('3', '4') THEN 1 ELSE 0 END), 0) as high_wildfire_risk,
            COALESCE(SUM(CASE WHEN nathan_river_flood_hazardzone IN ('50', '100') THEN 1 ELSE 0 END), 0) as high_river_flood_risk,
            COALESCE(SUM(CASE WHEN nathan_flash_flood_hazardzone IN ('5', '6') THEN 1 ELSE 0 END), 0) as high_flash_flood_risk,
            COALESCE(SUM(CASE WHEN nathan_hail_hazardzone IN ('4', '5', '6') THEN 1 ELSE 0 END), 0) as high_hail_risk,
            COALESCE(SUM(CASE WHEN nathan_lightning_hazardzone IN ('4', '5', '6') THEN 1 ELSE 0 END), 0) as high_lightning_risk,
            COUNT(*) as total_locations
        FROM ux_all_info_consolidated
        WHERE company_number = :company_number
        """
        
        df = self.database_service.execute_query_raw(sql_query, company_number)
        
        if df.empty:
            return {}
        
        result = df.to_dict('records')[0]
        # Convert all values to int
        return {k: int(v) if pd.notna(v) else 0 for k, v in result.items()}
    
    def _get_business_unit_breakdown(self, company_number: str, currency_symbol: str) -> List[Dict[str, Any]]:
        """Get breakdown by business unit"""
        sql_query = """
        SELECT 
            COALESCE(business_unit, 'Not Specified') as business_unit,
            COUNT(*) as location_count,
            SUM(derived_total_insured_value) as total_tiv,
            AVG(derived_total_insured_value) as avg_tiv,
            SUM(derived_building_values) as building_value,
            SUM(derived_content_values) as content_value,
            SUM(derived_business_interrupt_val) as bi_value
        FROM ux_all_info_consolidated
        WHERE company_number = :company_number
        GROUP BY business_unit
        ORDER BY total_tiv DESC NULLS LAST
        LIMIT 15
        """
        
        df = self.database_service.execute_query_raw(sql_query, company_number)
        
        if not df.empty:
            df['location_count'] = df['location_count'].astype(int)
            # Keep numeric values for stacked chart, format avg_tiv for display
            df['avg_tiv'] = df['avg_tiv'].apply(lambda x: CurrencyFormatter.format_currency(x, currency_symbol) if pd.notna(x) else '')
        
        return df.to_dict('records') if not df.empty else []
    
    def _get_data_quality_metrics(self, company_number: str) -> Dict[str, Any]:
        """Get data quality metrics"""
        sql_query = """
        SELECT 
            COUNT(*) as total_records,
            SUM(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 ELSE 0 END) as geocoded_count,
            SUM(CASE WHEN construction IS NOT NULL THEN 1 ELSE 0 END) as construction_complete,
            SUM(CASE WHEN occupancy IS NOT NULL THEN 1 ELSE 0 END) as occupancy_complete,
            SUM(CASE WHEN year_built IS NOT NULL AND year_built != '12/31/99' THEN 1 ELSE 0 END) as year_built_complete,
            SUM(CASE WHEN derived_total_insured_value IS NOT NULL AND derived_total_insured_value > 0 THEN 1 ELSE 0 END) as tiv_complete,
            SUM(CASE WHEN ad_flag_value = true THEN 1 ELSE 0 END) as address_quality_issues,
            SUM(CASE WHEN gc_flag_value_new = true THEN 1 ELSE 0 END) as geocoding_issues,
            SUM(CASE WHEN values_flag_value = true THEN 1 ELSE 0 END) as value_issues
        FROM ux_all_info_consolidated
        WHERE company_number = :company_number
        """
        
        df = self.database_service.execute_query_raw(sql_query, company_number)
        
        if df.empty:
            return {}
        
        metrics = df.to_dict('records')[0]
        
        # Convert to integers and calculate percentages
        total = int(metrics.get('total_records', 1) or 1)  # Avoid division by zero
        return {
            'total_records': total,
            'geocoding_completeness': round((int(metrics.get('geocoded_count', 0) or 0) / total) * 100, 1),
            'construction_completeness': round((int(metrics.get('construction_complete', 0) or 0) / total) * 100, 1),
            'occupancy_completeness': round((int(metrics.get('occupancy_complete', 0) or 0) / total) * 100, 1),
            'year_built_completeness': round((int(metrics.get('year_built_complete', 0) or 0) / total) * 100, 1),
            'tiv_completeness': round((int(metrics.get('tiv_complete', 0) or 0) / total) * 100, 1),
            'address_quality_score': round(((total - int(metrics.get('address_quality_issues', 0) or 0)) / total) * 100, 1),
            'geocoding_quality_score': round(((total - int(metrics.get('geocoding_issues', 0) or 0)) / total) * 100, 1),
            'value_quality_score': round(((total - int(metrics.get('value_issues', 0) or 0)) / total) * 100, 1)
        }