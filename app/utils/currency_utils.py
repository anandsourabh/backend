import re
from typing import Dict, Any, Union, Optional
from app.utils.logging import logger

class CurrencyFormatter:
    """Utility class for currency formatting and symbol mapping"""
    
    # Common currency symbols mapping
    CURRENCY_SYMBOLS = {
        'USD': '$',
        'EUR': '€',
        'GBP': '£',
        'JPY': '¥',
        'CAD': 'C$',
        'AUD': 'A$',
        'CHF': 'CHF',
        'CNY': '¥',
        'INR': '₹',
        'KRW': '₩',
        'BRL': 'R$',
        'MXN': '$',
        'ZAR': 'R',
        'SGD': 'S$',
        'HKD': 'HK$',
        'NOK': 'kr',
        'SEK': 'kr',
        'DKK': 'kr',
        'PLN': 'zł',
        'CZK': 'Kč',
        'HUF': 'Ft',
        'RUB': '₽',
        'TRY': '₺',
        'ILS': '₪',
        'AED': 'د.إ',
        'SAR': '﷼',
        'THB': '฿',
        'MYR': 'RM',
        'IDR': 'Rp',
        'PHP': '₱',
        'VND': '₫',
        'NZD': 'NZ$',
        'CLP': '$',
        'COP': '$',
        'PEN': 'S/',
        'UYU': '$U',
        'EGP': '£',
        'QAR': '﷼',
        'KWD': 'د.ك',
        'BHD': '.د.ب',
        'OMR': '﷼',
        'JOD': 'د.ا',
        'LBP': '£',
        'MAD': 'د.م.',
        'TND': 'د.ت',
        'DZD': 'د.ج',
        'LYD': 'ل.د',
        'SDG': 'ج.س.',
        'ETB': 'Br',
        'KES': 'KSh',
        'UGX': 'USh',
        'TZS': 'TSh',
        'RWF': 'RF',
        'BWP': 'P',
        'ZMW': 'ZK',
        'MWK': 'MK',
        'SZL': 'L',
        'LSL': 'L',
        'NAD': '$',
        'MZN': 'MT',
        'AOA': 'Kz',
        'CVE': '$',
        'GMD': 'D',
        'GNF': 'FG',
        'LRD': '$',
        'SLL': 'Le',
        'STD': 'Db',
        'SHP': '£',
        'FKP': '£',
        'GIP': '£',
        'JEP': '£',
        'GGP': '£',
        'IMP': '£',
        'XCD': '$',
        'BBD': '$',
        'BZD': 'BZ$',
        'BMD': '$',
        'KYD': '$',
        'JMD': 'J$',
        'TTD': 'TT$',
        'HTG': 'G',
        'DOP': 'RD$',
        'CUP': '₱',
        'AWG': 'ƒ',
        'ANG': 'ƒ',
        'SRD': '$',
        'GYD': '$',
        'FJD': '$',
        'TOP': 'T$',
        'WST': 'WS$',
        'VUV': 'VT',
        'SBD': '$',
        'PNG': 'K',
        'TVD': '$',
        'NRU': '$',
        'KID': '$',
        'CKD': '$',
        'XPF': '₣'
    }

    @staticmethod
    def clean_numeric_value(value: Union[str, int, float]) -> Optional[float]:
        """Clean and convert a value to numeric format."""
        if value is None or value == "":
            return None
            
        try:
            if isinstance(value, (int, float)):
                return float(value)
                
            if isinstance(value, str):
                cleaned = re.sub(r'[^\d.-]', '', value.replace(',', '').strip())
                if cleaned:
                    return float(cleaned)
                    
            return None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def format_currency(
        value: Union[str, int, float], 
        currency_symbol: str = '$', 
        decimal_places: int = 2,
        include_symbol: bool = True
    ) -> str:
        """Format a numeric value as currency."""
        if value is None or value == "":
            return str(value) if value is not None else ""
            
        numeric_value = CurrencyFormatter.clean_numeric_value(value)
        if numeric_value is None:
            return str(value)
            
        try:
            formatted_number = f"{numeric_value:,.{decimal_places}f}"
            return f"{currency_symbol} {formatted_number}" if include_symbol else formatted_number
                
        except (ValueError, TypeError):
            return str(value)

    @staticmethod
    def format_data_dict(
        data_dict: Dict[str, Any], 
        monetary_columns: set, 
        currency_symbol: str = '$'
    ) -> Dict[str, Any]:
        """Format monetary columns in a data dictionary."""
        formatted_dict = data_dict.copy()
        
        for key, value in formatted_dict.items():
            if key in monetary_columns:
                formatted_dict[key] = CurrencyFormatter.format_currency(value, currency_symbol)
                
        return formatted_dict

    @staticmethod
    def format_data_list(
        data_list: list, 
        monetary_columns: set, 
        currency_symbol: str = '$'
    ) -> list:
        """Format monetary columns in a list of dictionaries."""
        return [
            CurrencyFormatter.format_data_dict(item, monetary_columns, currency_symbol)
            for item in data_list
        ]

    @staticmethod
    def detect_monetary_columns(columns: list, known_monetary_columns: set) -> set:
        """Detect which columns in a list are likely monetary columns."""
        detected = set()
        
        for col in columns:
            if col.lower() in {c.lower() for c in known_monetary_columns}:
                detected.add(col)
                
        monetary_keywords = {
            'value', 'tiv', 'income', 'revenue', 'cost', 'price', 'amount', 
            'insured', 'damage', 'loss', 'rental', 'business', 'content',
            'building', 'equipment', 'machinery', 'inventory', 'stock'
        }
        
        for col in columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in monetary_keywords):
                if 'derived_' in col_lower or '_val' in col_lower or 'total_' in col_lower:
                    detected.add(col)
                    
        return detected

    @staticmethod
    def get_currency_symbol(currency_code: str) -> str:
        """Get currency symbols based on currency code."""
        currency_code = currency_code.upper().strip()
        symbol = CurrencyFormatter.CURRENCY_SYMBOLS.get(currency_code)
        if symbol:
            return symbol
        
        logger.warning(f"Currency symbol not found for code '{currency_code}', using code as symbol")
        return currency_code

# Convenience functions for backward compatibility
def format_currency_value(value, currency_symbol: str = '$') -> str:
    """Legacy function for formatting currency values."""
    return CurrencyFormatter.format_currency(value, currency_symbol)
