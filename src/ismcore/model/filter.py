import re
from enum import Enum
from typing import Optional, Dict, Union, List, Any
from pydantic import BaseModel


class FilterOperator(Enum):
    EQ = "EQ"
    NE = "NE"
    GT = "GT"
    GTE = "GTE"
    LT = "LT"
    LTE = "LTE"
    IN = "IN"
    NOT_IN = "NOT_IN"
    CONTAINS = "CONTAINS"
    NOT_CONTAINS = "NOT_CONTAINS"
    STARTS_WITH = "STARTS_WITH"
    ENDS_WITH = "ENDS_WITH"
    REGEX = "REGEX"
    REGEX_CASE_INSENSITIVE = "REGEX_I"
    EXISTS = "EXISTS"
    NOT_EXISTS = "NOT_EXISTS"
    IS_NULL = "IS_NULL"
    IS_NOT_NULL = "IS_NOT_NULL"
    BETWEEN = "BETWEEN"
    NOT_BETWEEN = "NOT_BETWEEN"


class FilterItem(BaseModel):
    key: str
    operator: Optional[FilterOperator] = FilterOperator.EQ
    value: Union[str, int, bool, float, List[Any], Dict[str, Any], None]
    secondary_value: Optional[Union[str, int, bool, float]] = None  # For BETWEEN operations


class Filter(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    user_id: Optional[str] = None
    filter_items: Optional[Dict[str, FilterItem]] = None

    def add_filter_item(self, filter_item: FilterItem) -> FilterItem:
        if not self.filter_items:
            self.filter_items = {}

        self.filter_items[filter_item.key] = filter_item
        return filter_item

    def get_filter_item(self, key) -> Optional[FilterItem]:
        if not self.filter_items or key not in self.filter_items:
            return None

        return self.filter_items[key]

    def apply_filter_on_data(self, data: Union[Dict[str, Any], Any]) -> bool:
        if not isinstance(data, dict):
            raise NotImplementedError(f'unable to apply filters on non-dictionary types, currently not supported')

        if not self.filter_items:
            return True

        # Apply filters
        for key, filter_item in self.filter_items.items():
            # Handle nested keys (e.g., "user.name")
            data_value = self._get_nested_value(data, key)
            op = filter_item.operator
            filter_value = filter_item.value

            # Check filter result
            if not self._evaluate_filter(data_value, op, filter_value, filter_item.secondary_value):
                return False

        return True

    def _get_nested_value(self, data: Dict[str, Any], key: str) -> Any:
        """Get value from nested dictionary using dot notation."""
        keys = key.split('.')
        value = data
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return None
        return value

    def _convert_to_numeric_if_possible(self, value: Any) -> Any:
        """Convert string to numeric if it represents a number."""
        if isinstance(value, (int, float)):
            return value
        if isinstance(value, str):
            # Try to convert to numeric
            try:
                # Check if it's an integer
                if '.' not in value:
                    return int(value)
                else:
                    return float(value)
            except ValueError:
                # Not a numeric string, return as is
                return value
        return value

    def _evaluate_filter(self, data_value: Any, operator: FilterOperator, filter_value: Any, secondary_value: Any = None) -> bool:
        """Evaluate a single filter condition."""
        # Handle EXISTS/NOT_EXISTS
        if operator == FilterOperator.EXISTS:
            return data_value is not None
        elif operator == FilterOperator.NOT_EXISTS:
            return data_value is None
        
        # Handle IS_NULL/IS_NOT_NULL
        if operator == FilterOperator.IS_NULL:
            return data_value is None
        elif operator == FilterOperator.IS_NOT_NULL:
            return data_value is not None

        # For other operators, None values typically fail the filter
        if data_value is None and operator not in [FilterOperator.NE, FilterOperator.NOT_IN, FilterOperator.NOT_CONTAINS]:
            return False

        # Auto-convert numeric strings for comparison operations
        if operator in [FilterOperator.EQ, FilterOperator.NE, FilterOperator.GT, FilterOperator.GTE, 
                       FilterOperator.LT, FilterOperator.LTE, FilterOperator.BETWEEN, FilterOperator.NOT_BETWEEN]:
            data_value = self._convert_to_numeric_if_possible(data_value)
            filter_value = self._convert_to_numeric_if_possible(filter_value)
            if secondary_value is not None:
                secondary_value = self._convert_to_numeric_if_possible(secondary_value)

        # Comparison operators
        if operator == FilterOperator.EQ:
            return data_value == filter_value
        elif operator == FilterOperator.NE:
            return data_value != filter_value
        elif operator == FilterOperator.GT:
            return data_value > filter_value
        elif operator == FilterOperator.GTE:
            return data_value >= filter_value
        elif operator == FilterOperator.LT:
            return data_value < filter_value
        elif operator == FilterOperator.LTE:
            return data_value <= filter_value
        
        # Collection operators
        elif operator == FilterOperator.IN:
            if isinstance(filter_value, list):
                return data_value in filter_value
            return False
        elif operator == FilterOperator.NOT_IN:
            if isinstance(filter_value, list):
                return data_value not in filter_value
            return True
        
        # String operators
        elif operator == FilterOperator.CONTAINS:
            if isinstance(data_value, str) and isinstance(filter_value, str):
                return filter_value in data_value
            elif isinstance(data_value, list):
                return filter_value in data_value
            return False
        elif operator == FilterOperator.NOT_CONTAINS:
            if isinstance(data_value, str) and isinstance(filter_value, str):
                return filter_value not in data_value
            elif isinstance(data_value, list):
                return filter_value not in data_value
            return True
        elif operator == FilterOperator.STARTS_WITH:
            if isinstance(data_value, str) and isinstance(filter_value, str):
                return data_value.startswith(filter_value)
            return False
        elif operator == FilterOperator.ENDS_WITH:
            if isinstance(data_value, str) and isinstance(filter_value, str):
                return data_value.endswith(filter_value)
            return False
        
        # Regex operators
        elif operator == FilterOperator.REGEX:
            if isinstance(data_value, str) and isinstance(filter_value, str):
                try:
                    return re.search(filter_value, data_value) is not None
                except re.error:
                    return False
            return False
        elif operator == FilterOperator.REGEX_CASE_INSENSITIVE:
            if isinstance(data_value, str) and isinstance(filter_value, str):
                try:
                    return re.search(filter_value, data_value, re.IGNORECASE) is not None
                except re.error:
                    return False
            return False
        
        # Range operators
        elif operator == FilterOperator.BETWEEN:
            if secondary_value is not None:
                return filter_value <= data_value <= secondary_value
            return False
        elif operator == FilterOperator.NOT_BETWEEN:
            if secondary_value is not None:
                return not (filter_value <= data_value <= secondary_value)
            return True

        return False