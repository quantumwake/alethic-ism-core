from enum import Enum
from typing import Optional, Dict, Union
from pydantic import BaseModel


class FilterOperator(Enum):
    EQ = "EQ"
    GT = "GT"
    LT = "LT"


class FilterItem(BaseModel):
    key: str
    operator: Optional[FilterOperator] = FilterOperator.EQ
    value: Union[str, int, bool, float]


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

    def apply_filter_on_data(self, data: Union[str, int, bool, float]):
        if not isinstance(data, dict):
            raise NotImplementedError(f'unable to apply filters on none dictionary types, currently not supported')

        # Apply simple filters
        for key, filter_item in self.filter_items.items():
            data_value = data.get(key)
            op = filter_item.operator

            if op == FilterOperator.EQ and data_value != filter_item.value:
                return False
            elif op == FilterOperator.GT and data_value <= filter_item.value:
                return False
            elif op == FilterOperator.LT and data_value >= filter_item.value:
                return False

        return True