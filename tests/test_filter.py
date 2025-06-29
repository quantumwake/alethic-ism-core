import pytest
from src.ismcore.model.filter import Filter, FilterItem, FilterOperator


class TestFilterOperators:
    def test_equality_operators(self):
        # Test EQ
        filter_eq = Filter()
        filter_eq.add_filter_item(FilterItem(key="age", operator=FilterOperator.EQ, value=25))
        assert filter_eq.apply_filter_on_data({"age": 25}) is True
        assert filter_eq.apply_filter_on_data({"age": 26}) is False
        
        # Test NE
        filter_ne = Filter()
        filter_ne.add_filter_item(FilterItem(key="status", operator=FilterOperator.NE, value="active"))
        assert filter_ne.apply_filter_on_data({"status": "inactive"}) is True
        assert filter_ne.apply_filter_on_data({"status": "active"}) is False

    def test_comparison_operators(self):
        # Test GT
        filter_gt = Filter()
        filter_gt.add_filter_item(FilterItem(key="score", operator=FilterOperator.GT, value=80))
        assert filter_gt.apply_filter_on_data({"score": 85}) is True
        assert filter_gt.apply_filter_on_data({"score": 80}) is False
        assert filter_gt.apply_filter_on_data({"score": 75}) is False
        
        # Test GTE
        filter_gte = Filter()
        filter_gte.add_filter_item(FilterItem(key="score", operator=FilterOperator.GTE, value=80))
        assert filter_gte.apply_filter_on_data({"score": 85}) is True
        assert filter_gte.apply_filter_on_data({"score": 80}) is True
        assert filter_gte.apply_filter_on_data({"score": 75}) is False
        
        # Test LT
        filter_lt = Filter()
        filter_lt.add_filter_item(FilterItem(key="price", operator=FilterOperator.LT, value=100))
        assert filter_lt.apply_filter_on_data({"price": 95}) is True
        assert filter_lt.apply_filter_on_data({"price": 100}) is False
        assert filter_lt.apply_filter_on_data({"price": 105}) is False
        
        # Test LTE
        filter_lte = Filter()
        filter_lte.add_filter_item(FilterItem(key="price", operator=FilterOperator.LTE, value=100))
        assert filter_lte.apply_filter_on_data({"price": 95}) is True
        assert filter_lte.apply_filter_on_data({"price": 100}) is True
        assert filter_lte.apply_filter_on_data({"price": 105}) is False

    def test_numeric_string_conversion(self):
        # Test automatic numeric conversion
        filter_gt = Filter()
        filter_gt.add_filter_item(FilterItem(key="amount", operator=FilterOperator.GT, value="100.5"))
        assert filter_gt.apply_filter_on_data({"amount": "150.7"}) is True
        assert filter_gt.apply_filter_on_data({"amount": "50.3"}) is False
        
        # Test integer conversion
        filter_eq = Filter()
        filter_eq.add_filter_item(FilterItem(key="count", operator=FilterOperator.EQ, value="42"))
        assert filter_eq.apply_filter_on_data({"count": 42}) is True
        assert filter_eq.apply_filter_on_data({"count": "42"}) is True

    def test_collection_operators(self):
        # Test IN
        filter_in = Filter()
        filter_in.add_filter_item(FilterItem(key="category", operator=FilterOperator.IN, value=["electronics", "books", "music"]))
        assert filter_in.apply_filter_on_data({"category": "books"}) is True
        assert filter_in.apply_filter_on_data({"category": "food"}) is False
        
        # Test NOT_IN
        filter_not_in = Filter()
        filter_not_in.add_filter_item(FilterItem(key="status", operator=FilterOperator.NOT_IN, value=["banned", "suspended"]))
        assert filter_not_in.apply_filter_on_data({"status": "active"}) is True
        assert filter_not_in.apply_filter_on_data({"status": "banned"}) is False

    def test_string_operators(self):
        # Test CONTAINS
        filter_contains = Filter()
        filter_contains.add_filter_item(FilterItem(key="description", operator=FilterOperator.CONTAINS, value="python"))
        assert filter_contains.apply_filter_on_data({"description": "I love python programming"}) is True
        assert filter_contains.apply_filter_on_data({"description": "I love java programming"}) is False
        
        # Test NOT_CONTAINS
        filter_not_contains = Filter()
        filter_not_contains.add_filter_item(FilterItem(key="tags", operator=FilterOperator.NOT_CONTAINS, value="deprecated"))
        assert filter_not_contains.apply_filter_on_data({"tags": "stable, production"}) is True
        assert filter_not_contains.apply_filter_on_data({"tags": "deprecated, legacy"}) is False
        
        # Test STARTS_WITH
        filter_starts = Filter()
        filter_starts.add_filter_item(FilterItem(key="email", operator=FilterOperator.STARTS_WITH, value="admin@"))
        assert filter_starts.apply_filter_on_data({"email": "admin@example.com"}) is True
        assert filter_starts.apply_filter_on_data({"email": "user@example.com"}) is False
        
        # Test ENDS_WITH
        filter_ends = Filter()
        filter_ends.add_filter_item(FilterItem(key="filename", operator=FilterOperator.ENDS_WITH, value=".py"))
        assert filter_ends.apply_filter_on_data({"filename": "test.py"}) is True
        assert filter_ends.apply_filter_on_data({"filename": "test.js"}) is False

    def test_regex_operators(self):
        # Test REGEX
        filter_regex = Filter()
        filter_regex.add_filter_item(FilterItem(key="phone", operator=FilterOperator.REGEX, value=r"^\d{3}-\d{3}-\d{4}$"))
        assert filter_regex.apply_filter_on_data({"phone": "123-456-7890"}) is True
        assert filter_regex.apply_filter_on_data({"phone": "1234567890"}) is False
        
        # Test REGEX_CASE_INSENSITIVE
        filter_regex_i = Filter()
        filter_regex_i.add_filter_item(FilterItem(key="name", operator=FilterOperator.REGEX_CASE_INSENSITIVE, value=r"john"))
        assert filter_regex_i.apply_filter_on_data({"name": "John Doe"}) is True
        assert filter_regex_i.apply_filter_on_data({"name": "JOHNNY"}) is True
        assert filter_regex_i.apply_filter_on_data({"name": "Jane Doe"}) is False

    def test_existence_operators(self):
        # Test EXISTS
        filter_exists = Filter()
        filter_exists.add_filter_item(FilterItem(key="email", operator=FilterOperator.EXISTS, value=True))
        assert filter_exists.apply_filter_on_data({"email": "test@test.com"}) is True
        assert filter_exists.apply_filter_on_data({"email": None}) is False
        assert filter_exists.apply_filter_on_data({"name": "John"}) is False
        
        # Test NOT_EXISTS
        filter_not_exists = Filter()
        filter_not_exists.add_filter_item(FilterItem(key="deleted_at", operator=FilterOperator.NOT_EXISTS, value=True))
        assert filter_not_exists.apply_filter_on_data({"created_at": "2023-01-01"}) is True
        assert filter_not_exists.apply_filter_on_data({"deleted_at": "2023-01-01"}) is False

    def test_null_operators(self):
        # Test IS_NULL
        filter_null = Filter()
        filter_null.add_filter_item(FilterItem(key="archived_at", operator=FilterOperator.IS_NULL, value=True))
        assert filter_null.apply_filter_on_data({"archived_at": None}) is True
        assert filter_null.apply_filter_on_data({"archived_at": "2023-01-01"}) is False
        
        # Test IS_NOT_NULL
        filter_not_null = Filter()
        filter_not_null.add_filter_item(FilterItem(key="verified_at", operator=FilterOperator.IS_NOT_NULL, value=True))
        assert filter_not_null.apply_filter_on_data({"verified_at": "2023-01-01"}) is True
        assert filter_not_null.apply_filter_on_data({"verified_at": None}) is False

    def test_range_operators(self):
        # Test BETWEEN
        filter_between = Filter()
        filter_between.add_filter_item(FilterItem(key="age", operator=FilterOperator.BETWEEN, value=18, secondary_value=65))
        assert filter_between.apply_filter_on_data({"age": 25}) is True
        assert filter_between.apply_filter_on_data({"age": 18}) is True
        assert filter_between.apply_filter_on_data({"age": 65}) is True
        assert filter_between.apply_filter_on_data({"age": 17}) is False
        assert filter_between.apply_filter_on_data({"age": 66}) is False
        
        # Test NOT_BETWEEN
        filter_not_between = Filter()
        filter_not_between.add_filter_item(FilterItem(key="score", operator=FilterOperator.NOT_BETWEEN, value=0, secondary_value=60))
        assert filter_not_between.apply_filter_on_data({"score": 75}) is True
        assert filter_not_between.apply_filter_on_data({"score": -5}) is True
        assert filter_not_between.apply_filter_on_data({"score": 30}) is False

    def test_nested_keys(self):
        # Test nested key access
        filter_nested = Filter()
        filter_nested.add_filter_item(FilterItem(key="user.profile.age", operator=FilterOperator.GT, value=18))
        
        data = {
            "user": {
                "profile": {
                    "age": 25,
                    "name": "John"
                }
            }
        }
        assert filter_nested.apply_filter_on_data(data) is True
        
        data["user"]["profile"]["age"] = 16
        assert filter_nested.apply_filter_on_data(data) is False

    def test_multiple_filters(self):
        # Test multiple filter conditions
        filter_multi = Filter()
        filter_multi.add_filter_item(FilterItem(key="age", operator=FilterOperator.GTE, value=18))
        filter_multi.add_filter_item(FilterItem(key="status", operator=FilterOperator.EQ, value="active"))
        filter_multi.add_filter_item(FilterItem(key="country", operator=FilterOperator.IN, value=["US", "CA", "UK"]))
        
        # All conditions match
        assert filter_multi.apply_filter_on_data({
            "age": 25,
            "status": "active",
            "country": "US"
        }) is True
        
        # One condition fails
        assert filter_multi.apply_filter_on_data({
            "age": 16,  # Fails age check
            "status": "active",
            "country": "US"
        }) is False

    def test_list_contains(self):
        # Test CONTAINS on lists
        filter_list_contains = Filter()
        filter_list_contains.add_filter_item(FilterItem(key="tags", operator=FilterOperator.CONTAINS, value="python"))
        assert filter_list_contains.apply_filter_on_data({"tags": ["python", "java", "go"]}) is True
        assert filter_list_contains.apply_filter_on_data({"tags": ["java", "go"]}) is False

    def test_edge_cases(self):
        # Test with empty filter
        filter_empty = Filter()
        assert filter_empty.apply_filter_on_data({"any": "data"}) is True
        
        # Test non-dict data
        filter_test = Filter()
        filter_test.add_filter_item(FilterItem(key="test", operator=FilterOperator.EQ, value="value"))
        with pytest.raises(NotImplementedError):
            filter_test.apply_filter_on_data("not a dict")
        
        # Test invalid regex
        filter_bad_regex = Filter()
        filter_bad_regex.add_filter_item(FilterItem(key="text", operator=FilterOperator.REGEX, value="[invalid"))
        assert filter_bad_regex.apply_filter_on_data({"text": "some text"}) is False

    def test_filter_methods(self):
        # Test add_filter_item and get_filter_item
        filter_obj = Filter()
        item = FilterItem(key="test_key", operator=FilterOperator.EQ, value="test_value")
        added_item = filter_obj.add_filter_item(item)
        
        assert added_item == item
        assert filter_obj.get_filter_item("test_key") == item
        assert filter_obj.get_filter_item("nonexistent") is None
        
        # Test filter with id, name, user_id
        filter_full = Filter(id="123", name="Test Filter", user_id="user456")
        assert filter_full.id == "123"
        assert filter_full.name == "Test Filter"
        assert filter_full.user_id == "user456"