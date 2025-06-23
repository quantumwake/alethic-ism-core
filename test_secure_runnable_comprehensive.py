#!/usr/bin/env python3
"""Comprehensive test cases for secure_runnable.py functionality"""

import sys
sys.path.insert(0, '/Users/kasrarasaee/Development/quantumwake/alethic-ism-core/src')

from ismcore.compiler.secure_runnable import SecureRunnableBuilder, SecurityConfig
import json
import math

# Test configuration
config = SecurityConfig(
    max_memory_mb=100,
    max_cpu_time_seconds=5,
    max_requests=50,
    allowed_domains=["api.github.com", "*.example.com", "httpbin.org"],
    execution_timeout=10,
    enable_resource_limits=False,
    max_container_length=1000
)

test_cases = [
    {
        "name": "Basic functionality with context storage",
        "description": "Tests basic context storage and retrieval",
        "code": """
class Runnable(BaseSecureRunnable):
    def init(self):
        self.context['initialized'] = True
        self.context['counter'] = 0
        self.context['data'] = {'key': 'value'}
    
    def process(self, queries):
        results = []
        for query in queries:
            self.context['counter'] = self.context['counter'] + 1
            result = {
                'query_id': query.get('id', 'unknown'),
                'counter': self.context['counter'],
                'initialized': self.context['initialized'],
                'stored_data': self.context['data']
            }
            results.append(result)
        return results
    
    def process_stream(self, queries):
        for query in queries:
            yield {'stream': True, 'data': query}
""",
        "test_queries": [
            {"id": "test1"},
            {"id": "test2"}
        ]
    },
    
    {
        "name": "String manipulation and list comprehensions",
        "description": "Tests string operations, splitting, and list comprehensions",
        "code": """
class Runnable(BaseSecureRunnable):
    def init(self):
        self.context['separator'] = ','
    
    def process(self, queries):
        results = []
        for query in queries:
            # Test string splitting and list comprehension
            if 'tags' in query:
                tags = query['tags'].split(self.context['separator'])
                cleaned_tags = [tag.strip().upper() for tag in tags if tag.strip()]
                
                # Test string methods
                first_tag = cleaned_tags[0] if cleaned_tags else 'NONE'
                result = {
                    'original': query['tags'],
                    'parsed_tags': cleaned_tags,
                    'count': len(cleaned_tags),
                    'first_tag': first_tag,
                    'joined': '-'.join(cleaned_tags)
                }
            else:
                result = {'error': 'No tags provided'}
            
            results.append(result)
        return results
    
    def process_stream(self, queries):
        pass
""",
        "test_queries": [
            {"tags": "python, javascript, rust"},
            {"tags": "machine-learning,  ai,  deep-learning  "},
            {"tags": "single"},
            {"no_tags": True}
        ]
    },
    
    {
        "name": "Tuple unpacking and multiple assignment",
        "description": "Tests various forms of unpacking and assignment",
        "code": """
class Runnable(BaseSecureRunnable):
    def init(self):
        self.context['pairs'] = [(1, 2), (3, 4), (5, 6)]
    
    def process(self, queries):
        results = []
        
        # Test tuple unpacking from list
        a, b, c = [10, 20, 30]
        
        # Test unpacking in loop
        sums = []
        for x, y in self.context['pairs']:
            sums.append(x + y)
        
        # Test unpacking with different lengths
        first, second = "AB"
        
        # Test nested unpacking
        data = [('a', 1), ('b', 2)]
        letters = []
        numbers = []
        for letter, number in data:
            letters.append(letter)
            numbers.append(number)
        
        for query in queries:
            result = {
                'query_id': query.get('id', 'unknown'),
                'unpacked_values': [a, b, c],
                'pair_sums': sums,
                'chars': [first, second],
                'letters': letters,
                'numbers': numbers
            }
            results.append(result)
        
        return results
    
    def process_stream(self, queries):
        pass
""",
        "test_queries": [
            {"id": "unpack_test"}
        ]
    },
    
    {
        "name": "Dictionary operations and nested data",
        "description": "Tests dictionary manipulation and nested data structures",
        "code": """
class Runnable(BaseSecureRunnable):
    def init(self):
        self.context['users'] = {}
        self.context['settings'] = {
            'debug': False,
            'version': '1.0.0',
            'features': ['auth', 'api', 'ui']
        }
    
    def process(self, queries):
        results = []
        
        for query in queries:
            if query.get('action') == 'add_user':
                user_id = query.get('user_id')
                user_data = {
                    'name': query.get('name', 'Anonymous'),
                    'active': True,
                    'created_at': 'now'
                }
                self.context['users'][user_id] = user_data
                
                result = {
                    'action': 'user_added',
                    'user_id': user_id,
                    'total_users': len(self.context['users'])
                }
            
            elif query.get('action') == 'list_users':
                result = {
                    'action': 'user_list',
                    'users': dict(self.context['users']),  # Create a copy
                    'count': len(self.context['users'])
                }
            
            elif query.get('action') == 'get_settings':
                # Test dictionary methods
                features = self.context['settings'].get('features', [])
                result = {
                    'action': 'settings',
                    'debug': self.context['settings']['debug'],
                    'version': self.context['settings']['version'],
                    'feature_count': len(features),
                    'has_auth': 'auth' in features
                }
            
            else:
                result = {'error': 'Unknown action'}
            
            results.append(result)
        
        return results
    
    def process_stream(self, queries):
        pass
""",
        "test_queries": [
            {"action": "add_user", "user_id": "u1", "name": "Alice"},
            {"action": "add_user", "user_id": "u2", "name": "Bob"},
            {"action": "list_users"},
            {"action": "get_settings"}
        ]
    },
    
    {
        "name": "List operations and filtering",
        "description": "Tests list manipulation, filtering, and mapping",
        "code": """
class Runnable(BaseSecureRunnable):
    def init(self):
        self.context['numbers'] = list(range(1, 11))
    
    def process(self, queries):
        results = []
        
        for query in queries:
            numbers = self.context['numbers']
            
            # Test filter
            evens = list(filter(lambda x: x % 2 == 0, numbers))
            
            # Test map
            squares = list(map(lambda x: x * x, numbers))
            
            # Test list comprehension with condition
            odds_squared = [x * x for x in numbers if x % 2 == 1]
            
            # Test sum, min, max
            total = sum(numbers)
            minimum = min(numbers)
            maximum = max(numbers)
            
            # Test all, any
            all_positive = all(x > 0 for x in numbers)
            any_greater_5 = any(x > 5 for x in numbers)
            
            # Test sorted and reversed
            if 'values' in query:
                values = query['values']
                sorted_vals = sorted(values)
                reversed_vals = list(reversed(values))
            else:
                sorted_vals = []
                reversed_vals = []
            
            result = {
                'evens': evens,
                'squares': squares[:5],  # First 5 squares
                'odds_squared': odds_squared,
                'sum': total,
                'min': minimum,
                'max': maximum,
                'all_positive': all_positive,
                'any_greater_5': any_greater_5,
                'sorted': sorted_vals,
                'reversed': reversed_vals
            }
            results.append(result)
        
        return results
    
    def process_stream(self, queries):
        pass
""",
        "test_queries": [
            {"values": [3, 1, 4, 1, 5, 9, 2, 6]},
            {"values": [10, 5, 0, -5, -10]}
        ]
    },
    
    {
        "name": "Math and random operations",
        "description": "Tests math functions and random number generation",
        "code": """
class Runnable(BaseSecureRunnable):
    def init(self):
        # Seed random for reproducibility
        random.seed(42)
    
    def process(self, queries):
        results = []
        
        for query in queries:
            value = query.get('value', 10)
            
            # Math operations
            sqrt_val = math.sqrt(abs(value))
            log_val = math.log(abs(value) + 1)  # +1 to avoid log(0)
            sin_val = math.sin(value)
            cos_val = math.cos(value)
            
            # Random operations
            rand_float = random.random()
            rand_int = random.randint(1, 100)
            rand_choice = random.choice(['A', 'B', 'C', 'D'])
            
            # Create a list and shuffle it
            items = list(range(5))
            random.shuffle(items)
            
            result = {
                'input': value,
                'sqrt': round(sqrt_val, 3),
                'log': round(log_val, 3),
                'sin': round(sin_val, 3),
                'cos': round(cos_val, 3),
                'random_float': round(rand_float, 3),
                'random_int': rand_int,
                'random_choice': rand_choice,
                'shuffled': items
            }
            results.append(result)
        
        return results
    
    def process_stream(self, queries):
        pass
""",
        "test_queries": [
            {"value": 16},
            {"value": math.pi},
            {"value": -4}
        ]
    },
    
    {
        "name": "JSON operations",
        "description": "Tests JSON encoding and decoding",
        "code": """
class Runnable(BaseSecureRunnable):
    def init(self):
        self.context['json_data'] = json.dumps({
            'name': 'Test',
            'values': [1, 2, 3],
            'nested': {'key': 'value'}
        })
    
    def process(self, queries):
        results = []
        
        # Parse stored JSON
        stored_data = json.loads(self.context['json_data'])
        
        for query in queries:
            if 'encode' in query:
                # Encode to JSON
                encoded = json.dumps(query['encode'], indent=2)
                result = {
                    'action': 'encoded',
                    'json': encoded,
                    'length': len(encoded)
                }
            elif 'decode' in query:
                # Decode from JSON
                try:
                    decoded = json.loads(query['decode'])
                    # Determine type without using type() function
                    if isinstance(decoded, dict):
                        data_type = 'dict'
                    elif isinstance(decoded, list):
                        data_type = 'list'
                    elif isinstance(decoded, str):
                        data_type = 'str'
                    elif isinstance(decoded, (int, float)):
                        data_type = 'number'
                    elif isinstance(decoded, bool):
                        data_type = 'bool'
                    else:
                        data_type = 'unknown'
                    
                    result = {
                        'action': 'decoded',
                        'data': decoded,
                        'type': data_type
                    }
                except json.JSONDecodeError as e:
                    result = {
                        'action': 'decode_error',
                        'error': str(e)
                    }
            else:
                result = {
                    'stored_data': stored_data,
                    'stored_name': stored_data['name']
                }
            
            results.append(result)
        
        return results
    
    def process_stream(self, queries):
        for query in queries:
            yield json.dumps(query, indent=2)
""",
        "test_queries": [
            {"info": "get_stored"},
            {"encode": {"user": "test", "scores": [90, 85, 88]}},
            {"decode": '{"valid": true, "count": 42}'},
            {"decode": "invalid json"}
        ]
    },
    
    {
        "name": "Regular expressions",
        "description": "Tests regex pattern matching and substitution",
        "code": """
class Runnable(BaseSecureRunnable):
    def init(self):
        self.context['email_pattern'] = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}'
        self.context['phone_pattern'] = r'\\d{3}-\\d{3}-\\d{4}'
    
    def process(self, queries):
        results = []
        
        for query in queries:
            text = query.get('text', '')
            
            # Find emails
            emails = re.findall(self.context['email_pattern'], text)
            
            # Find phone numbers
            phones = re.findall(self.context['phone_pattern'], text)
            
            # Test substitution
            masked_text = re.sub(self.context['email_pattern'], '[EMAIL]', text)
            masked_text = re.sub(self.context['phone_pattern'], '[PHONE]', masked_text)
            
            # Test split
            if 'split_pattern' in query:
                parts = re.split(query['split_pattern'], text)
            else:
                parts = []
            
            result = {
                'original': text,
                'emails_found': emails,
                'phones_found': phones,
                'masked': masked_text,
                'split_parts': parts
            }
            results.append(result)
        
        return results
    
    def process_stream(self, queries):
        pass
""",
        "test_queries": [
            {
                "text": "Contact us at info@example.com or support@test.org. Call 555-123-4567",
                "split_pattern": r"\\s+or\\s+"
            },
            {
                "text": "Email: user@domain.com, Phone: 123-456-7890",
                "split_pattern": r",\\s*"
            }
        ]
    },
    
    {
        "name": "Error handling in user code",
        "description": "Tests how user code handles errors gracefully",
        "code": """
class Runnable(BaseSecureRunnable):
    def init(self):
        self.context['safe_mode'] = True
    
    def process(self, queries):
        results = []
        
        for query in queries:
            try:
                # Potentially risky operations
                if 'divide_by' in query:
                    result_val = 100 / query['divide_by']
                    result = {'success': True, 'result': result_val}
                
                elif 'get_item' in query:
                    items = [1, 2, 3]
                    index = query['get_item']
                    if 0 <= index < len(items):
                        result = {'success': True, 'item': items[index]}
                    else:
                        result = {'success': False, 'error': 'Index out of range'}
                
                elif 'parse_int' in query:
                    try:
                        value = int(query['parse_int'])
                        result = {'success': True, 'value': value}
                    except ValueError:
                        result = {'success': False, 'error': 'Invalid integer'}
                
                else:
                    result = {'success': False, 'error': 'Unknown operation'}
                    
            except ZeroDivisionError:
                result = {'success': False, 'error': 'Division by zero'}
            except Exception as e:
                result = {'success': False, 'error': str(e)}
            
            results.append(result)
        
        return results
    
    def process_stream(self, queries):
        pass
""",
        "test_queries": [
            {"divide_by": 5},
            {"divide_by": 0},
            {"get_item": 1},
            {"get_item": 10},
            {"parse_int": "123"},
            {"parse_int": "not_a_number"}
        ]
    },
    
    {
        "name": "Complex data transformations",
        "description": "Tests complex data manipulation and transformations",
        "code": """
class Runnable(BaseSecureRunnable):
    def init(self):
        self.context['inventory'] = {
            'apple': {'price': 0.5, 'stock': 100},
            'banana': {'price': 0.3, 'stock': 150},
            'orange': {'price': 0.6, 'stock': 80}
        }
    
    def process(self, queries):
        results = []
        
        for query in queries:
            if query.get('action') == 'purchase':
                item = query.get('item')
                quantity = query.get('quantity', 1)
                
                if item in self.context['inventory']:
                    product = self.context['inventory'][item]
                    if product['stock'] >= quantity:
                        # Update stock
                        product['stock'] = product['stock'] - quantity
                        total_price = product['price'] * quantity
                        
                        result = {
                            'success': True,
                            'item': item,
                            'quantity': quantity,
                            'total_price': round(total_price, 2),
                            'remaining_stock': product['stock']
                        }
                    else:
                        result = {
                            'success': False,
                            'error': 'Insufficient stock',
                            'available': product['stock']
                        }
                else:
                    result = {
                        'success': False,
                        'error': 'Item not found'
                    }
            
            elif query.get('action') == 'inventory_report':
                # Calculate inventory value
                total_value = 0
                report_items = []
                
                for item_name, item_data in self.context['inventory'].items():
                    value = item_data['price'] * item_data['stock']
                    total_value = total_value + value
                    report_items.append({
                        'item': item_name,
                        'stock': item_data['stock'],
                        'price': item_data['price'],
                        'value': round(value, 2)
                    })
                
                # Sort by value
                report_items = sorted(report_items, key=lambda x: x['value'], reverse=True)
                
                result = {
                    'action': 'inventory_report',
                    'total_value': round(total_value, 2),
                    'items': report_items,
                    'low_stock': [item['item'] for item in report_items if item['stock'] < 100]
                }
            
            else:
                result = {'error': 'Unknown action'}
            
            results.append(result)
        
        return results
    
    def process_stream(self, queries):
        pass
""",
        "test_queries": [
            {"action": "purchase", "item": "apple", "quantity": 10},
            {"action": "purchase", "item": "banana", "quantity": 200},
            {"action": "purchase", "item": "grape", "quantity": 5},
            {"action": "inventory_report"}
        ]
    }
]

def run_test_case(test_case):
    """Run a single test case and return results"""
    print(f"\n{'='*70}")
    print(f"Test: {test_case['name']}")
    print(f"Description: {test_case['description']}")
    print(f"{'='*70}")
    
    try:
        # Build and compile
        builder = SecureRunnableBuilder(config)
        runnable = builder.compile(test_case['code'].strip())
        print("✅ Compilation successful!")
        
        # Run test queries
        print("\nRunning test queries:")
        results = runnable.process(test_case['test_queries'])
        
        # Pretty print results
        for i, (query, result) in enumerate(zip(test_case['test_queries'], results)):
            print(f"\nQuery {i+1}:")
            print(f"Input: {json.dumps(query, indent=2)}")
            print(f"Output: {json.dumps(result, indent=2)}")
        
        return True, results
        
    except Exception as e:
        print(f"\n❌ Error: {type(e).__name__}")
        print(f"Message: {str(e)}")
        return False, str(e)

def run_all_tests():
    """Run all test cases and summarize results"""
    print("Running comprehensive test suite for secure_runnable.py")
    print(f"Total test cases: {len(test_cases)}")
    
    passed = 0
    failed = 0
    
    for test_case in test_cases:
        success, _ = run_test_case(test_case)
        if success:
            passed += 1
        else:
            failed += 1
    
    print(f"\n{'='*70}")
    print("Test Summary:")
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failed}")
    print(f"Total: {len(test_cases)}")
    print(f"Success rate: {(passed/len(test_cases)*100):.1f}%")

if __name__ == "__main__":
    run_all_tests()