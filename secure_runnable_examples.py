#!/usr/bin/env python3
"""Practical examples of secure_runnable.py usage"""

import sys
sys.path.insert(0, '/Users/kasrarasaee/Development/quantumwake/alethic-ism-core/src')

from ismcore.compiler.secure_runnable import SecureRunnableBuilder, SecurityConfig

# Example 1: Data Processing Pipeline
data_processor_code = """
class Runnable(BaseSecureRunnable):
    def init(self):
        # Initialize processing pipeline configuration
        self.context['transformations'] = []
        self.context['statistics'] = {
            'processed': 0,
            'errors': 0,
            'transformations_applied': 0
        }
    
    def process(self, queries):
        results = []
        
        for query in queries:
            result = {'original': query}
            data = query.get('data', {})
            
            try:
                # Apply transformations
                if 'uppercase' in query.get('transforms', []):
                    data = self._apply_uppercase(data)
                    self.context['statistics']['transformations_applied'] += 1
                
                if 'clean_numbers' in query.get('transforms', []):
                    data = self._clean_numbers(data)
                    self.context['statistics']['transformations_applied'] += 1
                
                if 'validate_email' in query.get('transforms', []):
                    data = self._validate_emails(data)
                    self.context['statistics']['transformations_applied'] += 1
                
                result['processed'] = data
                result['success'] = True
                self.context['statistics']['processed'] += 1
                
            except Exception as e:
                result['error'] = str(e)
                result['success'] = False
                self.context['statistics']['errors'] += 1
            
            results.append(result)
        
        # Add statistics to last result
        if results:
            results[-1]['statistics'] = dict(self.context['statistics'])
        
        return results
    
    def _apply_uppercase(self, data):
        if isinstance(data, str):
            return data.upper()
        elif isinstance(data, dict):
            return {k: self._apply_uppercase(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._apply_uppercase(item) for item in data]
        return data
    
    def _clean_numbers(self, data):
        if isinstance(data, str):
            # Remove non-numeric characters from strings that look like numbers
            import re
            if re.match(r'^[\\d\\s\\-\\.\\$,]+$', data):
                cleaned = re.sub(r'[^\\d\\.]', '', data)
                try:
                    return float(cleaned) if '.' in cleaned else int(cleaned)
                except ValueError:
                    return data
        elif isinstance(data, dict):
            return {k: self._clean_numbers(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._clean_numbers(item) for item in data]
        return data
    
    def _validate_emails(self, data):
        import re
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
        
        if isinstance(data, str):
            if '@' in data:
                return {
                    'value': data,
                    'is_valid_email': bool(re.match(email_pattern, data))
                }
        elif isinstance(data, dict):
            return {k: self._validate_emails(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._validate_emails(item) for item in data]
        return data
    
    def process_stream(self, queries):
        # Stream processing for real-time data
        for query in queries:
            # Process and yield immediately
            result = self.process([query])[0]
            yield result
"""

# Example 2: API Data Aggregator
api_aggregator_code = """
class Runnable(BaseSecureRunnable):
    def init(self):
        self.context['cache'] = {}
        self.context['api_calls'] = 0
        self.context['cache_hits'] = 0
    
    def process(self, queries):
        results = []
        
        for query in queries:
            endpoint = query.get('endpoint', '')
            use_cache = query.get('use_cache', True)
            cache_key = f"{endpoint}:{query.get('params', {})}"
            
            # Check cache first
            if use_cache and cache_key in self.context['cache']:
                self.context['cache_hits'] += 1
                result = {
                    'data': self.context['cache'][cache_key],
                    'from_cache': True,
                    'cache_hits': self.context['cache_hits']
                }
            else:
                # Simulate API call (in real scenario, use self.requests)
                self.context['api_calls'] += 1
                
                # Mock different endpoints
                if endpoint == '/users':
                    data = self._mock_users_endpoint(query.get('params', {}))
                elif endpoint == '/posts':
                    data = self._mock_posts_endpoint(query.get('params', {}))
                elif endpoint == '/stats':
                    data = self._mock_stats_endpoint()
                else:
                    data = {'error': 'Unknown endpoint'}
                
                # Cache the result
                if use_cache:
                    self.context['cache'][cache_key] = data
                
                result = {
                    'data': data,
                    'from_cache': False,
                    'api_calls': self.context['api_calls']
                }
            
            results.append(result)
        
        return results
    
    def _mock_users_endpoint(self, params):
        user_id = params.get('id', 1)
        return {
            'id': user_id,
            'name': f'User {user_id}',
            'email': f'user{user_id}@example.com',
            'active': user_id % 2 == 0
        }
    
    def _mock_posts_endpoint(self, params):
        limit = params.get('limit', 5)
        posts = []
        for i in range(1, limit + 1):
            posts.append({
                'id': i,
                'title': f'Post {i}',
                'views': i * 100
            })
        return {'posts': posts, 'total': limit}
    
    def _mock_stats_endpoint(self):
        return {
            'total_users': 1000,
            'active_users': 750,
            'total_posts': 5000,
            'api_version': '2.0'
        }
    
    def process_stream(self, queries):
        pass
"""

# Example 3: Rule Engine
rule_engine_code = """
class Runnable(BaseSecureRunnable):
    def init(self):
        # Define rules
        self.context['rules'] = [
            {
                'name': 'high_value_order',
                'conditions': lambda order: order.get('total', 0) > 1000,
                'actions': ['apply_discount', 'flag_for_review']
            },
            {
                'name': 'new_customer',
                'conditions': lambda order: order.get('customer', {}).get('orders_count', 0) == 0,
                'actions': ['send_welcome_email', 'apply_first_time_discount']
            },
            {
                'name': 'bulk_order',
                'conditions': lambda order: any(item.get('quantity', 0) > 50 for item in order.get('items', [])),
                'actions': ['apply_bulk_discount', 'notify_warehouse']
            }
        ]
        
        self.context['applied_rules'] = []
    
    def process(self, queries):
        results = []
        
        for order in queries:
            applied_rules = []
            actions_to_take = set()
            
            # Evaluate each rule
            for rule in self.context['rules']:
                try:
                    if rule['conditions'](order):
                        applied_rules.append(rule['name'])
                        actions_to_take.update(rule['actions'])
                except Exception as e:
                    self.logger.error(f"Error evaluating rule {rule['name']}: {e}")
            
            # Execute actions
            order_result = dict(order)  # Copy original order
            
            if 'apply_discount' in actions_to_take:
                order_result['discount'] = 0.1  # 10% discount
                order_result['final_total'] = order.get('total', 0) * 0.9
            
            if 'apply_first_time_discount' in actions_to_take:
                existing_discount = order_result.get('discount', 0)
                order_result['discount'] = existing_discount + 0.15  # Additional 15%
                order_result['final_total'] = order.get('total', 0) * (1 - order_result['discount'])
            
            if 'apply_bulk_discount' in actions_to_take:
                existing_discount = order_result.get('discount', 0)
                order_result['discount'] = existing_discount + 0.05  # Additional 5%
                order_result['final_total'] = order.get('total', 0) * (1 - order_result['discount'])
            
            # Record which rules were applied
            order_result['applied_rules'] = applied_rules
            order_result['actions_taken'] = list(actions_to_take)
            
            # Update context
            self.context['applied_rules'].extend(applied_rules)
            
            results.append(order_result)
        
        return results
    
    def process_stream(self, queries):
        pass
"""

# Example 4: Data Validator and Sanitizer
data_validator_code = """
class Runnable(BaseSecureRunnable):
    def init(self):
        self.context['validation_rules'] = {
            'email': {
                'pattern': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$',
                'message': 'Invalid email format'
            },
            'phone': {
                'pattern': r'^\\+?1?\\d{9,15}$',
                'message': 'Invalid phone number'
            },
            'url': {
                'pattern': r'^https?://[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}',
                'message': 'Invalid URL format'
            },
            'alphanumeric': {
                'pattern': r'^[a-zA-Z0-9]+$',
                'message': 'Only letters and numbers allowed'
            }
        }
        
        self.context['sanitization_stats'] = {
            'total_processed': 0,
            'fields_sanitized': 0,
            'validation_errors': 0
        }
    
    def process(self, queries):
        results = []
        
        for query in queries:
            result = {
                'original': query,
                'sanitized': {},
                'validation_errors': {},
                'is_valid': True
            }
            
            # Process each field
            for field_name, field_value in query.items():
                if field_name.startswith('_'):  # Skip metadata fields
                    continue
                
                # Sanitize the value
                sanitized_value = self._sanitize_value(field_value)
                result['sanitized'][field_name] = sanitized_value
                
                # Check if field has validation rules
                validation_type = query.get(f'_{field_name}_type')
                if validation_type and validation_type in self.context['validation_rules']:
                    rule = self.context['validation_rules'][validation_type]
                    import re
                    if not re.match(rule['pattern'], str(sanitized_value)):
                        result['validation_errors'][field_name] = rule['message']
                        result['is_valid'] = False
                        self.context['sanitization_stats']['validation_errors'] += 1
            
            self.context['sanitization_stats']['total_processed'] += 1
            result['stats'] = dict(self.context['sanitization_stats'])
            results.append(result)
        
        return results
    
    def _sanitize_value(self, value):
        '''Sanitize input values to prevent XSS and injection attacks'''
        if isinstance(value, str):
            # Remove potentially dangerous characters
            sanitized = value
            
            # HTML encode special characters
            replacements = {
                '<': '&lt;',
                '>': '&gt;',
                '"': '&quot;',
                "'": '&#x27;',
                '&': '&amp;'
            }
            
            for char, replacement in replacements.items():
                sanitized = sanitized.replace(char, replacement)
            
            # Remove null bytes
            sanitized = sanitized.replace('\\x00', '')
            
            # Trim whitespace
            sanitized = sanitized.strip()
            
            if sanitized != value:
                self.context['sanitization_stats']['fields_sanitized'] += 1
            
            return sanitized
        
        elif isinstance(value, (list, tuple)):
            return [self._sanitize_value(item) for item in value]
        
        elif isinstance(value, dict):
            return {k: self._sanitize_value(v) for k, v in value.items()}
        
        else:
            return value
    
    def process_stream(self, queries):
        pass
"""

# Example 5: Time Series Data Analyzer
time_series_analyzer_code = """
class Runnable(BaseSecureRunnable):
    def init(self):
        self.context['data_points'] = []
        self.context['window_size'] = 10
        self.context['calculations'] = {
            'count': 0,
            'sum': 0,
            'min': float('inf'),
            'max': float('-inf')
        }
    
    def process(self, queries):
        results = []
        
        for query in queries:
            if query.get('action') == 'add_data':
                # Add new data point
                value = query.get('value', 0)
                timestamp = query.get('timestamp', 'now')
                
                data_point = {
                    'value': value,
                    'timestamp': timestamp
                }
                
                self.context['data_points'].append(data_point)
                
                # Update running calculations
                self.context['calculations']['count'] += 1
                self.context['calculations']['sum'] += value
                self.context['calculations']['min'] = min(self.context['calculations']['min'], value)
                self.context['calculations']['max'] = max(self.context['calculations']['max'], value)
                
                # Keep only window_size points
                if len(self.context['data_points']) > self.context['window_size']:
                    removed = self.context['data_points'].pop(0)
                    # We'd need to recalculate min/max properly here in production
                
                result = {
                    'action': 'data_added',
                    'current_count': len(self.context['data_points'])
                }
            
            elif query.get('action') == 'get_stats':
                # Calculate statistics
                if self.context['data_points']:
                    values = [dp['value'] for dp in self.context['data_points']]
                    
                    # Moving average
                    moving_avg = sum(values) / len(values)
                    
                    # Standard deviation
                    variance = sum((x - moving_avg) ** 2 for x in values) / len(values)
                    std_dev = variance ** 0.5
                    
                    # Trend (simple linear regression slope)
                    if len(values) > 1:
                        x_values = list(range(len(values)))
                        x_mean = sum(x_values) / len(x_values)
                        y_mean = moving_avg
                        
                        numerator = sum((x - x_mean) * (y - y_mean) 
                                      for x, y in zip(x_values, values))
                        denominator = sum((x - x_mean) ** 2 for x in x_values)
                        
                        trend = numerator / denominator if denominator != 0 else 0
                    else:
                        trend = 0
                    
                    result = {
                        'action': 'statistics',
                        'count': len(values),
                        'mean': round(moving_avg, 2),
                        'std_dev': round(std_dev, 2),
                        'min': min(values),
                        'max': max(values),
                        'trend': round(trend, 4),
                        'trend_direction': 'up' if trend > 0 else 'down' if trend < 0 else 'flat'
                    }
                else:
                    result = {
                        'action': 'statistics',
                        'error': 'No data points available'
                    }
            
            elif query.get('action') == 'detect_anomaly':
                # Simple anomaly detection
                if self.context['data_points']:
                    values = [dp['value'] for dp in self.context['data_points']]
                    mean = sum(values) / len(values)
                    variance = sum((x - mean) ** 2 for x in values) / len(values)
                    std_dev = variance ** 0.5
                    
                    # Check if query value is anomalous (outside 2 std dev)
                    test_value = query.get('value', 0)
                    z_score = (test_value - mean) / std_dev if std_dev > 0 else 0
                    
                    result = {
                        'action': 'anomaly_detection',
                        'value': test_value,
                        'z_score': round(z_score, 2),
                        'is_anomaly': abs(z_score) > 2,
                        'severity': 'high' if abs(z_score) > 3 else 'medium' if abs(z_score) > 2 else 'low'
                    }
                else:
                    result = {
                        'action': 'anomaly_detection',
                        'error': 'Insufficient data for anomaly detection'
                    }
            
            else:
                result = {'error': 'Unknown action'}
            
            results.append(result)
        
        return results
    
    def process_stream(self, queries):
        # Stream processing for real-time analytics
        for query in queries:
            result = self.process([query])[0]
            yield result
"""

def demo_example(name, code, queries):
    """Run a demo of an example"""
    print(f"\n{'='*70}")
    print(f"Example: {name}")
    print(f"{'='*70}")
    
    config = SecurityConfig(
        max_memory_mb=100,
        max_cpu_time_seconds=5,
        max_requests=50,
        allowed_domains=["*"],
        execution_timeout=10,
        enable_resource_limits=False
    )
    
    try:
        builder = SecureRunnableBuilder(config)
        runnable = builder.compile(code.strip())
        
        print("Running example queries:")
        results = runnable.process(queries)
        
        for i, (query, result) in enumerate(zip(queries, results)):
            print(f"\n--- Query {i+1} ---")
            print(f"Input: {query}")
            print(f"Output: {result}")
            
    except Exception as e:
        print(f"Error: {type(e).__name__}: {str(e)}")


if __name__ == "__main__":
    # Demo 1: Data Processing Pipeline
    demo_example(
        "Data Processing Pipeline",
        data_processor_code,
        [
            {
                'data': {'name': 'john doe', 'email': 'john@example.com', 'amount': '$1,234.56'},
                'transforms': ['uppercase', 'clean_numbers', 'validate_email']
            },
            {
                'data': ['test@email.com', 'not-an-email', 'another@test.org'],
                'transforms': ['validate_email']
            }
        ]
    )
    
    # Demo 2: API Aggregator
    demo_example(
        "API Data Aggregator with Caching",
        api_aggregator_code,
        [
            {'endpoint': '/users', 'params': {'id': 1}},
            {'endpoint': '/users', 'params': {'id': 1}},  # Should hit cache
            {'endpoint': '/posts', 'params': {'limit': 3}},
            {'endpoint': '/stats'},
        ]
    )
    
    # Demo 3: Rule Engine
    demo_example(
        "Business Rule Engine",
        rule_engine_code,
        [
            {
                'order_id': 1,
                'total': 1500,
                'customer': {'id': 1, 'orders_count': 5},
                'items': [{'product': 'A', 'quantity': 10}]
            },
            {
                'order_id': 2,
                'total': 500,
                'customer': {'id': 2, 'orders_count': 0},  # New customer
                'items': [{'product': 'B', 'quantity': 60}]  # Bulk order
            }
        ]
    )
    
    # Demo 4: Data Validator
    demo_example(
        "Data Validator and Sanitizer",
        data_validator_code,
        [
            {
                'username': 'user123',
                'email': 'test@example.com',
                'website': 'https://example.com',
                'comment': '<script>alert("xss")</script>',
                '_username_type': 'alphanumeric',
                '_email_type': 'email',
                '_website_type': 'url'
            }
        ]
    )
    
    # Demo 5: Time Series Analyzer
    demo_example(
        "Time Series Data Analyzer",
        time_series_analyzer_code,
        [
            {'action': 'add_data', 'value': 10, 'timestamp': '2024-01-01'},
            {'action': 'add_data', 'value': 12, 'timestamp': '2024-01-02'},
            {'action': 'add_data', 'value': 11, 'timestamp': '2024-01-03'},
            {'action': 'add_data', 'value': 15, 'timestamp': '2024-01-04'},
            {'action': 'add_data', 'value': 18, 'timestamp': '2024-01-05'},
            {'action': 'get_stats'},
            {'action': 'detect_anomaly', 'value': 30},  # Should be anomaly
            {'action': 'detect_anomaly', 'value': 13},  # Should be normal
        ]
    )