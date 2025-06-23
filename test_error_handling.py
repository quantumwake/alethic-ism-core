#!/usr/bin/env python3
"""Test improved error handling in secure_runnable.py"""

import sys
sys.path.insert(0, '/Users/kasrarasaee/Development/quantumwake/alethic-ism-core/src')

from ismcore.compiler.secure_runnable import SecureRunnableBuilder, SecurityConfig

config = SecurityConfig(
    max_memory_mb=100,
    max_cpu_time_seconds=5,
    max_requests=50,
    allowed_domains=["*"],
    execution_timeout=10,
    enable_resource_limits=False
)

# Test cases for error handling
test_cases = [
    {
        "name": "Forbidden import statement",
        "code": """
import os
class Runnable(BaseSecureRunnable):
    def init(self):
        pass
    def process(self, queries):
        return queries
    def process_stream(self, queries):
        pass
"""
    },
    {
        "name": "Syntax error",
        "code": """
class Runnable(BaseSecureRunnable):
    def init(self):
        self.context['test' = 123  # Missing closing bracket
    def process(self, queries):
        return queries
    def process_stream(self, queries):
        pass
"""
    },
    {
        "name": "Type annotation error",
        "code": """
class Runnable(BaseSecureRunnable):
    def init(self):
        myvar: List[dict] = []  # Type annotations not supported
        self.context['data'] = myvar
    def process(self, queries):
        return queries
    def process_stream(self, queries):
        pass
"""
    },
    {
        "name": "Missing Runnable class",
        "code": """
class MyClass(BaseSecureRunnable):
    def init(self):
        pass
    def process(self, queries):
        return queries
    def process_stream(self, queries):
        pass
"""
    },
    {
        "name": "Wrong base class",
        "code": """
class Runnable:
    def init(self):
        pass
    def process(self, queries):
        return queries
    def process_stream(self, queries):
        pass
"""
    },
    {
        "name": "Successful code with unpacking",
        "code": """
class Runnable(BaseSecureRunnable):
    def init(self):
        self.context['counter'] = 0
    
    def process(self, queries):
        results = []
        for query in queries:
            # Test unpacking
            if "actions" in query:
                actions = query["actions"].split(",")
                parts = [action.strip() for action in actions if action.strip()]
                query["parsed_actions"] = parts
            
            # Test tuple unpacking
            data = [1, 2, 3]
            a, b, c = data
            query["unpacked"] = [a, b, c]
            
            results.append(query)
        return results
    
    def process_stream(self, queries):
        pass
"""
    }
]

# Run tests
builder = SecureRunnableBuilder(config)

for test in test_cases:
    print(f"\n{'='*60}")
    print(f"Test: {test['name']}")
    print(f"{'='*60}")
    
    try:
        runnable = builder.compile(test['code'].strip())
        print("✅ Compilation successful!")
        
        # Test execution
        result = runnable.process([{"test": "data", "actions": "create,read,update"}])
        print(f"Result: {result}")
        
    except Exception as e:
        print(f"❌ Error: {type(e).__name__}")
        print(f"Message: {str(e)}")
        
        # Show additional error details if available
        if hasattr(e, 'line_no'):
            print(f"Line number: {e.line_no}")
        if hasattr(e, 'pattern'):
            print(f"Pattern: {e.pattern}")
        if hasattr(e, 'errors'):
            print(f"Errors: {e.errors}")
        if hasattr(e, 'warnings'):
            print(f"Warnings: {e.warnings}")