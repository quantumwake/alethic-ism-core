#!/usr/bin/env python3
"""Test file to reproduce and fix unpacking issues"""

# Test the unpacking issue
test_code = '''
class Runnable(BaseSecureRunnable):
    def init(self):
        self.context['counter'] = 0

    def process(self, queries: List[Dict]) -> List[Dict]:
        results = []
        for query in queries:
            # Test unpacking with split
            if "actions" in query:
                actions = query["actions"].split(",")
                parts = [action.strip() for action in actions if action.strip()]
                query["parsed_actions"] = parts
            
            # Test list without type annotation
            myvar = []
            myvar.append({"test": "data"})
            
            # Test tuple unpacking
            data = [1, 2, 3]
            a, b, c = data
            
            results.append({
                **query,
                "counter": self.context['counter'],
                "myvar": myvar,
                "unpacked": [a, b, c]
            })
            self.context['counter'] = self.context['counter'] + 1
        
        return results

    def process_stream(self, queries: List[Any]) -> Any:
        pass
'''

if __name__ == "__main__":
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
    
    try:
        builder = SecureRunnableBuilder(config)
        runnable = builder.compile(test_code)
        
        # Test with some queries
        test_queries = [
            {"actions": "create,read,update,delete", "type": "crud"},
            {"actions": "test, demo, example", "type": "sample"},
            {"no_actions": True}
        ]
        
        result = runnable.process(test_queries)
        print("Results:", result)
        
    except Exception as e:
        print(f"Error: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()