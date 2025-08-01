import json
from ismcore.compiler.secure_runnable import SecureRunnableBuilder, SecurityConfig

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

# Template implementation for state querying
test_code = """
class Runnable(BaseSecureRunnable):
    def init(self):
        self.context['counter'] = 0
    
    def process(self, queries):
        results = []
        for query in queries:
            response_id = query['response_id']
            result = self.query_state_data(
                state_id='8a516a57-f473-4027-b01c-ba5268571831',
                filters=[{
                    "column": "response_id",
                    "operator": "=",
                    "value": response_id
                }]
            )
            result = self.pivot_list_of_dicts(result)
            results.append(result)
        
        return results
    
    def process_stream(self, query):
        yield json.dumps({
            **query,
            **self.query_stock(query)
        }, indent=2)
"""

# Test queries
test_queries = [
    {"response_id": "6711c9816bba2c051ed1b0c8"},
    # {"response_id": "resp_002"},
    # {"response_id": "resp_003"}
]

def test_query_api():
    """Run the simple state query test"""
    print("Running simple state query test")
    print("=" * 70)
    
    try:
        # Build and compile
        builder = SecureRunnableBuilder(config)
        runnable = builder.compile(test_code.strip())
        print("✅ Compilation successful!")
        
        # Run test queries
        print("\nExecuting test queries:")
        results = runnable.process(test_queries)
        
        # Display results
        for i, (query, result) in enumerate(zip(test_queries, results)):
            print(f"\nQuery {i+1}:")
            print(f"Input: {json.dumps(query, indent=2)}")
            print(f"Output: {json.dumps(result, indent=2)}")
        
    except Exception as e:
        print(f"\n❌ Error: {type(e).__name__}")
        print(f"Message: {str(e)}")
        return False
    
    return True