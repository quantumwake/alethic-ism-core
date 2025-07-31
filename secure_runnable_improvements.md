# Recommended Improvements for secure_runnable.py

## 1. Missing Guards and Features

### Add these RestrictedPython guards:
```python
# In create_restricted_globals method, add:
'_apply_': default_guarded_apply,  # For function calls with *args
'_delattr_': guarded_delattr,      # For del obj.attr operations
'__import__': None,                # Explicitly block import
'__builtins__': safe_builtins,     # Use RestrictedPython's safe_builtins
```

### Add PrintCollector for safe print:
```python
from RestrictedPython import PrintCollector

# In create_restricted_globals:
'_print_': PrintCollector,
'_print': PrintCollector,
```

## 2. Type Annotations Support

Since RestrictedPython doesn't support type annotations (AnnAssign), add documentation:
```python
# In class docstring or validate_code method:
"""
Note: Type annotations like 'myvar: List[dict] = []' are not supported.
Use 'myvar = []' instead and document types in comments.
"""
```

## 3. Enhanced Security Validation

Replace basic string pattern matching with regex:
```python
import re

def validate_code(code: str) -> bool:
    forbidden_patterns = [
        r'\bimport\b',
        r'\bfrom\b.*\bimport\b',
        r'eval\s*\(',
        r'exec\s*\(',
        r'compile\s*\(',
        r'__.*__',  # Dunder methods
        r'getattr\s*\(',
        r'setattr\s*\(',
        r'delattr\s*\(',
        r'vars\s*\(',
        r'dir\s*\('
    ]
    
    for pattern in forbidden_patterns:
        if re.search(pattern, code):
            return False
    return True
```

## 4. Better Error Messages

Add context to compilation errors:
```python
def compile(self, code: str) -> BaseSecureRunnable:
    try:
        compiled = compile_restricted(code, '<string>', 'exec')
        
        # Check for compilation errors
        if compiled.errors:
            raise ValueError(f"Code compilation errors: {compiled.errors}")
        
        if compiled.warnings:
            logger.warning(f"Code compilation warnings: {compiled.warnings}")
            
    except SyntaxError as e:
        raise ValueError(f"Syntax error at line {e.lineno}: {e.msg}")
```

## 5. Add List/Dict Comprehension Support

Ensure comprehensions work properly:
```python
# These guards should already handle comprehensions, but verify:
'_iter_unpack_sequence_': guarded_iter_unpack_sequence,
'_unpack_sequence_': guarded_unpack_sequence,
```

## 6. Add Safe Built-in Functions

Consider adding these useful but safe functions:
```python
# In get_secure_builtins or create_restricted_globals:
'isinstance': isinstance,
'hasattr': hasattr,
'getattr': safer_getattr,  # Use the guarded version
'chr': chr,
'ord': ord,
'hex': hex,
'bin': bin,
'format': format,
```

## 7. Add Context Manager Support

For with statements:
```python
'__enter__': lambda self: self,
'__exit__': lambda self, *args: None,
```

## 8. Add Timeout for Individual Operations

Consider adding per-operation timeouts:
```python
import functools
import signal

def timeout_decorator(seconds):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Implementation similar to timeout_context
            pass
        return wrapper
    return decorator
```

## 9. Add Memory Usage Tracking

Track memory usage during execution:
```python
import psutil
import os

def get_memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024  # MB
```

## 10. Add Execution Metrics

Track execution statistics:
```python
@dataclass
class ExecutionMetrics:
    execution_time: float
    memory_used: float
    requests_made: int
    errors_count: int
```

## Simple Implementation Priority:
1. Add missing guards (_apply_, _delattr_, safe_builtins)
2. Add PrintCollector for safe print
3. Improve error messages
4. Add useful safe built-ins (isinstance, hasattr, etc.)
5. Document type annotation limitation