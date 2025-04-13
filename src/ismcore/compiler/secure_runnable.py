import hashlib
import json
import math
import random
import re
import signal
import resource
from abc import ABC, abstractmethod
from datetime import time, datetime
from fnmatch import fnmatch
from typing import List, Dict, Any, Set, Type
from dataclasses import dataclass
from contextlib import contextmanager
from logging import getLogger, Logger

from RestrictedPython import compile_restricted, safe_globals
from RestrictedPython.Eval import default_guarded_getitem, default_guarded_getiter
from RestrictedPython.Guards import (
    safer_getattr,
    guarded_iter_unpack_sequence,
    guarded_setattr,
    _write_wrapper,
)

logger = getLogger(__name__)


class BaseSecureRunnable(ABC):
    """Base class for secure runnables that user code will extend"""

    def __init__(self, security_config: 'SecurityConfig'):
        self._config = security_config
        self.context = SecureContext()
        self.logger = SecureLogger(logger)
        self.requests = RestrictedRequests(security_config)

    @abstractmethod
    def process(self, queries: List[Dict]) -> List[Dict]:
        """Process a single query"""
        pass

    @abstractmethod
    def process_stream(self, queries: List[Any]) -> Any:
        """Process query|ies and yield new output values"""
        pass

    @abstractmethod
    def init(self):
        """Initialize the runnable"""
        pass


    def pivot_list_of_dicts(self, data):
        result = []  # List to hold the pivoted rows
        current_dict = {}  # Dictionary to hold the current row
        current_index = 1  # Track the current index

        if not data:
            return result

        logger.info("pivoting data to table")
        # Iterate over the data
        for row in data:
            # Check if we're starting a new index
            if row['data_index'] != current_index:
                # If current_dict is not empty, add it to the result list
                if current_dict:
                    result.append(current_dict)

                # Start a new dictionary for the new index
                current_dict = {}
                current_index = row['data_index']

            current_dict = {**current_dict, row['column_name']: row['data_value']}

        logger.info("appending final dictionary to list")
        # Append the final dictionary to the result list
        if current_dict:
            result.append(current_dict)

        return result

    # Function to call /api/v1/query/:state
    def call_api_query(self, state_id: str, user_id: str, filters: List[Dict]) -> Dict:
        base_url = "https://api.ism.quantumwake.io"
        url = f"{base_url}/api/v1/query/state/{state_id}"

        # Construct the request payload
        payload = {
            "filter_groups": [
                {
                    "filters": filters,
                    # "filters": [
                    #     {"column": "instruction", "operator": "like", "value": "%problematic%"},
                    #     {"column": "animal", "operator": "=", "value": "cat"}
                    # ],
                    "group_logic": "AND"
                }
            ],
            "state_id": state_id,
            "user_id": user_id
        }

        # Headers to ensure JSON request and response
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        # Initialize the session locally
        with requests.Session() as session:
            response = session.post(url, headers=headers, json=payload)

        # Handle the response
        if response.status_code == 200:
            return response.json()  # Return JSON if successful
        else:
            return str(response)

    ## TODO example -- rip this out, this was only for a demo.
    def get_stock_data(self, ticker: str):
        api_key = "<>"  # Replace with your actual API key
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval=5min&apikey={api_key}"

        logger.info(f"here is the url {url}, requests: {requests}")

        # Create a session
        response = requests.get(url)

        logger.info(f"here is the session {response}")

        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()  # Parse the response as JSON
            return data  # Return the JSON data for further processing
        else:
            return f"Error: Received status code {response.status_code}"

    def query_stock(self, query: Dict):
        self.logger.info("test message")
        # ticker = "AAPL
        ticker = query['ticker']
        self.logger.info("stock ticker info")

        stock_data = self.get_stock_data(ticker)
        if stock_data:
            logger.info(f"stock data 1: {stock_data}")
            stock_data = stock_data['Time Series (5min)']
            logger.info(f"stock data 2: {stock_data}")
            stock_data = list(stock_data.items())[0]
            logger.info(f"stock data 3: {stock_data}")
            stock_data = stock_data[1]

            updated_data = {
                "ticker_open": stock_data["1. open"],
                "ticker_high": stock_data["2. high"],
                "ticker_low": stock_data["3. low"],
                "ticker_close": stock_data["4. close"],
                "ticker_volume": stock_data["5. volume"],
            }

        return {"ticker": ticker, **updated_data}


@dataclass
class SecurityConfig:
    """Configuration for security controls"""
    max_memory_mb: int = 10
    max_cpu_time_seconds: int = 5
    max_requests: int = 50
    allowed_domains: List[str] = None
    execution_timeout: int = 10
    max_string_length: int = 1000000
    max_container_length: int = 10000
    enable_resource_limits: bool = False


class SecureLogger:
    """Restricted logging wrapper"""

    def __init__(self, logger: Logger):
        self._logger = logger

    def info(self, message: str):
        if len(str(message)) > 1000:  # Prevent log flooding
            message = f"{str(message)[:1000]}... (truncated)"
        # self._logger.info(message)
        print(message)

    def error(self, message: str):
        if len(str(message)) > 1000:
            message = f"{str(message)[:1000]}... (truncated)"
        # self._logger.error(message)
        print(message)


from urllib.parse import urlparse
import requests
import threading

class RestrictedRequests:
    """Secure wrapper for requests with rate limiting and domain restrictions"""

    def __init__(self, security_config):
        self._config = security_config
        self._request_count = 0
        self._lock = threading.Lock()

    def _validate_url(self, url: str) -> bool:
        if not self._config.allowed_domains:
            return False

        domain = urlparse(url).netloc

        # Check if the domain matches any allowed domain with potential wildcards
        return any(fnmatch(domain, allowed) for allowed in self._config.allowed_domains)

    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        with self._lock:
            if self._request_count >= self._config.max_requests:
                raise Exception("Maximum request limit exceeded")

            if not self._validate_url(url):
                raise Exception(f"Domain not allowed. Must match: {self._config.allowed_domains}")

            self._request_count += 1

        try:
            response = requests.request(method, url, **kwargs, timeout=5)
            return response
        except Exception as e:
            raise Exception(f"Request failed: {str(e)}")

    # Add specific HTTP methods
    def get(self, url: str, **kwargs) -> requests.Response:
        """Sends a GET request."""
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> requests.Response:
        """Sends a POST request."""
        return self.request("POST", url, **kwargs)

    def put(self, url: str, **kwargs) -> requests.Response:
        """Sends a PUT request."""
        return self.request("PUT", url, **kwargs)

    def delete(self, url: str, **kwargs) -> requests.Response:
        """Sends a DELETE request."""
        return self.request("DELETE", url, **kwargs)



class ResourceLimiter:
    """Handles system resource limitations"""

    @staticmethod
    def set_limits(security_config: SecurityConfig):
        """Set resource limits for memory and CPU"""

        def set_mem_limit():
            try:
                # Get current memory limits
                _, hard_mem_limit = resource.getrlimit(resource.RLIMIT_AS)
                requested_mem = security_config.max_memory_mb * 1024 * 1024

                # Set memory limit - use the minimum of requested and hard limit
                mem_limit = min(requested_mem, hard_mem_limit) if hard_mem_limit > 0 else requested_mem
                resource.setrlimit(resource.RLIMIT_AS, (mem_limit, hard_mem_limit))
                return mem_limit
            except ValueError as e1:
                logger.warning(f"Could not set exact resource limits: {str(e1)}")
            except resource.error as e2:
                logger.warning(f"Resource limit setting failed: {str(e2)}")
                # Fall back to system defaults
                pass

        def set_cpu_limit():
            try:
                # Get current CPU limits
                _, hard_cpu_limit = resource.getrlimit(resource.RLIMIT_CPU)
                requested_cpu = security_config.max_cpu_time_seconds

                # Set CPU limit - use the minimum of requested and hard limit
                cpu_limit = min(requested_cpu, hard_cpu_limit) if hard_cpu_limit > 0 else requested_cpu
                resource.setrlimit(resource.RLIMIT_CPU, (cpu_limit, hard_cpu_limit))
                return cpu_limit
            except ValueError as e1:
                logger.warning(f"Could not set exact resource limits: {str(e1)}")
            except resource.error as e2:
                logger.warning(f"Resource limit setting failed: {str(e2)}")
                # Fall back to system defaults
                pass

        final_cpu_limit = set_cpu_limit()
        final_mem_limit = set_mem_limit()
        logger.info(f"Resource limits set - Memory: {final_mem_limit} bytes, CPU: {final_cpu_limit} seconds")


class SecureBuiltins:
    """Secure wrappers for built-in functions with resource limits"""

    def __init__(self, max_container_size: int = 10000):
        self.max_container_size = max_container_size

    def _check_size(self, iterable, operation: str):
        """Check if operation would exceed size limits"""
        try:
            size = len(iterable)
            if size > self.max_container_size:
                raise ValueError(
                    f"{operation} operation exceeds maximum size limit "
                    f"of {self.max_container_size} elements"
                )
        except TypeError:
            # If len() doesn't work, we'll limit during iteration
            pass

    def sorted(self, iterable, **kwargs):
        """Resource-limited sorted"""
        self._check_size(iterable, "sorted")
        result = sorted(iterable, **kwargs)
        if len(result) > self.max_container_size:
            raise ValueError("Result exceeds maximum size limit")
        return result

    def map(self, func, *iterables):
        """Resource-limited map"""
        for it in iterables:
            self._check_size(it, "map")

        class LimitedMap:
            def __init__(self, func, iterables, max_size):
                self.func = func
                self.iterables = iterables
                self.max_size = max_size
                self.count = 0
                self.map_iter = map(func, *iterables)

            def __iter__(self):
                return self

            def __next__(self):
                if self.count >= self.max_size:
                    raise ValueError("Map iteration exceeded maximum size limit")
                self.count += 1
                return next(self.map_iter)

        return LimitedMap(func, iterables, self.max_container_size)

    def filter(self, func, iterable):
        """Resource-limited filter"""
        self._check_size(iterable, "filter")

        class LimitedFilter:
            def __init__(self, func, iterable, max_size):
                self.func = func
                self.iterable = iterable
                self.max_size = max_size
                self.count = 0
                self.filter_iter = filter(func, iterable)

            def __iter__(self):
                return self

            def __next__(self):
                if self.count >= self.max_size:
                    raise ValueError("Filter iteration exceeded maximum size limit")
                self.count += 1
                return next(self.filter_iter)

        return LimitedFilter(func, iterable, self.max_container_size)

    def range(self, *args):
        """Resource-limited range"""
        # Calculate range size based on args
        if len(args) == 1:
            size = args[0]
        elif len(args) == 2:
            size = args[1] - args[0]
        elif len(args) == 3:
            size = (args[1] - args[0]) // args[2]
        else:
            raise TypeError("Range takes 1-3 arguments")

        if size > self.max_container_size:
            raise ValueError(
                f"Range size {size} exceeds maximum size limit "
                f"of {self.max_container_size}"
            )
        return range(*args)

    def zip(self, *iterables):
        """Resource-limited zip"""
        for it in iterables:
            self._check_size(it, "zip")

        class LimitedZip:
            def __init__(self, iterables, max_size):
                self.iterables = iterables
                self.max_size = max_size
                self.count = 0
                self.zip_iter = zip(*iterables)

            def __iter__(self):
                return self

            def __next__(self):
                if self.count >= self.max_size:
                    raise ValueError("Zip iteration exceeded maximum size limit")
                self.count += 1
                return next(self.zip_iter)

        return LimitedZip(iterables, self.max_container_size)

    # Safe operations that don't need limits
    enumerate = enumerate
    reversed = reversed
    sum = sum
    min = min
    max = max
    round = round
    abs = abs
    all = all
    any = any

    # Safe type constructors
    dict = dict
    list = list
    tuple = tuple
    set = set
    str = str
    int = int
    float = float
    bool = bool
    len = len


def get_secure_builtins(max_container_size: int = 10000):
    """Get dictionary of secure built-in functions"""
    secure_builtins = SecureBuiltins(max_container_size)

    return {
        'dict': secure_builtins.dict,
        'list': secure_builtins.list,
        'tuple': secure_builtins.tuple,
        'set': secure_builtins.set,
        'str': secure_builtins.str,
        'int': secure_builtins.int,
        'float': secure_builtins.float,
        'bool': secure_builtins.bool,
        'len': secure_builtins.len,
        'range': secure_builtins.range,
        'enumerate': secure_builtins.enumerate,
        'zip': secure_builtins.zip,
        'map': secure_builtins.map,
        'filter': secure_builtins.filter,
        'sum': secure_builtins.sum,
        'min': secure_builtins.min,
        'max': secure_builtins.max,
        'round': secure_builtins.round,
        'abs': secure_builtins.abs,
        'all': secure_builtins.all,
        'any': secure_builtins.any,
        'sorted': secure_builtins.sorted,
        'reversed': secure_builtins.reversed,
    }


@contextmanager
def timeout_context(seconds: int):
    """Context manager for code execution timeout"""

    def timeout_handler(signum, frame):
        raise TimeoutError("Code execution timed out")

    original_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(seconds)

    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, original_handler)


class SecureContext:
    """Container for secure variable storage"""

    def __init__(self):
        self._data = {}
        self._is_secure_storage = True

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        self._data[key] = value

    def get(self, key, default=None):
        return self._data.get(key, default)


def create_secure_write_guard(additional_safe_types: Set[Type] = None):
    """
    Creates a write guard that allows writes to specified safe types
    and our secure storage while maintaining restrictions on other objects
    """
    # Base safe types plus any additional ones provided
    safetypes = {dict, list}
    if additional_safe_types:
        safetypes.update(additional_safe_types)

    Wrapper = _write_wrapper()

    def guard(ob):
        # Allow writes to our secure storage
        if hasattr(ob, '_is_secure_storage'):
            return ob

        # Don't wrap safe types or objects that handle their own security
        if type(ob) in safetypes or hasattr(ob, '_guarded_writes'):
            return ob

        # Wrap all other objects
        return Wrapper(ob)

    return guard


def inplacevar_wrapper(op, x, y):
    """Safe wrapper for augmented assignment"""
    if op == '+=':
        return x + y
    elif op == '-=':
        return x - y
    elif op == '*=':
        return x * y
    elif op == '/=':
        return x / y
    elif op == '//=':
        return x // y
    elif op == '%=':
        return x % y
    elif op == '**=':
        return x ** y
    elif op == '>>=':
        return x >> y
    elif op == '<<=':
        return x << y
    elif op == '&=':
        return x & y
    elif op == '^=':
        return x ^ y
    elif op == '|=':
        return x | y
    raise ValueError(f"Unsupported augmented assignment operator: {op}")


class SecureRunnableBuilder:
    """Base class for secure code execution"""

    def __init__(self, security_config: SecurityConfig):
        self._config = security_config

        # accessible
        self.context = SecureContext()
        self.logger = SecureLogger(logger)
        self.requests = RestrictedRequests(security_config)

    @staticmethod
    def validate_code(code: str) -> bool:
        """Validate code for potentially dangerous patterns"""
        # forbidden_patterns = [
        #     "import",
        #     # "__",
        #     "eval",
        #     "exec",
        #     "subprocess",
        #     "os.",
        #     "sys.",
        #     "open",
        #     "file",
        #     "breakpoint",
        #     "globals",
        #     "locals"
        # ]

        # Add "__" rule specifically to avoid risky uses of double underscores
        forbidden_patterns = [
            r"\bimport\b",  # Matches 'import' as a standalone word
            r"eval\(",  # Direct eval function call
            r"exec\(",  # Direct exec function call
            r"subprocess\.",  # Accessing subprocess module
            r"os\.",  # Accessing os module
            r"sys\.",  # Accessing sys module
            r"open\(",  # Direct open function call
            r"file\(",  # Direct file function call
            r"breakpoint\(",  # Direct breakpoint function call
            r"globals\(",  # Direct globals function call
            r"locals\(",  # Direct locals function call
            r"(^|[^a-zA-Z0-9_])__[^a-zA-Z0-9_]",  # Matches "__" at the beginning or end of words
        ]
        return not any(pattern in code for pattern in forbidden_patterns)

    def create_restricted_globals(self):
        """Create restricted global environment"""

        # Create write guard with our secure container type
        secure_write_guard = create_secure_write_guard({
            SecureContext,  # Allow writes to our container
            dict,  # Allow dict operations
            list  # Allow list operations
        })

        restricted = safe_globals.copy()
        restricted.update({
            # Base class
            'BaseSecureRunnable': BaseSecureRunnable,

            # Guards
            '_write_': secure_write_guard,
            '_getattr_': safer_getattr,
            '_getitem_': default_guarded_getitem,
            '_getiter_': default_guarded_getiter,
            '_iter_unpack_sequence_': guarded_iter_unpack_sequence,
            '_setattr_': guarded_setattr,
            '_inplacevar_': inplacevar_wrapper,

            # Rest of your globals...
            'dict': dict,
            'list': list,
            'tuple': tuple,
            'set': set,
            'str': str,
            'int': int,
            'float': float,
            'bool': bool,
            'len': len,
            'range': range,
            'enumerate': enumerate,
            'zip': zip,
            'map': map,
            'filter': filter,
            'sum': sum,
            'min': min,
            'max': max,
            'round': round,
            'abs': abs,
            'all': all,
            'any': any,
            'sorted': sorted,
            'reversed': reversed,

            # Restricted utilities
            'requests': self.requests,
            'logger': self.logger,
            'context': self.context,
            # 'print': print,

            # More restricted utilities
            'math': math,
            'random': random,
            'hashlib': hashlib,
            'json': json,
            'time': time,
            'datetime': datetime,
            're': re,

            # Type hints
            'List': List,
            'Dict': Dict,
            'Any': Any,

            # Required for class definition
            '__name__': '__main__',
            '__metaclass__': type,

            # Additional utilities from RestrictedPython
            **get_secure_builtins(self._config.max_container_length)
        })
        return restricted

    def compile(self, code: str) -> BaseSecureRunnable:
        """Compile code into a secure runnable instance"""
        if not self.validate_code(code):
            raise ValueError("Code contains forbidden patterns")

        try:
            # Set resource limits if enabled
            if self._config.enable_resource_limits:
                ResourceLimiter.set_limits(self._config)

            # Compile the restricted code
            compiled = compile_restricted(code, '<string>', 'exec')

            # Prepare execution environment
            restricted_globals = self.create_restricted_globals()
            local_dict = {}

            # Execute the code to define the class
            with timeout_context(self._config.execution_timeout):
                exec(compiled, restricted_globals, local_dict)

            # Get the user-defined Runnable class
            if 'Runnable' not in local_dict:
                raise ValueError("Code must define a 'Runnable' class that extends BaseSecureRunnable")

            runnable_class = local_dict['Runnable']

            # Verify the class extends BaseSecureRunnable
            if not issubclass(runnable_class, BaseSecureRunnable):
                raise ValueError("Runnable class must extend BaseSecureRunnable")

            # Instantiate the class
            instance = runnable_class(
                security_config=self._config
            )

            # Initialize the instance
            instance.init()
            return instance

        except Exception as e1:
            logger.error(f"Failed to compile secure runnable: {str(e1)}")
            raise


# Example usage
if __name__ == "__main__":
    config = SecurityConfig(
        max_memory_mb=100,
        max_cpu_time_seconds=5,
        max_requests=50,
        allowed_domains=["*"],
        execution_timeout=10,
        enable_resource_limits=False
    )

    # Example user code that defines a runnable class
    user_code = """
class Runnable(BaseSecureRunnable):
    def init(self):
        self.context['counter'] = 0

    def process(self, queries: List[Any]) -> List[Any]:

        c = self.context['counter']
        self.context['counter'] = c + 1
        self.context['other'] = f"other_{c}"

        return [{
            'index': self.context['counter'],
            **query
        } for query in queries]

    def process_stream(self, queries: List[Any]) -> Any:
        # yield from (self.process(query) for query in queries)
        pass

""".lstrip()

    user_code2 = """
class Runnable(BaseSecureRunnable):
    def init(self):
        self.context['counter'] = 0

    def process(self, queries: List[Any]) -> List[Any]:
        self.logger.info("test message")
        ticker = "AAPL"
        stock_data = self.get_stock_data(ticker)
        if stock_data:
            stock_data = stock_data['Time Series (5min)']
            stock_data = list(stock_data.items())[0]
            stock_data = stock_data[1]

        return [{
            'ticker': ticker,
            **stock_data,
            **query
        } for query in queries]

    def process_stream(self, query: Dict) -> Any:
        yield json.dumps(query, indent=2)
    
    """

    user_code3 = """
class Runnable(BaseSecureRunnable):
    def init(self):
        self.context['counter'] = 0

    def process(self, queries: List[Any]) -> List[Any]:
        return [{
            **self.query_stock(query),
            **query
        } for query in queries]

    def process_stream(self, query: Dict) -> Any:
        yield json.dumps({
            **query,
            **self.query_stock(query)
        }, indent=2)
    
"""

    try:
        # Create builder
        builder = SecureRunnableBuilder(config)

        # Compile and instantiate the runnable
        runnable = builder.compile(user_code3)

        # Use the runnable
        # for i in range(5):
        #   result = runnable.process(queries=[{'test': 'data'}])
        #   print(f"Query result: {result}")

        result = runnable.process(queries=[{'is_stock_question': 'true', 'ticker': 'AAPL'}])
        print(f"Query result: {result}")

        # batch_result = runnable.process_async([
        #     {'test': 'data1'},
        #     {'test': 'data2'}
        # ])
        # print(f"Batch result: {batch_result}")
    except Exception as e:
        print(f"Error: {str(e)}")
