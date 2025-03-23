import json
from logging import log
from typing import List, Dict, Any

import requests
from RestrictedPython import compile_restricted, safe_globals, utility_builtins
from RestrictedPython.Eval import default_guarded_getitem, default_guarded_getiter
from RestrictedPython.Guards import full_write_guard, safer_getattr, guarded_iter_unpack_sequence, guarded_setattr

from ismcore.messaging.base_message_route_model import BaseRoute
from ismcore.model.processor_state import State
from ismcore.storage.processor_state_storage import StateMachineStorage
from ismcore.utils.ism_logger import ism_logger

logging = ism_logger(__name__)

class RestrictedLogger:

    def __init__(self, monitor_route: BaseRoute):
        self.monitor_route = monitor_route

    def log(self, message):
        logging.info(message)


# base class for runnable python scripts that take query inputs and expect outputs
class BaseRunnable:
    def __init__(self,
                 state: State,
                 storage: StateMachineStorage,
                 **kwargs):
        self._storage = storage
        self._state = state
        self.properties = {}

        super().__init__()

    def process_query_states(self, query_states: List[Dict]) -> List[Dict]:
        raise NotImplementedError()

    def process_stream(self, query_state: Any):
        raise NotImplementedError()

    def instantiate(self, code: str) -> 'BaseRunnable':
        if not code:
            raise ValueError(f'unable execute blank template for state route id: {self}')

        # Compile the restricted code
        compiled_code = compile_restricted(code, '<string>', 'exec')

        # Prepare the restricted execution environment
        restricted_globals = safe_globals.copy()
        restricted_globals.update({
            'BaseRunnable': BaseRunnable,
            '__metaclass__': type,
            '__name__': '__main__',
            '__file__': '<string>',
            '_write_': full_write_guard,  # allow writes
            '_getattr_': safer_getattr,  # allow gets
            '_getitem_': default_guarded_getitem,
            '_getiter_': default_guarded_getiter,
            '_iter_unpack_sequence_': guarded_iter_unpack_sequence,
            '_setattr_': guarded_setattr,
            'requests': requests,  # Allow 'requests' module
            'List': List,
            'Dict': Dict,
            'dict': dict,
            'list': list,
            'str': str,
            'int': int,
            'float': float,
            'bool': bool,
            'Any': Any,
            'json': json,
            'log': log,
            **utility_builtins
        })

        # Execute the restricted code
        exec(compiled_code, restricted_globals)

        # Access the newly created class from the restricted globals
        runnable_class = restricted_globals['Runnable']

        # Instantiate the new class of type Runnable locally named runnable_class
        runnable = runnable_class(
            state=self._state,
            storage=self._storage
        )

        runnable.init()

        # return the runnable instance of the class
        return runnable
