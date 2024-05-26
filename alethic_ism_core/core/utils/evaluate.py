import math

from RestrictedPython import compile_restricted
from RestrictedPython.Eval import default_guarded_getitem, default_guarded_getiter
from RestrictedPython.Guards import guarded_iter_unpack_sequence, safe_builtins, safer_getattr


def compile_code(code):
    # TODO need to enable caching of code compiled
    compiled_code = compile_restricted(code, '<string>', 'eval')
    return compiled_code


def safer_evaluate(code, allowed_vars=None, allowed_funcs=None):
    compiled_code = compile_code(code)
    restricted_globals = {
        "__builtins__": {
            **safe_builtins,
            'sum': sum,
            'range': range,
            'math': math
        },
        '_getattr_': safer_getattr,  # allow gets
        "_getitem_": default_guarded_getitem,
        "_getiter_": default_guarded_getiter,
        '_iter_unpack_sequence_': guarded_iter_unpack_sequence,
    }

    if allowed_funcs:
        restricted_globals.update(allowed_funcs)

    if allowed_vars is None:
        allowed_vars = {}

    result = eval(compiled_code, restricted_globals, allowed_vars)
    return result
