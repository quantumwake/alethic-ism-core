def compile_code(code):
    from RestrictedPython import compile_restricted
    # TODO need to enable caching for the compiled code
    compiled_code = compile_restricted(code, '<string>', 'eval')
    return compiled_code

def hashit(input_string, length=8):
    import hashlib
    full_hash = hashlib.sha256(input_string.encode()).hexdigest()
    return full_hash[:length]

def now() -> str:
    from datetime import datetime
    return datetime.now().isoformat()

def now_utc() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()

def safer_evaluate(code, allowed_vars=None, allowed_funcs=None):
    import hashlib
    import random
    import math
    from RestrictedPython.Eval import default_guarded_getitem, default_guarded_getiter
    from RestrictedPython.Guards import guarded_iter_unpack_sequence, safe_builtins, safer_getattr
    compiled_code = compile_code(code)
    restricted_globals = {
        "__builtins__": {
            **safe_builtins,
            'sum': sum,
            'range': range,
            'math': math,
            'random': random,
            'hashlib': hashlib,
        },
        '_getattr_': safer_getattr,  # allow gets
        "_getitem_": default_guarded_getitem,
        "_getiter_": default_guarded_getiter,
        '_iter_unpack_sequence_': guarded_iter_unpack_sequence,
        'hashit': hashit,  # Add hash function to globals
        'now': now,  # add date time string function
        'utc': now_utc(),  # add date time string function (returns utc only)
        'rand_hash': lambda x: hashit(str(random.random()) + str(x)),  # random hash function
    }

    if allowed_funcs:
        restricted_globals.update(allowed_funcs)

    if allowed_vars is None:
        allowed_vars = {}

    result = eval(compiled_code, restricted_globals, allowed_vars)
    return result
