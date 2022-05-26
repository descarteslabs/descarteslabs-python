# -*- coding: utf-8 -*-

import timeit
import collections

from .. import syntax
from . import exceptions
from .scopedchainmap import ScopedChainMap

DebugState = collections.namedtuple("DebugState", "depth")


def interpret(graft, builtins=None, debug=False):
    """
    Turn a top-level Graft into a function.

    Parameters
    ----------
    graft: Mapping
        A top-level graft function, containing the key "returns".
    builtins: Mapping[str, Any] or None
        Functions (or objects) to make available when evaluating this graft.

    Returns
    -------
    function: Callable
    """
    if isinstance(builtins, ScopedChainMap):
        env = builtins
    else:
        env = ScopedChainMap()
        if builtins is not None:
            env.update(builtins)
    debug = DebugState(depth=0) if debug else None
    return as_function(graft, ScopedChainMap(), env, debug=debug)


def get(key, body, env, debug=None):
    """
    Get a key's value from a graft function body.

    If the key exists in `env` (pre-evaluated keys) at a closer scope than in `body`,
    it's returned from `env`.
    Otherwise, the associated expression is evaluated, and *`env` is updated*
    to store the result, at the scope level in which `key` was defined in body.

    Parameters
    ----------
    key: str
        The key to look up
    body: ScopedChainMap[str, Any]
        The body of a graft function in which to look up `key`
    env: ScopedChainMap[str, Any]
        Keys and their already-evaluated values as Python objects,
        i.e. the scope in which to look up `key`. If `key` is not in env,
        it will be once this function returns.

    Returns
    -------
    result: object
    """
    if key in ("returns", "parameters"):
        raise exceptions.GraftSyntaxError("Cannot depend on the {!r} key".format(key))
    out_of_scope = len(env.maps)
    precomputed, env_level = env.getlevel(key, default_level=out_of_scope)
    expr, body_level = body.getlevel(key, default_level=out_of_scope)

    if body_level < env_level:
        # key may exist in env, but it's re-defined in a closer scope in `body` which we haven't evaluated yet

        if debug is not None:
            indents = "|   " * debug.depth
            formatted_expr = repr(expr)[: max(10, 80 - len(indents))]
            print("{}┌── {!r}: {}".format(indents, key, formatted_expr))
            debug = DebugState(debug.depth + 1)
            start = timeit.default_timer()

        result = evaluate(expr, body, env, debug=debug)

        if debug is not None:
            elapsed = timeit.default_timer() - start
            formatted_result = repr(result)[: max(10, 80 - len(indents))].replace(
                "\n", " "
            )
            print(
                "{}└── {!r}: {:.3f}s -> {}".format(
                    indents, key, elapsed, formatted_result
                )
            )

        env.setlevel(key, result, body_level)
        return result
    else:
        if env_level == out_of_scope:
            # we check env_level instead of body_level because the key may be in builtins of env, but not in body at all
            raise exceptions.GraftNameError(key)
        # key was precomputed in env at closer or equal scope, so we can use it

        if debug is not None:
            indents = "|   " * debug.depth
            formatted_result = repr(precomputed)[: max(10, 80 - len(indents))].replace(
                "\n", " "
            )
            print("{}  * Precomputed {!r}: {}".format(indents, key, formatted_result))

        return precomputed


def evaluate(expr, body, env, debug=None):
    """
    Evaluate an expression, given a graft body (AST) and environment of pre-evaluated keys.

    If `expr` depends on other keys, they will be recursively evaluated,
    and `env` will be updated to contain them.

    Returns
    -------
    result: object
    """
    if syntax.is_application(expr):
        func = get(expr[0], body, env, debug=debug)

        if syntax.is_key(expr[-1]):
            expr.append({})

        positional_args = tuple(get(key, body, env, debug=debug) for key in expr[1:-1])
        named_args = {
            name: get(key, body, env, debug=debug) for name, key in expr[-1].items()
        }

        return func(*positional_args, **named_args)
    elif syntax.is_literal(expr):
        return expr
    elif syntax.is_graft(expr):
        return as_function(expr, body, env, debug=debug)
    elif syntax.is_quoted_json(expr):
        return expr[0]
    else:
        raise exceptions.GraftSyntaxError("Not a valid expression: {}".format(expr))


def as_function(expr, body, env, debug=None):
    "Turn a graft function into a Python function"
    try:
        returns = expr["returns"]
    except KeyError:
        raise exceptions.GraftSyntaxError(
            "Graft is missing a 'returns' key: {}".format(expr)
        )
    if not syntax.is_key(returns):
        raise exceptions.GraftSyntaxError(
            "Invalid value for a 'returns' key: {}".format(returns)
        )

    parameters = expr.get("parameters", ())
    if not syntax.is_params(parameters):
        raise exceptions.GraftSyntaxError(
            "Invalid parameters list {}".format(parameters)
        )

    subgraph = body.new_child(expr)

    def func(*args, **named_args):
        syntax.check_args(
            len(args),
            named_args.keys(),
            parameters,
            exception_type=exceptions.GraftTypeError,
        )

        named_args.update(zip(parameters, args))
        closure = env.new_child(named_args)
        return get(returns, subgraph, closure, debug=debug)

    return func
