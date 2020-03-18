# Graft: functional directed acyclic dependency graphs

A Graft describes a computation that depends on other computations. Unlike a plain DAG, though, the graft is *functional*: it takes parameters, can contain subgraphs (aka functions with parameters), and can apply those functions to values in the graph. Indeed, it's actually just the abstract syntax tree of a functional programming language.

It looks like this:

```yaml
params: ["p1", "p2"]
a: 10
b: ["multiply", "a", "p1"]
c: ["subtract", "b", "p2"]
returns: "c"
```

That's equivalent to:

```python
def f(param1, param2):
  a = 10
  b = a * param1
  c = b - param2
  return c
```

How about with functions (subgraphs)?

```yaml
params: []
ten: 10
xs: ["range", "ten"]
total: ["sum", "xs"]

diff_from_total:
  params: ["x"]
  diff: ["sub", "x", "total"]
  returns: "diff"

diffs: ["map", "diff_from_total", "xs"]
returns: "diffs"
```

That's equivalent to:

```python
xs = range(10)
total = sum(xs)

def diff_from_total(x):
  diff = x - total
  return diff

diffs = map(diff_from_total, xs)
return diffs
```

### Syntax

```
function    :: {
                 "parameters": [key, ...],  # optional
                 key: expression,
                 ...
                 "returns": key
               }

key         :: string

expression  :: literal
            :: application
            :: function
            :: quoted-json

literal     :: string
            :: number
            :: boolean
            :: null

quoted-json :: [list or mapping of any valid JSON]
               # arbitrary JSON can be inlined in the graft by
               # just wrapping it in a list. Mappings and sequences
               # containing any JSON-serializable objects (including nested lists
               # and mappings, of course) are acceptable. However, singleton literals
               # (strings, numbers, etc) should be given with the literal syntax
               # to avoid ambiguity with zero-argument function application.

application :: [key-that-refers-to-function, key, ...]                              # positional arguments
            :: [key-that-refers-to-function, {parameter-name: key, ...}]            # keyword arguments
            :: [key-that-refers-to-function, key, ..., {parameter-name: key, ...}]  # positional and/or keyword arguments
```

### Keys

Keys can be any string, except the reserved words `"parameters"` and `"returns"`, and any names given in `"parameters"`.
Every key in a function body must be unique (by definition, since the body is a dictionary).

Keys don't have to be unique between a graph and its subgraphs: a subgraph can re-define keys used in the parent. That brings us to...

### Scoping

Keys are looked up in this order:

1. The current graph body and parameters
2. Any parent graphs, recursively
3. Global builtins

Therefore, a function can shadow keys defined in its parent. That doesn't affect the values in the parent.

Functions are lexically scoped. A function's scope is a closure over the scope of its parent.

### Parameters

Parameter names are given in an optional list under the key `"parameters"`; if that key isn't present, the graft takes no parameters. Equivalently, the parameters list could be empty.

### Arguments

Functions (grafts or builtins) can be called with positional arguments, named arguments, or both. The function application expression is a list always starting with a string key referring to a callable (builtin or graft), followed by 0 or more keys as positional arguments. Optionally, the last value can be a mapping of parameter names to keys, as named arguments. The same parameter cannot be given both positionally and by name.

For example, calling a function with no arguments: `result: ["func"]`

Calling a function with positional arguments: `result: ["func", "key1", "key2"]`.

Calling a function with positional and named arguments: `result: ["func", "key1", "key2", {"param_x": "key3"}]`.

Calling a function with only named arguments: `result: ["func", {"param_x": "key3"}]`.

For grafts, named arguments aren't particularly useful, since there are no default arguments, and every parameter is required. (The one benefit might be readability, in just being more explicit about what each argument means.) Named arguments are useful for builtin functions, though, especially since builtin functions might take arbitrary keyword arguments, or have default values for parameters, etc.

### Recursion

would violate the "acyclic" invariant, so it is not possible. However, this is not currently validated, so recursion will instead cause overflow, especially since there is no control flow and thus no way to end it.

Similarly, a key may not be referenced in its expression, or any of its dependencies, since that would create a cycle. This is similarly not validated.

### Evaluation

A key is evaluated by recursively looking up and evaluating its dependencies.

Note that the body of a function is an unordered mapping of keys to expressions, so the order in which keys are defined in the source code is irrelevant. An expression can depend on a key that's defined after it in the text. Likewise, a function's scope includes keys defined after it in the text.
