# Understanding Ibis
Mostly unintelligible notes on Ibis concepts:

Expressions
- the client API for Ibis
- thinly wrap Operations (all expressions are instantiated from operations)
- expressions are represetned in an AST
Operations
- represent things that can be done when interacting with a backend
- not all backends implement all operations, and this is reflected in the Dialect
- operations can be converted to expressions from their `output_type`
- "Operation" is a broad term, `conn.table` returns an `operation`
- all operations are nodes in an AST
Rules
- define validators for operations that validate inputs client side based on Ibis intermediate representations
Compilation
- take an AST and convert it to a statment (or SQLAlchemy expression)
- `ibis.sql.compiler.Dialect` is the nominal base for all backend compilers (looks like HDF5 doesn't use this, but Pandas does even though it doesn't actually generate an serializable representation of the expression tree).  SQL is assumed to be the
representation for the Expression tree.
- Dialect relies on ExprTranslator to determine the specifics of how the targeted backend
represents each compiled operation
  - `Dialect.make_context` constructs a `QueryContext` class, passing in the `Dialect` and any params
  - When ready to compile, construct a `QueryBuilder` from the Expression tree and context
  - `QueryBuilder.get_result` constructs a `QueryAST` from the main query (setup and teardown queries can mostly be ignored for the moment)
    - `_make_union` and `_make_select` use special `Union` and `SelectBuilder` classes to construct the main query, `SelectBuilder` is the most interesting for the moment (also the most complex)
    - `SelectBuilder.get_result` constructs the list of queries that `QueryAST` cares about, each element in the list is a `Select` instance which compiles its components directly into SQL.  THIS IS THE MAIN COMPONENT TO OVERRIDE
    - `ExprTranslator` contains the rules about how to convert each operation to a backend specific representation.  Each component that `compiles` (`Select`, and `Union`, but ignore it for now) uses the rules defined here to represent each operation, these rules will need to be rewritten
      - `ExprTranslator` has a registry of operations that map an operation type to a formatter for that operation, will need to implement formatters? The core of these formatters are defined in `ibis.impala.compiler` 
      - `ExprTranslator.compiles` is used to register formatters for a specific `ExprTranslator` 
  - `QueryAST.compile` collapses compiled list of `Select`s into a single statement. THIS WILL ALSO NEED TO BE OVERRIDDEN

