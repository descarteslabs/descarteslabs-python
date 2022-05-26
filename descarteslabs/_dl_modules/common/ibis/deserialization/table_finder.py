import ibis.expr.operations as ops
import ibis.expr.types as ir


class PhysicalTableFinder:
    """Finds names of physical tables.

    Recurses through all the roots of an expression to find
    references to find unique PhysicalTable names.

    `roots` refers to all the `TableExpr`s associated with
    an expression.  Since a `TableExpr` may itself contain
    other `TableExpr` we need to traverse all the roots in
    the AST to discover distinct table names.
    """

    __slots__ = ("expr", "op", "_table_names")

    def __init__(self, expr, op=None, table_names=None):
        self.expr = expr

        if op is None:
            op = self.expr.op()
        self.op = op

        if table_names is None:
            table_names = set()
        self._table_names = table_names

    def __call__(self):
        # base case
        if isinstance(self.op, ops.PhysicalTable):
            self._table_names.add(self.op.name)
            return self._table_names

        # find the roots in the top level expression
        distinct_roots = ops.distinct_roots(self.expr)
        for root in distinct_roots:
            root_expr = root.to_expr()

            # Either an expression has a single `TableExpr`,
            # or it has nested expressions that have `roots`
            # of their own.
            # E.G. A `Selection` has an single `InnerJoin`
            # for it's root, but we need to traverse the
            # `InnerJoin` for it's roots, because they may also
            # be complicated structures.
            table_expr = getattr(root, "table", None)

            if table_expr is not None:
                self._table_names = PhysicalTableFinder(
                    table_expr, table_names=self._table_names
                )()
            elif isinstance(root, ops.Union):
                # unions are a special case that don't have "table",
                # but "left"/"right". Can't fall back to recursion
                # in this case because `distinct_roots` points at
                # the parent Union, resulting in infinite recursion
                for prop in ("left", "right"):
                    table_expr = getattr(root, prop, None)
                    self._table_names = PhysicalTableFinder(
                        table_expr, table_names=self._table_names
                    )()

            else:
                self._table_names = PhysicalTableFinder(
                    root_expr, op=root, table_names=self._table_names
                )()
        return self._table_names


def find_tables(expr: ir.Expr):
    """Finds all the PhysicalTable names from an expression."""
    finder = PhysicalTableFinder(expr)
    return finder()
