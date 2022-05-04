import pytest

import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import ibis.client

from .. import find_tables


@pytest.fixture
def client():
    return ibis.client.Client()


def table(client, table_name, **cols):
    return ops.DatabaseTable(table_name, sch.Schema.from_dict(cols), client).to_expr()


@pytest.fixture
def table1(client):
    return table(client, "table1", name=dt.String(), value=dt.Int64())


@pytest.fixture
def table2(client):
    return table(client, "table2", name=dt.String(), value=dt.Int64())


@pytest.fixture
def table3(client):
    return table(client, "table3", name=dt.String(), value=dt.Int64())


def test_find_table(table1):
    res = find_tables(table1)

    assert res == {"table1"}


def test_find_table_projection(table1):
    expr = table1[table1.value]

    res = find_tables(expr)

    assert res == {"table1"}


def test_find_table_join(table1, table2):
    expr = table1.inner_join(table2, table1.name == table2.name)[
        table1.name, table2.value
    ]

    res = find_tables(expr)

    assert res == {"table1", "table2"}


def test_find_tables_complex_join(table1, table2, table3):
    t1_view = table1.view()
    j1 = table3.inner_join(t1_view, table3.name == t1_view.name)[t1_view.name]
    j2 = table2.inner_join(j1, table2.name == j1.name)[table2.value]
    expr = table1.inner_join(j2, table1.value == j2.value)[table1.name]

    res = find_tables(expr)

    assert res == {"table1", "table2", "table3"}


def test_find_table_union(table1, table2):
    expr = table1.union(table2)

    res = find_tables(expr)

    assert res == {"table1", "table2"}


def test_find_table_self_union(table1):
    t1_view = table1.view()

    expr = table1.union(t1_view)

    res = find_tables(expr)

    assert res == {"table1"}


def test_find_table_window(table1):
    # something like:
    # SELECT
    #   name
    #   , value - LAG(value) OVER (PARTITION BY name ORDER BY value) AS diff
    # FROM table1
    expr = (
        table1.group_by(table1.name)
        .order_by(table1.value)
        .mutate(diff=table1.value - table1.value.lag())
    )

    res = find_tables(expr)

    assert res == {"table1"}
