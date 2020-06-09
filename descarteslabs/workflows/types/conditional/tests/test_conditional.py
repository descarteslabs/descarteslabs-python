import operator
import mock

import pytest

from descarteslabs.common.graft.interpreter import interpret
from descarteslabs.workflows.types import Function, Int
from .. import ifelse


def test_same_types():
    with pytest.raises(TypeError, match="must be the same type"):
        ifelse(True, 1, "foo")


def test_shared_outer_scope():
    shared = Function[{}, Int]("func")()

    result = ifelse(shared > 0, shared + 1, shared - 1)
    assert isinstance(result, Int)

    func_result = 10
    calls = 0

    def func():
        nonlocal calls
        calls += 1
        return func_result

    interpreted = interpret(
        result.graft,
        builtins={
            "func": func,
            "wf.ifelse": lambda cond, t, f: t() if cond else f(),
            "wf.numpy.add": operator.add,
            "wf.numpy.subtract": operator.sub,
            "wf.numpy.greater": operator.gt,
        },
    )()

    assert interpreted == func_result + 1
    assert calls == 1


def test_short_circuit():
    func1 = Function[{}, Int]("func1")
    func2 = Function[{}, Int]("func2")

    result = ifelse(True, func1(), func2())
    assert isinstance(result, Int)

    func1_mock = mock.Mock(return_value=1)
    func2_mock = mock.Mock(return_value=2)
    interpreted = interpret(
        result.graft,
        builtins={
            "func1": func1_mock,
            "func2": func2_mock,
            "wf.ifelse": lambda cond, t, f: t() if cond else f(),
        },
    )()

    assert interpreted == 1
    func1_mock.assert_called_once()
    func2_mock.assert_not_called()
