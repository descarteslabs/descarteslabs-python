import pytest


def operator_test(obj, all_values_to_try, operator, accepted_types, return_type):
    """
    Try calling the method named ``operator`` on ``obj`` for every value in ``all_values_to_try``.
    If ``type(value being tried)`` is in ``accepted_types``, expect it to return ``return_type``.
    Otherwise, expect it to raise TypeError.

    Return types: can be a plain type, or a dict of {other_value_type: return_type}.
    If a dict, it can have a ``"default"`` key, which is used if ``type(value being tried)``
    is not in the dict.

    If ``accepted_types`` is an empty tuple, ``operator`` is just called ``obj`` with no arguments,
    and the return value is checked to be an instance of ``return_type``.
    ``return_type`` should not be a dict in this case.

    Example::
    ---------

    obj = Foo()
    all_values_to_try = [
        Int(1),
        Float(2.2),
        Bool(True),
        Foo(),
        FooCollection([Foo()]),
        Bar(),
        NoneType(None),
        List[Foo]([Foo()]),
    ]

    @pytest.mark.parametrize(
        "operator, accepted_types, return_type",
        [
            ["__le__", (Foo, Float), Bool],
            ["__eq__", (Foo, Float, Bool), Bool],
            ["__invert__", (), Foo],
            ["__and__", (Foo, FooCollection, Float, Bool), {FooCollection: FooCollection, "default": Foo}],
        ],
    )
    def test_all_operators(operator, accepted_types, return_type):
        operator_test(obj, all_values_to_try, operator, accepted_types, return_type)

    """
    method = getattr(obj, operator)
    if len(accepted_types) == 0:
        result = method()
        err = "Expected {}.{}() to return {}, not {}".format(
            type(obj).__name__, operator, return_type, result
        )
        assert isinstance(result, return_type), err
    else:
        for value in all_values_to_try:
            value_type = type(value)
            if value_type in accepted_types:
                result = method(value)

                if isinstance(return_type, dict):
                    try:
                        expected_type = return_type[value_type]
                    except KeyError:
                        expected_type = return_type["default"]
                else:
                    expected_type = return_type

                err = "Expected {}.{}() called with {} to return {}, not {}".format(
                    type(obj).__name__, operator, value, expected_type, result
                )
                assert isinstance(result, expected_type), err
            else:
                try:
                    result = method(value)
                    # if method doesn't raise a TypeError, need to check for methods
                    # that return NotImplemented
                    if result is not NotImplemented:
                        pytest.fail(
                            "Expected {}.{}() called with {} to raise TypeError".format(
                                type(obj).__name__, operator, value
                            )
                        )
                except TypeError:
                    # TypeError is expected
                    pass
