import mock
import pytest
from .. import Retry, RetryError, truncated_delay_generator
from ..retry import _DEFAULT_DELAY_INITIAL, _retry


class FakeException(Exception):
    pass


def fake_delay_generator(*args, **kwargs):
    while True:
        yield 0


def fake_func(x):
    return x


def fake_failing_func():
    raise Exception("error")


@pytest.mark.parametrize("initial", [0, None, 1, 1.0])
def test_truncated_delay_generator_initial(initial):
    if initial is None:
        delay = _DEFAULT_DELAY_INITIAL
    else:
        delay = initial

    assert next(truncated_delay_generator(initial=initial)) == delay


@pytest.mark.parametrize("maximum", [0, 1, 1.0])
def test_truncated_delay_generator_maximum(maximum):
    for initial in range(0, 10):
        assert (
            next(truncated_delay_generator(initial=initial, maximum=maximum)) <= maximum
        )


@pytest.mark.parametrize("jitter", [(0, 1.0)])
def test_truncated_delay_generator_jitter(jitter):
    initial = 0
    assert next(truncated_delay_generator(initial=initial, jitter=jitter)) > initial


@pytest.mark.parametrize("multiplier", [(1, 1.2)])
def test_truncated_delay_generator_multiplier(multiplier):
    delay_generator = truncated_delay_generator(initial=1, multiplier=multiplier)

    first = next(delay_generator)
    second = next(delay_generator)

    assert first * multiplier == second


def test__retry_exceptions():
    _retry(
        mock.Mock(side_effect=[FakeException, True]),
        fake_delay_generator(),
        exceptions=(Exception,),
    )

    with pytest.raises(FakeException):
        _retry(
            mock.Mock(side_effect=[FakeException("")]),
            fake_delay_generator(),
            exceptions=(TypeError,),
        )


def test__retry_blacklist():
    _retry(
        mock.Mock(side_effect=[FakeException, ValueError, True]),
        fake_delay_generator(),
        blacklist=(TypeError,),
    )

    with pytest.raises(FakeException):
        _retry(
            mock.Mock(side_effect=[FakeException("")]),
            fake_delay_generator(),
            blacklist=(FakeException,),
        )


def test__retry_retries():
    _retry(
        mock.Mock(side_effect=[FakeException, True]),
        fake_delay_generator(),
        retries=1,
        exceptions=(FakeException,),
    )

    with pytest.raises(RetryError) as exc_info:
        _retry(
            mock.Mock(side_effect=[FakeException, True]),
            fake_delay_generator(),
            retries=0,
            exceptions=(FakeException,),
        )

    assert len(exc_info.value.exceptions) == 1

    with pytest.raises(RetryError) as exc_info:
        _retry(
            mock.Mock(side_effect=[FakeException, FakeException, True]),
            fake_delay_generator(),
            retries=1,
            exceptions=(FakeException,),
        )

    assert len(exc_info.value.exceptions) == 2


def test__retry_deadline():
    _retry(
        mock.Mock(side_effect=[FakeException, True]),
        fake_delay_generator(),
        deadline=10,
        exceptions=(FakeException,),
    )

    with pytest.raises(RetryError) as exc_info:
        _retry(
            mock.Mock(side_effect=[FakeException, True]),
            fake_delay_generator(),
            deadline=0,
            exceptions=(FakeException,),
        )

    assert len(exc_info.value.exceptions) == 1


def test_RetryError_message():
    with pytest.raises(
        RetryError,
        match="^Maximum retry attempts calling descarteslabs.common.retry.tests.test_retry.fake_failing_func, exceptions: ",  # noqa
    ):
        _retry(
            fake_failing_func,
            fake_delay_generator(),
            retries=0,
            exceptions=(Exception,),
        )


def test__retry_bad_delay_generator():
    with pytest.raises(ValueError):
        _retry(mock.Mock(side_effect=[FakeException]), [])

    with pytest.raises(ValueError):
        _retry(mock.Mock(side_effect=[FakeException]), [0])


def test__retry_predicate():
    _retry(
        mock.Mock(side_effect=[FakeException, True]),
        fake_delay_generator(),
        predicate=lambda e: True,
    )

    with pytest.raises(FakeException):
        _retry(
            mock.Mock(side_effect=[FakeException, True]),
            fake_delay_generator(),
            predicate=lambda e: False,
        )


def test_RetryError():
    exceptions = [FakeException("")]
    assert exceptions == RetryError("message", exceptions).exceptions


def test_Retry():
    retriable = Retry()(fake_func)

    assert retriable.__doc__ == fake_func.__doc__
    assert retriable(0) == fake_func(0)


def test_decorate():
    @Retry()
    def foo(x):
        return x

    assert foo(0) == 0
