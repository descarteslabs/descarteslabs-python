# Copyright 2018-2023 Descartes Labs.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from unittest import mock

import pytest

from .. import Retry, RetryError, truncated_delay_generator
from ..retry import _DEFAULT_DELAY_INITIAL


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
    Retry(exceptions=(Exception,))._retry(
        mock.Mock(side_effect=[FakeException, True]),
        fake_delay_generator(),
    )

    with pytest.raises(FakeException):
        Retry(exceptions=(TypeError,))._retry(
            mock.Mock(side_effect=[FakeException("")]),
            fake_delay_generator(),
        )


def test__retry_blacklist():
    Retry(blacklist=(TypeError,))._retry(
        mock.Mock(side_effect=[FakeException, ValueError, True]),
        fake_delay_generator(),
    )

    with pytest.raises(FakeException):
        Retry(blacklist=(FakeException,))._retry(
            mock.Mock(side_effect=[FakeException("")]),
            fake_delay_generator(),
        )


def test__retry_retries():
    Retry(retries=1, exceptions=(FakeException,))._retry(
        mock.Mock(side_effect=[FakeException, True]),
        fake_delay_generator(),
    )

    with pytest.raises(RetryError) as exc_info:
        Retry(retries=0, exceptions=(FakeException,))._retry(
            mock.Mock(side_effect=[FakeException, True]),
            fake_delay_generator(),
        )

    assert len(exc_info.value.exceptions) == 1

    with pytest.raises(RetryError) as exc_info:
        Retry(retries=1, exceptions=(FakeException,))._retry(
            mock.Mock(side_effect=[FakeException, FakeException, True]),
            fake_delay_generator(),
        )

    assert len(exc_info.value.exceptions) == 2


def test__retry_deadline():
    Retry(deadline=10, exceptions=(FakeException,))._retry(
        mock.Mock(side_effect=[FakeException, True]),
        fake_delay_generator(),
    )

    with pytest.raises(RetryError) as exc_info:
        Retry(deadline=0, exceptions=(FakeException,))._retry(
            mock.Mock(side_effect=[FakeException, True]),
            fake_delay_generator(),
        )

    assert len(exc_info.value.exceptions) == 1


def test__retry_delay_from_predicate():
    def noop_generator():
        assert False, "noop delay generator called"

        # this cannot be reached but yield is required to make this a generator
        while True:
            yield 0

    Retry(predicate=lambda e: (True, 0))._retry(
        mock.Mock(side_effect=[FakeException, FakeException, True]), noop_generator()
    )


def test__handle_exception_returns_delay():
    delay = Retry(predicate=lambda e: True)._handle_exception(FakeException, [])
    assert delay is None

    delay = Retry(predicate=lambda e: (True, 10))._handle_exception(FakeException, [])
    assert delay == 10


def test_RetryError_message():
    with pytest.raises(
        RetryError,
        match="^Maximum retry attempts calling .*\.fake_failing_func, exceptions: ",  # noqa
    ):
        Retry(retries=0, exceptions=(Exception,))._retry(
            fake_failing_func,
            fake_delay_generator(),
        )


def test__retry_bad_delay_generator():
    with pytest.raises(ValueError):
        Retry()._retry(mock.Mock(side_effect=[FakeException]), [])

    with pytest.raises(ValueError):
        Retry()._retry(mock.Mock(side_effect=[FakeException]), [0])


def test__retry_predicate():
    Retry(predicate=lambda e: True)._retry(
        mock.Mock(side_effect=[FakeException, True]),
        fake_delay_generator(),
    )

    with pytest.raises(FakeException):
        Retry(predicate=lambda e: False)._retry(
            mock.Mock(side_effect=[FakeException, True]),
            fake_delay_generator(),
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
