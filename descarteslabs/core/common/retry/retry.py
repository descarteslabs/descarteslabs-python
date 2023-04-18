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

import datetime
import functools
import inspect
import random
import time
from typing import Iterable

_DEFAULT_RETRIES = 3
_DEFAULT_DELAY_INITIAL = 0.1
_DEFAULT_DELAY_MULTIPLIER = 2.0
_DEFAULT_DELAY_MAXIMUM = 60
_DEFAULT_DELAY_JITTER = (0, 1)


def _name_of_func(f):
    module = inspect.getmodule(f)

    if module is not None:
        module = module.__name__
    else:
        module = "<unknown>"

    return "{}.{}".format(module, getattr(f, "__name__", f))


class Retry(object):
    """Retry class to wrap functions as a decorator or inline.

    Example
    -------

    >>> import descarteslabs as dl
    >>> retry = dl.common.retry.Retry(
    ...     maximum=30,
    ...     retries=5,
    ...     exceptions=(dl.exceptions.GatewayTimeoutError,)
    ... )
    >>> @retry
    ... def flaky(x):
    ...    return x
    >>> flaky("test")
    'test'
    >>> retry(lambda x: x)("test")
    'test'
    """

    def __init__(
        self,
        retries=_DEFAULT_RETRIES,
        exceptions=None,
        predicate=None,
        blacklist=None,
        deadline=None,
        initial=_DEFAULT_DELAY_INITIAL,
        maximum=_DEFAULT_DELAY_MAXIMUM,
        jitter=_DEFAULT_DELAY_JITTER,
        multiplier=_DEFAULT_DELAY_MULTIPLIER,
    ):
        """Instantiate a Retry object that can be used to wrap a callable.

        Parameters
        ----------
        retries : int, optional
            The number of retries allowed.
        exceptions : tuple, optional
            A tuple of Exceptions that should always be retried.
        predicate : function, optional
            A callable that takes an exception and returns either a bool or a Tuple[bool, int].
            If the bool value is true, the wrapped callable was determined to be retryable.
            This can be used for cases with a generic exception with variable attributes.

            If the return was a Tuple[bool, int], the int value will be used as the delay.
            This can be used for cases where an exception should only be retried after some
            variable amount of time.
            This is typically used for handling `Retry-After` headers in which the server is
            requesting the client wait for a specific amount of time.
        blacklist : tuple, optional
            A tuple of Exceptions that should never be retried.
        deadline : float, optional
            The deadline in seconds for retries.
        initial : float
            The amount of delay for the before the first retry.
        maximum : float
            The maximum amount of delay between retries.
        jitter : tuple, optional
            The bounds for a random amount to be added to each delay.
        multiplier : float, optional
            The multiple by which the delay increases.

        """

        self._retries = retries
        self._exceptions = exceptions
        self._predicate = predicate
        self._blacklist = blacklist
        self._deadline = deadline
        self._initial = initial
        self._maximum = maximum
        self._jitter = jitter
        self._multiplier = multiplier

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            target = functools.partial(func, *args, **kwargs)
            delay_generator = truncated_delay_generator(
                initial=self._initial,
                maximum=self._maximum,
                jitter=self._jitter,
                multiplier=self._multiplier,
            )

            return self._retry(target, delay_generator)

        return wrapper

    def _retry(self, func, delay_generator):
        deadline = self._deadline_datetime(self._deadline)
        retries = self._retries
        previous_exceptions = []

        # delay generator can be a list and should
        # be converted to an iterator to use with next
        delay_generator = iter(delay_generator)

        while True:
            try:
                return func()
            except Exception as e:
                delay = self._handle_exception(e, previous_exceptions)

            # predicate returned no delay use the generator
            if delay is None:
                try:
                    delay = next(delay_generator)
                except Exception:
                    raise ValueError("Bad delay generator")

            # will raise RetryError if deadline or retries exceeded
            retries = self._check_retries(
                retries, _name_of_func(func), deadline, previous_exceptions
            )
            time.sleep(delay)

    def _handle_exception(self, exception, previous_exceptions):
        delay = None

        if callable(self._predicate):
            # a predicate can either return a bool
            # or a an Iterable (tuple) containing a bool (if retryable) and a delay
            retryable = self._predicate(exception)
            if isinstance(retryable, Iterable):
                retryable, delay, *_ = retryable

            if not retryable:
                raise

        if self._blacklist is not None and isinstance(exception, self._blacklist):
            raise

        if self._exceptions is not None and not isinstance(exception, self._exceptions):
            raise

        previous_exceptions.append(exception)

        return delay

    def _check_retries(self, retries, name, deadline, previous_exceptions):
        # Raise RetryError if deadline exceeded
        if deadline is not None and deadline <= datetime.datetime.utcnow():
            raise RetryError(
                "Deadline of {:.1f}s exceeded while calling {}".format(deadline, name),
                previous_exceptions,
            ) from previous_exceptions[-1]

        # Raise RetryError if retries exhausted
        if retries is not None and retries == 0:
            raise RetryError(
                "Maximum retry attempts calling {}".format(name),
                previous_exceptions,
            ) from previous_exceptions[-1]

        if retries is not None:
            retries -= 1

        return retries

    @staticmethod
    def _deadline_datetime(deadline):
        if deadline is None:
            return None

        return datetime.datetime.utcnow() + datetime.timedelta(seconds=deadline)


class RetryError(Exception):
    """Error raised when the number of retries has been exhausted or the
    deadline has passed."""

    def __init__(self, message, exceptions):
        super(RetryError, self).__init__(message)
        self.message = message
        self._exceptions = exceptions

    @property
    def exceptions(self):
        """Get a list of exceptions that occurred.

        Returns
        -------
        list
            The list of exceptions
        """

        return self._exceptions

    def __str__(self):
        return "{}, exceptions: {}".format(self.message, self.exceptions)


def truncated_delay_generator(
    initial=None, maximum=None, jitter=None, multiplier=_DEFAULT_DELAY_MULTIPLIER
):
    """A generator for truncated exponential delay.

    Parameters
    ----------
    initial : float
        The amount of delay for the first generated value.
    maximum : float
        The maximum amount of delay.
    jitter : tuple, optional
        The bounds for a random amount to be added to each delay.
    multiplier : float, optional
        The multiple by which the delay increases.
    """

    if initial is None:
        initial = _DEFAULT_DELAY_INITIAL

    delay = initial

    while True:
        if jitter is not None:
            delay += random.uniform(*jitter)

        if maximum is not None:
            delay = min(delay, maximum)

        yield delay

        delay *= multiplier
