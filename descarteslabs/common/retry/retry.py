import datetime
import functools
import inspect
import random
import time

import six

_DEFAULT_RETRIES = 3
_DEFAULT_DELAY_INITIAL = 0.1
_DEFAULT_DELAY_MULTIPLIER = 2.0
_DEFAULT_DELAY_MAXIMUM = 60
_DEFAULT_DELAY_JITTER = (0, 1)
_SAFE_VALID_ASSIGNMENTS = ("__doc__",)


def _name_of_func(f):
    module = inspect.getmodule(f)
    if module is not None:
        module = module.__name__
    else:
        module = "<unknown>"
    return "{}.{}".format(module, getattr(f, "__name__", f))


class Retry(object):
    """
    Retry class to wrap functions as a decorator or inline.

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
    "test"
    >>> retry(lambda x: x)("test")
    "test"
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
        """
        Instantiate a Retry object that can be used to wrap a callable.

        Parameters
        ----------
        retries : int, optional
            The number of retries allowed.
        exceptions : tuple, optional
            A tuple of Exceptions that should always be retried.
        predicate : function, optional
            A callable that takes an exception and returns true if retryable.
            This can be used for cases where a generic exception with variable
            attributes.
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
        @_wraps(func)
        def wrapper(*args, **kwargs):
            target = functools.partial(func, *args, **kwargs)
            delay_generator = truncated_delay_generator(
                initial=self._initial,
                maximum=self._maximum,
                jitter=self._jitter,
                multiplier=self._multiplier,
            )

            return _retry(
                target,
                retries=self._retries,
                predicate=self._predicate,
                exceptions=self._exceptions,
                blacklist=self._blacklist,
                delay_generator=delay_generator,
                deadline=self._deadline,
            )

        return wrapper


class RetryError(Exception):
    """
    Error raised when the number of retries has been exhausted or the
    deadline has passed.
    """

    def __init__(self, message, exceptions):
        super(RetryError, self).__init__(message)
        self.message = message
        self._exceptions = exceptions

    @property
    def exceptions(self):
        """
        Get a list of exceptions that occurred.

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
    """
    A generator for truncated exponential delay.

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


def _wraps(wrapped):
    """
    A helper that handles functions not having all attributes in Python 2.
    """

    if isinstance(wrapped, functools.partial) or not hasattr(wrapped, "__name__"):
        return six.wraps(wrapped, assigned=_SAFE_VALID_ASSIGNMENTS)
    else:
        return six.wraps(wrapped)


def _retry(
    func,
    delay_generator,
    retries=None,
    predicate=None,
    exceptions=None,
    blacklist=None,
    deadline=None,
):
    if deadline is not None:
        deadline_datetime = datetime.datetime.utcnow() + datetime.timedelta(
            seconds=deadline
        )
    else:
        deadline_datetime = None

    previous_exceptions = []

    for delay in delay_generator:

        try:
            return func()
        except Exception as exception:  # noqa
            if callable(predicate) and not predicate(exception):
                raise

            if blacklist is not None and isinstance(exception, blacklist):
                raise

            if exceptions is not None and not isinstance(exception, exceptions):
                raise

            previous_exceptions.append(exception)

        # Raise RetryError if deadline exceeded
        if (
            deadline_datetime is not None
            and deadline_datetime <= datetime.datetime.utcnow()
        ):
            six.raise_from(
                RetryError(
                    "Deadline of {:.1f}s exceeded while calling {}".format(
                        deadline, _name_of_func(func)
                    ),
                    previous_exceptions,
                ),
                previous_exceptions[-1],
            )

        # Raise RetryError if retries exhausted
        if retries is not None and retries == 0:
            six.raise_from(
                RetryError(
                    "Maximum retry attempts calling {}".format(_name_of_func(func)),
                    previous_exceptions,
                ),
                previous_exceptions[-1],
            )

        if retries is not None:
            retries -= 1

        time.sleep(delay)
    else:
        raise ValueError("Bad delay generator")
