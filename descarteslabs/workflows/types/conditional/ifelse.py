from descarteslabs.common.graft import client

from descarteslabs.workflows.types.core import typecheck_promote
from descarteslabs.workflows.types.primitives import Bool
from descarteslabs.workflows.types.proxify import proxify


@typecheck_promote(Bool, None, None)
def ifelse(condition, true_value, false_value):
    """
    An if-else statement: returns ``true_value`` if ``condition`` is True, otherwise ``false_value``.

    ``true_value`` and ``false_value`` must be the same type. (Note this is different from a Python
    if-else statement.)

    `ifelse` "short-circuits" like a Python conditional: only one of ``true_value`` or ``false_value``
    will actually get computed.

    Note
    ----
    Since Workflows objects cannot be used in Python ``if`` statements (their actual
    values aren't known until they're computed), the `ifelse` function lets you express
    conditional logic in Workflows operations.

    However, `ifelse` should be a last resort for large blocks of logic: in most cases,
    you can write code that is more efficient and easier to read using functionality
    like ``filter``, ``map``, :ref:`empty Image/ImageCollection handling <empty-rasters>`,
    ``pick_bands(allow_missing=True)``, `.Dict.get`, etc.

    Parameters
    ----------
    condition: Bool
        The condition
    true_value:
        Value returned if ``condition`` is True. Must be the same type as ``false_value``
    false_value:
        Value returned if ``condition`` is False. Must be the same type as ``true_value``

    Returns
    -------
    result: same as ``true_value`` and ``false_value``
        ``true_value`` if ``condition`` is True, otherwise ``false_value``

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> wf.ifelse(True, "yep!", "nope").inspect()  # doctest: +SKIP
    "yep!"
    >>> wf.ifelse(False, "yep!", "nope").inspect()  # doctest: +SKIP
    "nope"
    """
    true_value = proxify(true_value)
    false_value = proxify(false_value)

    if type(true_value) is not type(false_value):
        raise TypeError(
            "Both cases of `ifelse` must be the same type. "
            "Got type {} for the true case, and type {} for the false case.".format(
                type(true_value).__name__, type(false_value).__name__
            )
        )

    first_guid = client.guid()
    delayed_true = client.function_graft(true_value, first_guid=first_guid)
    delayed_false = client.function_graft(false_value, first_guid=first_guid)

    return true_value._from_apply("wf.ifelse", condition, delayed_true, delayed_false)
