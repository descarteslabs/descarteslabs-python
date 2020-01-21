__channel__ = "v0-11"


def _set_channel(channel):
    """
    Change the name of the default channel within this session.

    Parameters
    ----------
    channel: str
        A new channel name. Calls to ``.compute`` will use this new name by default.

    Note
    ----
    ``descarteslabs.workflows.__channel__`` will still be the default channel
    name after calling this method; this is normal.
    """
    global __channel__
    __channel__ = channel
