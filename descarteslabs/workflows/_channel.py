__channel__ = "v0-19"
DEFAULT_GRPC_HOST = "workflows-api.descarteslabs.com"
DEFAULT_HTTP_HOST = "workflows.descarteslabs.com"


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

    # reset global clients that may have the channel already stored
    from .client import client
    from .inspect import client as inspect_client

    client.global_grpc_client = None
    inspect_client.global_inspect_client = None
