import six


def add_bearer(token):
    """For use with Authorization headers, add "Bearer "."""
    if token:
        return (u"Bearer " if isinstance(token, six.text_type) else b"Bearer ") + token
    else:
        return token


def remove_bearer(token):
    """For use with Authorization headers, strip any "Bearer "."""
    if isinstance(token, (six.text_type, six.binary_type)) and token.lower().startswith(
        u"bearer " if isinstance(token, six.text_type) else b"bearer "
    ):
        return token[7:]
    else:
        return token


def add_basic(token):
    """For use with Authorization headers, add "Basic "."""
    if token:
        return (u"Basic " if isinstance(token, six.text_type) else b"Basic ") + token
    else:
        return token


def remove_basic(token):
    """For use with Authorization headers, strip any "Basic "."""
    if isinstance(token, (six.text_type, six.binary_type)) and token.lower().startswith(
        u"basic " if isinstance(token, six.text_type) else b"basic "
    ):
        return token[6:]
    else:
        return token
