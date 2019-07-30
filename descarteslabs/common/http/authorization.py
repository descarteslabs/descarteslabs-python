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
