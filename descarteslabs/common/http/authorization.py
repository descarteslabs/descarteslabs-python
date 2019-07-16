import six


def add_bearer(token):
    """For use with Authorization headers, add "Bearer "."""
    if token:
        return "Bearer {}".format(token)
    else:
        return token


def remove_bearer(token):
    """For use with Authorization headers, strip any "Bearer "."""
    if isinstance(token, six.string_types) and token.lower().startswith("bearer "):
        return token[7:]
    else:
        return token
