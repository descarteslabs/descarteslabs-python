import time
import functools32
import traceback
import logging


def backoff(f, show_traceback=False):
    functools32.wraps(f)
    def wrapper(*args, **kwargs):
        itry = 0 
        while True:
            try:
                return f(*args, **kwargs)
            except Exception as e:
                tb = traceback.format_exc() if show_traceback else ""
                logging.warning("Trying exponential backoff %i after %s\n%s", itry, tb, e)
                time.sleep(2**itry)
            itry += 1
    return wrapper
