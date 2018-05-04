import cloudpickle
from hashlib import sha1
import logging
import time
import inspect


def cached(storage_client, minimum_runtime=0.0):
    def qualified_cached(f):
        FUNC_HASH = sha1("".join(inspect.getsourcelines(f)[0][1:]))\
            .hexdigest().encode("utf-8")

        def wrapper(*args, **kwargs):
            h = "/".join([
                FUNC_HASH,
                sha1(
                    cloudpickle.dumps((args, kwargs))
                ).hexdigest().encode("utf-8")
            ])

            # Check cache
            try:
                cached_result = storage_client.get(h, storage_type='cache')
                result = cloudpickle.loads(cached_result)
                logging.debug("Using cached result")
                return result
            except BaseException:
                pass

            t1 = -time.time()
            result = f(*args, **kwargs)
            t1 += time.time()

            if t1 > minimum_runtime:
                storage_client.set(h, cloudpickle.dumps(result), storage_type='cache')
                logging.debug("Cached result")

            return result
        return wrapper
    return qualified_cached
