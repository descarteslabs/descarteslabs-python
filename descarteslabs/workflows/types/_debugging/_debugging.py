from ... import env
from ..core import typecheck_promote
from ..primitives import Int


@typecheck_promote(Int)
def _sleep(secs):
    return type(secs)._from_apply("wf.debugging.sleep", secs, token=env._token)
