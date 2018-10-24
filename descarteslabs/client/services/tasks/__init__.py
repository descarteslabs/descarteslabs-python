from .tasks import AsyncTasks, Tasks, CloudFunction, as_completed

# Backwards compatibility
from descarteslabs.common.tasks import FutureTask, TransientResultError
TransientResultException = TransientResultError

__all__ = ["AsyncTasks", "Tasks", "TransientResultException", "FutureTask", "CloudFunction", "as_completed"]
