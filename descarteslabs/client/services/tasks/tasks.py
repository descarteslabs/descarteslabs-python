# Copyright 2018-2020 Descartes Labs.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
from collections import defaultdict, OrderedDict
import dis
import glob
import importlib
import inspect
import io
import itertools
import json
import logging
import os
import re
import six
from six.moves import zip_longest
import sys
import time
from warnings import warn
from tempfile import NamedTemporaryFile
import zipfile

import cloudpickle

from descarteslabs.client.auth import Auth
from descarteslabs.client.exceptions import ConflictError
from descarteslabs.client.services.service import Service, ThirdPartyService
from descarteslabs.common.dotdict import DotDict, DotList
from descarteslabs.common.services.tasks.constants import (
    ENTRYPOINT,
    FunctionType,
    DIST,
    DATA,
    REQUIREMENTS,
)
from descarteslabs.common.tasks import FutureTask


OFFSET_DEPRECATION_MESSAGE = (
    "Keyword arg `offset` has been deprecated and will be removed in "
    "future versions of the library. Use `continuation_token`."
)

CREATE_OR_GET_DEPRECATION_MESSAGE = (
    "The `create_or_get_function` method has been deprecated. Please use "
    "the `create_function` method."
)

GET_FUNCTION_DEPRECATION_MESSAGE = (
    "The behavior of `get_function` is deprecated and will be changed in "
    "future versions to get function by group `id` and not `name`."
)

CREATE_NAMESPACE_DEPRECATION_MESSAGE = (
    "Manually creating a namespace is no longer required."
)


class GroupTerminalException(Exception):
    pass


class BoundGlobalError(NameError):
    """
    Raised when a global is referenced in a function where it won't be available
    when executed remotely.
    """

    pass


class Tasks(Service):
    """
    The Tasks API allows you to easily execute parallel computations on cloud
    infrastructure with high-throughput access to imagery.
    """

    TASK_RESULT_BATCH_SIZE = 100
    RERUN_BATCH_SIZE = 200
    COMPLETION_POLL_INTERVAL_SECONDS = 5
    _ENTRYPOINT_TEMPLATE = "{source}\nmain = {function_name}\n"
    _IMPORT_TEMPLATE = "from {module} import {obj}"
    _IS_GLOB_PATTERN = re.compile(r"[\*\?\[]")

    def __init__(self, url=None, auth=None, retries=None):
        """
        :param str url: A HTTP URL pointing to a version of the storage service
            (defaults to current version)
        :param Auth auth: A custom user authentication (defaults to the user
            authenticated locally by token information on disk or by environment
            variables)
        :param urllib3.util.retry.Retry retries: A custom retry configuration
            used for all API requests (defaults to a reasonable amount of retries)
        """
        if auth is None:
            auth = Auth()

        if url is None:
            url = os.environ.get(
                "DESCARTESLABS_TASKS_URL", "https://platform.descarteslabs.com/tasks/v1"
            )

        self._gcs_upload_service = ThirdPartyService()

        super(Tasks, self).__init__(url, auth=auth, retries=retries)

    def _create_namespace(self):
        """
        Creates a namespace for the user and sets up authentication within it
        from the current client id and secret. Must be called once per user
        before creating any tasks.

        :return: `True` if successful, `False` otherwise.
        """
        data = {
            "CLIENT_ID": self.auth.client_id,
            "CLIENT_SECRET": self.auth.client_secret,
        }
        r = self.session.post("/namespaces/secrets/auth", json=data)
        return r.status_code == 201

    def create_namespace(self):
        """
        Creates a namespace for the user and sets up authentication within it
        from the current client id and secret. Must be called once per user
        before creating any tasks.

        :return: `True` if successful, `False` otherwise.
        """
        warn(CREATE_NAMESPACE_DEPRECATION_MESSAGE, DeprecationWarning)
        return self._create_namespace()

    def new_group(
        self,
        function,
        container_image,
        name=None,
        cpus=1,
        gpus=0,
        memory="2Gi",
        maximum_concurrency=None,
        minimum_concurrency=None,
        minimum_seconds=None,
        task_timeout=1800,
        include_modules=None,
        include_data=None,
        requirements=None,
        **kwargs
    ):
        """
        Creates a new task group.

        :param function function: The function to be called in a task.
            The function cannot contain any globals or ``BoundGlobalError``
            will be raised
        :param str container_image: The location of a docker image to be used for
            the environment in which the function is executed.
        :param str name: An optional name used to later help identify the function.
        :param int cpus: The number of CPUs requested for a single task. A task
            might be throttled if it uses more CPU. Default: 1. Maximum: 16.
        :param int gpus: The number of GPUs requested for a single task. As of
            right now, a maximum of 1 GPU is supported. Default: 0. Maximum: 1.
        :param str memory: The maximum memory requirement for a single task. If a
            task uses substantially more memory it will be killed. The value
            should be a string and can use postfixes such as Mi, Gi, MB, GB, etc
            (e.g. `"4Gi"`, `"500MB"`). If no unit is specified it is assumed to be
            in bytes. Default: 2Gi. Maximum: 96Gi.
        :param int maximum_concurrency: The maximum number of tasks to run in
            parallel. Default: 500. Maximum: 500. If you need higher concurrency
            contact your Descartes Labs customer success representative.
        :param int minimum_concurrency: The minimum number of tasks to run right
            away in parallel. Concurrency is usually scaled up slowly when
            submitting new tasks. Setting this can mean more immediate processing
            of this many newly submitted tasks. Note that setting this means the
            equivalent resources of this many permanently running tasks will be
            charged to your account while this group is active. Default: 0.
            Maximum: 4.
        :param int minimum_seconds: The number of seconds to wait for new tasks
            before scaling down concurrency, after a task is finished. Default: 0.
            Maximum: 600.
        :param int task_timeout: Maximum runtime for a single task in seconds. A
            task will be killed if it exceeds this limit. Default: 30 minutes.
            Minimum: 10 seconds. Maximum: 24 hours.
        :param list(str) include_modules: Locally importable python names to include as
            modules in the task group, which can be imported by the entrypoint function, `function`.
        :param list(str) include_data: Non python data files to include in the task group. Data
            path must be descendant of system path or python path directories.
        :param list(str) requirements: A list of Python dependencies required by this function
            or a path to a file listing those dependencies, in standard setuptools
            notation (see PEP 508 https://www.python.org/dev/peps/pep-0508/).
            For example, if the packages `foo` and `bar` are required, then
            `['foo', 'bar']` or `['foo>2.0', 'bar>=1.0']` are possible values.

        :return: A dictionary representing the group created.
        :rtype: DotDict

        :raises ~descarteslabs.client.services.tasks.tasks.BoundGlobalError:
            Raised if the given function refers to global variables.
        :raises ~descarteslabs.client.exceptions.BadRequest: Raised if any of
            the supplied parameters are invalid.
        """

        payload = {
            "image": container_image,
            "function_python_version": ".".join(
                str(part) for part in sys.version_info[:3]
            ),
            "cpu": cpus,
            "gpu": gpus,
            "mem": memory,
            "worker_timeout": task_timeout,
            "maximum_concurrency": maximum_concurrency,
            "minimum_concurrency": minimum_concurrency,
            "minimum_seconds": minimum_seconds,
        }

        if name is not None:
            payload["name"] = name

        payload.update(kwargs)

        bundle_path = None
        try:
            if (
                include_data is not None
                or include_modules is not None
                or requirements is not None
            ):
                bundle_path = self._build_bundle(
                    function, include_data, include_modules, requirements
                )
                payload.update({"function_type": FunctionType.PY_BUNDLE})
            else:
                payload.update(
                    {
                        "function": _serialize_function(function),
                        "function_type": FunctionType.PY_PICKLE,
                    }
                )

            try:
                r = self.session.post("/groups", json=payload)
            except ConflictError as e:
                error_message = json.loads(str(e))["message"]
                if error_message != "namespace is missing authentication":
                    raise

                if not self._create_namespace():
                    raise

                r = self.session.post("/groups", json=payload)
            group = r.json()

            if bundle_path is not None:
                url = group.pop("upload_url")
                with io.open(bundle_path, mode="rb") as bundle:
                    self._gcs_upload_service.session.put(url, data=bundle)
        finally:
            if bundle_path and os.path.exists(bundle_path):
                os.remove(bundle_path)

        return DotDict(group)

    def list_groups(
        self,
        status=None,
        created=None,
        updated=None,
        sort_field=None,
        include=None,
        sort_order="asc",
        limit=100,
        continuation_token=None,
    ):
        """
        Retrieves a limited list of task groups matching the given criteria.

        :param str status: Filter groups to this status.
            Allowed are ['running', 'terminated'].
        :param str created: Filter groups by creation date after this timestamp.
        :param str updated: Filter groups by updated date after this timestamp.
        :param str sort_field: The field to sort groups on. Allowed are
            ['created', 'updated'].
        :param str sort_order: Allowed are ['asc', 'desc']. Default: 'asc'.
        :param list[str] include: extra fields to include in groups in the response.
            allowed are: ['build_log_url']
        :param int limit: The number of results to get (max 1000 per page).
        :param str continuation_token: A string returned from a previous call to
            `list_groups()`, which you can use to get the next page of results.

        :return: A dictionary with two keys; `groups` containing the list of
            matching groups, `continuation_token` containting a string if there
            are further matching groups.
        :rtype: DotDict
        """
        params = {"limit": limit}
        for field in [
            "status",
            "created",
            "updated",
            "sort_field",
            "sort_order",
            "include",
            "continuation_token",
        ]:
            if locals()[field] is not None:
                params[field] = locals()[field]
        r = self.session.get("/groups", params=params)
        r.raise_for_status()
        return DotDict(r.json())

    def iter_groups(
        self, status=None, created=None, updated=None, sort_field=None, sort_order="asc"
    ):
        """
        Iterates over all task groups matching the given criteria.

        :param str status: Filter groups to this status.
            Allowed are ['running', 'terminated'].
        :param str created: Filter groups by creation date after this timestamp.
        :param str updated: Filter groups by updated date after this timestamp.
        :param str sort_field: The field to sort groups on. Allowed are
            ['created', 'updated'].
        :param str sort_order: Allowed are ['asc', 'desc']. Default: 'asc'.
        :param int limit: The number of results to get (max 1000 per page).
        :param str continuation_token: A string returned from a previous call to
            `list_groups()`, which you can use to get the next page of results.

        :return: An iterator over matching task groups.
        :rtype: generator(DotDict)
        """
        continuation_token = None
        while True:
            page = self.list_groups(
                status=status,
                created=created,
                updated=updated,
                sort_field=sort_field,
                sort_order=sort_order,
                continuation_token=continuation_token,
            )
            for group in page.groups:
                yield group

            continuation_token = page.continuation_token
            if continuation_token is None:
                break

    def get_group(self, group_id, include=None):
        """
        Retrieves a single task group by id.

        :param str group_id: The group id.
        :param list(str) include: extra fields to include in groups in the response.
            allowed are: ['build_log_url, 'build_log']. Note that build logs over
            10 Mi will not be returned, request the build log url instead.

        :return: A dictionary representing the task group.
        :rtype: DotDict

        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            task group cannot be found.
        """
        r = self.session.get(
            "/groups/{}".format(group_id), params={"include": include or ()}
        )
        r.raise_for_status()
        return DotDict(r.json())

    get_group_by_id = get_group

    def get_group_by_name(self, name, status="running"):
        """
        Retrieves a single task group by name. Names are not unique; if there are
        multiple matches, returns the newest group.

        :param str group_id: The group name.
        :param str status: Only consider groups with this status.
            Allowed are ['running', 'terminated']. Default: 'running'.

        :return: A dictionary representing the task group, or `None` if no group
            with the given name exists.
        :rtype: DotDict
        """
        groups = self.iter_groups(
            status=status, sort_field="created", sort_order="desc"
        )
        for g in groups:
            if g.name == name:
                return g

    def terminate_group(self, group_id):
        """
        Terminates a task group by id. Once a group is terminated, no more tasks
        can be submitted to it and it stops using any resources. If the group
        with the given id is already terminated, nothing happens.

        :param str group_id: The group id.

        :return: A dictionary representing the terminated task group.
        :rtype: DotDict

        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            task group cannot be found.
        """
        r = self.session.delete("/groups/{uid}".format(uid=group_id))
        r.raise_for_status()
        return DotDict(r.json())

    delete_group_by_id = terminate_group

    def wait_for_completion(self, group_id, show_progress=False):
        """
        Waits until all submitted tasks for a given group are completed.

        If a task group stops accepting tasks, will raise
        :class:`GroupTerminalException` and stop waiting.

        :param str group_id: The group id.
        :param bool show_progress: Whether to log progress information.

        :raises: ``GroupTerminalException``
        """
        queue = self.get_group(group_id).queue
        while queue.pending > 0:
            completed = queue.failures + queue.successes
            if show_progress:
                logging.warning(
                    "Done with %i / %i tasks", completed, queue.pending + completed
                )
            time.sleep(self.COMPLETION_POLL_INTERVAL_SECONDS)

            # check for terminal states
            group = self.get_group(group_id)
            _raise_if_terminal_group(group_id, self, group)

            queue = group.queue

    def new_task(
        self, group_id, arguments=None, parameters=None, labels=None, retry_count=0
    ):
        """
        Submits a new task to a group. All positional and keyword arguments
        to the group's function must be JSON-serializable (i.e., booleans,
        numbers, strings, lists, dictionaries).

        :param str group_id: The group id to submit to.
        :param list arguments: The positional arguments to call the group's
            function with.
        :param dict parameters: The keyword arguments to call the group's
            function with.
        :param list labels: An optional list of labels to attach to the
            task. Task results can later be filtered by these labels.
        :param int retry_count: Number of times to retry the task if it
            fails (maximum 5).

        :return: A dictionary with one key `tasks` containing a list with
            one element representing the submitted task.
        :rtype: DotDict

        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            task group cannot be found.
        :raises ~descarteslabs.client.exceptions.BadRequest: Raised if any of
            the supplied parameters are invalid.
        """
        return self.new_tasks(
            group_id,
            list_of_arguments=[arguments or []],
            list_of_parameters=[parameters or {}],
            list_of_labels=[labels],
            retry_count=retry_count,
        )

    def new_tasks(
        self,
        group_id,
        list_of_arguments=None,
        list_of_parameters=None,
        list_of_labels=None,
        retry_count=0,
    ):
        """
        Submits multiple tasks to a group. All positional and keyword arguments
        to the group's function must be JSON-serializable (i.e., booleans,
        numbers, strings, lists, dictionaries).

        :param str group_id: The group id to submit to.
        :param list(list) arguments: The positional arguments to call the
            group's function with, for each task.
        :param list(dict) parameters: The keyword arguments to call the group's
            function with, for each task.
        :param list(list) labels: An optional list of labels to attach, for each
            task. Task results can later be filtered by these labels.
        :param int retry_count: Number of times to retry the tasks if they
            fails (maximum 5).

        :return: A dictionary with one key `tasks` containing a list of
            dictionaries representing the submitted tasks.
        :rtype: DotDict

        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            task group cannot be found.
        :raises ~descarteslabs.client.exceptions.BadRequest: Raised if any of
            the supplied parameters are invalid.
        """
        list_of_arguments = list_of_arguments if list_of_arguments is not None else [[]]
        list_of_parameters = (
            list_of_parameters if list_of_parameters is not None else [{}]
        )
        list_of_labels = list_of_labels if list_of_labels is not None else [None]
        msgs = []
        for args, kwargs, labels in zip_longest(
            list_of_arguments, list_of_parameters, list_of_labels, fillvalue=None
        ):
            args = args or []
            params = kwargs or {}
            attributes = {"labels": labels} if labels else {}
            msg = {
                "arguments": args,
                "parameters": params,
                "attributes": attributes,
                "retry_count": retry_count,
            }
            msgs.append(msg)

        r = self.session.post(
            "/groups/{group_id}/tasks".format(group_id=group_id), json={"tasks": msgs}
        )
        r.raise_for_status()
        return DotDict(r.json())

    def get_task_result(self, group_id, task_id, include=None):
        """
        Retrieves a single task result.

        :param str group_id: The group to get task results from.
        :param str task_id: Specific ID of task to retrieve.
        :param list(str) include: Extra fields to include in the task results.
            Allowed values are ['arguments', 'stacktrace', 'result', 'logs',
            'result_url', 'logs_url'].

        :return: A dictionary representing the task result.
        :rtype: DotDict

        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            task group or task itself cannot be found.
        """
        params = {"include": include} if include is not None else {}
        r = self.session.get(
            "/groups/{uid}/tasks/{task_uid}/results".format(
                uid=group_id, task_uid=task_id
            ),
            params=params,
        )
        r.raise_for_status()
        result = r.json()
        if "result" in result:
            result["result"] = base64.b64decode(result["result"] or "")
        return DotDict(result)

    def get_task_result_batch(self, group_id, task_ids, include=None):
        """
        Retrieves a multiple task results by id.

        :param str group_id: The group to get task results from.
        :param list(str) task_ids: A list of task ids to retrieve, maximum 500.
        :param list(str) include: Extra fields to include in the task results.
            Allowed values are ['arguments', 'stacktrace', 'result_url', 'logs_url'].

        :return: A dictionary with a key `results` containing the list of
            matching results. Results are in the order of the ids provided.
            Unknown ids are ignored.
        :rtype: DotDict
        """
        data = {"ids": task_ids}
        if include is not None:
            data["include"] = include
        r = self.session.post(
            "/groups/{uid}/results/batch".format(uid=group_id), json=data
        )
        r.raise_for_status()
        results = r.json()
        return DotDict(results)

    def list_task_results(
        self,
        group_id,
        limit=TASK_RESULT_BATCH_SIZE,
        offset=None,
        status=None,
        failure_type=None,
        updated=None,
        created=None,
        webhook=None,
        labels=None,
        include=None,
        sort_field="created",
        sort_order="asc",
        continuation_token=None,
    ):
        """
        Retrieves a portion of task results matching the given criteria.

        :param str group_id: The group to get task results from.
        :param int limit: The number of results to get (max 1000 per page).
        :param int offset: Where to start when getting task results
            (deprecated; use continuation_token).
        :param str status: Filter tasks to this status.
            Allowed are ['FAILURE', 'SUCCESS'].
        :param str failure_type: Filter tasks to this type of failure.
            Allowed are ['exception', 'oom', 'timeout', 'internal', 'unknown', 'py_version_mismatch'].
        :param str updated: Filter tasks by updated date after this timestamp.
        :param str created: Filter tasks by creation date after this timestamp.
        :param str webhook: Filter by the webhook uid which spawned the task.
        :param list(str) labels: Labels that must be present in tasks labels list.
        :param list(str) include: Extra fields to include in the task results.
            Allowed values are ['arguments', 'stacktrace', 'result_url', 'logs_url'].
        :param str sort_field: The field to sort results on. Allowed are
            ['created', 'runtime', 'peak_memory_usage']. Default: 'created'.
        :param str sort_order: Allowed are ['asc', 'desc']. Default: 'asc'.
        :param str continuation_token: A string returned from a previous call to
            `list_task_results()`, which you can use to get the next page of results.

        :return: A dictionary with two keys; `results` containing the list of
            matching results, `continuation_token` containting a string if there
            are further matching results.
        :rtype: DotDict
        """
        if offset is not None:
            warn(OFFSET_DEPRECATION_MESSAGE, DeprecationWarning)

        params = {"limit": limit}
        for field in [
            "offset",
            "status",
            "failure_type",
            "updated",
            "created",
            "webhook",
            "labels",
            "include",
            "sort_field",
            "sort_order",
            "continuation_token",
        ]:
            if locals()[field] is not None:
                params[field] = locals()[field]
        r = self.session.get(
            "/groups/{uid}/results".format(uid=group_id), params=params
        )
        r.raise_for_status()
        return DotDict(r.json())

    get_task_results = list_task_results

    def iter_task_results(
        self,
        group_id,
        status=None,
        failure_type=None,
        updated=None,
        created=None,
        webhook=None,
        labels=None,
        include=None,
        sort_field="created",
        sort_order="asc",
    ):
        """
        Iterates over all task results matching the given criteria.

        :param str group_id: The group to get task results from.
        :param str status: Filter tasks to this status.
            Allowed are ['FAILURE', 'SUCCESS'].
        :param str failure_type: Filter tasks to this type of failure.
            Allowed are ['exception', 'oom', 'timeout', 'internal', 'unknown', 'py_version_mismatch'].
        :param str updated: Filter tasks by updated date after this timestamp.
        :param str created: Filter tasks by creation date after this timestamp.
        :param str webhook: Filter by the webhook uid which spawned the task.
        :param list(str) include: Extra fields to include in the task results.
            Allowed values are ['arguments', 'stacktrace', 'result_url', 'logs_url'].
        :param list(str) labels: Labels that must be present in tasks labels list.
        :param str sort_field: The field to sort results on. Allowed are
            ['created', 'runtime', 'peak_memory_usage']. Default: 'created'.
        :param str sort_order: Allowed are ['asc', 'desc']. Default: 'asc'.

        :return: An iterator over matching task results.
        :rtype: generator(DotDict)
        """
        params = {}
        for field in [
            "status",
            "failure_type",
            "updated",
            "created",
            "webhook",
            "labels",
            "include",
        ]:
            if locals()[field] is not None:
                params[field] = locals()[field]

        continuation_token = None
        while True:
            page = self.get_task_results(
                group_id, continuation_token=continuation_token, **params
            )
            for result in page.results:
                yield result

            continuation_token = page.continuation_token
            if continuation_token is None:
                break

    def rerun_failed_tasks(self, group_id, retry_count=0):
        """
        Submits all failed tasks for a rerun, except for out-of-memory or
        version mismatch failures.
        These tasks will be run again with the same arguments as before.

        Tasks that are currently already being rerun will be ignored.

        :param str group_id: The group in which to rerun tasks.
        :param int retry_count: Number of times to retry a task if it fails
                                (maximum 5)

        :return: A list of dictionaries representing the tasks that have been submitted.
        :rtype: DotList
        """
        rerun_tasks = []
        for failure_type in ["exception", "timeout", "internal", "unknown"]:
            rerun_tasks += self.rerun_matching_tasks(
                group_id, failure_type=failure_type, retry_count=retry_count
            )
        return rerun_tasks

    def rerun_matching_tasks(
        self,
        group_id,
        status=None,
        failure_type=None,
        updated=None,
        created=None,
        webhook=None,
        labels=None,
        retry_count=0,
    ):
        """
        Submits all completed tasks matching the given search arguments for a rerun.
        These tasks will be run again with the same arguments as before.

        Tasks that are currently already being rerun will be ignored.

        :param str group_id: The group in which to rerun tasks.
        :param str status: Filter tasks to this status.
            Allowed are ['FAILURE', 'SUCCESS'].
        :param str failure_type: Filter tasks to this type of failure.
            Allowed are ['exception', 'oom', 'timeout', 'internal', 'unknown'].
        :param str updated: Filter tasks by updated date after this timestamp.
        :param str created: Filter tasks by creation date after this timestamp.
        :param str webhook: Filter by the webhook uid which spawned the task.
        :param list(str) labels: Labels that must be present in tasks labels list.
        :param int retry_count: Number of times to retry a task if it fails
                                (maximum 5)

        :return: A list of dictionaries representing the tasks that have been submitted.
        :rtype: DotList
        """
        results = self.iter_task_results(
            group_id,
            status=status,
            failure_type=failure_type,
            updated=updated,
            created=created,
            webhook=webhook,
            labels=labels,
        )
        return self.rerun_tasks(
            group_id, (t.id for t in results), retry_count=retry_count
        )

    def rerun_tasks(self, group_id, task_id_iterable, retry_count=0):
        """
        Submits a list of completed tasks specified by ids for a rerun. The completed tasks
        with the given ids will be run again with the same arguments as before.

        Tasks that are currently already being rerun will be ignored. Unknown or invalid
        task ids will be ignored.

        :param str group_id: The group in which to rerun tasks.
        :param iterable(str) task_id_iterable: An iterable of the task ids to be rerun.
        :param int retry_count: Number of times to retry a task if it fails
                                (maximum 5)

        :return: A list of dictionaries representing the tasks that have been submitted.
        :rtype: DotList

        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            task group cannot be found.
        """
        rerun = []
        task_ids = list(itertools.islice(task_id_iterable, self.RERUN_BATCH_SIZE))
        while task_ids:
            r = self.session.post(
                "/groups/{group_id}/tasks/rerun".format(group_id=group_id),
                json={"task_ids": task_ids, "retry_count": retry_count},
            )
            r.raise_for_status()
            rerun += r.json()["tasks"]
            task_ids = list(itertools.islice(task_id_iterable, self.RERUN_BATCH_SIZE))
        return DotList(rerun)

    def create_function(
        self,
        f,
        image,
        name=None,
        cpus=1,
        gpus=0,
        memory="2Gi",
        maximum_concurrency=None,
        minimum_concurrency=None,
        minimum_seconds=None,
        task_timeout=1800,
        retry_count=0,
        include_modules=None,
        include_data=None,
        requirements=None,
        **kwargs
    ):
        """
        Creates a new task group from a function and returns an asynchronous
        function that can be called to submit tasks to the group.

        :param function f: The function to be called in a task.
        :param str image: The location of a docker image to be used for the
            environment in which the function is executed.
        :param str name: An optional name used to later help identify the function.
        :param int cpus: The number of CPUs requested for a single task. A task
            might be throttled if it uses more CPU. Default: 1. Maximum: 32.
        :param int gpus: The number of GPUs requested for a single task. As of
            right now, a maximum of 1 GPU is supported. Default: 0. Maximum: 1.
        :param str memory: The maximum memory requirement for a single task. If a
            task uses substantially more memory it will be killed. The value
            should be a string and can use postfixes such as Mi, Gi, MB, GB, etc
            (e.g. `"4Gi"`, `"500MB"`). If no unit is specified it is assumed to be
            in bytes. Default: 2Gi. Maximum: 96Gi.
        :param int maximum_concurrency: The maximum number of tasks to run in
            parallel. Default: 500. Maximum: 500. If you need higher concurrency
            contact your Descartes Labs customer success representative.
        :param int minimum_concurrency: The minimum number of tasks to run right
            away in parallel. Concurrency is usually scaled up slowly when
            submitting new tasks. Setting this can mean more immediate processing
            of this many newly submitted tasks. Note that setting this means the
            equivalent resources of this many permanently running tasks will be
            charged to your account while this group is active. Default: 0.
            Maximum: 4.
        :param int minimum_seconds: The number of seconds to wait for new tasks
            before scaling down concurrency, after a task is finished. Default: 0.
            Maximum: 600.
        :param int task_timeout: Maximum runtime for a single task in seconds. A
            task will be killed if it exceeds this limit. Default: 30 minutes.
            Minimum: 10 seconds. Maximum: 24 hours.
        :param int retry_count: Number of times to retry a task if it fails
            Default: 0. Maximum: 5.
        :param list(str) include_modules: Locally importable python (or cython) names to include as
            modules in the task group, which can be imported by the entrypoint function, `function`.
        :param list(str) include_data: Non python data files to include in the task group. Data
            path must be descendant of system path or python path directories.
        :param list(str) requirements: A list of Python dependencies required by this function
            or a path to a file listing those dependencies, in standard setuptools
            notation (see PEP 508 https://www.python.org/dev/peps/pep-0508/).
            For example, if the packages `foo` and `bar` are required, then
            `['foo', 'bar']` or `['foo>2.0', 'bar>=1.0']` might be possible values.

        :return: A :class:`CloudFunction`.
        :rtype: :class:`CloudFunction`

        :raises ~descarteslabs.client.exceptions.BadRequest: Raised if any of
            the supplied parameters are invalid.
        """
        group_info = self.new_group(
            f,
            container_image=image,
            name=name,
            cpus=cpus,
            gpus=gpus,
            memory=memory,
            maximum_concurrency=maximum_concurrency,
            minimum_concurrency=minimum_concurrency,
            minimum_seconds=minimum_seconds,
            task_timeout=task_timeout,
            include_modules=include_modules,
            include_data=include_data,
            requirements=requirements,
            **kwargs
        )

        return CloudFunction(
            group_info.id, name=name, client=self, retry_count=retry_count
        )

    def get_function_by_id(self, group_id):
        """
        Get an asynchronous function by group id.

        :param str group_id: The group id.

        :return: A :class:`CloudFunction`.
        :rtype: :class:`CloudFunction`

        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            task group cannot be found.
        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """
        group = self.get_group(group_id)
        return CloudFunction(group.id, name=group.name, client=self)

    def get_function(self, name):
        """
        Gets an asynchronous function by name (the last function created with that
        name).

        :param str name: The name of the function to lookup.

        :return: A :class:`CloudFunction`, or `None` if no function with the
            given name exists.
        :rtype: :class:`CloudFunction`
        """
        warn(GET_FUNCTION_DEPRECATION_MESSAGE, DeprecationWarning)

        group_info = self.get_group_by_name(name)
        if group_info is None:
            return None

        return CloudFunction(group_info.id, name=name, client=self)

    def create_or_get_function(
        self,
        f,
        image,
        name=None,
        cpus=1,
        gpus=0,
        memory="2Gi",
        maximum_concurrency=None,
        minimum_concurrency=None,
        minimum_seconds=None,
        task_timeout=1800,
        retry_count=0,
        **kwargs
    ):
        """
        Creates or gets an asynchronous function. If a task group with the given
        name exists, returns an asynchronous function for the newest existing
        group with that. Otherwise creates a new task group.

        :param function f: The function to be called in a task.
        :param str image: The location of a docker image to be used for the
            environment in which the function is executed.
        :param str name: An optional name used to later help identify the function.
        :param int cpus: The number of CPUs requested for a single task. A task
            might be throttled if it uses more CPU. Default: 1. Maximum: 32.
        :param int gpus: The number of GPUs requested for a single task. As of
            right now, a maximum of 1 GPU is supported. Default: 0. Maximum: 1.
        :param str memory: The maximum memory requirement for a single task. If a
            task uses substantially more memory it will be killed. The value
            should be a string and can use postfixes such as Mi, Gi, MB, GB, etc
            (e.g. `"4Gi"`, `"500MB"`). If no unit is specified it is assumed to be
            in bytes. Default: 2Gi. Maximum: 96Gi.
        :param int maximum_concurrency: The maximum number of tasks to run in
            parallel. Default: 500. Maximum: 500. If you need higher concurrency
            contact your Descartes Labs customer success representative.
        :param int minimum_concurrency: The minimum number of tasks to run right
            away in parallel. Concurrency is usually scaled up slowly when
            submitting new tasks. Setting this can mean more immediate processing
            of this many newly submitted tasks. Note that setting this means the
            equivalent resources of this many permanently running tasks will be
            charged to your account while this group is active. Default: 0.
            Maximum: 4.
        :param int minimum_seconds: The number of seconds to wait for new tasks
            before scaling down concurrency, after a task is finished. Default: 0.
            Maximum: 600.
        :param int task_timeout: Maximum runtime for a single task in seconds. A
            task will be killed if it exceeds this limit. Default: 30 minutes.
            Minimum: 10 seconds. Maximum: 24 hours.
        :param int retry_count: Number of times to retry a task if it fails
            Default: 0. Maximum: 5.

        :return: A :class:`CloudFunction`.
        :rtype: :class:`CloudFunction`

        :raises ~descarteslabs.client.exceptions.BadRequest: Raised if any of
            the supplied parameters are invalid.
        """

        warn(CREATE_OR_GET_DEPRECATION_MESSAGE, DeprecationWarning)

        if name:
            cached = self.get_function(name)
            if cached is not None:
                return cached
        return self.create_function(
            f,
            image=image,
            name=name,
            cpus=cpus,
            gpus=gpus,
            memory=memory,
            maximum_concurrency=maximum_concurrency,
            minimum_concurrency=minimum_concurrency,
            minimum_seconds=minimum_seconds,
            task_timeout=task_timeout,
            retry_count=retry_count,
            **kwargs
        )

    def create_webhook(
        self, group_id, name=None, label_path=None, label_separator=None
    ):
        """
        Create a new webhook for submitting tasks to task group.

        Once a POST request is made to the webhook's URL, a new task will be
        submitted. If the request contains a valid JSON payload, that payload
        will be used as the function's parameters (i.e, `f(**payload)`).

        Optionally, `label_path` and `label_separator` provide a way to attach
        labels to the submitted task for future filtering. The labels will be
        extracted correspondingly from the request payload.

        For example, given an invocation `{"a": {"b": "foo, bar"}}` with
        `label_path` set to `a.b` and `label_separator` as `,`, the labels
        `foo` and `bar` will be attached to the task. Note that the field used
        for labels will not be removed from the invocation of the function.

        :param str group_id: The task group id.
        :param str name: Desired name for the webhook.
        :param str label_path: An optional path to the field to be used as
            task's labels. Note that JSONPath is not supported--a JSONPath
            expression such as `$.foo.bar` must look like `foo.bar` instead.
        :param str label_separator: An optional separator to be used if
            `label_path` refers to a string. If not provided, the whole field
            will be used as label(s).

        :return: A dictionary with properties of the newly created webhook.
        :rtype: DotDict

        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            task group cannot be found.
        """

        data = {}
        if name is not None:
            data["name"] = name
        if label_path is not None:
            data["label_path"] = label_path
        if label_separator is not None:
            data["label_separator"] = label_separator

        r = self.session.post(
            "/groups/{group_id}/webhooks".format(group_id=group_id), json=data
        )
        r.raise_for_status()
        return DotDict(r.json())

    def list_webhooks(self, group_id):
        """
        List all webhooks for a task group.

        :param str group_id: The task group id.

        :return: A dictionary with one key `webhooks` containing a list of
            dictionaries representing the webhooks.
        :rtype: DotDict
        """

        r = self.session.get("/groups/{group_id}/webhooks".format(group_id=group_id))
        r.raise_for_status()
        return DotDict(r.json())

    get_webhooks = list_webhooks

    def get_webhook(self, webhook_id):
        """
        Returns a webhook's configuration.

        :param str webhook_id: The webhook id.

        :return: A dictionary of the webhook's properties.
        :rtype: DotDict

        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            webhook cannot be found.
        """
        r = self.session.get("/webhooks/{webhook_id}".format(webhook_id=webhook_id))
        r.raise_for_status()
        return DotDict(r.json())

    def delete_webhook(self, group_id, webhook_id):
        """
        Delete an existing webhook.

        :param str webhook_id: The webhook id.

        :return: A boolean indicating if the deletion was successful.
        :rtype: bool

        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            webhook cannot be found.
        """
        r = self.session.delete("/webhooks/{webhook_id}".format(webhook_id=webhook_id))
        r.raise_for_status()
        return True

    def _sys_paths(self):
        if not hasattr(self, "_cached_sys_paths"):
            # use longest matching path entries.
            self._cached_sys_paths = sorted(
                six.moves.map(os.path.abspath, sys.path), key=len, reverse=True
            )
        return self._cached_sys_paths

    def _get_globals(self, func):
        # Disassemble the function and capture the output
        buffer = six.StringIO()
        save_stdout = sys.stdout

        try:
            sys.stdout = buffer
            dis.dis(func)
        finally:
            sys.stdout = save_stdout

        # Search for LOAD_GLOBAL instruction and capture the var name
        search_expr = ".* LOAD_GLOBAL .*\\((.*)\\)"
        compiled_search = re.compile(search_expr)

        # Non-builtin globals are collected here
        globs = set()

        for line in buffer.getvalue().split("\n"):
            result = compiled_search.match(line)

            if result:
                name = result.group(1)

                if not hasattr(six.moves.builtins, name):
                    globs.add(name)

        return sorted(globs)

    def _find_object(self, name):
        """Search for an object as specified by a fully qualified name.
        The fully qualified name must refer to an object that can be resolved
        through the module search path.

        :returns: The object, the module path as list, and the
                  function path as list
        """
        module_path = []  # Fully qualified module path
        object_path = []  # Fully qualified object path

        obj = None
        parts = name.split(".")

        for part in parts:
            error = None

            if hasattr(obj, part):
                # Could be any object (module, class, etc.)
                obj = getattr(obj, part)

                if inspect.ismodule(obj) and not object_path:
                    module_path.append(part)
                else:
                    object_path.append(part)
            else:
                # If not found, assume it's a module that must be loaded
                if object_path:
                    error = "'{}' has no attribute '{}'".format(type(obj), part)
                    raise NameError(
                        "Cannot resolve function name '{}': {}".format(name, error)
                    )
                else:
                    module_path.append(part)

                    current_module_path = ".".join(module_path)
                    try:
                        obj = importlib.import_module(current_module_path)
                    except Exception as ex:
                        traceback = sys.exc_info()[2]
                        six.reraise(
                            NameError,
                            NameError(
                                "Cannot resolve function name '{}', error importing module {}: {}".format(
                                    name, current_module_path, ex
                                )
                            ),
                            traceback,
                        )

        # When we're at the end, we should have found a valid object
        return obj, module_path, object_path

    def _build_bundle(
        self, group_function, include_data, include_modules, requirements=None
    ):
        data_files = self._find_data_files(include_data or [])

        try:
            with NamedTemporaryFile(delete=False, suffix=".zip", mode="wb") as f:
                with zipfile.ZipFile(
                    f, mode="w", compression=zipfile.ZIP_DEFLATED
                ) as bundle:
                    self._write_main_function(group_function, bundle)
                    self._write_data_files(data_files, bundle)

                    if include_modules:
                        self._write_include_modules(include_modules, bundle)

                    if requirements:
                        bundle.writestr(
                            REQUIREMENTS, self._requirements_string(requirements)
                        )
            return f.name
        except Exception:
            if os.path.exists(f.name):
                os.remove(f.name)
            raise

    def _find_data_files(self, include_data):
        data_files = []

        for pattern in include_data:
            is_glob = self._IS_GLOB_PATTERN.search(pattern)
            matched_paths = glob.glob(pattern)

            if not matched_paths:
                if is_glob:
                    warn("Include data glob pattern had no matches: {}".format(pattern))
                else:
                    raise ValueError("No data file found for path: {}".format(pattern))
            for path in six.moves.map(os.path.abspath, matched_paths):
                if os.path.exists(path):
                    sys_path = self._sys_path_prefix(path)

                    if os.path.isdir(path):
                        raise ValueError(
                            "Cannot pass directories as included data: `{}`".format(
                                path
                            )
                        )
                    else:
                        data_files.append(
                            (path, self._archive_path(path, DATA, sys_path=sys_path))
                        )
                else:
                    raise ValueError(
                        "Data file location does not exist: {}".format(path)
                    )

        return data_files

    def _write_main_function(self, f, archive):
        is_named_function = isinstance(f, six.string_types)

        if is_named_function:
            f, module_path, function_path = self._find_object(f)

            if not callable(f):
                raise ValueError(
                    "Tasks main function must be a callable: `{}`".format(f)
                )

            # Simply import the module
            source = self._IMPORT_TEMPLATE.format(
                module=".".join(module_path), obj=function_path[0]
            )
            function_name = ".".join(function_path)
        else:
            if not inspect.isfunction(f):
                raise ValueError(
                    "Tasks main function must be user-defined function: `{}`".format(f)
                )

            # We can't get the code for a given lambda
            if f.__name__ == "<lambda>":
                raise ValueError(
                    "Task main function cannot be a lambda expression: `{}`".format(f)
                )

            # Furthermore, the given function cannot refer to globals
            bound_globals = self._get_globals(f)

            if bound_globals:
                raise BoundGlobalError(
                    "Illegal reference to one or more global variables in your "
                    "function: {}".format(bound_globals)
                )

            try:
                source = inspect.getsource(f).strip()
                function_name = f.__name__
            except IOError as ioe:  # from the inpect.getsource call.
                raise ValueError("Cannot get function source for {}: {}".format(f, ioe))

        entrypoint_source = self._ENTRYPOINT_TEMPLATE.format(
            source=source, function_name=function_name
        )
        archive.writestr("{}/{}".format(DIST, ENTRYPOINT), entrypoint_source)

    def _write_data_files(self, data_files, archive):
        for path, archive_path in data_files:
            archive.write(path, archive_path)

    def _find_module_file(self, mod_name):
        """Search for module file in python path. Raise ImportError if not found"""

        try:
            mod = importlib.import_module(mod_name)
            mod_file = mod.__file__.replace(".pyc", ".py", 1)
            return mod_file

        except ImportError as ie:
            # Search for possible pyx file
            mod_basename = "{}.pyx".format(mod_name.replace(".", "/"))
            for s in sys.path:
                mod_file_option = os.path.join(s, mod_basename)
                if os.path.isfile(mod_file_option):
                    # Check that found cython source not in CWD (causes build problems)
                    if os.getcwd() == os.path.dirname(os.path.abspath(mod_file_option)):
                        raise ValueError(
                            "Cannot include cython modules from working directory: `{}`.".format(
                                mod_file_option
                            )
                        )
                    else:
                        return mod_file_option

            # Raise caught ImportError if we still haven't found the module
            raise ie

    def _write_include_modules(self, include_modules, archive):
        for mod_name in include_modules:
            mod_file = self._find_module_file(mod_name)

            # detect system packages from distribution or virtualenv locations.
            if re.match(".*(?:site|dist)-packages", mod_file) is not None:
                raise ValueError(
                    "Cannot include system modules: `{}`.".format(mod_file)
                )

            if not os.path.exists(mod_file):
                raise IOError(
                    "Source code for module is missing, only byte code exists: `{}`.".format(
                        mod_name
                    )
                )
            sys_path = self._sys_path_prefix(mod_file)

            self._include_init_files(os.path.dirname(mod_file), archive, sys_path)
            archive_names = archive.namelist()
            # this is a package, get all decendants if they exist.
            if os.path.basename(mod_file) == "__init__.py":
                for dirpath, dirnames, filenames in os.walk(os.path.dirname(mod_file)):
                    for file_ in [f for f in filenames if f.endswith((".py", ".pyx"))]:
                        path = os.path.join(dirpath, file_)
                        arcname = self._archive_path(path, DIST, sys_path)
                        if arcname not in archive_names:
                            archive.write(path, arcname=arcname)
            else:
                archive.write(
                    mod_file, arcname=self._archive_path(mod_file, DIST, sys_path)
                )

    def _include_init_files(self, dir_path, archive, sys_path):
        relative_dir_path = os.path.relpath(dir_path, sys_path)
        archive_names = archive.namelist()
        # have we walked this path before?
        if os.path.join(DIST, relative_dir_path, "__init__.py") not in archive_names:
            partial_path = ""
            for path_part in relative_dir_path.split(os.sep):
                partial_path = os.path.join(partial_path, path_part)
                rel_init_location = os.path.join(partial_path, "__init__.py")
                abs_init_location = os.path.join(sys_path, rel_init_location)
                arcname = os.path.join(DIST, rel_init_location)
                if not os.path.exists(abs_init_location):
                    raise IOError(
                        "Source code for module is missing: `{}`.".format(
                            abs_init_location
                        )
                    )
                if arcname not in archive_names:
                    archive.write(abs_init_location, arcname=arcname)

    def _requirements_string(self, requirements):
        if not pkg_resources:
            warn(
                "Your Python does not have a recent version of `setuptools`. "
                "For a better experience update your environment by running `pip install -U setuptools`."
            )
        if isinstance(requirements, six.string_types):
            return self._requirements_file_string(requirements)
        else:
            return self._requirements_list_string(requirements)

    def _requirements_file_string(self, requirements):
        if not os.path.isfile(requirements):
            raise ValueError(
                "Requirements file at {} not found. Did you mean to specify a single requirement? "
                "Pass it wrapped in a list.".format(requirements)
            )
        with open(requirements) as f:
            requirements_string = f.read()
        if pkg_resources:
            try:
                list(pkg_resources.parse_requirements(requirements_string))
            except ValueError as ex:
                raise ValueError(
                    "Invalid Python requirement in file: {}".format(str(ex))
                )
        return requirements_string

    def _requirements_list_string(self, requirements):
        if pkg_resources:
            bad_requirements = []
            for requirement in requirements:
                try:
                    pkg_resources.Requirement.parse(requirement)
                except ValueError:
                    bad_requirements.append(requirement)
            if bad_requirements:
                raise ValueError(
                    "Invalid Python requirements: {}".format(",".join(bad_requirements))
                )
        return "\n".join(requirements)

    def _sys_path_prefix(self, path):
        absolute_path = os.path.abspath(path)
        for sys_path in self._sys_paths():
            if absolute_path.startswith(sys_path):
                return sys_path
        else:
            raise IOError("Location is not on system path: `{}`".format(path))

    def _archive_path(self, path, archive_prefix, sys_path):
        return os.path.join(archive_prefix, os.path.relpath(path, sys_path))


AsyncTasks = Tasks


class TransientResultException(Exception):
    """
    Raised when attempting to access results for a task that hasn't
    completed.
    """

    def __init__(self, message):
        super(Exception, self).__init__(message)


class ResultType(object):
    """
    Possible types of return values for a function.
    """

    JSON = "json"
    LEGACY_PICKLE = "pickle"


class CloudFunction(object):
    """
    Represents the asynchronous function of a task group. When called, new
    tasks are submitted to the group with the positional and keyword arguments
    given. A `map()` method allows submitting multiple tasks more efficiently
    than making individual function calls.
    """

    TASK_SUBMIT_SIZE = 100

    def __init__(self, group_id, name=None, client=None, retry_count=0):
        self.group_id = group_id
        self.name = name
        self.client = client
        self.retry_count = retry_count

    def __call__(self, *args, **kwargs):
        """
        Submits a task calling the function with the given positional and
        keyword arguments.

        All positional and keyword arguments must be JSON-serializable (i.e.,
        booleans, numbers, strings, lists, dictionaries).

        :return: The submitted task.
        :rtype: descarteslabs.client.services.tasks.FutureTask
        """
        tasks = self.client.new_task(
            self.group_id,
            arguments=args,
            parameters=kwargs,
            retry_count=self.retry_count,
        )
        task_info = tasks.tasks[0]
        return FutureTask(
            self.group_id, task_info.id, client=self.client, args=args, kwargs=kwargs
        )

    def map(self, args, *iterargs):
        """
        Submits multiple tasks efficiently with positional argument to each function
        call, mimicking the behaviour of the builtin `map()` function. When
        submitting multiple tasks this is preferred over calling the function
        repeatedly.

        All positional arguments must be JSON-serializable (i.e., booleans, numbers,
        strings, lists, dictionaries).

        :param iterable args: An iterable of arguments. A task will be submitted
            with each element of the iterable as the first positional argument
            to the function.
        :param list(iterable) iterargs: If additional iterable arguments are
            passed, the function must take that many arguments and is applied
            to the items from all iterables in parallel (mimicking builtin
            `map()` behaviour).

        :return: A list of all submitted tasks.
        :rtype: list(descarteslabs.client.services.tasks.FutureTask)
        """
        arguments = zip_longest(args, *iterargs)

        futures = []
        batch = list(itertools.islice(arguments, self.TASK_SUBMIT_SIZE))
        while batch:
            tasks_info = self.client.new_tasks(
                self.group_id, list_of_arguments=batch, retry_count=self.retry_count
            )
            futures += [
                FutureTask(
                    self.group_id, task_info.id, client=self.client, args=task_args
                )
                for task_info, task_args in zip(tasks_info.tasks, batch)
            ]
            batch = list(itertools.islice(arguments, self.TASK_SUBMIT_SIZE))

        return futures

    def wait_for_completion(self, show_progress=False):
        """
        Waits until all tasks submitted through this function are completed.

        If a task group stops accepting tasks, will raise
        :class:`GroupTerminalException` and stop waiting.

        :param bool show_progress: Whether to log progress information.
        """
        self.client.wait_for_completion(self.group_id, show_progress=show_progress)


def _raise_if_terminal_group(group_id, client, group=None):
    if group is None:
        group = client.get_group(group_id)

    if group.status in ["terminated", "build_failed"]:
        msg = "Group no longer running tasks. Group status: {}.".format(group.status)
        if group.status == "build_failed":
            msg = "{} Check the build log and fix any errors then resubmit your tasks.".format(
                msg
            )
        raise GroupTerminalException(msg)


def as_completed(tasks, show_progress=True):
    """
    Yields completed tasks from the list of given tasks as they become
    available, finishing when all given tasks have been completed.

    If you don't care about the particular results of the tasks and only
    want to wait for all tasks to complete, use
    :meth:`wait_for_completion <CloudFunction>`.

    If a task group stops accepting tasks, will raise
    :class:`GroupTerminalException` and stop waiting.

    :param list(descarteslabs.client.services.tasks.FutureTask) tasks: List of
        :class:`descarteslabs.client.services.tasks.FutureTask` objects.
    :param bool show_progress: Whether to log progress information.
    """
    total_tasks = len(tasks)
    remaining = OrderedDict(((t.guid, t.tuid), t) for t in tasks)

    while len(remaining) > 0:
        batch = itertools.islice(remaining.values(), Tasks.TASK_RESULT_BATCH_SIZE)
        by_group = defaultdict(list)
        for task in batch:
            by_group[task.guid].append(task)

        for group_id, group_tasks in by_group.items():
            client = group_tasks[0].client

            # stop waiting if the group hits a terminal state
            _raise_if_terminal_group(group_id, client)

            task_ids = [task.tuid for task in group_tasks]
            try:
                results = client.get_task_result_batch(
                    group_id, task_ids, include=["stacktrace"]
                )
            except BaseException:
                logging.warning(
                    "Task retrieval for group %s failed with fatal error",
                    group_id,
                    exc_info=True,
                )
            else:
                for result in results["results"]:
                    task = remaining.pop((group_id, result.id))
                    task._task_result = result
                    yield task

        if show_progress:
            logging.warning(
                "Done with %i / %i tasks", total_tasks - len(remaining), total_tasks
            )
        time.sleep(Tasks.COMPLETION_POLL_INTERVAL_SECONDS)


def _serialize_function(function):
    # Note; In Py3 cloudpickle and base64 handle bytes objects only, so we need to
    # decode it into a string to be able to json dump it again later.
    cp_version = getattr(cloudpickle, "__version__", None)
    if cp_version is None or cp_version != "0.4.0":
        warn(
            (
                "You must use version 0.4.0 of cloudpickle for compatibility with the Tasks client. {} found."
            ).format(cp_version)
        )

    encoded_bytes = base64.b64encode(cloudpickle.dumps(function))
    return encoded_bytes.decode("ascii")


def maybe_get_pkg_resources():
    """
    Return the pkg_resources module or None, depending on whether we think that whatever
    the system has available is going to do an ok job at parsing requirements patterns.
    We don't want to strictly require any particular version of setuptools to not force
    the user to mess with their system.
    """
    try:
        import pkg_resources

        try:
            pkg_resources.Requirement.parse('foo[bar]==2.0;python_version>"2.7"')
        except (ValueError, AttributeError):
            return None
        else:
            return pkg_resources
    except ImportError:
        return None


pkg_resources = maybe_get_pkg_resources()
