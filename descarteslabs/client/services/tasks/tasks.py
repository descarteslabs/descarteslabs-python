# Copyright 2018 Descartes Labs.
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
import itertools
import json
import logging
import os
import sys
import time
from warnings import warn
from six.moves import zip_longest

import cloudpickle

from descarteslabs.client.auth import Auth
from descarteslabs.client.exceptions import ConflictError
from descarteslabs.client.services.service import Service
from descarteslabs.common.dotdict import DotDict, DotList
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


class Tasks(Service):

    TASK_RESULT_BATCH_SIZE = 100
    RERUN_BATCH_SIZE = 200
    COMPLETION_POLL_INTERVAL_SECONDS = 5

    def __init__(self, url=None, auth=None):
        if auth is None:
            auth = Auth()

        if url is None:
            url = os.environ.get(
                "DESCARTESLABS_TASKS_URL",
                "https://platform.descarteslabs.com/tasks/v1"
            )

        super(Tasks, self).__init__(url, auth=auth)

    def _create_namespace(self):
        """
        Creates a namespace for the user and sets up authentication within it
        from the current client id and secret. Must be called once per user
        before creating any tasks.

        :return: `True` if successful, `False` otherwise.
        """
        data = {
            'CLIENT_ID': self.auth.client_id,
            'CLIENT_SECRET': self.auth.client_secret
        }
        r = self.session.post('/namespaces/secrets/auth', json=data)
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
            container_image=None,
            name=None,
            cpus=1,
            memory='2Gi',
            maximum_concurrency=None,
            minimum_concurrency=None,
            minimum_seconds=None,
            task_timeout=1800,
            **kwargs
    ):
        """
        Creates a new task group.

        :param function function: The function to be called in a task.
        :param str container_image: The location of a docker image to be used for
            the environment in which the function is executed.
        :param str name: An optional name used to later help identify the function.
        :param int cpus: The number of CPUs requested for a single task. A task
            might be throttled if it uses more CPU. Default: 1. Maximum: 32.
        :param str memory: The maximum memory requirement for a single task. If a
            task uses substantially more memory it will be killed. The value
            should be a string and can use postfixes such as Mi, Gi, MB, GB, etc
            (e.g. `"4Gi"`, `"500MB"`). If no unit is specified it is assumed to be
            in bytes. Default: 2Gi. Maximum: 64Gi.
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

        :return: A dictionary representing the group created.
        """
        if container_image is None:
            container_image = "us.gcr.io/dl-ci-cd/images/tasks/public/alpha/py2/default:v2018.04.26"

        payload = {
            'image': container_image,
            'function': _serialize_function(function),
            'function_python_version': ".".join(str(part) for part in sys.version_info[:3]),
            'cpu': cpus,
            'mem': memory,
            'worker_timeout': task_timeout,
            'maximum_concurrency': maximum_concurrency,
            'minimum_concurrency': minimum_concurrency,
            'minimum_seconds': minimum_seconds,
        }

        if name is not None:
            payload['name'] = name

        payload.update(kwargs)

        try:
            r = self.session.post("/groups", json=payload)
        except ConflictError as e:
            error_message = json.loads(str(e))['message']
            if error_message != 'namespace is missing authentication':
                raise

            if not self._create_namespace():
                raise

            r = self.session.post("/groups", json=payload)

        return DotDict(r.json())

    def list_groups(
        self,
        status=None,
        created=None,
        updated=None,
        sort_field=None,
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
        :param int limit: The number of results to get (max 1000 per page).
        :param str continuation_token: A string returned from a previous call to
            `list_groups()`, which you can use to get the next page of results.

        :return: A dictionary with two keys; `groups` containing the list of
            matching groups, `continuation_token` containting a string if there
            are further matching groups.
        """
        params = {'limit': limit}
        for field in ['status', 'created', 'updated', 'sort_field', 'sort_order', 'continuation_token']:
            if locals()[field] is not None:
                params[field] = locals()[field]
        r = self.session.get("/groups", params=params)
        r.raise_for_status()
        return DotDict(r.json())

    def iter_groups(
        self,
        status=None,
        created=None,
        updated=None,
        sort_field=None,
        sort_order="asc",
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
        """
        continuation_token = None
        while True:
            page = self.list_groups(status=status, created=created, updated=updated,
                                    sort_field=sort_field, sort_order=sort_order,
                                    continuation_token=continuation_token)
            for group in page.groups:
                yield group

            continuation_token = page.continuation_token
            if continuation_token is None:
                break

    def get_group(self, group_id):
        """
        Retrieves a single task group by id.

        :param str group_id: The group id.

        :return: A dictionary representing the task group.
        """
        r = self.session.get(
            "/groups/{}".format(group_id),
        )
        r.raise_for_status()
        return DotDict(r.json())

    get_group_by_id = get_group

    def get_group_by_name(self, name, status='running'):
        """
        Retrieves a single task group by name. Names are not unique; if there are
        multiple matches, returns the newest group.

        :param str group_id: The group name.
        :param str status: Only consider groups with this status.
            Allowed are ['running', 'terminated']. Default: 'running'.

        :return: A dictionary representing the task group, or `None` if no group
            with the given name exists.
        """
        groups = self.iter_groups(status=status, sort_field="created", sort_order="desc")
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
        """
        r = self.session.delete(
            "/groups/{uid}".format(uid=group_id)
        )
        r.raise_for_status()
        return DotDict(r.json())

    delete_group_by_id = terminate_group

    def wait_for_completion(self, group_id, show_progress=False):
        """
        Waits until all submitted tasks for a given group are completed.

        :param str group_id: The group id.
        :param bool show_progress: Whether to log progress information.
        """
        queue = self.get_group(group_id).queue
        while queue.pending > 0:
            completed = queue.failures + queue.successes
            if show_progress:
                logging.warning("Done with %i / %i tasks", completed, queue.pending + completed)
            time.sleep(self.COMPLETION_POLL_INTERVAL_SECONDS)
            queue = self.get_group(group_id).queue

    def new_task(self, group_id, arguments=None, parameters=None,
                 labels=None, retry_count=0):
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
        """
        return self.new_tasks(
            group_id,
            list_of_arguments=[arguments or []],
            list_of_parameters=[parameters or {}],
            list_of_labels=[labels],
            retry_count=retry_count
        )

    def new_tasks(self, group_id, list_of_arguments=None,
                  list_of_parameters=None, list_of_labels=None,
                  retry_count=0):
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
        """
        list_of_arguments = list_of_arguments if \
            list_of_arguments is not None else [[]]
        list_of_parameters = list_of_parameters if \
            list_of_parameters is not None else [{}]
        list_of_labels = list_of_labels if \
            list_of_labels is not None else [None]
        msgs = []
        for args, kwargs, labels in zip_longest(list_of_arguments, list_of_parameters, list_of_labels, fillvalue=None):
            args = args or []
            params = kwargs or {}
            attributes = {'labels': labels} if labels else {}
            msg = {
                'arguments': args,
                'parameters': params,
                'attributes': attributes,
                'retry_count': retry_count,
            }
            msgs.append(msg)

        r = self.session.post(
            "/groups/{group_id}/tasks".format(group_id=group_id),
            json={
                'tasks': msgs
            }
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
        """
        params = {'include': include} if include is not None else {}
        r = self.session.get(
            '/groups/{uid}/tasks/{task_uid}/results'.format(
                uid=group_id, task_uid=task_id
            ),
            params=params,
        )
        r.raise_for_status()
        result = r.json()
        if 'result' in result:
            result['result'] = base64.b64decode(result['result'])
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
        """
        data = {'ids': task_ids}
        if include is not None:
            data['include'] = include
        r = self.session.post(
            '/groups/{uid}/results/batch'.format(uid=group_id),
            json=data,
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
            sort_field='created',
            sort_order='asc',
            continuation_token=None,
    ):
        """
        Retrieves a limited list of task results matching the given criteria.

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
        """
        if offset is not None:
            warn(OFFSET_DEPRECATION_MESSAGE, DeprecationWarning)

        params = {'limit': limit}
        for field in ['offset', 'status', 'failure_type', 'updated', 'created', 'webhook',
                      'labels', 'include', 'sort_field', 'sort_order', 'continuation_token']:
            if locals()[field] is not None:
                params[field] = locals()[field]
        r = self.session.get(
            '/groups/{uid}/results'.format(uid=group_id),
            params=params
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
            sort_field='created',
            sort_order='asc',
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
        """
        params = {}
        for field in ['status', 'failure_type', 'updated', 'created', 'webhook', 'labels', 'include']:
            if locals()[field] is not None:
                params[field] = locals()[field]

        continuation_token = None
        while True:
            page = self.get_task_results(group_id, continuation_token=continuation_token, **params)
            for result in page.results:
                yield result

            continuation_token = page.continuation_token
            if continuation_token is None:
                break

    def rerun_failed_tasks(self, group_id, retry_count=0):
        """
        Submits all failed tasks for a rerun, except for tasks that had an
        out-of-memory or version mismatch failure.
        These tasks will be run again with the same arguments as before.

        Tasks that are currently already being rerun will be ignored.

        :param str group_id: The group in which to rerun tasks.
        :param int retry_count: Number of times to retry a task if it fails
                                (maximum 5)

        :return: A list of dictionaries representing the tasks that have been submitted.
        """
        rerun_tasks = []
        for failure_type in ['exception', 'timeout', 'internal', 'unknown']:
            rerun_tasks += self.rerun_matching_tasks(group_id, failure_type=failure_type, retry_count=retry_count)
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
            retry_count=0
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
        """
        results = self.iter_task_results(group_id, status=status, failure_type=failure_type, updated=updated,
                                         created=created, webhook=webhook, labels=labels)
        return self.rerun_tasks(group_id, (t.id for t in results), retry_count=retry_count)

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
        """
        rerun = []
        task_ids = list(itertools.islice(task_id_iterable, self.RERUN_BATCH_SIZE))
        while task_ids:
            r = self.session.post(
                "/groups/{group_id}/tasks/rerun".format(group_id=group_id),
                json={
                    'task_ids': task_ids,
                    'retry_count': retry_count,
                }
            )
            r.raise_for_status()
            rerun += r.json()['tasks']
            task_ids = list(itertools.islice(task_id_iterable, self.RERUN_BATCH_SIZE))
        return DotList(rerun)

    def create_function(self, f, image=None, name=None, cpus=1,
                        memory='2Gi',
                        maximum_concurrency=None,
                        minimum_concurrency=None,
                        minimum_seconds=None,
                        task_timeout=1800,
                        retry_count=0,
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
        :param str memory: The maximum memory requirement for a single task. If a
            task uses substantially more memory it will be killed. The value
            should be a string and can use postfixes such as Mi, Gi, MB, GB, etc
            (e.g. `"4Gi"`, `"500MB"`). If no unit is specified it is assumed to be
            in bytes. Default: 2Gi. Maximum: 64Gi.
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
        """
        group_info = self.new_group(
            f, container_image=image, name=name,
            cpus=cpus, memory=memory,
            maximum_concurrency=maximum_concurrency,
            minimum_concurrency=minimum_concurrency,
            minimum_seconds=minimum_seconds,
            task_timeout=task_timeout,
        )

        return CloudFunction(group_info.id, name=name, client=self, retry_count=retry_count)

    def get_function(self, name):
        """
        Gets an asynchronous function by name (the last function created with that
        name).

        :param str name: The name of the function to lookup.

        :return: A :class:`CloudFunction`, or `None` if no function with the
            given name exists.
        """
        warn(GET_FUNCTION_DEPRECATION_MESSAGE, DeprecationWarning)

        group_info = self.get_group_by_name(name)
        if group_info is None:
            return None

        return CloudFunction(group_info.id, name=name, client=self)

    def create_or_get_function(self, f, image=None, name=None, cpus=1,
                               memory='2Gi',
                               maximum_concurrency=None,
                               minimum_concurrency=None,
                               minimum_seconds=None,
                               task_timeout=1800,
                               retry_count=0,
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
        :param str memory: The maximum memory requirement for a single task. If a
            task uses substantially more memory it will be killed. The value
            should be a string and can use postfixes such as Mi, Gi, MB, GB, etc
            (e.g. `"4Gi"`, `"500MB"`). If no unit is specified it is assumed to be
            in bytes. Default: 2Gi. Maximum: 64Gi.
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
        """

        warn(CREATE_OR_GET_DEPRECATION_MESSAGE, DeprecationWarning)

        if name:
            cached = self.get_function(name)
            if cached is not None:
                return cached
        return self.create_function(
            f, image=image, name=name, cpus=cpus,
            memory=memory,
            maximum_concurrency=maximum_concurrency,
            minimum_concurrency=minimum_concurrency,
            minimum_seconds=minimum_seconds,
            task_timeout=task_timeout,
            retry_count=retry_count,
        )

    def create_webhook(self, group_id, name=None, label_path=None, label_separator=None):
        data = {}
        if name is not None:
            data['name'] = name
        if label_path is not None:
            data['label_path'] = label_path
        if label_separator is not None:
            data['label_separator'] = label_separator
        r = self.session.post(
            '/groups/{group_id}/webhooks'.format(group_id=group_id),
            json=data
        )
        r.raise_for_status()
        return DotDict(r.json())

    def list_webhooks(self, group_id):
        r = self.session.get(
            '/groups/{group_id}/webhooks'.format(group_id=group_id),
        )
        r.raise_for_status()
        return DotDict(r.json())

    get_webhooks = list_webhooks

    def get_webhook(self, group_id, webhook_id):
        r = self.session.get(
            '/groups/{group_id}/webhooks/{webhook_id}'.format(
                group_id=group_id,
                webhook_id=webhook_id,
            ),
        )
        r.raise_for_status()
        return DotDict(r.json())

    def delete_webhook(self, group_id, webhook_id):
        r = self.session.delete(
            '/groups/{group_id}/webhooks/{webhook_id}'.format(
                group_id=group_id,
                webhook_id=webhook_id,
            ),
        )
        r.raise_for_status()
        return True


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

        :return: A :class:`FutureTask` for the submitted task.
        """
        tasks = self.client.new_task(
            self.group_id,
            arguments=args,
            parameters=kwargs,
            retry_count=self.retry_count,
        )
        task_info = tasks.tasks[0]
        return FutureTask(self.group_id, task_info.id, client=self.client, args=args, kwargs=kwargs)

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

        :return: A list of :class:`FutureTask` for all submitted tasks.
        """
        arguments = zip_longest(args, *iterargs)

        futures = []
        batch = list(itertools.islice(arguments, self.TASK_SUBMIT_SIZE))
        while batch:
            tasks_info = self.client.new_tasks(
                self.group_id,
                list_of_arguments=batch,
                retry_count=self.retry_count,
            )
            futures += [
                FutureTask(self.group_id, task_info.id, client=self.client, args=task_args)
                for task_info, task_args in zip(tasks_info.tasks, batch)
            ]
            batch = list(itertools.islice(arguments, self.TASK_SUBMIT_SIZE))

        return futures

    def wait_for_completion(self, show_progress=False):
        """
        Waits until all tasks submitted through this function are completed.

        :param bool show_progress: Whether to log progress information.
        """
        self.client.wait_for_completion(self.group_id, show_progress=show_progress)


def as_completed(tasks, show_progress=True):
    """
    Yields completed tasks from the list of given tasks as they become
    available, finishing when all given tasks have been completed.

    If you don't care about the particular results of the tasks and only
    want to wait for all tasks to complete, use
    :meth:`wait_for_completion <CloudFunction>`.

    :param list tasks: List of :class:`FutureTask` objects.
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
            task_ids = [task.tuid for task in group_tasks]
            try:
                results = client.get_task_result_batch(group_id, task_ids, include=['stacktrace'])
            except BaseException:
                logging.warning("Task retrieval for group %s failed with fatal error", group_id, exc_info=True)
            else:
                for result in results['results']:
                    task = remaining.pop((group_id, result.id))
                    task._task_result = result
                    yield task

        if show_progress:
            logging.warning("Done with %i / %i tasks", total_tasks - len(remaining), total_tasks)
        time.sleep(Tasks.COMPLETION_POLL_INTERVAL_SECONDS)


def _serialize_function(function):
    # Note; In Py3 cloudpickle and base64 handle bytes objects only, so we need to
    # decode it into a string to be able to json dump it again later.
    encoded_bytes = base64.b64encode(cloudpickle.dumps(function))
    return encoded_bytes.decode('ascii')
