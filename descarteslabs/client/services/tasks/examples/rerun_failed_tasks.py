#!/usr/bin/env python
"""
Reruns failed tasks for a given group

Example invocation:

    ./rerun_failed_tasks.py group_id [--retry_count=N]

Reruns tasks for the group with id `group_id`, retrying them `N` times
(maximum 5) if they fail again.
"""
import argparse
from pprint import pprint

from descarteslabs.client.services.tasks import Tasks


def rerun_failed_tasks(group_id, retry_count=0):
    at = Tasks()
    return at.rerun_failed_tasks(group_id, retry_count=retry_count)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--retry_count", type=int, default=0, help='number of times to retry failed tasks')
    parser.add_argument("group_id")
    args = parser.parse_args()

    tasks = rerun_failed_tasks(args.group_id, args.retry_count)
    print("Submitted", len(tasks), "tasks for rerun")
    if tasks:
        pprint(tasks)
