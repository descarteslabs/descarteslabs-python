#!/usr/bin/env python
from pprint import pprint

from descarteslabs.client.services.tasks import Tasks


def print_group_status():
    at = Tasks()

    groups = at.iter_groups(status='running')

    print("{0:^20}{1:^20}{2:^10}{3:^12}"
          "{4:^13}{5:^13}{6:^13}{7:^13}{8:^13}".format(
              "name",
              "created",
              "id",
              "status",
              "workers",
              "worker-fail",
              "pending",
              "successes",
              "failures",
          ))
    print("=" * 140)
    first_g = None
    sort_order = {
        'running': 2,
        'pending': 1,
        'terminated': 0,
        'unknown': -1
    }
    for g in sorted(groups, key=lambda g: (sort_order[g.status], g.created), reverse=True):
        if g.status == 'unknown':
            continue
        first_g = g if first_g is None else first_g
        print(("{name:^20}{created:^20.19}{id:^10}{status:^12}"
               "{workers[running]:^13}"
               "{workers[failed]:^13}"
               "{queue[pending]:^13}"
               "{queue[successes]:^13}"
               "{queue[failures]:^13}"
               ).format(**g))
    pprint(first_g)


if __name__ == "__main__":
    print_group_status()
