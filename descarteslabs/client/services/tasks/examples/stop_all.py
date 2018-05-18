from descarteslabs.client.services.tasks import Tasks

at = Tasks()
for g in at.iter_groups(status='running'):
    print("Stopping {name}".format(**g))
    at.terminate_group(g.id)
