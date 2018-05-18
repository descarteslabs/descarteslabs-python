from descarteslabs.client.services.tasks import Tasks


def buggy_func():
    print("About to fail")
    return 1 / 0


at = Tasks()
async_func = at.create_or_get_function(buggy_func, name='buggy', retry_count=2)
task = async_func()

print(task.result)
if not task.is_success:
    print("Caught a %s exception!" % task.exception_name)
    print(task.log)
    print(task.stacktrace)
