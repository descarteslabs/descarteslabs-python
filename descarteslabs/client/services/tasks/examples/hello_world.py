from descarteslabs.client.services.tasks import Tasks, as_completed

at = Tasks()


def f(x, option=None):
    return "Hello World {} {}".format(x, option)


async_func = at.create_function(
    f,
    name='hello-world',
    image="us.gcr.io/dl-ci-cd/images/tasks/public/alpha/py2/default:v2018.04.26",
)

task1 = async_func(1)
task2 = async_func(2, option='hi')
print(task1.result)
print(task2.result)

tasks = async_func.map(range(100))
for task in as_completed(tasks):
    print(task.result)
