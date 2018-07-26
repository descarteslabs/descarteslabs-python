from descarteslabs.client.services.tasks import Tasks, as_completed


def f():
    import tensorflow as tf
    hello = tf.constant('Hello, TensorFlow!')

    # Start tf session
    sess = tf.Session()

    # Run the op
    return sess.run(hello)


at = Tasks()

async_function = at.create_function(
    f,
    name="hello-tensorflow",
    image="us.gcr.io/dl-ci-cd/images/tasks/public/py2/default:v2018.07.25",
)

for task in as_completed([async_function()]):
    print(task.result)
