a_global = "A global var"


def foo():
    print("foo")


def func_foo():
    a_local = "A local var"
    return a_local + a_global


class outer_class():
    class inner_class:
        @staticmethod
        def func_bar():
            a_local = "A local var"
            return a_local + a_global
