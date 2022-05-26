def tuple_del(t, i):
    if i < 0:
        i = len(t) + i
    return t[:i] + t[i + 1 :]


def tuple_insert(t, i, x):
    if i < 0:
        i = len(t) + i + 1
    return t[:i] + (x,) + t[i:]


def tuple_replace(t, i, x):
    if i < 0:
        i = len(t) + i + 1
    return t[:i] + (x,) + t[i + 1 :]


def tuple_move(t, i, i2):
    lst = list(t)
    x = lst.pop(i)
    lst.insert(i2, x)
    return tuple(lst)
