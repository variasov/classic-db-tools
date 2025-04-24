from types import SimpleNamespace

from classic.sql_tools import Module


# %timeit with_list(1000)
# 142 μs ± 11.9 μs per loop (mean ± std. dev. of 7 runs, 10,000 loops each)
def with_list(cnt):
    def func(arg, lst):
        lst.append(arg)
        return lst

    result = []
    for i in range(cnt):
        result = func(i, result)
    return hash(tuple(result))


# %timeit with_tuple(1000)
# 920 μs ± 3.03 μs per loop (mean ± std. dev. of 7 runs, 1,000 loops each)
def with_tuple(cnt):
    def func(arg, tpl):
        return tpl + (arg,)

    result = ()
    for i in range(cnt):
        result = func(i, result)
    return hash(result)


# %timeit with_str(1000)
# 306 μs ± 2.91 μs per loop (mean ± std. dev. of 7 runs, 1,000 loops each)
def with_str(cnt):
    def func(left, right):
        return left + right

    result = ''
    for i in range(cnt):
        result = func(str(i), result)
    return hash(result)


# %timeit with_dct(1000)
# 124 μs ± 400 ns per loop (mean ± std. dev. of 7 runs, 10,000 loops each)
def with_dct(cnt):
    def func(arg, dct):
        dct[arg] = None
        return dct

    result = {}
    for i in range(cnt):
        result = func(i, result)
    return hash(result.keys())


dirs = [
    's' + str(i)
    for i in range(1000)
]

first = SimpleNamespace()
last = first
for dir in dirs:
    new = SimpleNamespace()
    setattr(last, dir, new)
    last = new


# %timeit with_pre_cached_simple_namespaces(1000)
# 233 μs ± 20.9 μs per loop (mean ± std. dev. of 7 runs, 1,000 loops each)
def with_pre_cached_simple_namespaces(cnt):
    def func(arg, queries):
        return getattr(queries, arg)

    result = first
    for i in dirs:
        result = func(i, result)
    return result
