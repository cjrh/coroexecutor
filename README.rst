.. image:: https://github.com/cjrh/coroexecutor/workflows/Python%20application/badge.svg
    :target: https://github.com/cjrh/coroexecutor/actions

.. image:: https://img.shields.io/badge/stdlib--only-yes-green.svg
    :target: https://img.shields.io/badge/stdlib--only-yes-green.svg

.. image:: https://coveralls.io/repos/github/cjrh/coroexecutor/badge.svg?branch=master
    :target: https://coveralls.io/github/cjrh/coroexecutor?branch=master

.. image:: https://img.shields.io/pypi/pyversions/coroexecutor.svg
    :target: https://pypi.python.org/pypi/coroexecutor

.. image:: https://img.shields.io/github/tag/cjrh/coroexecutor.svg
    :target: https://img.shields.io/github/tag/cjrh/coroexecutor.svg

.. image:: https://img.shields.io/badge/install-pip%20install%20coroexecutor-ff69b4.svg
    :target: https://img.shields.io/badge/install-pip%20install%20coroexecutor-ff69b4.svg

.. image:: https://img.shields.io/pypi/v/coroexecutor.svg
    :target: https://img.shields.io/pypi/v/coroexecutor.svg

.. image:: https://img.shields.io/badge/calver-YYYY.MM.MINOR-22bfda.svg
    :target: http://calver.org/

**ALPHA**

coroexecutor
============

Provides an ``Executor`` interface for running a group of coroutines
together in asyncio-native applications.

Demo
----

.. code-block:: python3

    import asyncio
    from coroexecutor import CoroutineExecutor

    async def f(dt):
        await asyncio.sleep(dt)

    async def main():
        async with CoroutineExecutor() as exe:
            t1 = exe.submit(f, 0.01)
            t2 = exe.submit(f, 0.05)

        assert t1.done()
        assert t2.done()

    asyncio.run(main())

Discussion
----------

The ``Executor`` interface can't be exactly matched because
some functions in this interface need to be ``async`` functions. But we
can get close.

Some ideas from Trio's *nurseries* have been used as inspiration:

- The ``CoroutineExecutor`` waits until all submitted jobs are complete.
- If any jobs raise an exception, all other unfinished jobs are cancelled
  (they with have `CancelledError` raised inside them), and
  ``CoroutineExecutor`` re-raises that same exception.

Examples
--------

Using ``map``
^^^^^^^^^^^^^

The ``concurrent.futures.Executor`` interface also defines ``map()`` which
returns an iterator. However, it makes for sense for us to use an
*asynchronous generator* for this purpose. Here's an example from the tests:

.. code-block:: python3

    times = [0.01, 0.02, 0.03]

    async def f(dt):
        await asyncio.sleep(dt)
        return dt

    async def main():
        async with CoroutineExecutor() as exe:
            results = exe.map(f, times)
            assert [v async for v in results] == times

    run(main())

You can see how ``async for`` is used to asynchronously loop over the
result from calling ``map``.

If one of the function calls raises an error, all unfinished calls will
be cancelled, but you may still have received partial results. Here's
another example from the tests:

.. code-block:: python3

    times = [0.01, 0.02, 0.1, 0.2]
    results = []

    async def f(dt):
        await asyncio.sleep(dt)
        if dt == 0.1:
            raise Exception('oh noes')
        return dt

    async def main():
        async with CoroutineExecutor() as exe:
            async for r in exe.map(f, times):
                results.append(r)

    with pytest.raises(Exception):
        run(main())

    assert results == [0.01, 0.02]

The first two values of the batch finish quickly, and I saved these to the
``results`` list in the outer scope. Then, one of the jobs fails with
an exception. This results in the other pending jobs being cancelled (i.e.,
the "0.2" case in this example), the ``CoroutineExecutor`` instance
re-raising the exception, and in this example, the exception raises all
the way out to the invocation of the ``run()`` function itself. However,
note that we still have the results from jobs that succeeded.

Timeouts
^^^^^^^^

It seems convenient to let the ``CoroutineExecutor`` also apply timeouts
to the batch of jobs it manages. After all, it already manages the jobs,
so cancelling them all when a timeout is triggered seems like little
extra work.

This is how timeouts look (again, taken from one of the tests):

.. code-block:: python3

    tasks = []

    async def f(dt):
        await asyncio.sleep(dt)

    async def main():
        async with CoroutineExecutor(timeout=0.05) as exe:
            t1 = exe.submit(f, 0.01)
            t2 = exe.submit(f, 5)
            tasks.extend([t1, t2])

    with pytest.raises(asyncio.TimeoutError):
        run(main())

    t1, t2 = tasks
    assert t1.done() and not t1.cancelled()
    assert t2.done() and t2.cancelled()

Inside the executor, there is a fast job and a slow job. The timeout will
be applied after the fast one completes, but before the slow one completes.
The raised ``TimeoutError`` will cancel the slow job, and will be raised
out of the executor, and indeed all the way to the ``run()`` function (in
this example).

Nesting
^^^^^^^

You don't always have to submit tasks to the executor in a single function.
The executor instance can be passed around and work can be added to it
from several different places.

.. code-block:: python3

    from random import random

    async def f(dt):
        await asyncio.sleep(dt)

    async def producer1(executor: CoroutineExecutor):
        executor.submit(f, random())
        executor.submit(f, random())
        executor.submit(f, random())

    async def producer2(executor: CoroutineExecutor):
        executor.submit(f, random())
        executor.submit(f, random())
        executor.submit(f, random())

    async def main():
        async with CoroutineExecutor(timeout=0.5) as executor:
            executor.submit(f, random())
            executor.submit(f, random())
            executor.submit(f, random())

            executor.submit(producer1, executor)
            executor.submit(producer2, executor)

    run(main())

You can not only submit jobs within the executor context manager, but also
pass the instance around and collect jobs from other functions too. And the
timeout set when creating the ``CoroutineExecutor`` instance will still
be applied.
