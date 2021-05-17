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

    async def f(dt, msg=''):
        await asyncio.sleep(dt)
        print(f'completion message: {msg}')

    async def main():
        async with CoroutineExecutor(max_workers=10, max_backlog=1) as exe:
            t1 = await exe.submit(f, 0.01, msg="task 1")
            t2 = await exe.submit(f, 0.05, msg="task 2")

        assert t1.done()
        assert t2.done()

    asyncio.run(main())

- ``max_workers`` controls how many submitted jobs can run concurrently. These
  internal workers are lightweight of course, they're just ``asyncio.Task``
  instances.
- ``max_backlog`` controls how many jobs can be submitted before "backpressure"
  is applied. This is why ``.submit`` is awaitable: to provide backpressure.
  Backlog means "I will keep up to this many submitted jobs on a queue while
  they wait to be picked up by a worker.
- Jobs are submitted in this format: ``.submit(corofn, *args, **kwargs)``.
  The supplied job will be called as ``await corofn(*args, **kwargs)``.

Discussion
----------

The ``CoroutineExecutor`` context manager works very much like the
``Executor`` implementations in the ``concurrent.futures`` package in
the standard library. The basic components of the interface are:

- The executor applies a context over the creation of jobs
- Jobs are submitted to the executor
- All jobs must be complete when the context manager for the executor exits.

After creating a context manager using ``CoroutineExecutor``, the two
main features are the ``submit()`` method, and the ``map()`` method.

I can't exactly match ``Executor`` interface in the ``concurrent.futures``
package because some functions in this interface need to be ``async`` functions.
But we can get close; certainly close enough that a user with experience
using the ``ThreadPoolExecutor`` or ``ProcessPoolExecutor`` should be able
to figure things out pretty quickly.

Some ideas from Trio's *nurseries* have been used as inspiration:

- The ``CoroutineExecutor`` waits until all submitted jobs are complete.
- If any jobs raise an exception, all other unfinished jobs are cancelled
  (they will have ``CancelledError`` raised inside them), and
  ``CoroutineExecutor`` re-raises that same exception.

Throttling, a.k.a `max_workers`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Even though it is possible to concurrently execute a much larger number
of (IO bound) tasks with asyncio compared to threads or processes, there
will still be an upper limit the machine can handle based on either:

- memory limitations: many task object instances
- CPU limitations: too many concurrent task objects and events for the event loop to process.

Thus, we also have a ``max_workers`` setting to limit concurrency.

However, for *very* large concurrency, we add a second style of ``submit``
method for adding work to the executor: ``submit_queue``.  Here are the
differences:

**``def submit(fn, *args, **kwargs)``**

- This method mimics the ``submit`` method in the stdlib ``concurrent.futures``
  `package <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Executor.submit>`_
- Instead of returning a `concurrent.future.Future <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Future>`_,
  this method returns an `asyncio.Future <https://docs.python.org/3/library/asyncio-future.html?highlight=asyncio%20future#asyncio.Future>`_.
- Returning a future maintains parity with the corresponding method in ``concurrent.futures``, but it also means
  that a task object will have been created for every ``submit()`` call (to provide the ``Future`` result).
- Even though a task is created for every ``submit()`` call, there is an internal
  `asyncio.Semaphore <https://docs.python.org/3/library/asyncio-sync.html?highlight=semaphore#asyncio.Semaphore>`_
  that limits the concurrency of the *submitted jobs*.

The point is this: using ``submit()``, the ``max_workers`` setting will
limit concurrency of the submitted jobs, but
**will create a task for every ``submit()`` call**. For most "normal"
workloads, say up to 50k jobs, this will be fine; but it will cause memory
and/or CPU problems for millions of jobs.

For very large workloads, we have an additional method that does not mimic a corresponding version in
``concurrent.futures``:  ``submit_queue()``.

**``async def submit_queue(fn, *args, **kwargs) -> None``**

- This method does not mimic anything in ``concurrent.futures``, it is new.
- This method **does not return a Future**; thus, it doesn't need to create a task for every submitted job
- Internally, this method uses an `asyncio.Queue <https://docs.python.org/3/library/asyncio-queue.html?highlight=asyncio%20queue#asyncio.Queue>`_
  and a pool of "worker tasks" of size ``max_workers``.
- New ``Task`` objects are never created for submitted jobs: the fixed-size pool of worker tasks simply pull work
  off the internal ``asyncio.Queue`` instance. This dramatically saves memory.
- The size of the internal ``Queue`` is configurable via the ``max_backlog`` parameter of ``CoroutineExecutor``.
- The queue size (``max_backlog``) is important because, by default,
  if shutdown is initiated, say by ``CTRL-C``, then pending
  work already submitted on this queue is **not cancelled**,
  but will continue to be processed until those pending tasks have all
  been completed. Thus, having a smaller backlog will reduce the time it takes
  to shut down, since there will be less work on the queue. Generally
  speaking, a larger queue size will not improve performance, and having a
  smaller backlog limit also means that back-pressure can be propagated
  through the call stack. This is why ``submit_queue()`` is a coroutine
  function.
- Finally, because ``submit_queue()`` does not return a future (or anything),
  it means that obtaining results from jobs requires a different
  strategy. A good option is for the submitted job (coroutine function)
  to send results somewhere itself. For example, it could append to
  a global list, or send a result over a socket, or write out to a
  file, write to a database, and so on.

The ``submit_queue()`` method can be used to successfully process many
millions of tasks. The concurrency setting, ``max_workers``, can be
used to tweak how many submitted jobs are active at any one time, and
here you will need to experiment because the optimal value will depend on
the ratio of IO-bound vs. CPU-bound work being performed in submitted jobs.
If more CPU-bound, then ``max_workers`` should be reduced; if more
IO-bound, then ``max_workers`` can be increased.

As a rough guide, for IO-bound work you can start at ``max_workers=20_000`` and see how that
goes. For a more mixed IO/CPU workload, start at ``max_workers=100``.

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
