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

.. warning::
    This is alpha. Please don't rely on this in a production
    setting yet. I will remove this warning when it is ready.

coroexecutor
============

.. contents::
    :local:
    :depth: 2
    :backlinks: top

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
        async with CoroutineExecutor(max_workers=10) as exe:
            t1 = await exe.submit(f, 0.01, msg="task 1")
            t2 = await exe.submit(f, 0.05, msg="task 2")

        assert t1.done()
        assert t2.done()

    asyncio.run(main())

``max_workers`` controls how many submitted jobs can run concurrently.
These internal workers are lightweight of course, they're just
``asyncio.Task`` instances. Millions of jobs can be pushed through
the executor. As is normal for asyncio, concurrency requires
that these jobs be IO-bound, and the upper bound for setting
``max_workers`` is mainly going to depend on your CPU and RAM resources.

Discussion
----------

The ``CoroutineExecutor`` context manager works very much like
the ``Executor`` implementations in the ``concurrent.futures``
package in the standard library. This is the intention of
this package. The basic components of the interface are:

- The executor applies a context over the creation of jobs
- Jobs are submitted to the executor
- All jobs must be complete when the context manager for the executor exits.

After creating a context manager using ``CoroutineExecutor``, the two
main features are the ``submit()`` method, and the ``map()`` method.

It is impossible to *exactly* match the ``Executor`` interface in the
``concurrent.futures`` package because some functions in this interface
need to be ``async`` functions. But we can get close; certainly close
enough that a user with experience using the ``ThreadPoolExecutor`` or
``ProcessPoolExecutor`` should be able to figure things out pretty quickly.

There is a great deal of complexity that can arise. The "happy path" is
simple. You just submit jobs to the executor, and they will get
executed accordingly. But there are many corner cases:

- asyncio can concurrently execute thousands, or even tens of thousands
  of (IO-bound) jobs concurrently. But how to handle more, say, millions
  of jobs?
- If one job raises an exception, how to terminate all the other jobs?
  In the CTRL-C case, this is desired, but what about other cases? Do
  you always want a single task failure (with an unexpected exception)
  to cancel the entire batch? And is there a difference between
  a job raising ``CancelledError`` versus raising some other kind of
  exception?
- The ``CoroutineExectutor`` provides a context manager API: if
  some code within the body of the context manager (that is not a task)
  raises an exception, should all the submitted tasks also
  be cancelled?

Each of these will be discussed in more detail in the sections
that follow.

Throttling, using ``max_workers``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Even though it is possible to concurrently execute a much larger number
of (IO bound) tasks with asyncio compared to threads or processes, there
will still be an upper limit the machine can handle based on either:

- memory limitations: many task object instances
- CPU limitations: too many concurrent task objects and events for the event loop to process.

Thus, we also have a ``max_workers`` setting to limit concurrency. It might
not be obvious how that limitation is applied, say, in the scenario of
millions of jobs.

The ``CoroutineExecutor.submit()`` is an ``async def`` method. This means
that you will have to await it, like so:

.. code-block:: python3

    import asyncio
    from coroexecutor import CoroutineExecutor

    async def f():
        print('hi!')

    async def main():
        async with CoroutineExecutor(max_workers=10) as exe:
            t1 = await exe.submit(f)

    asyncio.run(main())

If the total number of jobs already submitted is less than ``max_workers``,
the call to ``await exe.submit()`` will return immediately: the job will
begin executing, and ``submit()`` returns an ``asyncio.Task`` instance
for that job. However, if the total number of concurrently-running jobs
is greater than the ``max_workers`` setting, this call will wait until
the number of currently-running jobs drops below the threshold before
adding the new job. This means that ``submit()`` applies *back-pressure*.

Say you have a file containing ten million URLs that you want to fetch
using aiohttp. That program might look something like this:

.. code-block:: python3

    import asyncio, aiohttp
    from coroexecutor import CoroutineExecutor

    async def fetch(url: str):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    print('body:', response.text())  # or whatever
        except Exception:
            print('Problem with url:', url)

    async def main():
        async with CoroutineExecutor(max_workers=10000) as exe:
            for line in open('urls.txt'):
                await exe.submit(fetch, line)

    asyncio.run(main())

Assuming it takes 3 seconds to fetch a single url, this program
should take around 1e7 / 1e4 => 1000 seconds to fetch all of them.
About 17 minutes, since even though there are 10 million urls, we're
doing 10k concurrently. (In practice, some of the endpoints will be
very slow to respond, if they respond at all. So for real code you're
going to want to either use aiohttp facilities for timeouts on the
``.get()``, or wrap the work inside an ``asyncio.wait_for()`` wrapper.)

Note that we're handling errors inside our job function ``fetch()``.
By default, if jobs raise exceptions these will cancel all pending jobs
inside the executor, and shut it down. For long batch jobs, that may
not be what we want, and this is discussed next.

Dealing with errors and cancellation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Generally, there are these kinds of error situations:

- A job is cancelled, and you want the executor to be shut down
- A job is cancelled, and the executor must NOT be shut down
- A job raises an exception (not ``CancelledError``), and
  you want the executor to shut down
- A job raises an exception (not ``CancelledError``), and the
  executor must NOT be shut down

Consider the previous example using aiohttp to fetch URLs: inside
the ``fetch()`` function, we're handling ``Exception``, which
includes ``asyncio.CancelledError``. In general, this is the
correct thing to do because you can control what happens in
each of the scenarios presented above. But what happens
if your code is not supplying the jobs and you don't control
how error handling inside them is being managed? By default,
if any job raises an exception (cancellation or otherwise)
that will initiate "shutdown" of the executor instance, and
all other pending jobs on that executor will be cancelled.

If you have a situation where this is not desired, you can
ask ``CoroutineExecutor`` to ignore all task errors for you:

.. code-block:: python3

    import asyncio, aiohttp
    from coroexecutor import CoroutineExecutor

    async def naive_fetch(url: str):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                print('body:', response.text())  # or whatever

    async def main():
        async with CoroutineExecutor(
                max_workers=10000,
                suppress_task_errors=True,
        ) as exe:
            for line in open('urls.txt'):
                await exe.submit(naive_fetch, line)

    asyncio.run(main())

In this modified example, the job function ``naive_fetch`` has
no error handling. No matter, the ``suppress_task_errors``
parameter will allow the executor to absorb them all. Be careful
with this. I recommend against doing this wherever possible, and
handle exceptions and ``CancelledError`` explicitly within
your job functions instead.

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

    asyncio.run(main())

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
        asyncio.run(main())

    assert results == [0.01, 0.02]

The first two values of the batch finish quickly, and I saved these to the
``results`` list in the outer scope. Then, one of the jobs fails with
an exception. This results in the other pending jobs being cancelled (i.e.,
the "0.2" case in this example), the ``CoroutineExecutor`` instance
re-raising the exception, and in this example, the exception raises all
the way out to the invocation of the ``run()`` function itself. However,
note that we still have the results from jobs that succeeded.

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
