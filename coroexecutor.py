from __future__ import annotations
import asyncio
from asyncio import Future, Task
from weakref import WeakKeyDictionary
import logging
from concurrent.futures import Executor


__all__ = ['CoroutineExecutor']
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


class CoroutineExecutor(Executor):
    def __init__(
            self,
            max_workers: int = 1000,
            max_backlog: int = 10,
            initializer=None,
            initargs=()
    ):
        """
        This is intended to be used as a context manager.

        :param max_workers: The number of concurrent tasks to allow. For
            IO-bound workloads, this may be large. For example, a value of
            10k might be fine. If you need to do more CPU-bound work, the
            number of concurrent tasks must be reduced. You will have to
            experiment to see what is tolerable. It depends on your specific
            workload.
        :param max_backlog: The number of jobs to *accept* before applying
            backpressure. What does that mean? The ``submit`` method is
            an ``async def`` function which you must ``await`` to use. Before
            the ``max_backlog`` is reached, submitted jobs will queue up
            in an internal queue (and obviously some of them will start
            being executed as running tasks, up to ``max_workers``). The
            size of that "pending" queue is ``max_backlog``. When that
            queue is full, the ``await executor.submit(...)`` call will
            wait until there is capacity in the queue. In practice, there's
            likely little value in having a large ``max_backlog``. Your
            program will probably work fine with a ``max_backlog`` of 1.
        :param initializer: The initializer is a callable that will be
            called immediately before each task is executed. The initializer
            can be an ``async def`` function or a normal sync ``def``
            function. No idea what you would use this for in this async
            executor. It makes more sense in a multiprocess executor because
            you might want to initialize the task process somehow.
            Nevertheless I've retained it for similarity to executors in
            ``concurrent.futures``.
        :param initargs: These will be passed to the initializer function
            if supplied, like this: ``await initializer(*initargs)``
        """
        self.subfut: WeakKeyDictionary[Future, Task] = WeakKeyDictionary()
        self._closed = False
        self._max_workers = max_workers
        self.initializer = initializer
        self.initargs = initargs

        self.q = asyncio.Queue(maxsize=max_backlog)
        # Long-running tasks for processing jobs.
        self.pool = [
            asyncio.create_task(
                self.pool_worker()
            ) for _ in range(self._max_workers)
        ]

    async def run_task(self, fn, *args, **kwargs):
        if self.initializer is not None:
            if asyncio.iscoroutinefunction(self.initializer):
                await self.initializer(*self.initargs)
            elif callable(self.initializer):
                self.initializer(*self.initargs)
            else:
                raise TypeError(
                    "Initializer type is unknown. It should be either a"
                    "coroutine function or a regular function."
                )

        return await fn(*args, **kwargs)

    async def submit(self, fn, *args, **kwargs) -> asyncio.Future:
        """Submit a job. The job will be called as

        .. code-block:: python

            fn(*args, **kwargs)

        This returns an ``asyncio.Future``, which can be used
        to retrieve a result, or cancel the job, or generally
        check the status of the job.

        The number of concurrently-running jobs will be limited
        by the ``max_workers`` parameter of ``CoroutineExecutor``.
        However, the number of *submitted* jobs is limited by
        the ``max_backlog`` parameter of ``CoroutineExecutor``.
        """
        if self._closed:
            raise RuntimeError('Executor is closed.')

        # We'll return the future which can be used to obtain
        # results, or for the called to cancel, etc. This
        # is analogous to how the ``submit`` function works
        # in ``concurrent.futures`` executors.
        f = asyncio.get_running_loop().create_future()
        # However, we also put submitted jobs on a queue, and
        # here ``await`` is used. This exerts backpressure
        # to prevent too many jobs being scheduled
        # concurrently. The queue size limit is independent
        # of the ``max_workers``. This queue is really
        # a backlog, not a concurrency limit.
        await self.q.put((fn, args, kwargs, f))
        # Record the future in the list, but no task created yet
        self.subfut[f] = None
        return f

    async def submit_queue(self, fn, *args, **kwargs) -> None:
        """ This is an optimization that avoids the creation of
        all task objects upfront, which is what happens in `submit`.
        The `submit` method works that way to mimic the corresponding
        method in the stdlib `concurrent.futures` module. It works
        well, but for very large workloads it is more (memory) efficient
        to control the number of concurrent task objects.

        This method does not return results, and cannot since we
        do not create task objects here. If you need results from
        the submitted coroutines, you should have the coroutine
        itself write the result somewhere.

        This is an async function so that the queue can exert
        backpressure on the caller.
        """
        if self._closed:  # pragma: no cover
            raise RuntimeError('Executor is closed.')

        await self.q.put((fn, args, kwargs))

    async def pool_worker(self):
        while True:
            try:
                item = await self.q.get()
            except asyncio.CancelledError:
                continue

            if item is None:
                # This is the only way to leave
                break

            fn, args, kwargs, *extra = item
            try:
                coro = self.run_task(fn , *args, **kwargs)
                if extra:
                    # The caller wants handles, so we'll create a
                    # task here. The main reason is just so that
                    # we can cancel the task if the caller calls
                    # ``.cancel`` on the future.
                    # t = asyncio.create_task(coro)
                    f: asyncio.Future = extra[0]
                    if f.done():
                        # Might already have been cancelled
                        del self.subfut[f]
                        continue

                    t = asyncio.create_task(coro)
                    self.subfut[f] = t

                    def done_callback(f: asyncio.Future):
                        """ This is called if the submitter of the
                        job cancels it."""
                        if f.cancelled() and not t.cancelled():
                            t.cancel()
                            # coro.throw(asyncio.CancelledError())
                        f.remove_done_callback(done_callback)
                        if f in self.subfut:
                            del self.subfut[f]

                    f.add_done_callback(done_callback)

                    try:
                        result = await t
                        # result = await coro
                    except asyncio.CancelledError:
                        f.cancel()
                        raise
                    except Exception as e:
                        f.set_exception(e)
                        raise
                    else:
                        f.set_result(result)
                    finally:
                        del self.subfut[f]
                else:
                    await coro

                self.q.task_done()
            except asyncio.CancelledError:
                # Not the right place to quit. The queue must be
                # drained.
                continue
            except Exception:
                logger.exception('task raised an error:')

    async def map(self, fn, *iterables):
        fs = [await self.submit(fn, *args) for args in zip(*iterables)]
        for f in fs:
            yield await f

    async def shutdown(self, wait=True):
        """Shut down the executor.

        If wait=True, shutdown will only proceed when all the currently
        active tasks are complete. If wait=False, all currently active
        tasks will be cancelled.
        """
        self._closed = True
        if not wait:
            self._cancel_all_tasks()
        await self.__aexit__(None, None, None)

    def _cancel_all_tasks(self):
        for f, t in self.subfut.items():
            if t:
                t.cancel()
            else:
                f.cancel()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            return await self.handle_exit(exc_type, exc_val, exc_tb)
        except Exception:
            logger.exception('Unexpected error in __aexit__:')
            raise

    async def handle_exit(self, exc_type, exc_val, exc_tb):
        # If an exception was raised in the body of the context manager,
        # need to handle. Cancel all pending tasks, run them to completion
        # and then propagate the exception.
        swallow = False
        if exc_type:
            self._closed = True
            self._cancel_all_tasks()
            swallow = True

        try:
            try:
                await self.wait_for_completion(swallow=swallow)
            except Exception:
                logger.exception('error during main wait')
                raise

            await self.shut_down_pool()
        except (asyncio.CancelledError, Exception):
            self._closed = True
            logger.exception('Exception raise in handle_exit wait')
            # Two ways to get here:
            #
            #   1. The task inside which the context manager is being used,
            #      is itself cancelled. This could result in the gather call,
            #      above, being interrupted. NOTE that the tasks there were
            #      being gathered were not cancelled.
            #   2. One of the tasks raises an exception.
            #   3. Timeout is triggered
            #
            # In either case, we want to bubble the exception to the caller,
            # but only after cancelling the existing tasks.
            self._cancel_all_tasks()

            # for i in range(self._max_workers):
            #     await self.q.put(None)

            try:
                await self.wait_for_completion(swallow=True)
            except Exception:  # pragma: no cover
                logger.exception('Unexpected error waiting during handler')

            try:
                await self.shut_down_pool()
            except Exception:  # pragma: no cover
                logger.exception('Unexpected error waiting pool during handler')

            raise
        finally:
            self._closed = True

    async def wait_for_completion(self, swallow=False):
        # This is the main place at which the tasks are executed, and
        # it covers both cases, of whether there is an active exception
        # (exc_type is not None) as well no exception (normal execution)
        # If there is an existing exception, we want to swallow all
        # exceptions and exit asap. If no exception, then allow any
        # raised exception to terminate the executor.

        # TODO: we might be able to use an asyncio.Event.  All this function
        #  needs to do is wait for the first exception or cancellation, and
        #  then cancel all other pending work. And then either this or
        #  the outer call (__aexit__) needs to wait for the pool workers to
        #  finish up.
        #  - if CancelledError is raised in __aexit__, put Nones in the queue
        #    and wait for pool workers.
        #  - If CancelledError is raise in a task, that counts as a
        #    "shut everything down" condition.
        #  - If an exception is raised in a task, that counts too.
        #  - If a future is cancelled, that also counts. (but should it?)
        #  We also need to decide properly where membership in self.subfut
        #  is managed.  And also whether it really should be a weakref? We
        #  could easily remove futures when they're done.

        to_raise = None
        pending_futures = [fut for fut in self.subfut]

        while pending_futures:
            try:
                done, pending = await asyncio.wait(
                    pending_futures,
                    return_when=asyncio.FIRST_COMPLETED
                )
            except asyncio.CancelledError as e:
                self._cancel_all_tasks()
                to_raise = e
            else:
                if not to_raise:
                    for f in done:
                        if f.cancelled() or f.exception():
                            for p in pending:
                                if self.subfut.get(p):
                                    # A task has already been created for
                                    # this job - cancel the task.
                                    self.subfut[p].cancel()
                                else:
                                    # No task is yet created, we have only
                                    # the future - cancel the future
                                    p.cancel()

                        if f.cancelled():
                            to_raise = asyncio.CancelledError()
                        elif f.exception():
                            to_raise = f.exception()

            pending_futures = [fut for fut in self.subfut]

        if to_raise and not swallow:
            raise to_raise

    async def shut_down_pool(self):
        logger.info('Shutting off the pool workers')
        for i in range(self._max_workers):
            await self.q.put(None)

        for t in self.pool:
            await t
