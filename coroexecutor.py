from __future__ import annotations
import asyncio
from asyncio import Future, Task
from weakref import WeakKeyDictionary, WeakSet
import logging
from concurrent.futures import Executor


__all__ = ['CoroutineExecutor']
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


class CoroutineExecutor(Executor):
    def __init__(
            self,
            max_workers: int = 1000,
            suppress_task_errors=False,
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
        self.shutting_down = asyncio.Event()
        self._max_workers = max_workers
        self._absorb_task_exceptions = suppress_task_errors
        self.initializer = initializer
        self.initargs = initargs
        self.running_tasks: WeakSet[asyncio.Task] = WeakSet()
        self.tokens = asyncio.Queue()
        for i in range(self._max_workers):
            self.tokens.put_nowait(None)

    def initiate_shutdown(self):
        self._closed = True
        self.shutting_down.set()
        self._cancel_all_tasks()

    async def run_task(self, job, token):
        try:
            fn, args, kwargs = job
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
        finally:
            # Return our token back to the pool of tokens.
            self.tokens.put_nowait(token)

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

        # This line will block until a token is available. This
        # applies backpressure to the caller.
        token = await self.tokens.get()
        t = asyncio.create_task(
            self.run_task((fn, args, kwargs), token)
        )
        self.running_tasks.add(t)
        return t

    async def map(self, fn, *iterables):
        """
        Async generator to map values into an async callable.

        .. code-block:: python

            async def job(item):
                ...

            async for result in map(job, items):
                print(f"Result: {result}")

        """
        # This can be accomplished with the following simple
        # commented-out code.
        #
        # However, there is a problem: we still accumulate a
        # potentially large list.

        # fs = [await self.submit(fn, *args) for args in zip(*iterables)]
        # for f in fs:
        #     yield await f

        # Instead, the following much more complicated code
        # can carefully do the same, but without the large
        # list. The key is decoupling the backpressure of
        # the `submit` method, from the provision of the
        # results to the caller.

        q = asyncio.Queue(maxsize=self._max_workers)

        async def submitter():
            for args in zip(*iterables):
                fut = await self.submit(fn, *args)
                await q.put(fut)
            await q.put(None)

        t = asyncio.create_task(submitter())

        while True:
            fut = await q.get()
            if fut is None:
                break

            yield await fut

        await t

    async def shutdown(self, wait=True):
        """Shut down the executor.

        If wait=True, shutdown will only proceed when all the currently
        active tasks are complete. If wait=False, all currently active
        tasks will be cancelled.
        """
        self._closed = True
        if not wait:
            self.initiate_shutdown()
        await self.__aexit__(None, None, None)

    def _cancel_all_tasks(self):
        for t in self.running_tasks:
            t.cancel()

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
        to_raise = None
        if exc_type:
            self.initiate_shutdown()

        while self.running_tasks:
            try:
                # Any tasks that have been cancelled before we call `wait`,
                # will not trigger the FIRST_EXCEPTION return requirement.
                # So we eagerly check for those here, and remove as
                # necessary.
                for t in self.running_tasks:  # type: asyncio.Task
                    if t.cancelled():
                        if not self._absorb_task_exceptions:
                            self.initiate_shutdown()

                # Wait until we receive the first exception out of all
                # the pending jobs.
                done, pending = await asyncio.wait(
                    self.running_tasks,
                    return_when=asyncio.FIRST_EXCEPTION
                )
                # Check to see if any of the completed jobs raised an
                # exception. If so, and depending on whether we need to
                # ignore them or not, we might initiate shutdown.
                for t in done:
                    if t.cancelled() or t.exception():
                        if self._absorb_task_exceptions:
                            pass
                        else:
                            self.initiate_shutdown()

                            # If an existing exception is not already causing
                            # shutting down, this task's error status will
                            # be the one. Note that we will not capture
                            # any other exceptions that occur after this
                            # one.
                            if not to_raise:
                                if t.cancelled():
                                    to_raise = asyncio.CancelledError()
                                elif t.exception():
                                    to_raise = t.exception()

                    # Make sure to remove completed tasks from the list
                    # of running tasks
                    self.running_tasks.discard(t)

            except asyncio.CancelledError:
                if not self._absorb_task_exceptions:
                    logger.exception('CoroutineExecutor was cancelled:')
                    self.initiate_shutdown()

        # If we reached here, the executor is definitely closed.
        self._closed = True

        # If an exception inside the body of the executor context manager
        # was the cause of completion, just return.
        if exc_type:
            return

        # Otherwise, if some other error caused us to shut down, and we
        # recorded that exception, then raise that.
        if to_raise:
            raise to_raise
