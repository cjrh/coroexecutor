import time
import asyncio
import weakref
import logging
from concurrent.futures import Executor


logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


class CoroutineExecutor(Executor):
    def __init__(
            self,
            timeout=None,
            max_workers=1000,
            max_backlog=10,
            initializer=None,
            initargs=()
    ):
        self.tasks = weakref.WeakSet()
        self._closed = False
        self.timeout = timeout
        self.exe_future = None

        self._max_workers = max_workers
        self.sem = asyncio.Semaphore(self._max_workers)
        self.initializer = initializer
        self.initargs = initargs

        self.q = asyncio.Queue(maxsize=max_backlog)
        self.pool = [
            asyncio.create_task(
                self.pool_worker()
            ) for i in range(self._max_workers)
        ]

    async def run_task(self, fn, *args, **kwargs):
        async with self.sem:
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

    def submit(self, fn, *args, **kwargs) -> asyncio.Future:
        if self._closed:
            raise RuntimeError('Executor is closed.')

        t = asyncio.create_task(
            self.run_task(fn, *args, **kwargs)
        )
        self.tasks.add(t)
        return t

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
        if self._closed:
            raise RuntimeError('Executor is closed.')

        await self.q.put((fn, args, kwargs))

    async def pool_worker(self):
        while True:
            try:
                item = await self.q.get()
                # print(f'got item {item}')
            except asyncio.CancelledError:
                continue

            if item is None:
                # This is the only way to leave
                # print('Got a none, leaving')
                break

            fn, args, kwargs = item
            try:
                await self.run_task(fn , *args, **kwargs)
                # print(f'ran task for item {item}')
                self.q.task_done()
            except asyncio.CancelledError:
                # Not the right place to quit. The queue must be
                # drained.
                continue
            except Exception:
                logger.exception('task raised an error:')

        # print('leaving pool worker')

    async def map(self, fn, *iterables, timeout=None):
        timeout = timeout or self.timeout
        if timeout is not None:
            end_time = timeout + time.monotonic()

        fs = [self.submit(fn, *args) for args in zip(*iterables)]

        for f in fs:
            if timeout is not None:
                yield await asyncio.wait_for(f, end_time - time.monotonic())
            else:
                yield await f

    async def shutdown(self, wait=True):
        if not wait:
            self._cancel_all_tasks()
        await self.__aexit__(None, None, None)

    def _cancel_all_tasks(self):
        for t in self.tasks:
            t.cancel()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # If an exception was raised in the body of the context manager,
        # need to handle. Cancel all pending tasks, run them to completion
        # and then propagate the exception.
        if exc_type:
            self._closed = True
            self._cancel_all_tasks()

        try:
            # This is the main place at which the tasks are executed, and
            # it covers both cases, of whether there is an active exception
            # (exc_type is not None) as well no exception (normal execution)
            # If there is an existing exception, we want to swallow all
            # exceptions and exit asap. If no exception, then allow any
            # raised exception to terminate the executor.
            while any(not t.done() for t in self.tasks):
                coro = asyncio.gather(*self.tasks, return_exceptions=bool(exc_type))
                if self.timeout:
                    await asyncio.wait_for(coro, self.timeout)
                else:
                    await coro

            # Shut off the pool workers
            # print(f'Shutting off the pool workers {self._max_workers}')
            logger.info('Shutting off the pool workers')
            for i in range(self._max_workers):
                # print(f'will put a none on queue {i}')
                await self.q.put(None)
                # print(f'Put a none on queue {i}')

            # print(self.pool)
            for t in self.pool:
                await t

        except (asyncio.CancelledError, Exception):
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
            self._closed = True
            self._cancel_all_tasks()
            for i in range(self._max_workers):
                await self.q.put(None)

            coro = asyncio.gather(*self.tasks, *self.pool, return_exceptions=True)
            # TODO: this isn't what the timeout setting is intended for
            #  and should probably be removed.
            if self.timeout:
                await asyncio.wait_for(coro, self.timeout)
            else:
                await coro
            raise
        finally:
            self._closed = True
