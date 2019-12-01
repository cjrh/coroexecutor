import time
import asyncio
import weakref
from concurrent.futures import Executor


class CoroutineExecutor(Executor):
    def __init__(self, timeout=None):
        self.tasks = weakref.WeakSet()
        self._closed = False
        self.timeout = timeout
        self.exe_future = None

    def submit(self, fn, *args, **kwargs) -> asyncio.Future:
        if self._closed:
            raise RuntimeError('Executor is closed.')
        t = asyncio.create_task(fn(*args, **kwargs))
        self.tasks.add(t)
        return t

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
            self._cancel_all_tasks()
            coro = asyncio.gather(*self.tasks, return_exceptions=True)
            if self.timeout:
                await asyncio.wait_for(coro, self.timeout)
            else:
                await coro
            raise
        finally:
            self._closed = True
