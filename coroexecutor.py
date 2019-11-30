import asyncio
import weakref
from concurrent.futures import Executor


class CoroutineExecutor(Executor):
    def __init__(self):
        self.tasks = weakref.WeakSet()

    def submit(self, fn, *args, **kwargs):
        t = asyncio.create_task(fn(*args, **kwargs))
        self.tasks.add(t)

    async def ashutdown(self, wait=True):
        coro = self.__aexit__()
        if wait:
            await coro

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # If an exception was raised in the body of the context manager,
        # need to handle. Cancel all pending tasks, run them to completion
        # and then propagate the exception.
        if exc_type:
            print("__aexit__ got exception!")
            for t in self.tasks:
                t.cancel()

                del t  # Allow weakref to clean up

        try:
            # This is the main place at which the tasks are executed, and
            # it covers both cases, of whether there is an active exception
            # (exc_type is not None) as well no exception (normal execution)
            # If there is an existing exception, we want to swallow all
            # exceptions and exit asap. If no exception, then allow any
            # raised exception to terminate the executor.
            await asyncio.gather(*self.tasks, return_exceptions=bool(exc_type))
        except (asyncio.CancelledError, Exception):
            # Two ways to get here:
            #
            #   1. The task inside which the context manager is being used,
            #      is itself cancelled. This could result in the gather call,
            #      above, being interruped. NOTE that the tasks there were
            #      being gathered were not cancelled.
            #   2. One of the tasks raises an exception.
            #
            # In either case, we want to bubble the exception to the caller,
            # but only after cancelling the existing tasks.
            for t in self.tasks:
                t.cancel()

                del t  # Allow weakref to clean up

            await asyncio.gather(*self.tasks, return_exceptions=True)
            raise
