from __future__ import annotations
import time
import asyncio
from asyncio import Future, Task
import weakref
from weakref import WeakSet, WeakKeyDictionary
import logging
import typing
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
        self.tasks: WeakSet[Task] = WeakSet()
        self.submission_futures: WeakSet[Future] = WeakSet()

        self.subfut: WeakKeyDictionary[Future, Task] = WeakKeyDictionary()

        self._closed = False
        self.timeout = timeout
        self.exe_future = None

        self._max_workers = max_workers
        self.initializer = initializer
        self.initargs = initargs

        self.q = asyncio.Queue(maxsize=max_backlog)
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
        # If the job was successfully submitted, we can
        # return the future.
        self.submission_futures.add(f)
        # No task created yet
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
                        continue

                    t = asyncio.create_task(coro)
                    self.tasks.add(t)
                    self.subfut[f] = t

                    def done_callback(f: asyncio.Future):
                        """ This is called if the submitter of the
                        job cancels it."""
                        if f.cancelled():
                            t.cancel()
                            # coro.throw(asyncio.CancelledError())
                        f.remove_done_callback(done_callback)

                    f.add_done_callback(done_callback)

                    try:
                        result = await t
                        # result = await coro
                    except asyncio.CancelledError:
                        print('cancellederror')
                        f.cancel()
                        raise
                    except Exception as e:
                        print('general exception')
                        f.set_exception(e)
                        raise
                    else:
                        # print('setting result')
                        f.set_result(result)
                    finally:
                        self.submission_futures.discard(f)
                        del self.subfut[f]
                        self.tasks.discard(t)
                else:
                    await coro

                self.q.task_done()
            except asyncio.CancelledError:
                # Not the right place to quit. The queue must be
                # drained.
                continue
            except Exception:
                logger.exception('task raised an error:')

    async def map(self, fn, *iterables, timeout=None):
        timeout = timeout or self.timeout
        if timeout is not None:
            end_time = timeout + time.monotonic()

        fs = [await self.submit(fn, *args) for args in zip(*iterables)]

        for f in fs:
            if timeout is not None:
                yield await asyncio.wait_for(f, end_time - time.monotonic())
            else:
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
        print(self.tasks, self.submission_futures)
        for f, t in self.subfut.items():
            if t:
                print('cancelling the task, not future')
                t.cancel()
            else:
                print('cancelling the future, not task')
                f.cancel()

        # for t in self.tasks:
        #     print('cancelling task')
        #     # If the task (actually coro) allows the CancelledError to
        #     # be raised out of it, then the outer future will also be
        #     # cancelled. Otherwise it won't!
        #     t.cancel()

        # for f in self.submission_futures:
        #     f.cancel()

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
        if exc_type:
            print('Exception raise in CM body')
            self._closed = True
            self._cancel_all_tasks()

        try:
            await self.wait_for_completion(swallow_exceptions=bool(exc_type))
            await self.shut_down_pool()

            # # Shut off the pool workers
            # # print(f'Shutting off the pool workers {self._max_workers}')
            # logger.info('Shutting off the pool workers')
            # for i in range(self._max_workers):
            #     await self.q.put(None)
            #
            # for t in self.pool:
            #     await t

        except (asyncio.CancelledError, Exception):
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

    async def wait_for_completion(self, swallow_exceptions=False):
        # This is the main place at which the tasks are executed, and
        # it covers both cases, of whether there is an active exception
        # (exc_type is not None) as well no exception (normal execution)
        # If there is an existing exception, we want to swallow all
        # exceptions and exit asap. If no exception, then allow any
        # raised exception to terminate the executor.
        pending_tasks = [t for t in self.tasks if not t.done()]
        pending_futures = [f for f in self.submission_futures if not f.done()]
        while any(pending_tasks) or any(pending_futures):
            coro = asyncio.gather(*self.tasks, *self.submission_futures, return_exceptions=swallow_exceptions)
            if self.timeout:
                await asyncio.wait_for(coro, self.timeout)
            else:
                await coro

            pending_tasks = [t for t in self.tasks if not t.done()]
            pending_futures = [f for f in self.submission_futures if not f.done()]

    async def shut_down_pool(self):
        # Shut off the pool workers
        # print(f'Shutting off the pool workers {self._max_workers}')
        logger.info('Shutting off the pool workers')
        for i in range(self._max_workers):
            await self.q.put(None)

        for t in self.pool:
            await t
