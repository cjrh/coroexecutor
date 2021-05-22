import asyncio
import pytest
from coroexecutor import CoroutineExecutor


async def f(dt, results=None, error=False, evt=None):
    """Utility function for testing various situations."""
    await asyncio.sleep(dt)
    if error:
        raise Exception('oh noes')

    if results is not None:
        results.append(1)

    if evt:
        asyncio.get_running_loop().call_soon(evt.set)


def test_cancel_outer_task():
    tasks = []

    async def outer(evt: asyncio.Event):
        async with CoroutineExecutor(suppress_task_errors=True) as exe:
            t1 = await exe.submit(f, 0.01, evt=evt)
            t2 = await exe.submit(f, 5.0)
            tasks.extend([t1, t2])
            await asyncio.sleep(1.0)

    async def main():
        evt = asyncio.Event()
        t = asyncio.create_task(outer(evt))
        await evt.wait()
        print('cancelling the outer task')
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t

    asyncio.run(main())

    t1, t2 = tasks
    assert t1.done() and not t1.cancelled()
    assert t2.done() and t2.cancelled()


def test_cancel_inner_task():
    tasks = []

    async def outer(evt: asyncio.Event):
        async with CoroutineExecutor(suppress_task_errors=True) as exe:
            t1 = await exe.submit(f, 0.1)
            t2 = await exe.submit(f, 0.1)
            tasks.extend([t1, t2])
            evt.set()
            # This await will *not* be cancelled.
            await asyncio.sleep(0.2)

    async def main():
        evt = asyncio.Event()
        t = asyncio.create_task(outer(evt))
        await asyncio.wait_for(evt.wait(), 5.0)
        print('cancelling task')
        tasks[0].cancel()
        await t

    asyncio.run(main())

    t1, t2 = tasks
    assert t1.done() and t1.cancelled()
    assert t2.done() and not t2.cancelled()
