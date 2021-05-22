from contextlib import suppress
import asyncio
from asyncio import run
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


def test_basic():
    results = []

    async def main():
        async with CoroutineExecutor() as exe:
            t1 = await exe.submit(f, 0.001, results)
            t2 = await exe.submit(f, 0.002, results)

        assert t1.done()
        assert t2.done()

    run(main())
    assert results == [1, 1]


@pytest.mark.parametrize('exc_delay,expected_results', [
    (0.01, []),  # Failing task finishes first
    (0.5, [1]),  # Failing task finishes last
])
def test_exception_cancels_all_tasks(exc_delay, expected_results):
    results = []

    async def main():
        async with CoroutineExecutor() as exe:
            t1 = await exe.submit(f, exc_delay, results, error=True)
            t2 = await exe.submit(f, 0.2, results)

        assert t1.done() and t1.exception() and not t1.cancelled()
        assert t2.done() and not t2.exception() and t2.cancelled()

    with pytest.raises(Exception, match=r'oh noes'):
        run(main())

    assert results == expected_results


def test_no_new_tasks():
    got_to_here = []

    async def main():
        evt = asyncio.Event()

        async with CoroutineExecutor() as exe:
            await exe.submit(f, 0.01, evt=evt)
            await exe.submit(f, 0.05)

        got_to_here.append(1)
        await evt.wait()
        await exe.submit(f, 0.02)

    with pytest.raises(RuntimeError):
        run(main())

    assert got_to_here


def test_reraise_unhandled():
    tasks = []

    async def main():
        evt = asyncio.Event()
        async with CoroutineExecutor() as exe:
            t1 = await exe.submit(f, 0.01, evt=evt)
            t2 = await exe.submit(f, 1.0)
            tasks.extend([t1, t2])
            await evt.wait()
            raise Exception('oh noes')

    with pytest.raises(Exception, match=r'oh noes'):
        run(main())

    t1, t2 = tasks
    assert t1.done() and not t1.cancelled()
    assert t2.done() and t2.cancelled()


def test_reraise_unhandled_nested():
    tasks = []

    async def main():
        evt = asyncio.Event()
        async with CoroutineExecutor():
            async with CoroutineExecutor():
                async with CoroutineExecutor() as exe3:
                    t1 = await exe3.submit(f, 0.01, evt=evt)
                    t2 = await exe3.submit(f, 0.50)
                    tasks.extend([t1, t2])
                    await evt.wait()
                    raise Exception('oh noes')

    with pytest.raises(Exception, match=r'oh noes'):
        run(main())

    t1, t2 = tasks
    assert t1.done() and not t1.cancelled()
    assert t2.done() and t2.cancelled()


def test_reraise_unhandled_nested2():
    tasks = []

    async def main():
        evt1 = asyncio.Event()
        evt2 = asyncio.Event()
        async with CoroutineExecutor() as exe1:
            t3 = await exe1.submit(f, 0.01, evt=evt1)
            t4 = await exe1.submit(f, 1.0)
            async with CoroutineExecutor() as exe2:
                t1 = await exe2.submit(f, 0.01, evt=evt2)
                t2 = await exe2.submit(f, 1.0)
                tasks.extend([t1, t2, t3, t4])
                await evt1.wait()
                await evt2.wait()
                raise Exception('oh noes')

    with pytest.raises(Exception, match=r'oh noes'):
        run(main())

    t1, t2, t3, t4 = tasks
    assert t1.done() and not t1.cancelled()
    assert t2.done() and t2.cancelled()

    assert t3.done() and not t3.cancelled()
    assert t4.done() and t4.cancelled()


def test_cancel_outer_task():
    tasks = []

    async def outer(evt: asyncio.Event):
        async with CoroutineExecutor() as exe:
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

    run(main())

    t1, t2 = tasks
    assert t1.done() and not t1.cancelled()
    assert t2.done() and t2.cancelled()


def test_cancel_outer_task_inside_aexit():
    tasks = []

    async def outer(evt: asyncio.Event):
        async with CoroutineExecutor() as exe:
            t1 = await exe.submit(f, 0.01, evt=evt)
            t2 = await exe.submit(f, 5.0)
            tasks.extend([t1, t2])

    async def main():
        evt = asyncio.Event()
        t = asyncio.create_task(outer(evt))
        await evt.wait()
        print('cancelling the outer task')
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t

    run(main())

    t1, t2 = tasks
    assert t1.done() and not t1.cancelled()
    assert t2.done() and t2.cancelled()


def test_cancel_inner_task():
    tasks = []

    async def outer(evt: asyncio.Event):
        async with CoroutineExecutor() as exe:
            t1 = await exe.submit(f, 4.0)
            t2 = await exe.submit(f, 4.0)
            tasks.extend([t1, t2])
            evt.set()
            # This await will *not* be cancelled.
            await asyncio.sleep(0.1)

    async def main():
        evt = asyncio.Event()
        t = asyncio.create_task(outer(evt))
        await asyncio.wait_for(evt.wait(), 5.0)
        print('cancelling task')
        tasks[0].cancel()
        with pytest.raises(asyncio.CancelledError):
            await t

    run(main())

    t1, t2 = tasks
    assert t1.done() and t1.cancelled()
    assert t2.done() and t2.cancelled()


def test_map():
    times = [0.01, 0.02, 0.03]

    async def f(dt):
        await asyncio.sleep(dt)
        return dt

    async def main():
        async with CoroutineExecutor() as exe:
            results = exe.map(f, times)
            assert [v async for v in results] == times

    run(main())


def test_map_error():
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

    assert results == times[:2]


def test_pass_executor_around():
    tasks = []

    async def g(dt):
        await asyncio.sleep(dt)
        return dt

    async def f(dt, executor: CoroutineExecutor):
        t = await executor.submit(g, dt + 0.02)
        tasks.append(t)
        await asyncio.sleep(dt)
        return dt

    async def main():
        async with CoroutineExecutor() as exe:
            tasks.append(await exe.submit(f, 0.01, exe))
            tasks.append(await exe.submit(f, 0.02, exe))

    run(main())

    assert all(t.done() and not t.cancelled() for t in tasks)


def test_pass_randoms():
    from random import random

    returned = []

    async def f(dt):
        await asyncio.sleep(dt)
        returned.append(dt)

    async def producer1(executor: CoroutineExecutor):
        await executor.submit(f, random())
        await executor.submit(f, random())
        await executor.submit(f, random())

    async def producer2(executor: CoroutineExecutor):
        await executor.submit(f, random())
        await executor.submit(f, random())
        await executor.submit(f, random())

    async def main():
        async with CoroutineExecutor() as executor:
            await executor.submit(f, random())
            await executor.submit(f, random())
            await executor.submit(f, random())

            await executor.submit(producer1, executor)
            await executor.submit(producer2, executor)

    run(main())
    assert len(returned) == 9


def test_shutdown():
    results = []

    async def f(dt):
        await asyncio.sleep(dt)
        results.append(1)

    async def main():
        exe = CoroutineExecutor()
        t1 = await exe.submit(f, 0.01)
        t2 = await exe.submit(f, 0.05)
        await exe.shutdown(wait=True)  # default

        assert t1.done() and not t1.cancelled()
        assert t2.done() and not t2.cancelled()

    run(main())
    assert results == [1, 1]


@pytest.mark.parametrize(
    'with_interruption,sleep_time,t1cancelled,t2cancelled,raises', [
        (False, 0, True, True, True),
        (False, 0.01, True, True, True),
        # The tasks do not get cancelled, but `run()` raises CancelledError??
        # It seems like with sleep(0) the task is created, but does not start
        # executing the coroutine yet?
        (True, 0, False, False, False),
        # The tasks do not get cancelled, and no CancelledError is raised.
        (True, 0.01, False, False, False),
    ]
)
def test_shutdown_nowait(
        with_interruption,
        sleep_time,
        t1cancelled,
        t2cancelled,
        raises
):
    results = []

    async def f(dt):
        with suppress(asyncio.CancelledError):
            print('beginning sleep')
            await asyncio.sleep(dt)
            print('ending sleep')
            results.append(1)
        print('leaving f')

    async def main():
        exe = CoroutineExecutor()
        t1 = await exe.submit(f, 0.5)
        t2 = await exe.submit(f, 0.9)
        if with_interruption:
            await asyncio.sleep(sleep_time)
        await exe.shutdown(wait=False)

        assert t1.done() and (t1.cancelled() == t1cancelled)
        assert t2.done() and (t2.cancelled() == t2cancelled)

    if raises:
        with pytest.raises(asyncio.CancelledError):
            run(main())
    else:
        run(main())

    assert results == []
