from contextlib import suppress
import asyncio
from asyncio import run
import pytest
from coroexecutor import CoroutineExecutor


def test_basic():
    results = []

    async def f(dt):
        await asyncio.sleep(dt)
        results.append(1)

    async def main():
        async with CoroutineExecutor() as exe:
            t1 = await exe.submit(f, 0.01)
            t2 = await exe.submit(f, 0.05)

        assert t1.done()
        assert t2.done()
        assert not exe.tasks or all(t.done() for t in list(exe.tasks))

    run(main())
    assert results == [1, 1]


@pytest.mark.parametrize('exc_delay,expected_results', [
    (0.01, []),  # Failing task finishes first
    (0.5, [1]),  # Failing task finishes last
])
def test_exception_cancels_all_tasks(exc_delay, expected_results):
    results = []

    async def f(dt, error=False):
        await asyncio.sleep(dt)
        if error:
            raise Exception('oh noes')
        results.append(1)

    async def main():
        async with CoroutineExecutor() as exe:
            t1 = await exe.submit(f, exc_delay, error=True)
            t2 = await exe.submit(f, 0.2)

        assert t1.done()
        assert t2.done()
        assert not exe.tasks or all(t.done() for t in list(exe.tasks))

    with pytest.raises(Exception, match=r'oh noes'):
        run(main())

    assert results == expected_results


def test_no_new_tasks():
    got_to_here = []

    async def f(dt):
        await asyncio.sleep(dt)

    async def main():
        async with CoroutineExecutor() as exe:
            await exe.submit(f, 0.01)
            await exe.submit(f, 0.05)

        got_to_here.append(1)
        await exe.submit(f, 0.02)

    with pytest.raises(RuntimeError):
        run(main())

    assert got_to_here


def test_reraise_unhandled():
    tasks = []

    async def f(dt):
        await asyncio.sleep(dt)

    async def main():
        async with CoroutineExecutor() as exe:
            t1 = await exe.submit(f, 0.01)
            t2 = await exe.submit(f, 0.10)
            tasks.extend([t1, t2])
            await asyncio.sleep(0.05)
            raise Exception('oh noes')

    with pytest.raises(Exception, match=r'oh noes'):
        run(main())

    t1, t2 = tasks
    assert t1.done() and not t1.cancelled()
    assert t2.done() and t2.cancelled()


def test_reraise_unhandled_nested():
    tasks = []

    async def f(dt):
        await asyncio.sleep(dt)

    async def main():
        async with CoroutineExecutor():
            async with CoroutineExecutor():
                async with CoroutineExecutor() as exe3:
                    t1 = await exe3.submit(f, 0.01)
                    t2 = await exe3.submit(f, 0.50)
                    tasks.extend([t1, t2])
                    await asyncio.sleep(0.1)
                    raise Exception('oh noes')

    with pytest.raises(Exception, match=r'oh noes'):
        run(main())

    t1, t2 = tasks
    assert t1.done() and not t1.cancelled()
    assert t2.done() and t2.cancelled()


def test_reraise_unhandled_nested2():
    tasks = []

    async def f(dt):
        await asyncio.sleep(dt)

    async def main():
        async with CoroutineExecutor() as exe1:
            t3 = await exe1.submit(f, 0.01)
            t4 = await exe1.submit(f, 0.50)
            async with CoroutineExecutor() as exe2:
                t1 = await exe2.submit(f, 0.01)
                t2 = await exe2.submit(f, 0.50)
                tasks.extend([t1, t2, t3, t4])
                await asyncio.sleep(0.1)
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

    async def f(dt):
        await asyncio.sleep(dt)

    async def outer():
        async with CoroutineExecutor() as exe:
            t1 = await exe.submit(f, 0.01)
            t2 = await exe.submit(f, 0.20)
            tasks.extend([t1, t2])

    async def main():
        t = asyncio.create_task(outer())
        await asyncio.sleep(0.1)
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t

    run(main())

    t1, t2 = tasks
    assert t1.done() and not t1.cancelled()
    assert t2.done() and t2.cancelled()


def test_cancel_inner_task():
    tasks = []

    async def f(dt):
        await asyncio.sleep(dt)

    async def outer():
        async with CoroutineExecutor() as exe:
            t1 = await exe.submit(f, 1.0)
            t2 = await exe.submit(f, 1.0)
            tasks.extend([t1, t2])

    async def main():
        t = asyncio.create_task(outer())
        await asyncio.sleep(0.1)
        t1, t2 = tasks
        t1.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t

    run(main())

    t1, t2 = tasks
    assert t1.done() and t1.cancelled()
    assert t2.done() and t2.cancelled()


def test_timeout():
    tasks = []

    async def f(dt):
        await asyncio.sleep(dt)

    async def main():
        async with CoroutineExecutor(timeout=0.05) as exe:
            t1 = await exe.submit(f, 0.01)
            t2 = await exe.submit(f, 5)
            tasks.extend([t1, t2])

    with pytest.raises(asyncio.TimeoutError):
        run(main())

    t1, t2 = tasks
    assert t1.done() and not t1.cancelled()
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


def test_map_timeout():
    times = [0.01, 0.02, 0.2, 0.3]
    results = []

    async def f(dt):
        await asyncio.sleep(dt)
        return dt

    async def main():
        async with CoroutineExecutor() as exe:
            async for r in exe.map(f, times, timeout=0.1):
                results.append(r)

    with pytest.raises(asyncio.TimeoutError):
        run(main())

    assert results == times[:2]


def test_map_outer_timeout():
    times = [0.01, 0.02, 0.1, 0.2]
    results = []

    async def f(dt):
        await asyncio.sleep(dt)
        return dt

    async def main():
        async with CoroutineExecutor(timeout=0.05) as exe:
            async for r in exe.map(f, times):
                results.append(r)

    with pytest.raises(asyncio.TimeoutError):
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
        async with CoroutineExecutor(timeout=0.1) as exe:
            tasks.append(await exe.submit(f, 0.01, exe))
            tasks.append(await exe.submit(f, 0.02, exe))

    run(main())

    assert all(t.done() and not t.cancelled() for t in tasks)


@pytest.mark.parametrize('timeout', [
    None,
    0.05,
])
def test_pass_randoms(timeout):
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
        async with CoroutineExecutor(timeout=timeout) as executor:
            await executor.submit(f, random())
            await executor.submit(f, random())
            await executor.submit(f, random())

            await executor.submit(producer1, executor)
            await executor.submit(producer2, executor)

    if timeout is not None:
        with pytest.raises(asyncio.TimeoutError):
            run(main())
    else:
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
        (False, 0, True, True, False),
        (False, 0.01, True, True, False),
        # The tasks do not get cancelled, but `run()` raises CancelledError??
        # It seems like with sleep(0) the task is created, but does not start
        # executing the coroutine yet?
        (True, 0, False, False, True),
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
