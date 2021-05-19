import random
import sys
from asyncio import sleep, run
from contextlib import contextmanager
import time

import pytest

from coroexecutor import CoroutineExecutor


@contextmanager
def elapsed():
    t0 = time.monotonic()
    yield lambda: t1 - t0
    t1 = time.monotonic()


async def job(i):
    await sleep(i)
    return i


items = [random.randint(4, 6) / 1000 for i in range(1000)]
random.shuffle(items)


def test_one_worker_serial():

    async def main():
        kwargs = dict(max_workers=1)
        with elapsed() as f:
            async with CoroutineExecutor(**kwargs) as exe:
                tasks = [await exe.submit(job, item) for item in items]

        assert all(t.done() for t in tasks)
        assert [t.result() for t in tasks] == items
        # Elapsed time is greater than the sum of each individual
        # time.
        print(f(), sum(items))
        assert f() > sum(items)

    run(main())


@pytest.mark.skipif(
    sys.platform == 'darwin', reason='too low concurrency value')
def test_one_worker_concurrent():

    async def main():
        kwargs = dict(max_workers=10)
        with elapsed() as f:
            async with CoroutineExecutor(**kwargs) as exe:
                tasks = [await exe.submit(job, item) for item in items]

        assert all(t.done() for t in tasks)
        assert [t.result() for t in tasks] == items
        # Speedup is roughly 10 times
        concurrency = sum(items) / f()
        print(f(), sum(items), concurrency)
        assert concurrency > 7

    run(main())


@pytest.mark.parametrize('sleep_time', [
    0,
    0.01
])
@pytest.mark.parametrize('n,w', [
    (100, 1),
    (1000, 50),

    (10000, 50),
    (10000, 500),
    (10000, 5000),
])
def test_many_workers(n, w, sleep_time):

    async def job():
        await sleep(sleep_time)
        return 123

    async def main():
        kwargs = dict(max_workers=w)
        with elapsed():
            async with CoroutineExecutor(**kwargs) as exe:
                tasks = [await exe.submit(job) for i in range(n)]

        assert all(t.done() for t in tasks)
        assert [t.result() for t in tasks] == [123] * n

    run(main())


@pytest.mark.parametrize('sleep_time', [
    0,
    0.0001
])
@pytest.mark.parametrize('n,w,b', [
    # Single worker test - 0.02 MB
    (100, 1, 5),
    # Many jobs, but 50 worker tasks and a queue backlog of 50 - 0.25 MB
    (10_000, 50, 50),
    # Many jobs, 50 worker tasks, large queue backlog - 1.5 MB memory
    (10_000, 50, 10000),
])
def test_many_workers_queue(n, w, b, sleep_time):
    results = []

    async def job():
        await sleep(sleep_time)
        results.append(None)

    async def main():
        kwargs = dict(max_workers=w, max_backlog=b)
        with elapsed():
            async with CoroutineExecutor(**kwargs) as exe:
                for i in range(n):
                    await exe.submit_queue(job)
                    # print(f'Submitted job {i}')

        assert len(results) == n

    run(main())
