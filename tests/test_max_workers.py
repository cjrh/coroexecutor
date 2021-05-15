import random
from asyncio import sleep, run
from contextlib import contextmanager
import time

import pytest

from coroexecutor import CoroutineExecutor


@contextmanager
def elapsed():
    t0 = time.perf_counter()
    yield lambda: t1 - t0
    t1 = time.perf_counter()


async def job(i):
    await sleep(i)
    return i


items = [random.randint(0, 10) / 1000 for i in range(100)]
random.shuffle(items)


def test_one_worker_serial():

    async def main():
        kwargs = dict(max_workers=1)
        with elapsed() as f:
            async with CoroutineExecutor(**kwargs) as exe:
                tasks = [exe.submit(job, item) for item in items]

        assert all(t.done() for t in tasks)
        assert [t.result() for t in tasks] == items
        # Elapsed time is greater than the sum of each individual
        # time.
        assert f() > sum(items)

    run(main())


def test_one_worker_concurrent():

    async def main():
        kwargs = dict(max_workers=10)
        with elapsed() as f:
            async with CoroutineExecutor(**kwargs) as exe:
                tasks = [exe.submit(job, item) for item in items]

        assert all(t.done() for t in tasks)
        assert [t.result() for t in tasks] == items
        # Speedup is roughly 10 times
        concurrency = sum(items) / f()
        assert concurrency > 7

    run(main())


@pytest.mark.parametrize('sleep_time', [
    0,
    0.01
])
@pytest.mark.parametrize('n,w,dp', [
    (100, 1, 0.25),
    (1000, 50, 3.0),

    (10000, 50, 30.0),
    (10000, 500, 40.0),
    (10000, 5000, 40.0),

    # Memory begins to climb with increasing max_workers
    pytest.param(100_000, 500, 270.0, marks=pytest.mark.slow),
    pytest.param(100_000, 5000, 280.0, marks=pytest.mark.slow),
    pytest.param(100_000, 50000, 400.0, marks=pytest.mark.slow),
])
def test_many_workers(n, w, dp, sleep_time):
    """ Memory usage:

    Current memory usage is 641.323396MB; Peak was 2633.067287MB

    Many tasks use a lot of memory.
    """
    import tracemalloc

    async def job():
        await sleep(sleep_time)
        return 123

    async def main():
        kwargs = dict(max_workers=w)
        with elapsed():
            async with CoroutineExecutor(**kwargs) as exe:
                tasks = [exe.submit(job) for i in range(n)]

        assert all(t.done() for t in tasks)
        assert [t.result() for t in tasks] == [123] * n

    tracemalloc.start()
    current0, peak0 = tracemalloc.get_traced_memory()
    print(f"Current memory usage is "
          f"{current0 / 10 ** 6}MB; Peak was {peak0 / 10 ** 6}MB")
    run(main())
    current1, peak1 = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f"Current memory usage is "
          f"{current1 / 10 ** 6}MB; Peak was {peak1 / 10 ** 6}MB")

    delta_peak_MB = (peak1 - current0) / 1e6
    print(f'delta_peak_MB={delta_peak_MB}')
    assert delta_peak_MB < dp


# @pytest.mark.skip(reason="Slow test, consumes ~ 1.2 GB memory")
@pytest.mark.parametrize('sleep_time', [
    0,
    0.0001
])
@pytest.mark.parametrize('n,w,b,dp', [
    # Single worker test - 0.02 MB
    (100, 1, 5, 0.02),
    # Many jobs, but 50 worker tasks and a queue backlog of 50 - 0.25 MB
    (10_000, 50, 50, 0.25),
    # Many jobs, 50 worker tasks, large queue backlog - 1.5 MB memory
    (10_000, 50, 10000, 1.6),

    # 100k jobs, 1k workers - 1.5 MB
    pytest.param(100_000, 10, 10, 1.0, marks=pytest.mark.slow),
    # 100k jobs, 1k workers - 1.5 MB
    pytest.param(100_000, 100, 10, 1.0, marks=pytest.mark.slow),
    # 100k jobs, 1k workers - 3.0 MB
    pytest.param(100_000, 1000, 10, 3.0, marks=pytest.mark.slow),
    # 100k jobs, 1k workers - 22 MB
    pytest.param(100_000, 10000, 10, 22.0, marks=pytest.mark.slow),

    # 100k jobs, 1k workers
    pytest.param(100_000, 10, 10000, 2.5, marks=pytest.mark.slow),
    # 100k jobs, 1k workers
    pytest.param(100_000, 100, 10000, 2.5, marks=pytest.mark.slow),
    # 100k jobs, 1k workers
    pytest.param(100_000, 1000, 10000, 6.0, marks=pytest.mark.slow),
    # 100k jobs, 1k workers - 40 MB
    pytest.param(100_000, 10000, 10000, 40, marks=pytest.mark.slow),
    # # many jobs, 10k workers - 48 MB
    # (500_000, 10_000, 10000, 48.0),
    # # many jobs, 20k workers - 77 MB
    # (500_000, 20_000, 10000, 77.0),
    # # yuge jobs, 50k workers - 120 MB
    # (1_000_000, 50_000, 10000, 140.0),
])
def test_many_workers_queue(n, w, b, dp, sleep_time):
    """ Memory usage:

    Current memory usage is 641.323396MB; Peak was 2633.067287MB

    Many tasks use a lot of memory.
    """
    import tracemalloc
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

    tracemalloc.start()
    current0, peak0 = tracemalloc.get_traced_memory()
    print(f"Current memory usage is "
          f"{current0 / 10 ** 6}MB; Peak was {peak0 / 10 ** 6}MB")
    run(main())
    current1, peak1 = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f"Current memory usage is "
          f"{current1 / 10 ** 6}MB; Peak was {peak1 / 10 ** 6}MB")

    delta_peak_MB = (peak1 - current0) / 1e6
    print(f'delta_peak_MB={delta_peak_MB}')
    assert delta_peak_MB < dp
