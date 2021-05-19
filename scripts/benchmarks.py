import sys
import time
import pytest
from asyncio import sleep, run
from contextlib import contextmanager

from coroexecutor import CoroutineExecutor


@contextmanager
def elapsed():
    t0 = time.monotonic()
    yield lambda: t1 - t0
    t1 = time.monotonic()


@pytest.mark.parametrize('sleep_time', [
    0,
    0.01
])
@pytest.mark.parametrize('n,w,dp', [
    (100, 1, 0.5),
    (1000, 50, 0.5),

    (10000, 50, 3.0),
    (10000, 500, 4.0),
    (10000, 5000, 20.0),

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
                tasks = [await exe.submit(job) for i in range(n)]

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


@pytest.mark.parametrize('sleep_time', [
    0,
    0.0001
])
@pytest.mark.parametrize('n,w,b,dp', [
    # Single worker test - 0.02 MB
    (100, 1, 5, 0.1),
    # Many jobs, but 50 worker tasks and a queue backlog of 50 - 0.25 MB
    (10_000, 50, 50, 0.3),
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
