import sys
import time
import pytest
from asyncio import sleep, run
from contextlib import contextmanager
from pathlib import Path

from coroexecutor import CoroutineExecutor


p = Path('stats.csv')
p.unlink(missing_ok=True)


def append_row(*row, filepath=p):
    with filepath.open('a+') as f:
        f.write(','.join(str(r) for r in row) + '\n')


append_row('jobs', 'max_workers', 'max_backlog', 'peak mem (MB)')



@contextmanager
def elapsed():
    t0 = time.monotonic()
    yield lambda: t1 - t0
    t1 = time.monotonic()


@pytest.mark.parametrize('sleep_time', [
    0.0001
])
@pytest.mark.parametrize('n', [
    100,
    1000,
    10000,
    100000,
])
@pytest.mark.parametrize('w', [
    50,
    500,
    5000,
    50000,
])
@pytest.mark.parametrize('b', [
    1,
    10,
    100,
    1000,
])
def test_many_workers(n, w, b, sleep_time):
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

    append_row(n, w, b, delta_peak_MB)


@pytest.mark.parametrize('sleep_time', [
    0.0001
])
@pytest.mark.parametrize('n', [
    100,
    1000,
    10000,
    100000,
])
@pytest.mark.parametrize('w', [
    50,
    500,
    5000,
    50000,
])
@pytest.mark.parametrize('b', [
    1,
    10,
    100,
    1000,
])
def test_many_workers_queue(n, w, b, sleep_time):
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

    append_row(n, w, b, delta_peak_MB)
