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


@pytest.mark.parametrize('mode', [
    'sync',
    'async',
])
@pytest.mark.parametrize('args,expected', [
    ([], 0),
    ([1, 2, 3], 6),
])
def test_init(mode, args, expected):
    results = []

    if mode == 'sync':
        def fn(*args):
            results.append(sum(args))
    elif mode == 'async':
        async def fn(*args):
            await asyncio.sleep(0)
            results.append(sum(args))

    async def main():
        async with CoroutineExecutor(initializer=fn, initargs=args) as exe:
            t1 = await exe.submit(f, 0.001)
            t2 = await exe.submit(f, 0.002)

        assert t1.done()
        assert t2.done()

    run(main())
    assert results == [expected, expected]


def test_init_error():

    async def main():
        async with CoroutineExecutor(initializer=123) as exe:
            await exe.submit(f, 0.001)
            await exe.submit(f, 0.002)

    with pytest.raises(TypeError, match='type is unknown'):
        run(main())
