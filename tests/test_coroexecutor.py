import asyncio
from asyncio import run
import random
from coroexecutor import CoroutineExecutor


async def p(text, raise_err=False):
    count = random.randint(1, 10)
    print("p is running", text, count)
    try:
        for i in range(count):
            if i == 5 and raise_err:
                raise Exception("bad " + text)
            print(text)
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        print("cancelled p", text)


async def main():
    from string import ascii_lowercase

    async with CoroutineExecutor() as exe:
        results = exe.map(p, ascii_lowercase)
        print(results)

def test_hello():
    run(main())
