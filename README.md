# aiosqlite3

[![Travis Build Status](https://www.travis-ci.org/zeromake/aiosqlite3.svg?branch=master)](https://www.travis-ci.org/zeromake/aiosqlite3)
[![codecov](https://codecov.io/gh/zeromake/aiosqlite3/branch/master/graph/badge.svg)](https://codecov.io/gh/zeromake/aiosqlite3)

## Basic Example

``` python
import asyncio
import aiosqlite3

async def test_example(loop):
    conn = await aiosqlite3.connect('sqlite.db', loop=loop)
    cur = await conn.cursor()
    await cur.execute("SELECT 42;")
    r = await cur.fetchall()
    print(r)
    await cur.close()
    await conn.close()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_example(loop))
```

or async with

``` python
import asyncio
import aiosqlite3

async def test_example(loop):
    async with aiosqlite3.connect('sqlite.db', loop=loop) as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 42;")
            r = await cur.fetchall()
            print(r)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_example(loop))
```
