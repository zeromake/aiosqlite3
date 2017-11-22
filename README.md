#aiosqlite3
![https://www.travis-ci.org/zeromake/aiosqlite3](https://www.travis-ci.org/zeromake/aiosqlite3.svg?branch=master)


## Basic Example

``` python
import asyncio
import aiosqlite3

loop = asyncio.get_event_loop()
async def test_example():
    conn = await aiosqlite3.connect('sqlite.db', loop=loop)

    cur = await conn.cursor()
    await cur.execute("SELECT 42;")
    r = await cur.fetchall()
    print(r)
    await cur.close()
    await conn.close()

loop.run_until_complete(test_example())
```

or async with

``` python
import asyncio
import aiosqlite3

loop = asyncio.get_event_loop()
async def test_example():
    async with aiosqlite3.connect('sqlite.db', loop=loop) as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 42;")
            r = await cur.fetchall()
            print(r)

loop.run_until_complete(test_example())
```
