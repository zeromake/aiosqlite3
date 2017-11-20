import aiosqlite3
import asyncio
import uvloop


async def funcname(loop):
    conn = await aiosqlite3.connect('./test.db', loop=loop)
    print(conn)
    async with conn.cursor() as cur:
        print(cur)
        await cur.execute('create table multiple_connections (i integer primary key asc, k integer)')

if __name__ == '__main__':
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(funcname(loop))