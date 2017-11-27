import aiosqlite3
import asyncio


async def main(loop):
    conn = await aiosqlite3.connect(':memory:', loop=loop)
    await conn.execute(
        "CREATE TABLE sa_tbl "
        "(id INTEGER PRIMARY KEY AUTOINCREMENT, name varchar(255))"
    )
    await conn.commit()
    await conn.execute(
        "INSERT INTO sa_tbl(`name`) VALUES('test')"
    )
    await conn.commit()
    cursor = await conn.execute(
        "SELECT * FROM sa_tbl"
    )
    print(cursor, cursor.native_cursor, cursor.description)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))

