import asyncio
import json
from .db import connect_db, init_table, insert_data
from .mqtt import connect_mqtt, subscribe

async def main():
    pool = await connect_db()
    if pool is None:
        return
    await init_table(pool)
    
    client = connect_mqtt()

    async with client:
        await subscribe(client)
        async for m in client.messages:
            try:
                msg = json.loads(m.payload.decode())
                await insert_data(pool, msg)
            except Exception as e:
                print("insert error:", e)


asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)

