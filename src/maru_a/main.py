import asyncio
from asyncua import Client
from datetime import timezone, timedelta

from .db import connect_db, init_table, insert_data
from .mqtt import connect_mqtt, insert_data_mqtt 
from .dataModel import MachineReading

thai_tz = timezone(timedelta(hours=7))



async def read_tags(client: Client, tags):
    nodes = [client.get_node(f"ns=2;s={name}") for name in tags]
    data_values = await client.read_data_value(nodes)
    values = data_values.Value.Value

    result = {name: dv.Value.Value for name, dv in zip(tags, data_values)}
    source_time_thai = data_values.SourceTimestamp.replace(tzinfo=timezone.utc).astimezone(thai_tz)
    # print("Thai time:", source_time_thai)

    return MachineReading(
        tags=result,
        recorded_at=source_time_thai,
        l0="tmt", l1="banpho", l2="assembly", l3="final3",
        l4="filling_equipment", l5="BFC", l6="BFC",
    )


async def main():
    pool = await connect_db()
    if pool is None:
        return
    await init_table(pool)

    async with Client(url="opc.tcp://<ip>:4840") as client, connect_mqtt() as mqtt_client:

        while True:
            data = await read_tags(client, opc_tags)
            # for name, val in result.items():
            #             print(f"{name}: {val}")
            await asyncio.gather(
                    insert_data(pool, data.to_postgres()),
                    insert_data_mqtt(mqtt_client, data.to_mqtt()),
                    return_exceptions=True
                )
            print("sent")
            await asyncio.sleep(1)


    await pool.close()
asyncio.run(main(),loop_factory=asyncio.SelectorEventLoop)