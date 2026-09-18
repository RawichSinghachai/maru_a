import asyncio
import aiomqtt
from asyncua import Client
from asyncua import ua 
from datetime import timezone, timedelta

from .db import connect_db, init_table, insert_data
from .mqtt import connect_mqtt, insert_data_mqtt 
from .dataModel import MachineReading

thai_tz = timezone(timedelta(hours=7))
opc_tags = [
    "Coutinue",
    "Master_on",
    "Machine_fault",
    "LH_ASN",
    "LH_BN",
    "LH_FillingPressure",
    "LH_FillingVolume",
    "LH_PGNum",
    "LH_StepNum",
    "LH_VacuumGun",
    "LH_VacuumPump",
    "LH_PumpVacuumCurrent",
    "RH_ASN",
    "RH_BN",
    "RH_FillingPressure",
    "RH_FillingVolume",
    "RH_PGNum",
    "RH_StepNum",
    "RH_VacuumGun",
    "RH_VacuumPump",
    "RH_PumpVacuumCurrent",
    "PumpDegassingCurrent",
    "PumpSupplyCurrent",
    "PumpFillingCurrent",
]


async def read_tags(client: Client, tags):
    nodes = [client.get_node(f"ns=2;s={name}") for name in tags]

    params = ua.ReadParameters()
    for node in nodes:
        rv = ua.ReadValueId()
        rv.NodeId = node.nodeid
        rv.AttributeId = ua.AttributeIds.Value
        params.NodesToRead.append(rv)

    data_values = await client.uaclient.read(params)  # list[ua.DataValue] ลำดับตรงกับ nodes/tags

    result = {name: dv.Value.Value for name, dv in zip(tags, data_values)}
    source_time_thai = data_values[0].SourceTimestamp.replace(tzinfo=timezone.utc).astimezone(thai_tz)

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

    async with Client(url="opc.tcp://127.0.0.1:52250") as client:

        while True:
            try:
                async with connect_mqtt() as mqtt_client:
                    while True:
                        data = await read_tags(client, opc_tags)

                        db_result, mqtt_result = await asyncio.gather(
                                insert_data(pool, data.to_postgres()),
                                insert_data_mqtt(mqtt_client, data.to_mqtt()),
                                return_exceptions=True
                            )
                        if isinstance(mqtt_result, Exception):
                            raise mqtt_result
                        if isinstance(db_result, Exception):
                            print(f"db error: {db_result}")

                        print("sent")
                        await asyncio.sleep(1)

            except aiomqtt.MqttError as e:
                print(f"MQTT disconnected: {e}, reconnecting in 5s")
                await asyncio.sleep(5)

    await pool.close()

asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)