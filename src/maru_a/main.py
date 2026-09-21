import asyncio
import aiomqtt
from asyncua import Client

from datetime import datetime, timezone, timedelta

from db import connect_db, init_table, insert_data
from mqtt import connect_mqtt, insert_data_mqtt 
from dataModel import MachineReading

thai_tz = timezone(timedelta(hours=7))
opc_tags = [
    "Continue",
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
    result = {}
    
    for tag in tags:
        try:
            node = client.get_node(f"ns=2;s=BFC.{tag}")
            data_value = await node.read_data_value()
            result[tag] = data_value.Value.Value
        except Exception as e:
            # Warn which tag has an issue and set value to None
            print(f"⚠️ Failed to read tag '{tag}': {e}")
            result[tag] = None
            
    # Record the overall timestamp for this reading cycle
    result["recorded_at"] = datetime.now(timezone.utc).astimezone(thai_tz)
    # Datetime from PLC
    # result["recorded_at"] = data_value.SourceTimestamp.replace(tzinfo=timezone.utc).astimezone(thai_tz)
   
    return MachineReading(
        tags=result,
        l0="tmt", l1="banpho", l2="assembly", l3="final3",
        l4="filling_equipment", l5="BFC", l6="BFC",
    )


async def main():
    pool = await connect_db()
    if pool is None:
        return
    await init_table(pool)

    while True:
        try:
            async with Client(url="opc.tcp://100.117.187.28:52250") as client:
                print("เชื่อมต่อ OPC UA สำเร็จ")
                async with connect_mqtt() as mqtt_client:
                    print("เชื่อมต่อ MQTT สำเร็จ")
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

        except (ConnectionError, OSError, Exception) as e:
            print(f"OPC UA connection error: {e}, reconnecting in 5s...")
            await asyncio.sleep(5)
        except aiomqtt.MqttError as e:
            print(f"MQTT disconnected: {e}, reconnecting in 5s")
            await asyncio.sleep(5)

    await pool.close()

if __name__ == "__main__":
    asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
