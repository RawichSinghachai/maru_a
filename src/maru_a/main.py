import asyncio
import aiomqtt
from asyncua import Client

from datetime import datetime, timezone, timedelta

from db import connect_db, init_table, insert_data
from mqtt import connect_mqtt, publish_cycle
from dataModel import MachineReading
from cycle import CycleBuffer

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
            print(f"⚠️ Failed to read tag '{tag}': {e}")
            result[tag] = None

    result["recorded_at"] = datetime.now(timezone.utc).astimezone(thai_tz)

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

    # buffer ประกอบคัน แยกซ้าย/ขวา
    buffers = {"LH": CycleBuffer("LH"), "RH": CycleBuffer("RH")}

    while True:
        try:
            async with Client(url="opc.tcp://100.117.187.28:52250") as client:
                print("เชื่อมต่อ OPC UA สำเร็จ")
                async with connect_mqtt() as mqtt_client:
                    print("เชื่อมต่อ MQTT สำเร็จ")
                    while True:
                        data = await read_tags(client, opc_tags)

                        # 1) เก็บ raw ลง DB ทุกวินาที (เหมือนเดิม — safety net ย้อนดูได้)
                        try:
                            await insert_data(pool, data.to_postgres())
                        except Exception as e:
                            print(f"db error: {e}")

                        # 2) ป้อนเข้า buffer ; คันไหนเติมจบ ส่งทั้งคันไปเครื่อง 2 ทันที
                        for side, buf in buffers.items():
                            cycle = buf.update(data.tags)
                            if cycle is not None:
                                await publish_cycle(mqtt_client, side, cycle)

                        await asyncio.sleep(1)

        except aiomqtt.MqttError as e:
            print(f"MQTT disconnected: {e}, reconnecting in 5s")
            await asyncio.sleep(5)
        except (ConnectionError, OSError, Exception) as e:
            print(f"OPC UA connection error: {e}, reconnecting in 5s...")
            await asyncio.sleep(5)

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
