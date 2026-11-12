import asyncio
import json
from datetime import datetime, timezone, timedelta

from asyncua import Client

from config import URL_OPC
from db import connect_db, init_table, insert_data
from mqtt import connect_mqtt, build_cycle_topic
from dataModel import MachineReading
from cycle import CycleBuffer

thai_tz = timezone(timedelta(hours=7))

# ตั้งผ่าน env URL_OPC ได้ ; ถ้าไม่ตั้งใช้ค่าเดิม
OPC_URL = URL_OPC or "opc.tcp://100.117.187.28:52250"
RETRY_SEC = 5
MQTT_QUEUE_MAX = 500  # กันกิน RAM ถ้า broker ล่มนานมาก

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
    """อ่านทุก tag 1 รอบ

    - tag ไหนอ่านไม่ได้ (เช่น node id ผิด) -> ใส่ None แล้วไปต่อ
    - ถ้า session/เครือข่ายหลุด (ConnectionError/Timeout/OSError) -> raise ทันที
      เพื่อให้ main() ไป reconnect (ไม่ต้องรอ timeout ครบ 24 tag)
    - ถ้าอ่านไม่ได้ "ทุก tag" -> raise เหมือนกัน (ไม่เขียนแถว NULL ล้วนลง DB)
    """
    result = {}
    failed = 0

    for tag in tags:
        try:
            node = client.get_node(f"ns=2;s=BFC.{tag}")
            data_value = await node.read_data_value()
            result[tag] = data_value.Value.Value
        except (ConnectionError, TimeoutError, OSError):
            raise
        except Exception as e:
            print(f"⚠️ Failed to read tag '{tag}': {e!r}")
            result[tag] = None
            failed += 1

    if failed == len(tags):
        raise ConnectionError("อ่านค่าจาก OPC UA ไม่ได้เลยสักตัว (ตรวจ namespace / ชื่อ tag / session)")

    result["recorded_at"] = datetime.now(timezone.utc).astimezone(thai_tz)

    return MachineReading(
        tags=result,
        l0="tmt", l1="banpho", l2="assembly", l3="final3",
        l4="filling_equipment", l5="BFC", l6="BFC",
    )


async def send_cycle(client, side, rows):
    """ส่งรถ 1 คันขึ้น MQTT ; ต่างจาก publish_cycle ใน mqtt.py ตรงที่ error จะ raise
    เพื่อให้ mqtt_worker รู้ว่า broker หลุดแล้ว reconnect (publish_cycle กลืน error ทิ้ง)
    """
    pgnum = rows[0].get(f"{side}_PGNum") if rows else None
    topic = build_cycle_topic(side, pgnum)
    if topic is None:
        print(f"skip publish cycle [{side}]: invalid pgnum={pgnum!r}")
        return
    msg = {"side": side, "pgnum": pgnum, "count": len(rows), "payload": rows}
    await client.publish(topic, payload=json.dumps(msg, default=str), qos=1)
    print(f"published cycle [{side}] pgnum={pgnum} {len(rows)} rows -> {topic}")


async def mqtt_worker(q: asyncio.Queue):
    """task แยกสำหรับ MQTT : ต่อ/ต่อใหม่เอง ; MQTT ล่มก็ไม่กระทบการอ่าน OPC และเขียน DB
    คันที่ค้างอยู่ (pending) จะถูกส่งซ้ำหลัง reconnect ไม่หาย
    """
    pending = None
    while True:
        try:
            async with connect_mqtt() as client:
                print("เชื่อมต่อ MQTT สำเร็จ")
                while True:
                    if pending is None:
                        pending = await q.get()
                    side, rows = pending
                    await send_cycle(client, side, rows)
                    pending = None
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"MQTT error: {e!r}, reconnecting in {RETRY_SEC}s")
            await asyncio.sleep(RETRY_SEC)


async def main():
    pool = await connect_db()
    if pool is None:
        return
    await init_table(pool)

    # buffer ประกอบคัน แยกซ้าย/ขวา
    buffers = {"LH": CycleBuffer("LH"), "RH": CycleBuffer("RH")}

    mqtt_queue = asyncio.Queue(maxsize=MQTT_QUEUE_MAX)
    mqtt_task = asyncio.create_task(mqtt_worker(mqtt_queue))

    try:
        while True:
            try:
                async with Client(url=OPC_URL, timeout=RETRY_SEC) as client:
                    print(f"เชื่อมต่อ OPC UA สำเร็จ ({OPC_URL})")
                    while True:
                        data = await read_tags(client, opc_tags)

                        # 1) เก็บ raw ลง DB ทุกวินาที (ไม่ขึ้นกับ MQTT)
                        try:
                            await insert_data(pool, data.to_postgres())
                        except Exception as e:
                            print(f"db error: {e}")

                        # 2) ป้อนเข้า buffer ; คันไหนเติมจบ ใส่คิวให้ mqtt_worker ส่ง
                        for side, buf in buffers.items():
                            cycle = buf.update(data.tags)
                            if cycle is not None:
                                try:
                                    mqtt_queue.put_nowait((side, cycle))
                                except asyncio.QueueFull:
                                    print(f"MQTT queue เต็ม ทิ้งคัน [{side}] ({len(cycle)} rows) — raw ยังอยู่ใน DB")

                        await asyncio.sleep(1)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"OPC UA error: {e!r}, reconnecting in {RETRY_SEC}s...")
                await asyncio.sleep(RETRY_SEC)
    finally:
        mqtt_task.cancel()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
