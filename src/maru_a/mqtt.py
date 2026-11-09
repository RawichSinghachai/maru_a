import json
import aiomqtt

MAIN_TOPIC = "tmt/banpho/assembly/final3/filling_equipment/BFC/BFC"
CYCLE_TOPIC_TEMPLATE = (
    "tmt/banpho/assembly/final3/filling_equipment/BFC/raw_data/{side}/{pgnum}"
)


def connect_mqtt(hostname="100.124.242.18", port=1883):
    return aiomqtt.Client(hostname=hostname, port=port)


def build_cycle_topic(side, pgnum):
    """สร้าง topic ต่อ side + pgnum เช่น .../raw_data/lh/20
    side ใช้ตัวพิมพ์เล็กใน topic (LH -> lh), pgnum แปลงเป็น int กัน 20.0
    คืน None ถ้า pgnum ใช้ไม่ได้ (None / ไม่ใช่ตัวเลข)
    """
    try:
        pg = int(pgnum)
    except (TypeError, ValueError):
        return None
    return CYCLE_TOPIC_TEMPLATE.format(side=str(side).lower(), pgnum=pg)


async def publish_cycle(client, side, rows):
    """ส่งรถ 1 คัน (ประกอบครบจากฝั่ง OPC แล้ว) เป็น 1 ข้อความ
    rows = list ของแถว (คัดคอลัมน์ข้างนี้แล้ว, เรียงตาม datetime)
    topic = .../raw_data/{side}/{pgnum}  (side, pgnum ยังอยู่ใน payload ด้วย)
    """
    try:
        pgnum = rows[0].get(f"{side}_PGNum") if rows else None
        topic = build_cycle_topic(side, pgnum)
        if topic is None:
            # ไม่รู้ pgnum -> ไม่รู้ว่าจะส่งไปโมเดลไหน ข้ามไป (raw ยังอยู่ใน DB)
            print(f"skip publish cycle [{side}]: invalid pgnum={pgnum!r}")
            return
        msg = {"side": side, "pgnum": pgnum, "count": len(rows), "payload": rows}
        payload = json.dumps(msg, default=str)   # datetime -> ISO string อัตโนมัติ
        await client.publish(topic, payload=payload, qos=1)  # qos=1 กันข้อความหาย
        print(f"published cycle [{side}] pgnum={pgnum} {len(rows)} rows -> {topic}")
    except Exception as e:
        print(f"publish_cycle error: {e}")
 
 
# ---- ของเดิม: ส่งรายวินาที (เก็บไว้เผื่อยังต้องใช้กับ receive) ----
async def insert_data_mqtt(client, data, topic="tmt/banpho/assembly/final3/filling_equipment/BEC/BFC"):
    try:
        payload = json.dumps(data, default=str)
        await client.publish(topic, payload=payload)
        print("mqtt send")
    except Exception as e:
        print(f"insert_data error: {e}")
