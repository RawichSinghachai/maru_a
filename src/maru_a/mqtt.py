import json
import aiomqtt

MAIN_TOPIC = "tmt/banpho/assembly/final3/filling_equipment/BFC/BFC"
CYCLE_TOPIC = "tmt/banpho/assembly/final3/filling_equipment/BFC/cycle"

def connect_mqtt(hostname="100.124.242.18", port=1883):
    return aiomqtt.Client(hostname=hostname, port=port)

async def publish_cycle(client, side, rows, topic=CYCLE_TOPIC):
    """ส่งรถ 1 คัน (ประกอบครบจากฝั่ง OPC แล้ว) เป็น 1 ข้อความ
    rows = list ของแถว (คัดคอลัมน์ข้างนี้แล้ว, เรียงตาม datetime)
    """
    try:
        msg = {"side": side, "payload": rows}
        payload = json.dumps(msg, default=str)   # datetime -> ISO string อัตโนมัติ
        await client.publish(topic, payload=payload, qos=1)  # qos=1 กันข้อความหาย
        print(f"published cycle [{side}] rows")
    except Exception as e:
        print(f"publish_cycle error: {e}")


async def insert_data_mqtt(client, data, topic=MAIN_TOPIC):
    try:
        payload = json.dumps(data, default=str)
        await client.publish(topic, payload=payload)
        print("mqtt send")
    except Exception as e:
        print(f"insert_data error: {e}")