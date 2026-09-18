import json
import aiomqtt


def connect_mqtt(hostname="100.124.242.18", port=1883):
    return aiomqtt.Client(hostname=hostname, port=port)


async def insert_data_mqtt(client, data, topic="tmt/banpho/assembly/final3/filling_equipment/BEC/BFC"):
    try:
        payload = json.dumps(data, default=str)
        await client.publish(topic, payload=payload)
    except Exception as e:
        print(f"insert_data error: {e}")