import aiomqtt

def connect_mqtt():
    return aiomqtt.Client(
        hostname="192.168.x.x",   # broker
        port=1883,
    )

async def subscribe(client, topic="tmt/banpho/assembly/final3/filling_equipment/BEC/BFC"):
    await client.subscribe(topic)