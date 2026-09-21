import aiomqtt

def connect_mqtt():
    return aiomqtt.Client(
        hostname="localhost",   # broker
        port=1883,
    )

async def subscribe(client, topic="tmt/banpho/assembly/final3/filling_equipment/BFC/BFC"):
    await client.subscribe(topic)