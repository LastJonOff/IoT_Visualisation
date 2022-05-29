import json
import random
import time

from paho.mqtt import client as mqtt_client

broker = 'localhost'
port = 1883
topic = "python_mqtt_2"

client_id = f'python-mqtt-{random.randint(0, 1000)}'
username = 'pub'
password = 'pubpass'

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client):
    while True:
        time.sleep(5)
        data = {
            "counter": random.randint(0, 1000),
            "message": "hello",
            "user": f'tester{random.randint(0, 1000)}',
        }
        data_dump = json.dumps(data)
        result = client.publish(topic, data_dump)
        status = result[0]
        if status == 0:
            print(f"Send `{data_dump}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")

client = connect_mqtt()
client.loop_start()
publish(client)
