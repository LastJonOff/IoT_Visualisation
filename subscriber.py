import random
import json
import window
import settings
from paho.mqtt import client as mqtt_client
from clickhouse_driver import Client

broker = 'localhost'
port = 1883
topic = "python_mqtt_2"
# generate client ID with sub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'
username = 'sub'
password = 'subpass'

def setWindowStatus(status):
    settings.IS_WINDOW_CLOSED = status

def saveData(topic, data):
    config = readConfig(topic)

    fieldNames = config['fieldNames']
    dataNames = config['dataNames']

    request = f'INSERT INTO topics.{topic} VALUES ('
    values = list(data.values())

    for i in range (0, len(dataNames)):
        if (type(values[i]) == int):
            request += f'{values[i]}, '
        else:
            request += f"'{values[i]}', "
        i += 1

    request = request[0:len(request)-2]
    request += ');'
    client = Client('localhost')
    res = client.execute(request) #вставляем данные

    res = client.execute(f"SELECT * FROM topics.{topic}")
    print(res)

def readConfig(filename):
    # получим объект файла
    config = open(f"{filename}.txt", "r")

    fieldNames = []
    dataNames = []
    while True:
        line = config.readline()

        if not line:
            break
        else:
            line = line.split(':')
            dataNames.append(line[0])
            fieldNames.append(line[1].rstrip())

    config.close
    return {'dataNames': dataNames, 'fieldNames': fieldNames}

def connect_mqtt() -> mqtt_client:
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

def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        payload = json.loads(msg.payload)
        print(f"Received `{payload}` from `{msg.topic}` topic")

        if msg.topic not in settings.topic_list:
            settings.IS_WINDOW_CLOSED = True
            settings.topic_list.append(msg.topic)
        else:
            saveData(msg.topic, payload)
            

        if settings.IS_WINDOW_CLOSED:
            setWindowStatus(False)
            window.start(payload, msg.topic)   

    client.subscribe(topic)
    client.on_message = on_message

settings.init() #global variables

client = connect_mqtt()
subscribe(client)
client.loop_forever()
