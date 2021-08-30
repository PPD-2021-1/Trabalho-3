import threading
from DHT import DHT
import paho.mqtt.client as mqtt 
from random import randrange, uniform
import time
import json
import math

def createNode():
    dht = DHT("mqtt.eclipseprojects.io")

t = False
for i in range(8):
    t = threading.Thread(target=createNode)
    t.start()


mqttBroker = "mqtt.eclipseprojects.io" 

client = mqtt.Client("teste_client")
client.connect(mqttBroker) 

while True:

    def on_message():
        #recebe todas as mensagens do topico [hash]
        #id da mensagem é o que vc envio
        #verifica se tem status
        # 201 pra put
        # 200 get
        # se for 200 pra get olha value foi o mesmo que vc enviou
        return

    randNumber = uniform(100.0, 150.0)
    id = math.floor(uniform(0, 2**32 - 1))

    data = {
        "id": id,
        "type": 'put',
        "key": id,
        "value": randNumber
    }

    client.publish("hash", json.dumps(data))
    time.sleep(1)
