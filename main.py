import threading
from DHT import *
import paho.mqtt.client as mqtt 
from random import randrange, uniform
import time
import json
import math

def createNode():
    dht = DHT("mqtt.eclipseprojects.io", "trabalhopdd1110002203")

t = False

for i in range(8):
    t = threading.Thread(target=createNode)
    t.start()


mqttBroker = "mqtt.eclipseprojects.io" 

client = mqtt.Client("teste_client")
client.connect(mqttBroker) 

while True:

    def on_message(self, message, status):
        #recebe todas as mensagens do topico [hash]
        message = {}

        #id da mensagem é o que vc envio
        if(message['id'] == data['id']):
            print()
            #verifica se tem status
            if(data['status'] != NULL):
                print("Tem status")
                # 201 pra put
                if(data['status'] == "201"):
                    print("put()")
                # 200 get
                elif(data['status'] == "200"):
                    print("get()")
                    # se for 200 pra get olha value foi o mesmo que vc enviou
                    if(data['value'] == 0):
                        return
                return
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
    time.sleep(2)
    input("Teste")
