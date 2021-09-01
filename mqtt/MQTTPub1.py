import paho.mqtt.client as mqtt 
from random import randrange, uniform
import time

mqttBroker = "127.0.0.1" 

client = mqtt.Client("Node_2")
client.connect(mqttBroker) 

while True:
    randNumber = uniform(100.0, 150.0)
    client.publish("rsv/light", randNumber)
    print("Just published " + str(randNumber) + " to topic rsv/light")
    time.sleep(1)
    
    