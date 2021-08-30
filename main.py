import threading
from DHT import DHT

def createNode():
    dht = DHT("mqtt.eclipseprojects.io")

t = False
for i in range(8):
    t = threading.Thread(target=createNode)
    t.start()
