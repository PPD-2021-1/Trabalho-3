import paho.mqtt.client as mqtt
import random
import math
import json
from io import StringIO

class DHT:

    def getAntecessorId(self):
        preview = -1
        for i in self.nodes:
            if i > preview and i < self.nodeID:
                preview = i
        return preview

    def updateBundaries(self):
        minorId = self.getAntecessorId()
        if minorId == -1:
            if len(self.nodes) > 0:
                minorId = self.nodes[len(self.nodes) - 1]
        self.initValue = minorId
        self.finalValue = self.nodeID
        print("node_"+str(self.nodeID)+" Ant/Suc:("+str(self.initValue)+"|"+str(self.finalValue)+")")
        return

    def handlerNewNodeInSys(self, nodeId):
        if nodeId != self.nodeID:
            self.nodes.append(nodeId)
            self.nodes.sort()
            self.updateBundaries()

    def handlerJoinMessage(self, message):
        if message['type'] == 'join':
            print("node_"+str(self.nodeID)+" receive join")
            self.handlerNewNodeInSys(message['id'])
        return


    def handlerGetPushMessage(self, message):
        if self.initValue < message['key'] and message['key'] <= self.finalValue:
            print("node_"+str(self.nodeID)+" receive message")
            data = {}
            data['id'] = message['id']
            if message['type'] == 'put':
                self.table[message['key']] = message['value']
                data.status = 201
            elif message['type'] == 'get':
                value = self.table[message['key']]
                if value:
                    data['status'] = 200
                    data['value'] = value
                else:
                    data['status'] = 404
            self.client.publish('hash', json.dumps(data))
        return

    def on_message(self, client, userdata, message):
        #io = StringIO()
        payload = json.loads(message.payload)
        if message.topic == "control":
            self.handlerJoinMessage(payload)
            return
        elif message.topic == "hash":
            self.handlerGetPushMessage(payload)
        return

    def on_connect(self, client, userdata, flags, rc):
        print("node_"+str(self.nodeID)+" connected")
        self.client.subscribe('control')
        self.client.subscribe('hash')
        joinMessage = {
            "type": "join",
            "id": self.nodeID
        }
        self.client.publish('control', json.dumps(joinMessage))

    def on_log(self, client, userdata, level, buf):
        #print(buf)
        return

    def __init__(self, brokenURL):
        self.nodes = []
        self.table = {}

        total = (2**32) - 1
        self.nodeID = math.floor(random.uniform(0, total))
        print("init node_"+ str(self.nodeID))

        self.initValue = 0
        self.finalValue = total

        self.client = mqtt.Client('Node_' + str(self.nodeID))
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_log = self.on_log
        self.client.enable_logger(logger=None)
        self.client.connect(brokenURL)

        self.client.loop_forever()
