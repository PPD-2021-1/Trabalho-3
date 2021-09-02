import paho.mqtt.client as mqtt
import random
import math
import json
from io import StringIO

class DHT:

    # Recuperar o id do cara anterior do anel
    def getAntecessorId(self):
        preview = -1
        for i in self.nodes:
            if i > preview and i < self.nodeID:
                preview = i
        return preview

    # atualiza os limites que o no atende
    def updateBundaries(self):
        minorId = self.getAntecessorId()
        if minorId == -1:
            if len(self.nodes) > 0:
                minorId = self.nodes[len(self.nodes) - 1]
        self.initValue = minorId
        self.finalValue = self.nodeID
        print("node_" + str(self.nodeID) + " NEW RANGE Ant/Suc:(" + str(self.initValue) + "|" + str(self.finalValue) + ")")
        return

    # Trata a mensagem de sincronia
    def handlerNewNodeInSys(self, nodeId):
        if nodeId != self.nodeID:
            self.nodes.append(nodeId)
            self.nodes.sort()
            self.updateBundaries()

    # Trata a mensagem do canal de controle
    def handlerControlMessage(self, message):
        try:
            if message['type'] == 'join':
                print("node_" + str(self.nodeID) + " receive join")
                self.handlerNewNodeInSys(message['id'])
            return
        except:
            pass

    def checkIfKeyInMyRange(self, key):
        if self.initValue < self.finalValue:
            return self.initValue < key and key <= self.finalValue
        else:
            # Trata caso range crusa o 0
            return (self.initValue < key and key < (2**32)) or (0 <= key and key <= self.finalValue)

    def handlerGetAndPutMessage(self, message):
        try:
            if 'key' in message and self.checkIfKeyInMyRange(message['key']):
                print("node_" + str(self.nodeID) + " (from:" + str(self.initValue) + "|to:" + str(self.finalValue) + ") receive message")
                data = {}
                # Repete o id para o usuário que enviou saber que a mensagem é dele
                data['id'] = message['id']
                data['type'] = 'server_response'
                if message['type'] == 'put':
                    self.table[message['key']] = message['value']
                    data['status'] = 201
                elif message['type'] == 'get':
                    if message['key'] in self.table:
                        data['status'] = 200
                        data['value'] = self.table[message['key']]
                    else:
                        data['status'] = 404
                self.client.publish(self.channelPrefix + 'hash', json.dumps(data))
        except:
            try:
                data = {}
                data['id'] = message['id']
                data['status'] = 500
                self.client.publish(self.channelPrefix + 'hash', json.dumps(data))
            except:
                pass
            pass
        return

    def on_message(self, client, userdata, message):
        payload = json.loads(message.payload)
        if message.topic == self.channelPrefix + "control":
            self.handlerControlMessage(payload)
            return
        elif message.topic == self.channelPrefix + "hash":
            self.handlerGetAndPutMessage(payload)
        return

    def on_connect(self, client, userdata, flags, rc):
        print("node_" + str(self.nodeID) + " connected")
        self.client.subscribe(self.channelPrefix + 'control')
        self.client.subscribe(self.channelPrefix + 'hash')
        # Mesagem de join anuncia a entra de um novo nó no anel, demais nó iram atualizar o seu intervalo quando receber essa mensagem
        joinMessage = {
            "type": "join",
            "id": self.nodeID
        }
        self.client.publish(self.channelPrefix + 'control', json.dumps(joinMessage))

    def on_log(self, client, userdata, level, buf):
        #print(buf)
        return

    def __init__(self, brokenURL, channelPrefix):
        # Nós do anel
        self.nodes = []
        self.table = {}

        if(channelPrefix and len(channelPrefix)):
            self.channelPrefix = channelPrefix + '/'
        else:
            self.channelPrefix = ''
            
        total = (2**32) - 1
        self.nodeID = math.floor(random.uniform(0, total))
        print("init node_" + str(self.nodeID))

        self.initValue = 0
        self.finalValue = total

        self.client = mqtt.Client('Node_' + str(self.nodeID))
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_log = self.on_log
        self.client.enable_logger(logger=None)
        self.client.connect(brokenURL)
        self.client.loop_forever()
