import paho.mqtt.client as mqtt

BROKER = "server.matmacsystem.it"
PORT = 1883
TOPIC = "mtds/sensor/#"


def on_connect(client, userdata, flags, rc):
    client.subscribe(TOPIC)


def on_message(client, userdata, msg):
    #print("Message received-> " + msg.topic.replace("mtds/", "").replace("/", ".") + " " + msg.payload.decode())
    print("Received from topic: " + msg.topic + " - Message: " + msg.payload.decode())


client = mqtt.Client("mtds-test-subscriber")
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER, PORT)
client.loop_forever()