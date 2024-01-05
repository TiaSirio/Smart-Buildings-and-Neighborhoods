import paho.mqtt.client as paho

BROKER = "server.matmacsystem.it"
PORT = 1883
TOPIC = "mtds/sensor/conf"
DATA = 'mtdssens-0006'

def on_publish(client,userdata,result):
    print("data published \n")
    pass


mqtt_client= paho.Client("mtds-test-publish-client")
mqtt_client.on_publish = on_publish
mqtt_client.connect(BROKER,PORT)
ret = mqtt_client.publish(TOPIC, DATA)