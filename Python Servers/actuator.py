import sys
import re
import paho.mqtt.client as mqtt
import json
import requests

BROKER = "test.mosquitto.org"
PORT = 1883
TOPIC = "mtds/sensor/data/"

HUM_LIM = 60
TEMP_LIM = 30

global hum_act_state
hum_act_state = 0

global temp_act_state
temp_act_state = 0

global init_location
init_location = "#"

def send_telegram_notification(text):
    #requests.get('https://api.telegram.org/bot5456641962:AAEzqKwRheRBsig65-4fjFd8ri2KbXWr_x4/sendMessage?chat_id=-781132811&text='+text)


def print_init_state(val):
    if val == 0:
        return "off"
    else:
        return "on"


def perform_operation_humidity(hum, location):
    global hum_act_state
    if hum > HUM_LIM and hum_act_state == 0:
        hum_act_state = 1
        send_telegram_notification("Humidity actuator " + location.replace("mtds/sensor/data/", "").replace("/", ".") + " on - Hum is " + str(hum))
        print("Humidity actuator on - Hum is " + str(hum))
    elif hum <= HUM_LIM and hum_act_state == 0:
        print("Humidity no changes")
    elif hum < HUM_LIM and hum_act_state == 1:
        hum_act_state = 0
        send_telegram_notification(
            "Humidity actuator " + location.replace("mtds/sensor/data/", "").replace("/", ".") + " off - Hum is " + str(
                hum))
        print("Humidity actuator off - Hum is " + str(hum))
    elif hum >= HUM_LIM and hum_act_state == 1:
        print("Humidity no changes")


def perform_operation_temperature(temp, location):
    global temp_act_state
    if temp > TEMP_LIM and temp_act_state == 0:
        temp_act_state = 1
        send_telegram_notification(
            "Temperature actuator " + location.replace("mtds/sensor/data/", "").replace("/", ".") + " on - Temp is " + str(
                temp))
        print("Temperature actuator on - Temp(°C) is " + str(temp))
    elif temp <= TEMP_LIM and temp_act_state == 0:
        print("Temperature no changes")
    elif temp < TEMP_LIM and temp_act_state == 1:
        temp_act_state = 0
        send_telegram_notification(
            "Temperature actuator " + location.replace("mtds/sensor/data/", "").replace("/", ".") + " off - Temp is " + str(
                temp))
        print("Temperature actuator off - Temp(°C) is " + str(temp))
    elif temp >= TEMP_LIM and temp_act_state == 1:
        print("Temperature no changes")

def parse_incoming_message(message, topic):
    data = json.loads(message)
    hum = data['d']['hum']
    temp = data['d']['temp_c']
    perform_operation_temperature(temp, topic)
    perform_operation_humidity(hum, topic)


def on_connect(client, userdata, flags, rc):
    print("Connected with result code {0}".format(str(rc)))
    print("Temperature actuator is " + print_init_state(temp_act_state))
    print("Humidity actuator is " + print_init_state(hum_act_state))
    client.subscribe(TOPIC+init_location)


def on_message(client, userdata, msg):
    parse_incoming_message(msg.payload.decode(), msg.topic)


#Script entry point
if __name__ == "__main__":
    if len(sys.argv) > 1:
        location = sys.argv[1]
        pattern = re.compile("^([A-Z0-9]+.[A-Z0-9]+.[A-Z0-9]+.[A-Z0-9]+)$")
        if pattern.match(location):
            location = location.replace(".", "/")
            init_location = location
        else:
            print("Invalid location given. Correct format is N.B.F.R \n Aborting...")
            exit(0)
    else:
        print("No location given, the wildcard will be used")
    print("Subscribing to " + TOPIC + init_location)
    client = mqtt.Client("mtds-actuator-0001")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, PORT)
    client.loop_forever()
