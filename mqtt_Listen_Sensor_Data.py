import paho.mqtt.client as mqtt
import json
from store_Sensor_Data_to_mysqlDB import sensor_Data_Handler

# Read config.ini file
with open("config/config.json") as json_config_data:
    config_data = json.load(json_config_data)
    mysqlConfig = config_data["mysql"]
    mqttConfig = config_data["mqtt"]

# MQTT Settings

MQTT_Broker = mqttConfig['broker']
MQTT_Port = mqttConfig['port']
MQTT_Topic = "#"
Keep_Alive_Interval = mqttConfig['alive_interval']

# Subscribe to all Sensors at Base Topic
def on_connect(mosq, obj, flags, rc):
	mqttc.subscribe(MQTT_Topic, 0)

# Save Data into DB Table
def on_message(mosq, obj, msg):
	print("MQTT Data Received...")
	print("MQTT Topic: ", msg.topic)
	print("Data: ", msg.payload)
	sensor_Data_Handler(msg.topic, msg.payload)

def on_subscribe(mosq, obj, mid, granted_qos):
	pass

mqttc = mqtt.Client()

# Assign event callbacks
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_subscribe = on_subscribe
mqttc.username_pw_set(mqttConfig['user'], mqttConfig['passwd'])

# Connect
mqttc.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))

# Continue the network loop
mqttc.loop_forever()
