import paho.mqtt.client as mqtt
import random, threading, json
from datetime import datetime

# Read config.ini file
with open("config/config.json") as json_config_data:
    config_data = json.load(json_config_data)
    mysqlConfig = config_data["mysql"]
    mqttConfig = config_data["mqtt"]

#====================================================
# MQTT Settings 
#MQTT_Broker = "iot.eclipse.org"
#MQTT_Broker = "test.mosquitto.org"
MQTT_Broker = mqttConfig['broker']
MQTT_Port = mqttConfig['port']
Keep_Alive_Interval = mqttConfig['alive_interval']
MQTT_Topic_State = "tele/sonofftest/STATE"
MQTT_Topic_Sensor = "tele/sonofftest/SENSOR"

#====================================================

def on_connect(client, userdata, rc):
    if rc != 0:
        pass
        print("Unable to connect to MQTT Broker...")
    else:
        print("Connected with MQTT Broker: ", str(MQTT_Broker))

def on_publish(client, userdata, mid):
    pass

def on_disconnect(client, userdata, rc):
    if rc !=0:
        pass

mqttc = mqtt.Client()
mqttc.on_connect = on_connect
mqttc.on_disconnect = on_disconnect
mqttc.on_publish = on_publish
mqttc.username_pw_set(mqttConfig['user'], mqttConfig['passwd'])
mqttc.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))

def publish_To_Topic(topic, message):
    mqttc.publish(topic,message)
    print("Published: ", str(message), " ", "on MQTT Topic: ", str(topic))
    print("")

#====================================================
# FAKE SENSOR 
# Dummy code used as Fake Sensor to publish some random values
# to MQTT Broker

toggle = 0

def publish_Fake_Sensor_Values_to_MQTT():
    threading.Timer(3.0, publish_Fake_Sensor_Values_to_MQTT).start()
    global toggle
    if toggle == 0:
        State_Fake_Value = float("{0:.2f}".format(random.uniform(50, 100)))

        State_Data = {}
        State_Data['Time'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S:%f")
        State_Data['Uptime'] = ""
        State_Data['Heap'] = ""
        State_Data['SleepMode'] = ""
        State_Data['Sleep'] = ""
        State_Data['LoadAvg'] = State_Fake_Value
        State_Data['POWER'] = "ON"
        State_Data['Wifi'] = {}
        State_Data['Wifi']['AP'] = ""
        State_Data['Wifi']['SSId'] = ""
        State_Data['Wifi']['BSSId'] = ""
        State_Data['Wifi']['Channel'] = ""
        State_Data['Wifi']['RSSI'] = ""
        State_Data['Wifi']['LinkCount'] = ""
        State_Data['Wifi']['Downtime'] = ""
        # State_Data['State'] = State_Fake_Value
        state_json_data = json.dumps(State_Data)

        print("Publishing fake State Value: ", str(State_Fake_Value), "...")
        publish_To_Topic(MQTT_Topic_State, state_json_data)
        toggle = 1

    else:
        Sensor_Fake_Value = float("{0:.2f}".format(random.uniform(1, 30)))

        Sensor_Data = {}
        Sensor_Data['Time'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S:%f")
        Sensor_Data['ENERGY'] = {}
        Sensor_Data['ENERGY']['TotalStartTime'] = ""
        Sensor_Data['ENERGY']['Total'] = Sensor_Fake_Value
        Sensor_Data['ENERGY']['Yesterday'] = ""
        Sensor_Data['ENERGY']['Today'] = Sensor_Fake_Value
        Sensor_Data['ENERGY']['Period'] = ""
        Sensor_Data['ENERGY']['Power'] = ""
        Sensor_Data['ENERGY']['ApparentPower'] = ""
        Sensor_Data['ENERGY']['ReactivePower'] = ""
        Sensor_Data['ENERGY']['Factor'] = ""
        Sensor_Data['ENERGY']['Voltage'] = ""
        Sensor_Data['ENERGY']['Current'] = ""
        # Sensor_Data['Sensor'] = Sensor_Fake_Value
        sensor_json_data = json.dumps(Sensor_Data)

        print("Publishing fake Sensor Value: ", str(Sensor_Fake_Value), "...")
        publish_To_Topic(MQTT_Topic_Sensor, sensor_json_data)
        toggle = 0

publish_Fake_Sensor_Values_to_MQTT()

#====================================================
