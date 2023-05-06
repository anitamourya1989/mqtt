#------------------------------------------
#--- Author: Anita Mourya
#--- Date: 20th January 2019
#--- Version: 1.0
#--- Python Ver: 2.7
#--- Details At: http://www.anitamourya.com
#------------------------------------------

import json
import mysql.connector
from mysql.connector import Error
import pytz
from datetime import datetime

# Function to save State to DB Table
def DHT22_State_Data_Handler(DeviceId, jsonData, Topic):
    #Parse Data
    json_Dict = json.loads(jsonData)
    # print 'yeah 4'
    DeviceId = Topic
    Time = json_Dict['Time']
    Uptime = json_Dict['Uptime']
    Heap = json_Dict['Heap']
    SleepMode = json_Dict['SleepMode']
    Sleep = json_Dict['Sleep']
    LoadAvg = json_Dict['LoadAvg']
    POWER = json_Dict['POWER']
    Wifi_AP = json_Dict['Wifi']['AP']
    Wifi_SSId = json_Dict['Wifi']['SSId']
    Wifi_BSSId = json_Dict['Wifi']['BSSId']
    Wifi_Channel = json_Dict['Wifi']['Channel']
    Wifi_RSSI = json_Dict['Wifi']['RSSI']
    Wifi_LinkCount = json_Dict['Wifi']['LinkCount']
    Wifi_Downtime = json_Dict['Wifi']['Downtime']

    # tz_India = pytz.timezone('Asia/Kolkata')
    # datetime_India = datetime.now(tz_India)
    # created_at = datetime_India.strftime("%Y-%m-%d %H:%M:%S")
    created_at = ''

    try:
        conn = mysql.connector.connect(host="localhost",database="techdeve_homeauto",user="techdeve_homeauto",passwd="vg&faA=2byWP")
        if conn.is_connected():
            cur = conn.cursor()

    except Error as e:
        print("Error while connecting to MySQL", e)

    # cur.execute("select * from devices_state_data")
    # print cur.fetchall()

    sql = ("INSERT INTO devices_state_data "
        "(DeviceId, `Time`, Uptime, Heap, SleepMode, Sleep, LoadAvg, POWER, Wifi_AP, Wifi_SSId, Wifi_BSSId, Wifi_Channel, Wifi_RSSI, Wifi_LinkCount, Wifi_Downtime)"
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    val = (DeviceId, Time, Uptime, Heap, SleepMode, Sleep, LoadAvg, POWER, Wifi_AP, Wifi_SSId, Wifi_BSSId, Wifi_Channel, Wifi_RSSI, Wifi_LinkCount, Wifi_Downtime)

    try:
        cur.execute(sql,val)
        conn.commit()
        # print("Inserted devices_state_data into Database.")
        # print("")
    except Error as e:
        print("Executing while connecting to MySQL", e)
        conn.rollback()

    # del dbObj
    # print("Inserted devices_state_data into Database.")
    print("")

# Function to save Regular, IIT and esp8266 data to DB
def DHT22_Sensor_Data_Handler(DeviceId, jsonData, Topic):
    #Parse Data
    # print("yeah 6")
    json_Dict = json.loads(jsonData)
    if 'ENERGY' in json_Dict:
        # DeviceId = DeviceId
        DeviceId = Topic
        DeviceType = 'Regular'
        Time = json_Dict['Time']
        ENERGY_TotalStartTime = json_Dict['ENERGY']['TotalStartTime']
        ENERGY_Total = json_Dict['ENERGY']['Total']
        ENERGY_Yesterday = json_Dict['ENERGY']['Yesterday']
        ENERGY_Today = json_Dict['ENERGY']['Today']
        ENERGY_Period = json_Dict['ENERGY']['Period']
        ENERGY_Power = json_Dict['ENERGY']['Power']
        ENERGY_ApparentPower = json_Dict['ENERGY']['ApparentPower']
        ENERGY_ReactivePower = json_Dict['ENERGY']['ReactivePower']
        ENERGY_Factor = json_Dict['ENERGY']['Factor']
        ENERGY_Voltage = json_Dict['ENERGY']['Voltage']
        ENERGY_Current = json_Dict['ENERGY']['Current']

        # tz_India = pytz.timezone('Asia/Kolkata')
        # datetime_India = datetime.now(tz_India)
        # created_at = datetime_India.strftime("%Y-%m-%d %H:%M:%S")
        created_at = ''

    if 'active_power' in json_Dict:
        # DeviceId = DeviceId
        DeviceId = Topic
        DeviceType = 'IIT'
        Time = json_Dict.get('time')
        ENERGY_TotalStartTime = ''
        ENERGY_Total = json_Dict.get('energy_consumed')
        ENERGY_Yesterday = ''
        ENERGY_Today = ''
        ENERGY_Period = ''
        ENERGY_Power = json_Dict.get('active_power')
        ENERGY_ApparentPower = json_Dict.get('apparent_power')
        ENERGY_ReactivePower = ''
        ENERGY_Factor = ''
        ENERGY_Voltage = json_Dict.get('voltage')
        ENERGY_Current = json_Dict.get('current')

        # tz_India = pytz.timezone('Asia/Kolkata')
        # datetime_India = datetime.now(tz_India)
        # created_at = datetime_India.strftime("%Y-%m-%d %H:%M:%S")
        created_at = ''

    if 'Freq' in json_Dict:
        # DeviceId = DeviceId
        DeviceId = Topic
        DeviceType = 'esp8266'
        Time = json_Dict.get('Time')
        ENERGY_TotalStartTime = ''
        ENERGY_Total = json_Dict.get('Energy')
        ENERGY_Yesterday = ''
        ENERGY_Today = ''
        ENERGY_Period = json_Dict.get('Freq')
        ENERGY_Power = json_Dict.get('Power')
        ENERGY_ApparentPower = ''
        ENERGY_ReactivePower = ''
        ENERGY_Factor = json_Dict.get('P.F.')
        ENERGY_Voltage = json_Dict.get('Voltage')
        ENERGY_Current = json_Dict.get('Current')

        # tz_India = pytz.timezone('Asia/Kolkata')
        # datetime_India = datetime.now(tz_India)
        # created_at = datetime_India.strftime("%Y-%m-%d %H:%M:%S")
        created_at = ''

    try:
        conn = mysql.connector.connect(host="localhost",database="techdeve_homeauto",user="techdeve_homeauto",passwd="vg&faA=2byWP")
        if conn.is_connected():
            cur = conn.cursor()

    except Error as e:
        print("Error while connecting to MySQL", e)

    # print cur.fetchall()

    sql = ("INSERT INTO devices_sensor_data "
        "(DeviceType, DeviceId, `Time`, ENERGY_TotalStartTime, ENERGY_Total, ENERGY_Yesterday, ENERGY_Today, ENERGY_Period, ENERGY_Power, ENERGY_ApparentPower, ENERGY_ReactivePower, ENERGY_Factor, ENERGY_Voltage, ENERGY_Current)"
        " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    val = (DeviceType, DeviceId, Time, ENERGY_TotalStartTime, ENERGY_Total, ENERGY_Yesterday, ENERGY_Today, ENERGY_Period, ENERGY_Power, ENERGY_ApparentPower, ENERGY_ReactivePower, ENERGY_Factor, ENERGY_Voltage, ENERGY_Current)

    try:
        # print sql
        # print val
        cur.execute(sql,val)
        # print("yeahh")
        conn.commit()
        print("Inserted devices_sensor_data into Database.")
        print("")
    except Error as e:
        print("Executing while connecting to MySQL", e)
        conn.rollback()

    # del dbObj
    # print("Inserted devices_sensor_data into Database.")
    print("")

# Function to save esp8266_meter3Phase data to DB
def DHT22_3phase_Sensor_Data_Handler(DeviceId, jsonData, Topic):
    #Parse Data
    json_Dict = json.loads(jsonData)
    # print 'yeah 4'
    DeviceId = Topic
    DeviceType = 'esp8266_meter3Phase'
    Time = json_Dict.get('Time')
    ENERGY_TotalStartTime = ''
    ENERGY_Total = json_Dict.get('D31')
    # Check if the topic is allowed
    allowed_topics = ["esp8266/meter3Phase4", "esp8266/meter3Phase5"]
    if Topic in allowed_topics:
        ENERGY_Total /= 1000
    ENERGY_Yesterday = ''
    ENERGY_Today = ''
    ENERGY_Period = json_Dict.get('D8')
    ENERGY_Power = json_Dict.get('D2')
    ENERGY_ApparentPower = json_Dict.get('D1')
    ENERGY_ReactivePower = json_Dict.get('D3')
    ENERGY_Factor = json_Dict.get('D4')
    ENERGY_Voltage = json_Dict.get('D6')
    ENERGY_Current = json_Dict.get('D7')
    line_to_line_voltage = json_Dict.get('D5')
    rphase_apparent_power = json_Dict.get('D9')
    rphase_active_power = json_Dict.get('D10')
    rphase_reactive_power = json_Dict.get('D11')
    rphase_power_factor = json_Dict.get('D12')
    r_yphase_voltage = json_Dict.get('D13')
    rphase_to_neutral_voltage = json_Dict.get('D14')
    rphase_current = json_Dict.get('D15')
    yphase_apparent_power = json_Dict.get('D16')
    yphase_active_power = json_Dict.get('D17')
    yphase_reactive_power = json_Dict.get('D18')
    yphase_power_factor = json_Dict.get('D19')
    y_bphase_voltage = json_Dict.get('D20')
    yphase_to_neutral_voltage = json_Dict.get('D21')
    yphase_current = json_Dict.get('D22')
    bphase_apparent_power = json_Dict.get('D23')
    bphase_active_power = json_Dict.get('D24')
    bphase_reactive_power = json_Dict.get('D25')
    bphase_power_factor = json_Dict.get('D26')
    b_rphase_voltage = json_Dict.get('D27')
    bphase_to_neutral_voltage = json_Dict.get('D28')
    bphase_current = json_Dict.get('D29')
    forward_apparent_energy = json_Dict.get('D30')
    # forward_active_energy = json_Dict.get('D31')
    forward_reactive_energy = json_Dict.get('D32')
    voltage_thd_rphase = json_Dict.get('D33')
    voltage_thd_yphase = json_Dict.get('D34')
    voltage_thd_bphase = json_Dict.get('D35')
    current_thd_rphase = json_Dict.get('D36')
    current_thd_yphase = json_Dict.get('D37')
    current_thd_bphase = json_Dict.get('D38')

    # tz_India = pytz.timezone('Asia/Kolkata')
    # datetime_India = datetime.now(tz_India)
    # created_at = datetime_India.strftime("%Y-%m-%d %H:%M:%S")
    created_at = ''

    try:
        conn = mysql.connector.connect(host="localhost",database="techdeve_homeauto",user="techdeve_homeauto",passwd="vg&faA=2byWP")
        if conn.is_connected():
            cur = conn.cursor()

    except Error as e:
        print("Error while connecting to MySQL", e)

    # print cur.fetchall()

    sql = ("INSERT INTO devices_sensor_data "
        "(`DeviceType`, `DeviceId`, `Time`, `ENERGY_TotalStartTime`, `ENERGY_Total`, `ENERGY_Yesterday`, `ENERGY_Today`, `ENERGY_Period`, `ENERGY_Power`, `ENERGY_ApparentPower`, `ENERGY_ReactivePower`, `ENERGY_Factor`, `ENERGY_Voltage`, `ENERGY_Current`, `line_to_line_voltage`, `rphase_apparent_power`, `rphase_active_power`, `rphase_reactive_power`, `rphase_power_factor`, `r_yphase_voltage`, `rphase_to_neutral_voltage`, `rphase_current`, `yphase_apparent_power`, `yphase_active_power`, `yphase_reactive_power`, `yphase_power_factor`, `y_bphase_voltage`, `yphase_to_neutral_voltage`, `yphase_current`, `bphase_apparent_power`, `bphase_active_power`, `bphase_reactive_power`, `bphase_power_factor`, `b_rphase_voltage`, `bphase_to_neutral_voltage`, `bphase_current`, `forward_apparent_energy`, `forward_reactive_energy`, `voltage_thd_rphase`, `voltage_thd_yphase`, `voltage_thd_bphase`, `current_thd_rphase`, `current_thd_yphase`, `current_thd_bphase`)"
        " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    val = (DeviceType, DeviceId, Time, ENERGY_TotalStartTime, ENERGY_Total, ENERGY_Yesterday, ENERGY_Today, ENERGY_Period, ENERGY_Power, ENERGY_ApparentPower, ENERGY_ReactivePower, ENERGY_Factor, ENERGY_Voltage, ENERGY_Current, line_to_line_voltage, rphase_apparent_power, rphase_active_power, rphase_reactive_power, rphase_power_factor, r_yphase_voltage, rphase_to_neutral_voltage, rphase_current, yphase_apparent_power, yphase_active_power, yphase_reactive_power, yphase_power_factor, y_bphase_voltage, yphase_to_neutral_voltage, yphase_current, bphase_apparent_power, bphase_active_power, bphase_reactive_power, bphase_power_factor, b_rphase_voltage, bphase_to_neutral_voltage, bphase_current, forward_apparent_energy, forward_reactive_energy, voltage_thd_rphase, voltage_thd_yphase, voltage_thd_bphase, current_thd_rphase, current_thd_yphase, current_thd_bphase)

    try:
        # print sql
        # print val
        cur.execute(sql,val)
        # print("yeahh")
        conn.commit()
        print("Inserted devices_sensor_data into Database from 3phase.")
        print("")
    except Error as e:
        print("Executing while connecting to MySQL", e)
        conn.rollback()

    # del dbObj
    # print("Inserted devices_state_data into Database.")
    print("")

# Function to save LCT data to DB
def LCT_Sensor_Data_Handler(DeviceId, jsonData, Topic):
    #Parse Data
    # print("LCT inside")
    json_Dict = json.loads(jsonData)
    DeviceArr = Topic.split('/')
    DeviceId = Topic
    DeviceType = DeviceArr[0]
    UT = json_Dict.get('UT')

    # Time
    tz = pytz.timezone('Asia/Kolkata')
    dt = datetime.fromtimestamp(UT, tz)
    Time = dt.strftime('%Y-%m-%d %H:%M:%S')# %Z%z

    if 'BV' in json_Dict:
        ENERGY_Current = json_Dict.get('C')
        ENERGY_Factor = json_Dict.get('PF')
        battery_voltage = json_Dict.get('BV')
        temp_1 = ""
        temp_2 = ""
        temp_3 = ""

        # tz_India = pytz.timezone('Asia/Kolkata')
        # datetime_India = datetime.now(tz_India)
        # created_at = datetime_India.strftime("%Y-%m-%d %H:%M:%S")
        created_at = ''

    if 'T1' in json_Dict:
        temp_1 = json_Dict.get('T1')
        temp_2 = json_Dict.get('T2')
        temp_3 = json_Dict.get('T3')
        ENERGY_Current = ""
        ENERGY_Factor = ""
        battery_voltage = ""

        # tz_India = pytz.timezone('Asia/Kolkata')
        # datetime_India = datetime.now(tz_India)
        # created_at = datetime_India.strftime("%Y-%m-%d %H:%M:%S")
        created_at = ''

    try:
        conn = mysql.connector.connect(host="localhost",database="techdeve_homeauto",user="techdeve_homeauto",passwd="vg&faA=2byWP")
        if conn.is_connected():
            cur = conn.cursor()

    except Error as e:
        print("Error while connecting to MySQL", e)

    # print cur.fetchall()

    sql = ("INSERT INTO devices_sensor_data "
        "(DeviceType, DeviceId, `Time`, ENERGY_Current, ENERGY_Factor, battery_voltage, temp_1, temp_2, temp_3)"
        " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
    val = (DeviceType, DeviceId, Time, ENERGY_Current, ENERGY_Factor, battery_voltage, temp_1, temp_2, temp_3)

    try:
        # print sql
        # print val
        cur.execute(sql,val)
        # print("yeahh")
        conn.commit()
        print("Inserted devices_sensor_data into Database from LCT.")
        print("")
    except Error as e:
        print("Executing while connecting to MySQL", e)
        conn.rollback()

    # del dbObj
    # print("Inserted devices_state_data into Database.")
    print("")

#===============================================================
# Master Function to Select DB Function based on MQTT Topic

def sensor_Data_Handler(Topic, jsonData):
    DeviceId = Topic.split('/')
    # print("in function")
    if (Topic.find("STATE") != -1):
        # print("yeah 2")
        DHT22_State_Data_Handler(DeviceId[1], jsonData, Topic)
    elif (Topic.find("SENSOR") != -1):
        # print("yeah 3")
        DHT22_Sensor_Data_Handler(DeviceId[1], jsonData, Topic)
    elif (Topic.find("iit") != -1):
        # print("yeah iit")
        DHT22_Sensor_Data_Handler(DeviceId[1], jsonData, Topic)
    elif (Topic.find("esp8266/meter3Phase") != -1):
        # print("yeah meter3Phase1")
        DHT22_3phase_Sensor_Data_Handler(DeviceId[1], jsonData, Topic)
    elif (Topic.find("LCT") != -1):
        # print("yeah LCT1C")
        LCT_Sensor_Data_Handler(DeviceId[1], jsonData, Topic)
    elif (Topic.find("esp8266") != -1):
        # print("yeah esp8266")
        DHT22_Sensor_Data_Handler(DeviceId[1], jsonData, Topic)

#===============================================================
