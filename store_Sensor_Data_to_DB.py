#------------------------------------------
#--- Author: Pradeep Singh
#--- Date: 20th January 2017
#--- Version: 1.0
#--- Python Ver: 2.7
#--- Details At: https://iotbytes.wordpress.com/store-mqtt-data-from-sensors-into-sql-database/
#------------------------------------------

import json
import sqlite3

# SQLite DB Name
DB_Name =  "IoT.db"

#===============================================================
# Database Manager Class

class DatabaseManager():
	def __init__(self):
		self.conn = sqlite3.connect(DB_Name)
		self.conn.execute('pragma foreign_keys = on')
		self.conn.commit()
		self.cur = self.conn.cursor()
		
	def add_del_update_db_record(self, sql_query, args=()):
		self.cur.execute(sql_query, args)
		self.conn.commit()
		return

	def __del__(self):
		self.cur.close()
		self.conn.close()

#===============================================================
# Functions to push Sensor Data into Database

# Function to save State to DB Table
def DHT22_State_Data_Handler(DeviceId, jsonData):
	#Parse Data 
	json_Dict = json.loads(jsonData)

	print json.dumps(json_Dict)
	DeviceId = DeviceId
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
	
	#Push into DB Table
	dbObj = DatabaseManager()
	dbObj.add_del_update_db_record("insert into State_Data (DeviceId, Time, Uptime, Heap, SleepMode, Sleep, LoadAvg, POWER, Wifi_AP, Wifi_SSId, Wifi_BSSId, Wifi_Channel, Wifi_RSSI, Wifi_LinkCount, Wifi_Downtime) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",[DeviceId, Time, Uptime, Heap, SleepMode, Sleep, LoadAvg, POWER, Wifi_AP, Wifi_SSId, Wifi_BSSId, Wifi_Channel, Wifi_RSSI, Wifi_LinkCount, Wifi_Downtime])
	del dbObj
	print "Inserted State_Data into Database."
	print ""

# Function to save Sensor to DB Table
def DHT22_Sensor_Data_Handler(DeviceId, jsonData):
	#Parse Data
	json_Dict = json.loads(jsonData)
	DeviceId = DeviceId
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

	#Push into DB Table
	dbObj = DatabaseManager()
	dbObj.add_del_update_db_record("insert into Sensor_Data (DeviceId, Time, ENERGY_TotalStartTime, ENERGY_Total, ENERGY_Yesterday, ENERGY_Today, ENERGY_Period, ENERGY_Power, ENERGY_ApparentPower, ENERGY_ReactivePower, ENERGY_Factor, ENERGY_Voltage, ENERGY_Current) values (?,?,?,?,?,?,?,?,?,?,?,?,?)",[DeviceId, Time, ENERGY_TotalStartTime, ENERGY_Total, ENERGY_Yesterday, ENERGY_Today, ENERGY_Period, ENERGY_Power, ENERGY_ApparentPower, ENERGY_ReactivePower, ENERGY_Factor, ENERGY_Voltage, ENERGY_Current])
	del dbObj
	print "Inserted Sensor_Data into Database."
	print ""

#===============================================================
# Master Function to Select DB Funtion based on MQTT Topic

def sensor_Data_Handler(Topic, jsonData):
	DeviceId = Topic.split('/')
	if (Topic.find("STATE") != -1):
		DHT22_State_Data_Handler(DeviceId[1], jsonData)
	elif (Topic.find("SENSOR") != -1):
		DHT22_Sensor_Data_Handler(DeviceId[1], jsonData)	

#===============================================================

