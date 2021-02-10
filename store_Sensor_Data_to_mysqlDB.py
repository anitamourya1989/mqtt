#------------------------------------------
#--- Author: Pradeep Singh
#--- Date: 20th January 2017
#--- Version: 1.0
#--- Python Ver: 2.7
#--- Details At: https://iotbytes.wordpress.com/store-mqtt-data-from-sensors-into-sql-database/
#------------------------------------------

import json
import mysql.connector
from mysql.connector import Error
# import pytz
# from datetime import datetime

#===============================================================
# Database Manager Class

class DatabaseManager():
	def __init__(self):

		try:
			self.conn = mysql.connector.connect(host = "localhost",database="techdeve_homeauto",user = "techdeve_homeauto",passwd = "vg&faA=2byWP")
			if self.conn.is_connected():
				self.cur = self.conn.cursor()
				return self.cur
				# db_Info = self.conn.get_server_info()
				# print("Connected to MySQL Server version ", db_Info)
				# self.cur.execute("select database();")
				# record = self.cur.fetchone()
				# print("You're connected to database: ", record)

		except Error as e:
			print("Error while connecting to MySQL", e)

		# finally:
			# if (conn.is_connected()):
			# 	cur.close()
			# 	conn.close()
			# 	print("MySQL conn is closed")

	def add_del_update_db_record(self,sql_query,args=()):
		self.cur.execute(sql_query,args)
		self.conn.commit()

	def executeQuery(self,sql_query):
		self.cur.execute(sql_query)
		# return self.cur.fetchone()

	def rows(self):
		return self.cur.rowcount

	# def __init__(self):
	# 	self.conn = sqlite3.connect(DB_Name)
	# 	self.conn.execute('pragma foreign_keys = on')
	# 	self.conn.commit()
	# 	self.cur = self.conn.cursor()

	# def add_del_update_db_record(self, sql_query, args=()):
	# 	self.cur.execute(sql_query, args)
	# 	self.conn.commit()
	# 	return

	def __del__(self):
		self.cur.close()
		self.conn.close()

#===============================================================
# Functions to push Sensor Data into Database

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

	#Push into DB Table
	# dbObj = DatabaseManager()
	# print 'yeah 5'
	# dbObj.executeQuery("desc devices_state_data")
	# dbObj.add_del_update_db_record("INSERT INTO devices_state_data (DeviceId, `Time`, Uptime, Heap, SleepMode, Sleep, LoadAvg, POWER, Wifi_AP, Wifi_SSId, Wifi_BSSId, Wifi_Channel, Wifi_RSSI, Wifi_LinkCount, Wifi_Downtime) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",[DeviceId, Time, Uptime, Heap, SleepMode, Sleep, LoadAvg, POWER, Wifi_AP, Wifi_SSId, Wifi_BSSId, Wifi_Channel, Wifi_RSSI, Wifi_LinkCount, Wifi_Downtime])

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
		# print "Inserted devices_state_data into Database."
		# print ""
	except Error as e:
		print("Executing while connecting to MySQL", e)
		conn.rollback()

	# del dbObj
	# print "Inserted devices_state_data into Database."
	print ""

# Function to save Sensor to DB Table
def DHT22_Sensor_Data_Handler(DeviceId, jsonData, Topic):
	#Parse Data
	# print 'yeah 6'
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

	#Push into DB Table
	# dbObj = DatabaseManager()
	# print 'yeah 7'
	# dbObj.add_del_update_db_record("INSERT INTO devices_sensor_data (DeviceId, `Time`, ENERGY_TotalStartTime, ENERGY_Total, ENERGY_Yesterday, ENERGY_Today, ENERGY_Period, ENERGY_Power, ENERGY_ApparentPower, ENERGY_ReactivePower, ENERGY_Factor, ENERGY_Voltage, ENERGY_Current) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",[DeviceId, Time, ENERGY_TotalStartTime, ENERGY_Total, ENERGY_Yesterday, ENERGY_Today, ENERGY_Period, ENERGY_Power, ENERGY_ApparentPower, ENERGY_ReactivePower, ENERGY_Factor, ENERGY_Voltage, ENERGY_Current])

	try:
		conn = mysql.connector.connect(host="localhost",database="techdeve_homeauto",user="techdeve_homeauto",passwd="vg&faA=2byWP")
		if conn.is_connected():
			cur = conn.cursor()

	except Error as e:
		print("Error while connecting to MySQL", e)

	# cur.execute("select * from devices_sensor_data")
	# print cur.fetchall()

	sql = ("INSERT INTO devices_sensor_data "
		"(DeviceType, DeviceId, `Time`, ENERGY_TotalStartTime, ENERGY_Total, ENERGY_Yesterday, ENERGY_Today, ENERGY_Period, ENERGY_Power, ENERGY_ApparentPower, ENERGY_ReactivePower, ENERGY_Factor, ENERGY_Voltage, ENERGY_Current)" 
		" VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
	val = (DeviceType, DeviceId, Time, ENERGY_TotalStartTime, ENERGY_Total, ENERGY_Yesterday, ENERGY_Today, ENERGY_Period, ENERGY_Power, ENERGY_ApparentPower, ENERGY_ReactivePower, ENERGY_Factor, ENERGY_Voltage, ENERGY_Current)
	try:
		# print sql
		# print val
		cur.execute(sql,val)
		# print "yeahh"
		conn.commit()
		print "Inserted devices_sensor_data into Database."
		print ""
	except Error as e:
		print("Executing while connecting to MySQL", e)
		conn.rollback()

	# del dbObj
	# print "Inserted devices_sensor_data into Database."
	print ""

#===============================================================
# Master Function to Select DB Funtion based on MQTT Topic

def sensor_Data_Handler(Topic, jsonData):
	DeviceId = Topic.split('/')
	# print 'yeah 1'
	if (Topic.find("STATE") != -1):
		# print 'yeah 2'
		DHT22_State_Data_Handler(DeviceId[1], jsonData, Topic)
	elif (Topic.find("SENSOR") != -1):
		# print 'yeah 3'
		DHT22_Sensor_Data_Handler(DeviceId[1], jsonData, Topic)
	elif (Topic.find("iit") != -1):
		# print 'yeah iit'
		DHT22_Sensor_Data_Handler(DeviceId[1], jsonData, Topic)
	elif (Topic.find("esp8266") != -1):
		# print 'yeah esp8266'
		DHT22_Sensor_Data_Handler(DeviceId[1], jsonData, Topic)

#===============================================================

