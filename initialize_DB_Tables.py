#------------------------------------------
#--- Author: Pradeep Singh
#--- Date: 20th January 2017
#--- Version: 1.0
#--- Python Ver: 2.7
#--- Details At: https://iotbytes.wordpress.com/store-mqtt-data-from-sensors-into-sql-database/
#------------------------------------------

import sqlite3

# SQLite DB Name
DB_Name =  "IoT.db"

# SQLite DB Table Schema
TableSchema="""
drop table if exists State_Data ;
create table State_Data (
  id integer primary key autoincrement,
  DeviceId text,
  Time text,
  Uptime text,
  Heap text,
  SleepMode text,
  Sleep text,
  LoadAvg text,
  POWER text,
  Wifi_AP text,
  Wifi_SSId text,
  Wifi_BSSId text,
  Wifi_Channel text,
  Wifi_RSSI text,
  Wifi_LinkCount text,
  Wifi_Downtime text
);

drop table if exists Sensor_Data ;
create table Sensor_Data (
  id integer primary key autoincrement,
  DeviceId text,
  Time text,
  ENERGY_TotalStartTime text,
  ENERGY_Total text,
  ENERGY_Yesterday text,
  ENERGY_Today text,
  ENERGY_Period text,
  ENERGY_Power text,
  ENERGY_ApparentPower text,
  ENERGY_ReactivePower text,
  ENERGY_Factor text,
  ENERGY_Voltage text,
  ENERGY_Current text
);
"""

#Connect or Create DB File
conn = sqlite3.connect(DB_Name)
curs = conn.cursor()

#Create Tables
sqlite3.complete_statement(TableSchema)
curs.executescript(TableSchema)

#Close DB
curs.close()
conn.close()

