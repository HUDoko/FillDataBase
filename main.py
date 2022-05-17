import sqlite3 as sql
import pandas as pd
import SensorInfo as si
import DateConvertor as dc


sensor_info = {"cabinet_210_bat": "http://sensors.mwlabs.ru/view/ABE679970903",
               "cabinet_210_wall": "http://sensors.mwlabs.ru/view/43E079971203",
               "cabinet_316_bat": "http://sensors.mwlabs.ru/view/0A7779970903",
               "cabinet_316_wall": "http://sensors.mwlabs.ru/view/94CC79971203",
               "cabinet_412a_bat": "http://sensors.mwlabs.ru/view/7D0679971403",
               "cabinet_412a_wall": "http://sensors.mwlabs.ru/view/A2BF79971203",
               "cabinet_420_bat": "http://sensors.mwlabs.ru/view/53C779971203",
               "cabinet_420_wall": "http://sensors.mwlabs.ru/view/6AE379971203",
               "cabinet_219_bat": "http://sensors.mwlabs.ru/view/BDF579970903",
               "cabinet_219_wall": "http://sensors.mwlabs.ru/view/F51A79970903"
               }

conn = sql.connect("weather.db")
cursor = conn.cursor()

Drop = "DROP TABLE IF EXISTS cabinets_tables;"
Create = "CREATE TABLE IF NOT EXISTS cabinets_tables (table_name text, type text, cab_name text );"
cursor.execute(Drop)
cursor.execute(Create)
conn.commit()

for sensor in sensor_info.keys():
    is_wall = "bat"
    if sensor.find('wall') != -1:
        is_wall = 'wall'
    sensor_num = sensor.split('_')[1]
    Insert = "INSERT INTO cabinets_tables VALUES ('{}', '{}', '{}');".format(sensor, is_wall, sensor_num)
    print(Insert)
    cursor.execute(Insert)
    conn.commit()
    cab_210 = si.get_sensor_info_from_url(sensor_info[sensor])
    dc.df_add_datetime(cab_210)
    cab_210 = cab_210.drop(labels=['date', 'time'], axis=1)
    # print(cab_210)
    Drop = "DROP TABLE IF EXISTS {};".format(sensor)
    cursor.execute(Drop)
    Create = "CREATE TABLE IF NOT EXISTS {} (date_time DATETIME , temp real);".format(sensor)
    cursor.execute(Create)
    print(Create)

    counter = 0
    for index, row in cab_210.iterrows():
        Insert = "INSERT INTO {}  VALUES ('{}', {});".format(sensor, row['date_time'], row['temp'])
        # print(Insert)
        cursor.execute(Insert)
        counter += 1
        # if counter % 500 == 0:
        #     conn.commit()

    conn.commit()
conn.close()







