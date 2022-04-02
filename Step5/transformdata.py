"""
Script to transform raw csv files into clean final format for loading
"""

import mysql.connector
from mysql.connector import errorcode
from pathlib import Path
import os
import glob
import pandas as pd
from sqlalchemy import  create_engine, types
import csv

def get_db_connection():
    """
    2.1 Setup database connection
    In order to make a query against the database table, we need to first connect to it. A connection
    can be established only when the user provides the proper target host, port, and user
    credentials
    """
    connection = None
    try:
        connection = mysql.connector.connect(user='root',password='',host='127.0.0.1',port='3306')
    except Exception as error:
        print("Error while connecting to database for job tracker", error)
    return connection

def initialize_database():
    cnx = get_db_connection()
    cursor = cnx.cursor()
    try:
        cursor.execute("DROP DATABASE IF EXISTS {}".format(DB_NAME))
        cursor.execute("CREATE DATABASE {}".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)
    try:
        cursor.execute("USE {}".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Database {} does not exists.".format(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor)
            print("Database {} created successfully.".format(DB_NAME))
            cnx.database = DB_NAME
        else:
            print(err)
            exit(1)

    TABLES = {}
    TABLES['details'] = (
        "CREATE TABLE details ("
        "  BEGIN_YEARMONTH VARCHAR(6),"
        "  BEGIN_DAY VARCHAR(2),"
        "  BEGIN_TIME VARCHAR(4),"
        "  END_YEARMONTH VARCHAR(6),"
        "  END_DAY VARCHAR(2),"
        "  END_TIME VARCHAR(4),"
        "  EPISODE_ID INT,"
        "  EVENT_ID INT,"
        "  STATE TEXT,"
        "  STATE_FIPS INT,"
        "  EVENT_TYPE TEXT,"
        "  CZ_TYPE VARCHAR(1),"
        "  CZ_FIPS INT,"
        "  CZ_NAME TEXT,"
        "  WFO VARCHAR(3),"
        "  CZ_TIMEZONE TEXT,"
        "  INJURIES_DIRECT INT,"
        "  INJURIES_INDIRECT INT,"
        "  DEATHS_DIRECT INT,"
        "  DEATHS_INDIRECT INT,"
        "  DAMAGE_PROPERTY TEXT,"
        "  DAMAGE_CROPS TEXT,"
        "  SOURCE TEXT,"
        "  MAGNITUDE DEC,"
        "  MAGNITUDE_TYPE VARCHAR(2),"
        "  FLOOD_CAUSE TEXT,"
        "  CATEGORY INT,"
        "  TOR_F_SCALE VARCHAR(3),"
        "  TOR_LENGTH DEC,"
        "  TOR_WIDTH INT,"
        "  TOR_OTHER_WFO VARCHAR(3),"
        "  TOR_OTHER_CZ_STATE VARCHAR(2),"
        "  TOR_OTHER_CZ_FIPS INT,"
        "  TOR_OTHER_CZ_NAME TEXT,"
        "  BEGIN_RANGE INT,"
        "  BEGIN_AZIMUTH VARCHAR(6),"
        "  BEGIN_LOCATION TEXT,"
        "  END_RANGE INT,"
        "  END_AZIMUTH VARCHAR(6),"
        "  END_LOCATION TEXT,"
        "  BEGIN_LAT DEC,"
        "  BEGIN_LON DEC,"
        "  END_LAT DEC,"
        "  END_LON DEC,"
        "  EPISODE_NARRATIVE TEXT,"
        "  EVENT_NARRATIVE TEXT"
        ")")

    TABLES['fatalities'] = (
        "CREATE TABLE fatalities ("
        "  FATALITY_ID INT,"
        "  EVENT_ID INT,"
        "  FATALITY_TYPE VARCHAR(1),"
        "  FATALITY_DATE VARCHAR(19),"
        "  FATALITY_AGE INT DEFAULT NULL,"
        "  FATALITY_SEX ENUM('M','F'),"
        "  FATALITY_LOCATION TEXT,"
        "  EVENT_YEARMONTH VARCHAR(6)"
        ")")

    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

    return cnx

def load_csv(filepath, table):
    df = pd.read_csv(filepath, sep = ',')
    df.to_sql(table,con=engine,index=False,if_exists='append')

def cut_details(filepath):
    #details needs cols 11 12 18 20 51 removed
    os.system("csvcut --columns=1,2,3,4,5,6,7,8,9,10,13,14,15,16,17,19,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50 " + filepath + " > details" + filepath[-18:])

def cut_fatals(filepath):
    #fatalities needs cols 1 2 3 removed
    os.system("csvcut --columns=4,5,6,7,8,9,10,11 " + filepath + " > fatalities" + filepath[-18:])

if __name__ == "__main__":
    print('***** Start of data transform script *****\n')
    DB_NAME = 'Capstone'
    cnx = initialize_database()
    engine = create_engine('mysql+pymysql://root@localhost/Capstone')
    detlist = sorted(glob.glob("/home/conner/Capstone/data/unzipped/storm_details_*"))
    fatlist = sorted(glob.glob("/home/conner/Capstone/data/unzipped/storm_fatalities_*"))
    for d in detlist:
        print('Cutting ' + d)
        cut_details(d)
        print('Loading ' + d[-25:])
        load_csv(d[-25:],'details')
        os.system('rm ' + d[-25:])
    for f in fatlist:
        print('Cutting ' + f)
        cut_fatals(f)
        print('Loading ' + f[-28:])
        load_csv(f[-28:],'fatalities')
        os.system('rm ' + f[-28:])
    print('***** End of script *****')
