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
        raise ConnectionError("Error while connecting to database for job tracker: ") from error
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
        "  EVENT_ID INT NOT NULL PRIMARY KEY,"
        "  STATE TEXT,"
        "  STATE_FIPS INT,"
        "  YEAR INT,"
        "  MONTH_NAME VARCHAR(10),"
        "  EVENT_TYPE TEXT,"
        "  CZ_TYPE VARCHAR(1),"
        "  CZ_FIPS INT,"
        "  CZ_NAME TEXT,"
        "  WFO VARCHAR(3),"
        "  BEGIN_DATE_TIME VARCHAR(20),"
        "  CZ_TIMEZONE TEXT,"
        "  END_DATE_TIME VARCHAR(20),"
        "  INJURIES_DIRECT INT,"
        "  INJURIES_INDIRECT INT,"
        "  DEATHS_DIRECT INT,"
        "  DEATHS_INDIRECT INT,"
        "  DAMAGE_PROPERTY TEXT,"
        "  DAMAGE_CROPS TEXT,"
        "  SOURCE TEXT,"
        "  MAGNITUDE DEC(9,2),"
        "  MAGNITUDE_TYPE VARCHAR(2),"
        "  FLOOD_CAUSE TEXT,"
        "  CATEGORY INT,"
        "  TOR_F_SCALE VARCHAR(3),"
        "  TOR_LENGTH DEC(9,2),"
        "  TOR_WIDTH DEC(9,2),"
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
        "  BEGIN_LAT DEC(9,4),"
        "  BEGIN_LON DEC(9,4),"
        "  END_LAT DEC(9,4),"
        "  END_LON DEC(9,4),"
        "  EPISODE_NARRATIVE TEXT,"
        "  EVENT_NARRATIVE TEXT,"
        "  DATA_SOURCE VARCHAR(3)"
        ")")

    TABLES['fatalities'] = (
        "CREATE TABLE fatalities ("
        "  FAT_YEARMONTH VARCHAR(6),"
        "  FAT_DAY VARCHAR(2),"
        "  FAT_TIME VARCHAR(4),"
        "  FATALITY_ID INT NOT NULL PRIMARY KEY,"
        "  EVENT_ID INT,"
        "  FATALITY_TYPE VARCHAR(1),"
        "  FATALITY_DATE VARCHAR(19),"
        "  FATALITY_AGE INT DEFAULT NULL,"
        "  FATALITY_SEX CHAR(1),"
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

def createview():
    cnx = get_db_connection()
    cursor = cnx.cursor()
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

    exstring = "CREATE VIEW details_fulldates AS SELECT CONCAT(BEGIN_YEARMONTH, \
    BEGIN_NEWD) AS BEGIN_FULLDATE,CONCAT(END_YEARMONTH,END_NEWD) AS END_FULLDATE \
    FROM (SELECT BEGIN_YEARMONTH,CASE WHEN LENGTH(BEGIN_DAY) = 1 THEN CONCAT('0',BEGIN_DAY) \
    ELSE BEGIN_DAY END AS BEGIN_NEWD, END_YEARMONTH,CASE WHEN LENGTH(END_DAY) = 1 THEN \
    CONCAT('0',END_DAY) ELSE END_DAY END AS END_NEWD FROM details) d;"

    try:
        cursor.execute(exstring)
    except mysql.connector.Error as err:
        print(err.msg)
    else:
        print('View creation successful.')

def load_csv(filepath, table):
    df = pd.read_csv(filepath, sep = ',')
    df.to_sql(table,con=engine,index=False,if_exists='append')

def cp_details(filepath):
    os.system("cp " + filepath + " details" + filepath[-18:])

def cp_fatals(filepath):
    os.system("cp " + filepath + " fatalities" + filepath[-18:])

if __name__ == "__main__":
    print('***** Start of data transform script *****\n')
    DB_NAME = 'SevereWeatherDB'
    cnx = initialize_database()
    engine = create_engine('mysql+pymysql://root@localhost/SevereWeatherDB')
    data_dir = f"{os.environ['HOME']}/SevereWeatherDB/data/unzipped"
    detlist = sorted(glob.glob(f"{data_dir}/storm_details_*"))
    fatlist = sorted(glob.glob(f"{data_dir}/storm_fatalities_*"))
    for d in detlist:
        print('Copying ' + d)
        cp_details(d)
        print('Loading ' + d[-25:])
        load_csv(d[-25:],'details')
        os.system('rm ' + d[-25:])
    for f in fatlist:
        print('Copying ' + f)
        cp_fatals(f)
        print('Loading ' + f[-28:])
        load_csv(f[-28:],'fatalities')
        os.system('rm ' + f[-28:])
    createview()
    print('***** End of script *****')
