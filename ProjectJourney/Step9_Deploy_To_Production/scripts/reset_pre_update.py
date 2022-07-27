"""
Script runs each month on 17th, after this years file should
contain new data (recent as of 65 days prior) and have an updated filename
based on modification date
"""
from datetime import date
import os
import sys
import mysql.connector

def reset_staging():
    table_description = (
        "DROP TABLE IF EXISTS STAGING_details;"
        "CREATE TABLE STAGING_details ("
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
        "  DATA_SOURCE VARCHAR(3));"
        "DROP TABLE IF EXISTS STAGING_fatalities;"
        "CREATE TABLE STAGING_fatalities ("
        "  FAT_YEARMONTH VARCHAR(6),"
        "  FAT_DAY VARCHAR(2),"
        "  FAT_TIME VARCHAR(4),"
        "  FATALITY_ID INT NOT NULL,"
        "  EVENT_ID INT NOT NULL,"
        "  FATALITY_TYPE VARCHAR(1),"
        "  FATALITY_DATE VARCHAR(19),"
        "  FATALITY_AGE INT DEFAULT NULL,"
        "  FATALITY_SEX CHAR(1),"
        "  FATALITY_LOCATION TEXT,"
        "  EVENT_YEARMONTH VARCHAR(6),"
        "  PRIMARY KEY (FATALITY_ID,EVENT_ID));")

    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    print("Creating empty staging tables")
    cursor.execute(table_description)
    cursor.close()
    conn.close()


def create_table_precounts():
    query = (f"DROP TABLE IF EXISTS {P}_PreDelete;"
        f"CREATE TABLE {P}_PreDelete AS" # U for Update Pipeline (vs New Pipeline)
    	"  SELECT * FROM"
    	"	(SELECT COUNT(*) AS D_PreDelete FROM details) AS d," # D for details
    	"	(SELECT COUNT(*) AS F_PreDelete FROM fatalities) AS f;" # F for fatalities
        f"DELETE FROM details WHERE SUBSTR(BEGIN_YEARMONTH,1,4) = '{targetyear}';"
        f"DELETE FROM fatalities WHERE SUBSTR(FAT_YEARMONTH,1,4) = '{targetyear}';"
        f"DROP TABLE IF EXISTS {P}_PostDelete;"
        f"CREATE TABLE {P}_PostDelete AS"
    	"  SELECT * FROM"
    	"	(SELECT COUNT(*) AS D_PostDeleteCount FROM details) AS d,"
    	"	(SELECT COUNT(*) AS F_PostDeleteCount FROM fatalities) AS f;")
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    print(f'Deleting {targetyear} rows from details and fatalities\n')
    print(f'Creating {P}_PreDelete and {P}_PostDelete')
    cursor.execute(query)
    cursor.close()
    conn.close()

config = {
  'host':'sevwethmysqlserv.mysql.database.azure.com',
  'user':'conner@sevwethmysqlserv',
  'password':os.environ["AZ_MYSQL_ADMIN_PASSWORD"],
  'database':'defaultdb',
  'client_flags': [mysql.connector.ClientFlag.SSL],
  'ssl_ca': f'{os.environ["HOME"]}/.ssh/DigiCertGlobalRootG2.crt.pem',
  'autocommit': True
}

targetyear = int(str(date.today())[0:4])
P = sys.argv[1] #pipeline type = 'U'[pdate] or 'N'[ew] cmd line var from data factory settings
create_table_precounts() #preserve state prior to update action for testing

reset_staging()
