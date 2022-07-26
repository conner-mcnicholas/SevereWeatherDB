#from __future__ import print_function
import os
import sys
from time import sleep
from azure.storage.blob import BlobServiceClient
from bs4 import BeautifulSoup
import requests
from pathlib import Path
from datetime import datetime
import mysql.connector
from mysql.connector import errorcode

config = {
  'host':'sevwethmysqlserv.mysql.database.azure.com',
  'user':'conner@sevwethmysqlserv',
  'password':os.environ["AZ_MYSQL_ADMIN_PASSWORD"],
  'database':'defaultdb',
  'client_flags': [mysql.connector.ClientFlag.SSL],
  'ssl_ca': f'{os.environ["HOME"]}/.ssh/DigiCertGlobalRootG2.crt.pem'
}

def listall(url):
    """
    Returns list of all files at the given URL
    """
    print("\n\n***** Start of method listfull *****\n")
    print("Connecting to url...")
    page = requests.get(url).text
    soup = BeautifulSoup(page, "html.parser")
    print("\nReturning parsed text....")
    return [url + "/" + node.get("href")
            for node in soup.find_all("a") if
            ((not node.get("href").startswith("StormEvents_locations")) and (node.get("href").endswith("csv.gz")))]


def listtarget(flist, start_year):
    """
    Create list of files that are past start date and target for upload to blob
    """
    targeted = []
    for file in flist:
        filename = Path(file).parts[-1]
        #print(f"Assessing: {filename}")
        felements = filename.split("_")
        ftype = felements[1].split("-")[0]
        fyear = felements[3][1:5]
        if int(fyear) >= start_year:
            #print(f"File is past start date({start_year}), adding...\n")
            targeted.append(filename)
        #else:
            #print(f"File is before start date({start_year}), skipping...\n")
    print("number of target files for loading to blob:" + str(len(targeted)))
    return targeted

def sendtoblob(url,thefiles):
    """
    uploads single file to blob
    """
    CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container = "allfiles"

    succeeded = []
    failed = []

    for thefile in thefiles:
        sourcefile = url+'/'+thefile
        print(f"\nuploading sourcefile: {thefile}")
        filename = Path(thefile).parts[-1]
        filetype = filename.split("_")[1].split("-")[0]

        copied_blob = blob_service_client.get_blob_client(f'{container}/{filetype}', thefile)

        copied_blob.start_copy_from_url(sourcefile)
        sleep(3)
        for i in range(12):
            props = copied_blob.get_blob_properties()
            status = props.copy.status
            print("Copy status: " + status)
            if status == "success":
                break
            else:
                #print("Copy not yet successful, waiting 2 seconds...")
                sleep(2)

        if status == "success":
            # Copy finished
            succeeded.append(thefile)
            print("Copy successful")
        else:
            # if not finished after 1 min, cancel the operation
            failed.append(thefile)
            print("Copy unsuccessful")
            print("Final copy status: " + status + "\nAborting copy...")
            copy_id = props.copy.id
            copied_blob.abort_copy(copy_id)
            props = copied_blob.get_blob_properties()
            print(props.copy.status)

    print("number of files successfully loaded to blob:" + str(len(succeeded)))
    print("number of files failed to load to blob:" + str(len(failed)))

def delete_and_create_tables():
    table_description = (
        "DROP TABLE IF EXISTS details;"
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
        "  DATA_SOURCE VARCHAR(3));"
        "DROP TABLE IF EXISTS fatalities;"
        "CREATE TABLE fatalities ("
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
    print("Creating empty final tables")
    cursor.execute(table_description)
    cursor.close()
    conn.close()

"""
Removed from main method as I want to execute this file as a script from databricks in ADF
"""
start_time = datetime.now().replace(microsecond=0)
startyear = int(sys.argv[1]) #passed as paramter from data factory databricks python file setting in init pipeline
url = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles"
print(f"Getting all files after {startyear}...\n")
allfiles = listall(url)
targetfiles = listtarget(allfiles, startyear)
sendtoblob(url,targetfiles)
end_time = datetime.now().replace(microsecond=0)
elapsed_time = end_time - start_time
print(f'Number Of Files Loaded: {len(targetfiles)}')
print(f'Elapsed Time: {elapsed_time}')

delete_and_create_tables()
