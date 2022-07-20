"""
Script runs each month on 17th, after this years file should
contain new data (recent as of 65 days prior) and have an updated filename
based on modification date
"""
from datetime import datetime,date,timedelta
from azure.storage.blob import BlobServiceClient
import os
import glob
import sys
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from time import sleep
import mysql.connector
from mysql.connector import errorcode

config = {
  'host':'sevwethmysqlserv.mysql.database.azure.com',
  'user':'conner@sevwethmysqlserv',
  'password':'Universal124!',
  'database':'defaultdb',
  'client_flags': [mysql.connector.ClientFlag.SSL],
  'ssl_ca': f'{os.environ["HOME"]}/.ssh/DigiCertGlobalRootG2.crt.pem',
  'autocommit': True
}

def listblobfiles(tabletype):
    """
    Given table type
    list all files in blob initial batch container
    """
    flist=[]
    print(f"Connecting to blob for list of {tabletype} files...")
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client=blob_service_client.get_container_client(batchcontainer)
    blob_list = container_client.list_blobs(name_starts_with=tabletype+"/")
    print("Transfer of blob file list complete...")
    for blob in blob_list:
        flist.append(blob)
    return flist


def findblobupdated(dictlist, tyear):
    """
    Given file list from blob
    Returns creation date of file from year of interest (usually this year)
    """
    for filedict in dictlist:
        file = filedict['name']
        filename = Path(str(file)).parts[-1]
        felements = filename.split("_")
        fyear = int(felements[3][1:5])
        fdateup = felements[-1][1:9]
        fdateup = datetime.strptime(fdateup, '%Y%m%d')
        fdateup = fdateup.date()
        #print(f"\nBLOB: {filename} does {fyear} = {tyear} ?")
        if fyear == tyear:
            print(f"YUP! returning target year's last updated date:{fdateup}")
            return fdateup
        #else:
            #print(f"no, file year is out of scope\n")
    print('processing complete, no file from target year exists in file list')

def listsourcefiles(url,tabletype):
    """
    Given source url
    Returns list of all files at the given source URL
    """
    print(f"Connecting to source url for list of {tabletype} files...")
    page = requests.get(url).text
    soup = BeautifulSoup(page, "html.parser")
    print("\nReturning parsed text....")
    return [url + "/" + node.get("href")
            for node in soup.find_all("a") if
            ((node.get("href").startswith(f"StormEvents_{tabletype}")) and (node.get("href").endswith("csv.gz")))]

def findupdatedfile(sourcefiles, targetyr, bloblatestupdate):
    """
    Given file list from source
    Find if file from target year is more recent than latest bloblatestupdate from blob
    """
    for file in sourcefiles:
        filename = Path(file).parts[-1]
        felements = filename.split("_")
        fyear = int(felements[3][1:5])
        fdateup = felements[-1][1:9]
        fdateup = datetime.strptime(fdateup, '%Y%m%d')
        fdateup = fdateup.date()
        #fdateup = fdateup + timedelta(days = 1) #faking for test
        #print(f"\nSOURCE: {filename} does {fyear} = {targetyr} ?")
        if fyear == targetyr:
            #print(f"YES, file year in scope. is creation date {fdateup} > {bloblatestupdate} ?")
            if fdateup > bloblatestupdate:
                print(f"Found updated file for this month: {filename}")
                return filename
            #elif fdateup == bloblatestupdate:
            #    print("no, target year file creation dates match - we already have file.")
            #else:
            #    print("no, file is older than blob file - how?")
        #else:
        #    print(f"no, file year out of scope\n")
    print(f'\nFailed to identify a new source file for {targetyr}, exiting')
    sys.exit()

def filetoblob(sourceurl, thefile, tabletype):
    """
    Given source url and filename
    uploads single source file to blob
    """
    sourcefile = sourceurl+'/'+thefile
    print(f"\ningesting {thefile} to {newcontainer}/{tabletype}")

    copied_blob = blob_service_client.get_blob_client(newcontainer+'/'+tabletype, thefile)
    copied_blob.start_copy_from_url(sourcefile)
    sleep(5)
    for i in range(12):
        props = copied_blob.get_blob_properties()
        status = props.copy.status
        #print("Copy status: " + status)
        if status == "success":
            break
        else:
            #print("Copy not yet successful, waiting 5 seconds...")
            sleep(5)

    if status == "success":
        # Copy finished
        print("Copy successful!")
    else:
        # if not finished after 1 min, cancel the operation
        print("Copy unsuccessful")
        print("Final copy status: " + status + "\nAborting copy...")
        copy_id = props.copy.id
        copied_blob.abort_copy(copy_id)
        props = copied_blob.get_blob_properties()
        print(props.copy.status)

def delete_and_create_staging_tables():
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
        "  EVENT_ID INT,"
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
    query = ("DROP TABLE IF EXISTS vPreDelete;"
        "CREATE TABLE vPreDelete AS"
    	"  SELECT d_PreDelete,f_PreDelete FROM"
    	"	(SELECT COUNT(*) AS d_PreDelete FROM details) AS d,"
    	"	(SELECT COUNT(*) AS f_PreDelete FROM fatalities) AS f;"
        "DELETE FROM details WHERE BEGIN_YEARMONTH = '202203';"
        "DELETE FROM fatalities WHERE FAT_YEARMONTH = '202203';"
        "DROP TABLE IF EXISTS vPostDelete;"
        "CREATE TABLE vPostDelete AS"
    	"  SELECT d_PostDelete,f_PostDelete FROM"
    	"	(SELECT COUNT(*) AS d_PostDelete FROM details) AS d,"
    	"	(SELECT COUNT(*) AS f_PostDelete FROM fatalities) AS f;")
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()
    conn.close()

create_table_precounts() #for testing,creates view of counts prior to update action, will compare after

CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
targetyear = int(str(date.today())[0:4])
sourceurl = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles"
blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
batchcontainer = 'allfiles'
newcontainer = "newfiles"

for tabletype in ['details','fatalities']:
    print(f"Picking up any updated {tabletype} file for {targetyear}")
    blobfiles = listblobfiles(tabletype)
    bloblatestupdate = findblobupdated(blobfiles,targetyear)
    sourcefiles = listsourcefiles(sourceurl,tabletype)
    updatedsourcefile = findupdatedfile(sourcefiles, targetyear, bloblatestupdate)
    filetoblob(sourceurl,updatedsourcefile,tabletype) # this method will exit if updated source file not found

delete_and_create_staging_tables()
