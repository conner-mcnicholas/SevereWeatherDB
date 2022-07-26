"""
Script runs each year on March 17th, after January data from this year has
had a chance to be recorded and new file dropped to source.  We first
check blob to verify we don't have a file for this year yet, then check
source to search for the expected file, if we find it send to blob
"""
from datetime import datetime,date,timedelta
from azure.storage.blob import BlobServiceClient
import os
import sys
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from time import sleep

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


def findblobnew(dictlist, tyear):
    """
    Given file list from blob and year of interest (i.e. the new year)
    Exits script if the blob file list already contains file for that year
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
            print(f"We already ingested latest year({tyear}) file: {filename}, exiting")
            dbutils.notebook.exit("Clean exiting databricks python activity")
        #else:
            #print(f"no, file year is out of scope\n")
    print('processing complete, no file from target year exists in blob file list')

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

def findnewfile(sourcefiles, targetyr):
    """
    Given file list from source
    Find if file from target year exists yet on source
    """
    for file in sourcefiles:
        filename = Path(file).parts[-1]
        felements = filename.split("_")
        fyear = int(felements[3][1:5])
        fdateup = felements[-1][1:9]
        fdateup = datetime.strptime(fdateup, '%Y%m%d')
        fdateup = fdateup.date()
        #fdateup = fdateup + timedelta(days = 1) #faking for test
        #rint(f"\nSOURCE: {filename} does {fyear} = {targetyr} ?")
        if fyear == targetyr:
            print(f"Found new file for {targetyr} created on {fdateup}")
            return filename

    print(f'\nFailed to identify a new source file for {targetyr}, exiting')
    dbutils.notebook.exit("Clean exiting databricks python activity")

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
        print("Copy status: " + status)
        if status == "success":
            break
        else:
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

CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
targetyear = int(str(date.today())[0:4])
sourceurl = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles"
blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
batchcontainer = 'allfiles'
newcontainer = "newfiles"

for tabletype in ['details','fatalities']:
    print(f"Picking up any new {tabletype} file for {targetyear}")
    blobfiles = listblobfiles(tabletype)
    findblobnew(blobfiles,targetyear) #this method will exit script if it determines a new file already exists in blob container
    sourcefiles = listsourcefiles(sourceurl,tabletype)
    newsourcefile = findnewfile(sourcefiles, targetyear) #this method will exit script if it fails to identify new file available at source url
    filetoblob(sourceurl,newsourcefile,tabletype)
