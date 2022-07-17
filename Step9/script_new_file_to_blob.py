"""
Script runs each year on March 17th, after January data from this year has
had a chance to be recorded and new file dropped to source.  We first
check blob to verify we don't have a file for this year yet, then check
source to search for the expected file, if we find it send to blob
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

def listblobfiles(tabletype):
    """
    Given table type
    list all files in blob initial batch container
    """
    print("\n\n\t\t***** START: listblobfiles *****\n")

    flist=[]
    print(f"Connecting to blob for list of {tabletype} files...")
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client=blob_service_client.get_container_client(batchcontainer)
    blob_list = container_client.list_blobs(name_starts_with=tabletype+"/")
    print("Transfer of blob file list complete...")
    for blob in blob_list:
        flist.append(blob)
    print("\n\t\t***** END: listblobfiles *****\n\n")
    return flist


def findblobnew(dictlist, tyear):
    """
    Given file list from blob
    Returns creation date of file from year of interest (usually this year)
    """
    print("\n\n\t\t***** START: findblobnew *****\n")
    for filedict in dictlist:
        file = filedict['name']
        filename = Path(str(file)).parts[-1]
        felements = filename.split("_")
        fyear = int(felements[3][1:5])
        fdateup = felements[-1][1:9]
        fdateup = datetime.strptime(fdateup, '%Y%m%d')
        fdateup = fdateup.date()
        print(f"\nBLOB: {filename} does {fyear} = {tyear} ?")
        if fyear == tyear:
            print(f"YUP! We already have file for this year, no need to continue")
            print("\n\t\t***** END: findblobnew *****\n\n")
            return fdateup
        else:
            print(f"no, file year is out of scope\n")
    print('processing complete, no file from target year exists in file list')
    print("\n\t\t***** END: findblobnew *****\n\n")
    return('NO NEW FILE')

def listsourcefiles(url,tabletype):
    """
    Given source url
    Returns list of all files at the given source URL
    """
    print("\n\n\t\t***** START: listsourcefiles *****\n")

    print(f"Connecting to source url for list of {tabletype} files...")
    page = requests.get(url).text
    soup = BeautifulSoup(page, "html.parser")
    print("\nReturning parsed text....")
    print("\n\t\t***** END: listsourcefiles *****\n\n")
    return [url + "/" + node.get("href")
            for node in soup.find_all("a") if
            ((node.get("href").startswith(f"StormEvents_{tabletype}")) and (node.get("href").endswith("csv.gz")))]

    print("\n\t\t***** END: listsourcefiles *****\n\n")

def findnewfile(sourcefiles, targetyr):
    """
    Given file list from source
    Find if file from target year exists yet on source
    """
    print("\n\n\t\t***** START: findnewfile *****\n")

    for file in sourcefiles:
        filename = Path(file).parts[-1]
        felements = filename.split("_")
        fyear = int(felements[3][1:5])
        fdateup = felements[-1][1:9]
        fdateup = datetime.strptime(fdateup, '%Y%m%d')
        fdateup = fdateup.date()
        #fdateup = fdateup + timedelta(days = 1) #faking for test
        print(f"\nSOURCE: {filename} does {fyear} = {targetyr} ?")
        if fyear == targetyr:
            print(f"YES, returning new file created on {fdateup}")
            return filename
        else:
            print(f"no, file year out of scope\n")

    print('\nprocessing has not identified a new file')
    print("\n\t\t***** END: findnewfile *****\n\n")
    return('NOT FOUND')

def filetoblob(sourceurl, thefile, tabletype):
    """
    Given source url and filename
    uploads single source file to blob
    """
    print("\n\n\t\t***** START: filetoblob *****\n")

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
            print("\n\t\t***** END: filetoblob *****\n\n")
            break
        else:
            print("Copy not yet successful, waiting 5 seconds...")
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

    print("\n\t\t***** END: filetoblob *****\n\n")

def run():
    """
    Recreated from main method to call from airflow as PythonOperator python callable
    """
    global CONNECTION_STRING
    global blob_service_client
    global batchcontainer
    global newcontainer

    try:
        CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    except KeyError:
        print("AZURE_STORAGE_CONNECTION_STRING must be set.")
        sys.exit(1)

    targetyear = int(str(date.today())[0:4])
    #targetyear = 1999
    sourceurl = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles"

    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    batchcontainer = 'severeweathercontainer'
    newcontainer = "newfiles"

    for tabletype in ['details','fatalities']:
        print(f"Picking up any new {tabletype} file for {targetyear}")
        blobfiles = listblobfiles(tabletype)
        blobnew = findblobnew(blobfiles,targetyear)
        if blobnew == 'NO NEW FILE':
            sourcefiles = listsourcefiles(sourceurl,tabletype)
            newsourcefile = findnewfile(sourcefiles, targetyear)
            if newsourcefile != 'NOT FOUND':
                #print(f'new file identified for {targetyear} - > {newsourcefile} (Placeholder for sending file to blob)\n')
                filetoblob(sourceurl,newsourcefile,tabletype)

if __name__ == "__main__":
    """
    Recreated as run method to call from airflow as PythonOperator python callable
    """
    print("--------------------- START OF  send_new_file_to_blob SCRIPT ---------------------")
    run()
    print("\n\n--------------------- END OF send_new_file_to_blob SCRIPT ---------------------")
