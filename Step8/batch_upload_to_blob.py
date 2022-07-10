#from __future__ import print_function
import os
import sys
from time import sleep
from azure.storage.blob import BlobServiceClient
from bs4 import BeautifulSoup
import requests
from pathlib import Path

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
    print("\n\n***** Start of method listtarget *****\n")
    targeted = []
    for file in flist:
        filename = Path(file).parts[-1]
        print(f"Assessing: {filename}")
        felements = filename.split("_")
        ftype = felements[1].split("-")[0]
        fyear = felements[3][1:5]
        if int(fyear) >= start_year:
            print(f"File is past start date({start_year}), adding...\n")
            targeted.append(filename)
        else:
            print(f"File is before start date({start_year}), skipping...\n")
    print("number of target files for loading to blob:" + str(len(targeted)))
    return targeted

def sendtoblob(url,thefiles):
    """
    uploads single file to blob
    """
    print("\n\n***** Start of method sendtoblob *****")
    print("***** Start of script *****\n")
    try:
        CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    except KeyError:
        print("AZURE_STORAGE_CONNECTION_STRING must be set.")
        sys.exit(1)
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container = "severeweathercontainer"

    succeeded = []
    failed = []

    for thefile in thefiles:
        sourcefile = url+'/'+thefile
        print("\nuploading sourcefile:" + thefile)
        filename = Path(thefile).parts[-1]
        filetype = filename.split("_")[1].split("-")[0]
        if filetype == 'fatalities':
            print("uploading to container fatalities folder")
            copied_blob = blob_service_client.get_blob_client(container+'/fatalities', thefile)
        elif filetype == 'details':
            print("uploading to container fatalities folder")
            copied_blob = blob_service_client.get_blob_client(container+'/details', thefile)
        else:
            print("filetype unknown, exiting!")
            sys.exit(1)

        copied_blob.start_copy_from_url(sourcefile)
        sleep(5)
        for i in range(12):
            props = copied_blob.get_blob_properties()
            status = props.copy.status
            print("Copy status: " + status)
            if status == "success":
                break
            else:
                print("Copy not yet successful, waiting 5 seconds...")
                sleep(5)


        if status == "success":
            # Copy finished
            succeeded.append(thefile)
            print("Copy successful!")
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

def listblobfiles(container,table):
    flist=[]
    try:
        CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    except KeyError:
        print("AZURE_STORAGE_CONNECTION_STRING must be set.")
        sys.exit(1)
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client=blob_service_client.get_container_client(container)
    blob_list = container_client.list_blobs(name_starts_with=table+"/")
    for blob in blob_list:
        flist.append(blob)
    return flist

def run():
    """
    Recreated from main method to call from airflow as PythonOperator python callable
    """
    global CONNECTION_STRING
    global blob_service_client
    global container

    try:
        CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    except KeyError:
        print("AZURE_STORAGE_CONNECTION_STRING must be set.")
        sys.exit(1)
    start_year = int(sys.argv[1])
    url = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles"
    print(f"Getting all files after {start_year}...\n")
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container = "severeweathercontainer"
    container_client=blob_service_client.get_container_client(container)
    allfiles = listall(url)
    targetfiles = listtarget(allfiles, start_year)
    sendtoblob(url,targetfiles)

if __name__ == "__main__":
    """
    Recreated as run method to call from airflow as PythonOperator python callable
    """
    print("--------------------- START OF batch_upload_to_blob SCRIPT ---------------------")
    run()
    print("\n\n--------------------- END OF batch_upload_to_blob SCRIPT ---------------------")
