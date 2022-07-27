#from __future__ import print_function
import os
import sys
from time import sleep
from azure.storage.blob import BlobServiceClient
from bs4 import BeautifulSoup
import requests
from pathlib import Path
from datetime import datetime
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

    #CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    CONNECTION_STRING = AZURE_STORAGE_CONNECTION_STRING
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container = "testbatch"

    succeeded = []
    failed = []

    for thefile in thefiles:
        sourcefile = url+'/'+thefile
        print("\nuploading sourcefile:" + thefile)
        filename = Path(thefile).parts[-1]
        filetype = filename.split("_")[1].split("-")[0]

        print("uploading to container fatalities folder")
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
                print("Copy not yet successful, waiting 2 seconds...")
                sleep(2)


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


def run():
    """
    Recreated from main method to call from airflow as PythonOperator python callable
    """
    start_time = datetime.now().replace(microsecond=0)
    startyear = START_YEAR
    url = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles"
    print(f"Getting all files after {startyear}...\n")
    allfiles = listall(url)
    targetfiles = listtarget(allfiles, startyear)
    sendtoblob(url,targetfiles)
    end_time = datetime.now().replace(microsecond=0)
    elapsed_time = end_time - start_time
    print(f'Number Of Files Loaded: {len(targetfiles)}')
    print(f'Elapsed Time: {elapsed_time}')

if __name__ == "__main__":
    """
    Recreated as run method to call from airflow as PythonOperator python callable
    """
    print("--------------------- START OF batch_upload_to_blob SCRIPT ---------------------")
    run()
    print("\n\n--------------------- END OF batch_upload_to_blob SCRIPT ---------------------")
