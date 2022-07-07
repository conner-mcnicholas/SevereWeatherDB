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


def listtarget(flist, sdate):
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
        if int(fyear) >= sdate:
            print(f"File is past start date({sdate}), adding...\n")
            targeted.append(filename)
        else:
            print(f"File is before start date({sdate}), skipping...\n")
    print("number of target files for loading to blob:" + str(len(targeted)))
    return targeted

def sendtoblob(thefiles):
    """
    uploads single file to blob
    """
    print("\n\n***** Start of method sendtoblob *****")
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
            print("Copy successful!")
        else:
            # if not finished after 100s, cancel the operation
            print("Copy unsuccessful")
            print("Final copy status: " + status + "\nAborting copy...")
            copy_id = props.copy.id
            copied_blob.abort_copy(copy_id)
            props = copied_blob.get_blob_properties()
            print(props.copy.status)

if __name__ == "__main__":
    print("***** Start of script *****\n")
    try:
        CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    except KeyError:
        print("AZURE_STORAGE_CONNECTION_STRING must be set.")
        sys.exit(1)
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container = "severeweathercontainer"
    url = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles"
    sdate = 2019
    print(f"Getting all files from {url} after {sdate}...\n")
    allfiles = listall(url)
    targetfiles = listtarget(allfiles, sdate)
    sendtoblob(targetfiles)
    print("***** End of script *****")
