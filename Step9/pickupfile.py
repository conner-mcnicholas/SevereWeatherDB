from datetime import date
from azure.storage.blob import BlobServiceClient
import os
import glob
import sys

def listblobfiles(tabletype):
    """
    Given table type
    list all files in blob initial batch container
    """
    print("\n\n***** START: listblobfiles *****\n")

    flist=[]
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client=blob_service_client.get_container_client(batchcontainer)
    blob_list = container_client.list_blobs(name_starts_with=tabletype+"/")
    for blob in blob_list:
        flist.append(blob)
    return flist

    print("\n***** END: listblobfiles *****\n\n")

def findblobupdated(flist, tyear):
    """
    Given file list from blob
    Returns creation date of file from year of interest (usually this year)
    """
    print("\n\n***** START: findblobupdated *****\n")

    for file in flist:
        filename = Path(file).parts[-1]
        felements = filename.split("_")
        fyear = int(felements[3][1:5])
        fdateup = felements[-1][1:9]
        print(f"\n{filename} is year of record = {targetyr} ?")
        if fyear == tyear:
            print(f"YUP! returning target year's last updated date:{fdateup}")
            return fdateup
        else:
            print(f"out of scope\n")
    print('processing complete, no file from target year exists in file list')

    print("\n***** END: findblobupdated *****\n\n")

def listsourcefiles(url):
    """
    Given source url
    Returns list of all files at the given source URL
    """
    print("\n\n***** START: listsourcefiles *****\n")

    print("Connecting to url...")
    page = requests.get(url).text
    soup = BeautifulSoup(page, "html.parser")
    print("\nReturning parsed text....")
    return [url + "/" + node.get("href")
            for node in soup.find_all("a") if
            ((not node.get("href").startswith("StormEvents_locations")) and (node.get("href").endswith("csv.gz")))]

    print("\n***** END: listsourcefiles *****\n\n")

def findupdatedfile(sourcefiles, targetyr, bloblatestupdate):
    """
    Given file list from source
    Find if file from target year is more recent than latest bloblatestupdate from blob
    """
    print("\n\n***** START: findupdatedfile *****\n")

    for file in sourcefiles:
        filename = Path(file).parts[-1]
        felements = filename.split("_")
        fyear = int(felements[3][1:5])
        fdateup = felements[-1][1:9]
        print(f"\n{filename} is year of record = {targetyr} ?")
        if fyear == targetyr:
            print(f"in scope. is creation date > {bloblatestupdate} ?")
            if fdateup > bloblatestupdate:
                print("returning new file!")
                return filename
            elif fdateup == bloblatestupdate:
                print("we already have file.")
            else:
                print("file is older than file on record?")
        else:
            print(f"out of scope\n")

    print('processing has not identified a new file')
    return('NOT FOUND')

    print("\n***** END: findupdatedfile *****\n\n")

def filetoblob(sourceurl, thefile, tabletype):
    """
    Given source url and filename
    uploads single source file to blob
    """
    print("\n\n***** START: filetoblob *****\n")

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

    print("\n***** END: filetoblob *****\n\n")


if __name__ == "__main__":
    print("--------------------- START OF SCRIPT ---------------------\n\n")

    try:
        CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    except KeyError:
        print("AZURE_STORAGE_CONNECTION_STRING must be set.")
        sys.exit(1)

    targetyear = str(date.today())[0:4]

    sourceurl = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles"

    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    batchcontainer = 'severeweathercontainer'
    newcontainer = "newfiles"

    for tabletype in ['details','fatalities']:
        blobfiles = listblobfiles(tabletype)
        bloblatestupdate = findblobupdated(blobfiles,targetyear)
        sourcefiles = listsourcefiles(sourceurl)
        updatedsourcefile = findupdatedfile(sourcefiles, targetyear, bloblatestupdate)
        if updatedsourcefile != 'NOT FOUND':
            filetoblob(sourceurl,updatedsourcefile,tabletype)
        else:
            print(f'NO UPDATED FILE IDENTIFIED IN SOURCE FOR {targetyear}')

    print("\n\n--------------------- END OF SCRIPT ---------------------")
