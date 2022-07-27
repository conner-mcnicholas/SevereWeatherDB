#from __future__ import print_function
import os
import sys
from time import sleep
from azure.storage.blob import BlobServiceClient
from bs4 import BeautifulSoup
import requests
from pathlib import Path


def listfiles(table):
    """
    only for runtests.py , which is why we send container seperately from the global in run()
    """
    CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container = "testbatch"
    container_client=blob_service_client.get_container_client(container)

    flist=[]
    blob_list = container_client.list_blobs(name_starts_with=table+"/")
    for blob in blob_list:
        flist.append(blob)
    return flist

if __name__ == "__main__":
    """
    Recreated as run method to call from airflow as PythonOperator python callable
    """
    print("--------------------- START OF getblobfiles SCRIPT ---------------------")
    for table in ['details','fatalities']:
        files=listfiles(table)
        print(f'{table} count = {len(files)}')
    print("\n\n--------------------- END OF getblobfiles SCRIPT ---------------------")
