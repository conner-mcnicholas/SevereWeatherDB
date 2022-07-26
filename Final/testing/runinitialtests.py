#from __future__ import print_function
import os
import sys
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from bs4 import BeautifulSoup
import requests
from pathlib import Path
import mysql.connector
from io import BytesIO
import pandas as pd
import pytest
from datetime import date

CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
container = "allfiles"

def listfiles(table):
    """
    only for runtests.py , which is why we send container seperately from the global in run()
    """
    container_client= ContainerClient.from_connection_string(CONNECTION_STRING,container)
    blob_list = container_client.list_blobs(name_starts_with=table+"/")
    return blob_list

def tablecount(tablename):
    config = {
        'host':'sevwethmysqlserv.mysql.database.azure.com',
        'user':'conner@sevwethmysqlserv',
        'password':'Universal124!',
        'database':'defaultdb',
        'client_flags': [mysql.connector.ClientFlag.SSL],
        'ssl_ca': f'{os.environ["HOME"]}/.ssh/DigiCertGlobalRootG2.crt.pem'
    }
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {tablename}")
    return int(cursor.fetchone()[0])

def countfilerows(tablename):
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    bloblist = listfiles(tablename)
    rows = 0
    for blobject in bloblist:
        blob = blob_service_client.get_blob_client(container='allfiles',blob=blobject)
        with BytesIO() as inputblob:
            blob.download_blob().download_to_stream(inputblob)
            inputblob.seek(0)
            df = pd.read_csv(inputblob, compression='gzip',low_memory=False)
            rows+=len(df)
    return rows

dtl_files = listfiles('details')
fty_files = listfiles('fatalities')
year = date.today().year
start_year = 1950

def test_details_uploaded():
    """verifies same # of details files exist in blob as years since start"""
    detuploaded = len(list(dtl_files))
    expected = 1+year-start_year
    assert detuploaded == expected

def test_fatalities_uploaded():
    """verifies same # of fatalities files exist in blob as years since start"""
    fatuploaded = len(list(fty_files))
    expected = 1+year-start_year
    assert fatuploaded == expected

def test_details_count():
    """verfies same # rows exist in details mysql table and the source details csv.gz file"""
    details_tablecount = tablecount('details')
    details_filecount = countfilerows('details')
    assert details_tablecount == details_filecount

def test_fatalities_count():
    """verfies same # rows exist in fatalities mysql table and the source details csv.gz file"""
    fatalities_tablecount = tablecount('fatalities')
    fatalities_filecount = countfilerows('fatalities')
    assert fatalities_tablecount == fatalities_filecount
