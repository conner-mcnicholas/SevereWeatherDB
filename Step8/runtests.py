import pytest
from datetime import date
from azure.storage.blob import BlobServiceClient
import os
import uploadtoblob
import delete_and_create_tables_azuremysql
import countcsvgz

year = date.today().year
sdate = 2000

def test_details_uploaded():
    detuploaded = len(uploadtoblob.listblobfiles('severeweathercontainer','details'))
    expected = 1+year-sdate
    assert detuploaded == expected

def test_fatalities_uploaded():
    fatuploaded = len(uploadtoblob.listblobfiles('severeweathercontainer','fatalities'))
    expected = 1+year-sdate
    assert fatuploaded == expected

def test_details_count():
    details_tablecount = delete_and_create_tables_azuremysql.tablecount('details')
    details_filecount = countcsvgz.countfilerows('details')
    assert details_tablecount == details_filecount

def test_fatalities_count():
    fatalities_tablecount = delete_and_create_tables_azuremysql.tablecount('fatalities')
    fatalities_filecount = countcsvgz.countfilerows('fatalities')
    assert fatalities_tablecount == fatalities_filecount

os.system('rm -r /home/conner/SevereWeatherDB/filecounter/*')
