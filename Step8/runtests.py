import pytest
from datetime import date
import os
import getblobfiles
import delete_and_create_tables_azuremysql
import count_srcfile_rows

year = date.today().year
start_year = 2019

def test_details_uploaded():
    """verifies same # of details files exist in blob as years since start"""
    detuploaded = len(getblobfiles.listfiles('details'))
    expected = 1+year-start_year
    assert detuploaded == expected

def test_fatalities_uploaded():
    """verifies same # of fatalities files exist in blob as years since start"""
    fatuploaded = len(getblobfiles.listfiles('fatalities'))
    expected = 1+year-start_year
    assert fatuploaded == expected

def test_details_count():
    """verfies same # rows exist in details mysql table and the source details csv.gz file"""
    details_tablecount = delete_and_create_tables_azuremysql.tablecount('details')
    details_filecount = count_srcfile_rows.countfilerows('details',start_year)
    assert details_tablecount == details_filecount

def test_fatalities_count():
    """verfies same # rows exist in fatalities mysql table and the source details csv.gz file"""
    fatalities_tablecount = delete_and_create_tables_azuremysql.tablecount('fatalities')
    fatalities_filecount = count_srcfile_rows.countfilerows('fatalities',start_year)
    assert fatalities_tablecount == fatalities_filecount

os.system(f"rm -r {os.environ['HOME']}/SevereWeatherDB/filecounter/*")
