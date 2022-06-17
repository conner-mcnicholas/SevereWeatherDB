"""
Script to pull severe weather event data from ncei.noaa.gov
File contents are denoted by type (details, fatalities, locations),year, and
upload date.  At this stage I am only targetting all files >= 2017 to prove out
the functionality, but later all files >= 2010 will be included.
"""

from bs4 import BeautifulSoup
import requests
from pathlib import Path
import os

def listFull(url):
    """
    Returns list of all files at the given URL
    """
    print('***** Start of method listFull *****\n')
    print('Connecting to url...')
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    print('\nReturning parsed text....')
    return [url + '/' + node.get('href') for node in soup.find_all('a') if ((not node.get('href').startswith('StormEvents_locations')) and (node.get('href').endswith('csv.gz')))]

def writeFiles(flist,sdate):
    """
    Writes files pertaining to date after sdate to /data folder
    """
    print('***** Start of method writeFiles *****\n')
    for file in flist:
        print(f'Processing %s' %file)
        felements = Path(file).parts[-1].split('_')
        fyear = felements[3][1:5]
        if int(fyear) >= sdate:
            print(f'File is past start date %s, connecting...'%sdate)
            ftype = felements[1].split('-')[0]
            fdateup = felements[-1][1:9]
            fname = 'data/storm_%s_%s_%s.csv.gz'%(ftype,fyear,fdateup)
            r = requests.get(file)
            with open(fname,'wb') as f:
                print('Writing locally...')
                f.write(r.content)
                print('Write complete!\n')
        else:
            print(f'File is before start date %s, skipping...\n'%sdate)

def unzipFiles():
    if Path('/home/conner/Capstone/data/unzipped').is_dir():
        print('"data/unzipped" directory for storing decompressed csv files already exists')
    else:
        print('Creating "data/unzipped" directory for storing decompressed csv files')
        os.system('mkdir data/unzipped')
    os.system('gzip -dk data/*.gz;mv data/*.csv data/unzipped')


if __name__ == "__main__":
    print('***** Start of script *****\n')
    url = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles'
    sdate = 2017
    print(f'Getting all files from %s after %s...\n'%(url,sdate))
    writeFiles(listFull(url),sdate)
    print('Decompressing all files.  They will be stored in data/unzipped dir.  Compressed source files to be retained in data dir')
    unzipFiles()
    print('***** End of script *****')
