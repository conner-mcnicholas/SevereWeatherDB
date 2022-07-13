from bs4 import BeautifulSoup
import requests
from pathlib import Path
import os
import glob
import gzip

tmp_write_dir = f"{os.environ['HOME']}/SevereWeatherDB/filecounter"

def listFull(url):
    """
    Returns list of all files at the given URL
    """
    print('Connecting to url...')
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    print('\nReturning parsed text....')
    return [url + '/' + node.get('href')
            for node in soup.find_all('a') if
            ((not node.get('href').startswith('StormEvents_locations')) and (node.get('href').endswith('csv.gz')))]

def writeFiles(flist, sdate):
    """
    Writes files pertaining to filecounter after sdate to /data folder
    """
    if Path(tmp_write_dir).is_dir() == False:
        os.system(f'mkdir {tmp_write_dir} ')
    for file in flist:
        print(f'Processing %s' % file)
        felements = Path(file).parts[-1].split('_')
        fyear = felements[3][1:5]
        if int(fyear) >= sdate:
            print(f'File is past start date %s, connecting...' % sdate)
            ftype = felements[1].split('-')[0]
            fdateup = felements[-1][1:9]
            fname = f'{tmp_write_dir}/storm_{ftype}_{fyear}_{fdateup}.csv.gz'
            r = requests.get(file)
            with open(fname, 'wb') as f:
                print('Writing locally...')
                f.write(r.content)
                print('Write complete!\n')
        else:
            print(f'File is before start date %s, skipping...\n' % sdate)

def countfilerows(tablename,start_year):
    writetopath = f'{tmp_write_dir}/storm_'
    if len(glob.glob(writetopath)) == 0:
        source_url = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles'
        flist = listFull(source_url)
        writeFiles(flist, start_year)

    tabpath = writetopath + tablename + '*'
    tabfiles = glob.glob(tabpath)
    rows = 0
    for myfile in tabfiles:
        with gzip.open(myfile, 'rb') as f:
            for i, l in enumerate(f):
                pass
        rows+=i
    return rows
