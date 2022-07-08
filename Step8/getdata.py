from bs4 import BeautifulSoup
import requests
from pathlib import Path
import os

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
    if Path('/home/conner/SevereWeatherDB/data/unzipped').is_dir() == False:
        os.system('mkdir /home/conner/SevereWeatherDB/filecounter ')
    for file in flist:
        print(f'Processing %s' % file)
        felements = Path(file).parts[-1].split('_')
        fyear = felements[3][1:5]
        if int(fyear) >= sdate:
            print(f'File is past start date %s, connecting...' % sdate)
            ftype = felements[1].split('-')[0]
            fdateup = felements[-1][1:9]
            fname = '/home/conner/SevereWeatherDB/filecounter/'+f'storm_{ftype}_{fyear}_{fdateup}.csv.gz'
            r = requests.get(file)
            with open(fname, 'wb') as f:
                print('Writing locally...')
                f.write(r.content)
                print('Write complete!\n')
        else:
            print(f'File is before start date %s, skipping...\n' % sdate)
