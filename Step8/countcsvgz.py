import gzip
import getdata
import os
import glob

flist = getdata.listFull('https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles')
getdata.writeFiles(flist, 2000)
dir_path = r'/home/conner/SevereWeatherDB/filecounter/storm_'

def countfilerows(tablename):
    tabpath = dir_path + tablename + '*'
    tabfiles = glob.glob(tabpath)
    rows = 0
    for myfile in tabfiles:
        with gzip.open(myfile, 'rb') as f:
            for i, l in enumerate(f):
                pass
        rows+=i
    return rows
