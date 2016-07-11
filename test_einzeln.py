#!/usr/bin/python
import csv
import unicodecsv
import os
import sys
import numpy as np
import pandas as pd
from influxdb import DataFrameClient
# from influxdb.exceptions import InfluxDBClientError






pfad = u'/home/ttaylan/Dokumente/Datenbank/NeueDaten/'
dbname = 'test2'
messung = 'neu2'
filename = '02-05-2016_14.48 Hochdrucksteuerung_1500_2.5.16_2.csv'
client = DataFrameClient('localhost', 8086, 'root', 'root', dbname)

datei = open(filename,'r')

inhalt=datei.read()

t=inhalt.split('\n')
         #fuer tags
tagsi=t[0]
tags=tagsi.replace('\r','').split(',')

day=t[1][0:2]
month=t[1][3:5]
year=t[1][6:10]
uhrzeit=t[5][0:8].replace('.',':')
datumzeit=year+'-'+month+'-'+day+' '+uhrzeit
tag = filename.replace('.csv', '')
print datumzeit

datei2 = pd.read_csv(filename, skiprows=3,index_col=False)
datei2 = datei2.fillna(0).replace([np.inf, -np.inf], np.nan)
datei2 = datei2.fillna(0)
try:

    del datei2['Zeit']
except:
    pass
try:
    del datei2['Kommentar']
except:
    pass
try:

    datei2['Q-Resorber']=datei2['Q-Resorber']*1000
except KeyError:
    pass
zeiti=pd.date_range(datumzeit, periods=len(datei2),freq='S')
datei2.index=zeiti
data = pd.DataFrame(datei2, index=zeiti)
data=data.astype('float64')



#tag=filename.replace('.csv','')
# print data.dtypes
client.write_points(data, messung)
print 'Ok neu:     %s'%(filename)
    

    # e = sys.exc_info()[0]
    # print "<p>Error: %s</p>%s" % (e,filename)