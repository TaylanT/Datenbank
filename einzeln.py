#!/usr/bin/python
import csv
import unicodecsv
import os
import sys

import pandas as pd
from influxdb import DataFrameClient
# from influxdb.exceptions import InfluxDBClientError




zaehler = 0
list = []
pfad = u'/home/ttaylan/Dokumente/Datenbank/NeueDaten/'
dbname = 'Test'
messung = 'neu199'
#os.chdir(pfad)

# list aller vorhandener ordner

dateiname = '21-12-2015_12.56 Verdichter2900.lvm'
test = dateiname.encode('utf8')

filename = test.replace('.lvm', '.csv').replace('\xd6', 'oe').replace('\xfc', 'ue').replace('\xe4', 'ae')
client = DataFrameClient('localhost', 8086, 'root', 'root', dbname)
tag = os.path.basename(filename).replace('.csv', '')





query_string="select COP from neu where Filename=\'%s\'" %tag
#zum debugggen
#print query_string

#fehler handling datenbank
try:
    check=client.query(query_string)
except:
    check=True

#wenn leer (tag nicht vorhanden)
if bool(check)==False:
    #print 'ist leer'


    # oeffnen csv und auslesen inhalt
    sr=open(dateiname, "rb")
    in_txt = csv.reader(sr, delimiter = '\t')
    output = open(filename, 'wb')
    writer=csv.writer(output)
    index=0
    a=[]
    b=[]

    datei = open(dateiname, 'r')

    inhalt2=datei.read()

    t2=inhalt2.split('\n')


    #Bedingung wenn alte Messung
    if t2[0]=='LabVIEW Measurement\t\r':
        pass

    else:
        # NEUE MESSUNG
        for row in in_txt:     
            if row:
                writer.writerow(row)
            
        sr.close()
        output.close()
        
        
        datei=open(filename,'r')
        
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
        
        
        try:
            datei2=pd.read_csv(filename, skiprows=3,index_col=False,dtype=float64).fillna(0)
            try:

                del datei2['Zeit']
            except:
                pass
            try:
                del datei2['Kommentar']
            except:
                

                pass
            datei2['Q-Resorber']=datei2['Q-Resorber']*1000
            zeiti=pd.date_range(datumzeit, periods=len(datei2),freq='S')
            datei2.index=zeiti
            data = pd.DataFrame(datei2, index=zeiti)
        
        
           
            #tag=filename.replace('.csv','')
            client.write_points(data, messung,{'Filename': tag})
            print 'Ok neu:     %s'%(filename)
            zaehler=zaehler+1
        except ValueError:
            e = sys.exc_info()[0]
            print "<p>Error: %s</p>%s" % (e,filename)

            
#wenn vorhanden
else:
    #print 'ist vorhanden'
      pass
                


print 'OK'
        


            
           
