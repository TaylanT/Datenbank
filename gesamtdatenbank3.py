#!/usr/bin/python
import csv
import unicodecsv
import os
import sys
import numpy as np

import pandas as pd
from influxdb import DataFrameClient
# from influxdb.exceptions import InfluxDBClientError
from dirsync import sync
sync('/home/ttaylan/Studenten/HT-Waermepumpe/Messdaten',
    '/home/ttaylan/Dokumente/Datenbank/NeueDaten/', 'sync')


def abfrage_datenbank(filename, messung, tag):
    """Dynamische Query fuer abrage ob eintrag in Datenbank existiert."""
    return"select COP from %s where Filename=\'%s\'" % (messung, tag)

def find_datumzeit(filename,status):
    """Zustand fuer Neue Messung."""
    datei = open(filename, 'r')

    inhalt = datei.read()

    t = inhalt.split('\n')

    if status == 'neu':
    # fuer tags

        day = t[1][0:2]
        month = t[1][3:5]
        year = t[1][6:10]
        uhrzeit = t[5][0:8].replace('.', ':')
    else:
        day = t[10][13:15]
        month = t[10][10:12]
        year = t[10][5:9]
        uhrzeit = t[11][5:10]
        

    return year+'-'+month+'-'+day+' '+uhrzeit

zaehler = 0
list = []
pfad = u'/home/ttaylan/Dokumente/Datenbank/NeueDaten/'
dbname = 'DataHeatPump'
messung = 'NeueDaten'
client = DataFrameClient('localhost', 8086, 'root', 'root', dbname)
os.chdir(pfad)

# list aller vorhandener ordner
thedir = pfad
ordner = [name for name in os.listdir(thedir) if
        os.path.isdir(os.path.join(thedir, name))]

# ordner=os.listdir(u"./")
for user in ordner:
    os.chdir(pfad+user)
    list = []
    # print os.getcwd()
    print user

    for file in os.listdir(u"./"):
        # print file
        if file.endswith(".lvm") and os.path.getsize(file) >= 1000000:
            list.append(pfad+user+'/'+file)

    for dateiname in list:
        # umwandlung dateiname lvm in csv und erstellung eines tags fuer speicherung in datenbank
        test = dateiname.encode('utf8')

        filename = test.replace('.lvm', '.csv').replace('\xd6', 'oe')\
            .replace('\xfc', 'ue').replace('\xe4', 'ae')
        
        tag = os.path.basename(filename).replace('.csv', '')

        if os.path.isfile(filename):
            pass
        else:
            index = 0

        # Abfrage ob Datei shon in Datenbank vorhanden

        try:
            query_str = abfrage_datenbank(filename, messung, tag)
            
            check = client.query(query_str)

        except:
            check = True
        # wenn leer (tag nicht vorhanden)
        if bool(check) is False:
            # oeffnen csv und auslesen inhalt

            sr = open(dateiname, "rb")
            in_txt = csv.reader(sr, delimiter='\t')
            output = open(filename, 'wb')
            writer = csv.writer(output)
            index = 0
            a = []
            b = []

            datei = open(dateiname, 'r')

            inhalt2 = datei.read()

            t2 = inhalt2.split('\n')
            # Bedingung wenn alte Messung
            if t2[0] == 'LabVIEW Measurement\t\r':
                pass
                
                # for row in in_txt:
                #     if index == 24:
                #         laenge = len(row)

                #         for spalte in row:
                #             conv = spalte.replace('\xd6', 'oe')\
                #                 .replace('\xfc', 'ue').replace('\xb3', '3')

                #             a.append(conv)

                #         del a[laenge-1]
                #         row = a

                #     if index > 24:
                #         del row[laenge-1]
                #         writer.writerow(row)
                #     else:
                #         writer.writerow(row)

                #     index = index+1

                # try:
                #     datumzeit = find_datumzeit(filename, 'alt')
                #     datei2 = pd.read_csv(filename,
                #                          skiprows=24,
                #                          index_col=False).fillna(0)
                #     try:
                #         del datei2['X_Value']
                #     except:
                #         pass
                #     zeiti = pd.date_range(datumzeit,
                #                           periods=len(datei2),
                #                           freq='S')
                #     datei2.index = zeiti
                #     data = pd.DataFrame(datei2, index=zeiti)

                #     client.write_points(data, messung, {'Filename': tag,
                #                                         'User': user})
                #     print 'alt erfolgreich! %s' % (filename)
                #     zaehler = zaehler+1
                # except:

                #     print 'DBError:     %s' % (filename)

            else:
                # NEUE MESSUNG############################################
                for row in in_txt:
                    if row:
                        writer.writerow(row)
                sr.close()
                output.close()

                try:
                    datumzeit = find_datumzeit(filename, 'neu')
                    
                    datei2 = pd.read_csv(filename, skiprows=3, index_col=False)

                    datei2 = datei2.replace([np.inf, -np.inf], np.nan)

                    datei2 = datei2.fillna(0)
                    # print datei2.head()
                    # datei2 = datei2.astype('float64')
                    print datumzeit
                    try:

                        del datei2['Zeit']
                    except:
                        pass
                    try:
                        del datei2['Kommentar']
                    except:
                        pass
                    try:

                        datei2['Q-Resorber'] = datei2['Q-Resorber']*1000
                    except KeyError:
                        pass
                    zeiti = pd.date_range(datumzeit,
                                          periods=len(datei2),
                                          freq='S')
                    datei2.index = zeiti
                    data = pd.DataFrame(datei2, index=zeiti)

                    # tag=filename.replace('.csv','')
                    # print data.dtypes
                    client.write_points(data, messung, {'Filename': tag,
                                                        'User': user})
                    print 'Ok neu:     %s' % (filename)

                except:
                    e = sys.exc_info()[0]
                    print "<p>Error: %s</p>%s" % (e, filename)
        # wenn vorhanden
        else:
            # print 'ist vorhanden'
            pass
print 'OK'
