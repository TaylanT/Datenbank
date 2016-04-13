#!/usr/bin/python
import csv
import unicodecsv
import os
import csv
import pandas as pd
from influxdb import DataFrameClient
from influxdb.exceptions import InfluxDBClientError
import os
import numpy as np



zaehler=0
list=[]
pfad=u'/home/ttaylan/Dokumente/Datenbank/NeueDaten/'
dbname='Messwerte'
messung='nachdarm4'
os.chdir(pfad)

#list aller vorhandener ordner
thedir=pfad
ordner=[ name for name in os.listdir(thedir) if os.path.isdir(os.path.join(thedir, name)) ]

#ordner=os.listdir(u"./")
for user in ordner:
    os.chdir(pfad+user)
    list=[]
    #print os.getcwd()
    print user
    
    
   
    
    for file in os.listdir(u"./"):
        #print file
        if file.endswith(".lvm") and os.path.getsize(file)>=1000000:
            list.append(pfad+user+'/'+file)

    for dateiname in list:
        #umwandlung dateiname lvm in csv und erstellung eines tags fuer speicherung in datenbank
        test=dateiname.encode('utf8')       
	        
        filename=test.replace('.lvm','.csv').replace('\xd6','oe').replace('\xfc','ue').replace('\xe4','ae')
        client = DataFrameClient('localhost', 8086, 'root', 'root', dbname)
        tag=os.path.basename(filename).replace('.csv','')



        if os.path.isfile(filename):
            pass
        else:
            
    
                       
        	index=0

        #Abfrage ob Datei shon in Datenbank vorhanden
        
        query_string="select COP from nachdarm4 where Filename=\'%s\'" %tag
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

                
                
                
                
            #oeffnen csv und auslesen inhalt     
            sr=open(dateiname, "rb")
            in_txt = csv.reader(sr, delimiter = '\t')
            output = open(filename, 'wb')
            writer=csv.writer(output)
            index=0
            a=[]
            b=[]
        
               
            datei=open(dateiname,'r')
                
            inhalt2=datei.read()
                
            t2=inhalt2.split('\n')
            
                
            #Bedingung wenn alte Messung
            if t2[0]=='LabVIEW Measurement\t\r':
              
                    
               
                for row in in_txt:    
                    if index==24:
                        laenge=len(row)
                                
                        for spalte in row:
                            conv=spalte.replace('\xd6','oe').replace('\xfc','ue').replace('\xb3','3')
                                  
                            a.append(conv)
                                    
                        del a[laenge-1]
                        row=a
                                
                           
                            
                                
                                
                    if index>24:
                        del row[laenge-1]
                        writer.writerow(row)
                    else:
                        writer.writerow(row)     
                    
                        
                   
                            
                    index=index+1
            
                
                    
                
             
                datei=open(filename,'r')
                
                inhalt=datei.read()
                
                t=inhalt.split('\n')
       


                day=t[10][13:15]
                month=t[10][10:12]
                year=t[10][5:9]
                uhrzeit=t[11][5:10]
                datumzeit=year+'-'+month+'-'+day+' '+uhrzeit
                datei2=pd.read_csv(filename, skiprows=24,index_col=False).fillna(0)
                #datei2=pd.read_csv(filename, skiprows=3,index_col=False).fillna(0)
                
                try:
                    datei2=pd.read_csv(filename, skiprows=24,index_col=False).fillna(0)
                    del datei2['X_Value']
                    zeiti=pd.date_range(datumzeit, periods=len(datei2),freq='S')
                    datei2.index=zeiti
                    data = pd.DataFrame(datei2, index=zeiti)
                    
                    
                    
                    client.write_points(data, messung,{'Filename': tag, 'User':user})
                    print 'alt erfolgreich! %s' % (filename)
                    zaehler=zaehler+1
                except:
                
                    
                    print 'DBError:     %s'%(filename)
                
               
                
                
                    
                    
                #Abschnitt neues Protokoll    
            else:
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
                    datei2=pd.read_csv(filename, skiprows=3,index_col=False).fillna(0)
                    del datei2['Zeit']
                    del datei2['Kommentar']
                    datei2['Q-Resorber']=datei2['Q-Resorber']*1000
                    zeiti=pd.date_range(datumzeit, periods=len(datei2),freq='S')
                    datei2.index=zeiti
                    data = pd.DataFrame(datei2, index=zeiti).replace([np.inf, -np.inf], np.nan).fillna(0)
                
                
                   
                    #tag=filename.replace('.csv','')
                    client.write_points(data, messung,{'Filename': tag,'User':user})
                    print 'Ok neu:     %s'%(filename)
                    zaehler=zaehler+1
                except KeyError:
            
                    print 'Error:     %s'%(filename)
        #wenn vorhanden
        else:
            #print 'ist vorhanden'
		      pass
	            
anzahl=len(list)

print 'es wurden %d Messungen von %d Messungen erfogreich importiert' %(zaehler,anzahl)
        


            
           