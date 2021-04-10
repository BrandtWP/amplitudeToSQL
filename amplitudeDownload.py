from zipfile import ZipFile
from io import BytesIO 
import gzip
import json
import pyodbc
import pandas as pd
import requests

class uploadData:
    
    def __init__(self, connection):
        self.cnxn = connection
        self.cursor = connection.cursor()
        self.cursor.fast_executemany = True

        self.createTables()

        self.columns = self.getColumns()

    def getColumns(self):
        columnsDict = {"events":[], "eventProperties":[], "users": []}

        for key in columnsDict.keys():
            for row in self.cursor.execute("select c.name from sys.columns c where c.object_id = object_id('%s');" % key):
                columnsDict[key].append(row[0])

        return columnsDict

    def placeholder(self, length):
        return ",".join("[?]" * length)

    def createTables(self):

        self.cursor.execute("IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'events')" + open("createTable.sql").read())
        self.cnxn.commit()

        eventCols = ["[%s] varchar(1024)" % col.replace("\t", "") for col in pd.read_csv("eventSchema.csv")["\tEvent Property"].unique().tolist() if col != "\t"]
        self.cursor.execute("IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'eventProperties') create table eventProperties (event_id int primary key, %s)" % ",".join(eventCols))
        self.cnxn.commit()
    
    def addColumn(self, columnName, columnType, tableName):
        self.cursor.execute("ALTER TABLE %s ADD %s %s NULL" % (tableName, columnName, columnType))
        self.cnxn.commit()
    
    def uploadRow(self, row):

        if row["event_properties"] != {}: self.uploadEventProperties(row["event_id"], row["event_properties"])


        row = {key:row[key] for key in self.columns["events"] if row[key] != None}

        stmt = "insert events([%s]) values (%s)" % ("], [".join(row.keys()), str(list(self.prepInput(row.values())))[1:-1])

        try: 
            self.cursor.execute(stmt)
        except:
            pass
    
    def prepInput(self, dirty):
        cleaned = []
        
        for item in dirty:
            if type(item) == type(False):
                cleaned.append(int(item))
            elif type(item) == type(""):
                cleaned.append(item.translate(str.maketrans({
                                            "]":  r"\]",
                                            "\\": r"\\",
                                            "^":  r"\^",
                                            "$":  r"\$",
                                            "*":  r"\*",
                                            "'": "\""
                                            })))
            else:
                cleaned.append(item)

        return cleaned
            
    
    def uploadEventProperties(self, event_id, event_properties):
        stmt = "insert eventProperties(event_id, [%s]) values (%s, '%s')" % ("],[".join(event_properties.keys()), event_id, "', '".join([str(val).replace("'","") for val in event_properties.values()]))
        try: 
            self.cursor.execute(stmt)
        except:
            pass

    def unzip(self, data):
        zf = ZipFile(data)

        for file in zf.infolist():
            for row in gzip.GzipFile(fileobj=BytesIO(zf.open(file.filename).read())):
                self.uploadRow(json.loads(row))
            
            self.cnxn.commit()

def generateFiles(startDate, apiKey, secretKey):
    while startDate < 20210501:
        print(startDate)
        yield BytesIO(requests.get("https://amplitude.com/api/2/export?start=%s&end=%s" % (startDate, startDate+100), auth=(apiKey, secretKey)).content)
        startDate = startDate + 100


connection = pyodbc.connect("DSN=secfi;PWD=LT978885NUYZfJLa;UID=SA")
uploader = uploadData(connection)

files = generateFiles(20210101, "d6436a7afc39b8fc01ac1750b01b114d", "af647cd915597b8c6f868931a57b4341")

while True:
    uploader.unzip(next(files))



