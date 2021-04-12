from zipfile import ZipFile
from io import BytesIO 
import gzip
import json
import psycopg2
import requests
import datetime
import json
from sys import stdout
import math
from os import environ

class uploadData:
    
    def __init__(self, cnxn):
        self.cnxn = cnxn
        self.cursor = cnxn.cursor()
        self.createTables()

        self.cursor.execute("select column_name from information_schema.columns where table_name = 'events';")
        self.columns = [row[0] for row in self.cursor.fetchall()]
        self.cursor.execute("select column_name from information_schema.columns where table_name = 'events' and udt_name = 'json';")
        self.jsonColumns = [row[0] for row in self.cursor.fetchall()]

        self.stmt = "insert into events(\"%s\") values (%s) ON CONFLICT DO NOTHING;" % ("\", \"".join(self.columns), ", ".join(["%%(%s)s" % column for column in self.columns]))

    def createTables(self):

        self.cursor.execute(open("createTable.sql").read())
        self.cnxn.commit()
    
    def uploadRow(self, row):

        for jsonColumn in self.jsonColumns:
            row[jsonColumn] = json.dumps(row[jsonColumn]) 

        try:
            self.cursor.execute(self.stmt, row)
        except Exception as e:
            print(row)
            print(e)
            exit()

    def unzip(self, data):
        zf = ZipFile(data)

        toolbar_width = 100

        files = zf.infolist()

        for i, file in enumerate(files):
            for row in gzip.GzipFile(fileobj=BytesIO(zf.open(file.filename).read())):
                self.uploadRow(json.loads(row))

            progress = math.floor((i+1) / len(files) * toolbar_width)
            stdout.write("\r[%s%s] %s%%" % ("-"*progress, " "*(toolbar_width-progress), int(100 * (i+1) / len(files))))
            stdout.flush()
        
        self.cnxn.commit()
        
        stdout.write("\n")


def generateFiles(startDate, apiKey, secretKey, interval=datetime.timedelta(days=30)):
    startDate = datetime.datetime.strptime(startDate, "%Y%m%d%H")

    while startDate < datetime.datetime.today():
        print(startDate)
        yield BytesIO(requests.get("https://amplitude.com/api/2/export?start=%s&end=%s" % (startDate.strftime("%Y%m%dT%H"), (startDate + interval).strftime("%Y%m%dT%H")), auth=(apiKey, secretKey)).content)
        startDate = startDate + interval


cnxn = psycopg2.connect(dbname="amplitude", host="localhost", user="amplitude", password=environ["dbpassword"])

uploader = uploadData(cnxn)

files = generateFiles("2020013100", environ["apikey"], environ["secretkey"])

# uploader.unzip(BytesIO(open("temp.zip","rb").read()))

while True:
    uploader.unzip(next(files))



