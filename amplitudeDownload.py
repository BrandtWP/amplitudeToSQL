from zipfile import ZipFile
from io import BytesIO 
import gzip
import json
import pyodbc
class downloadData:
    
    def __init__(self, connectionString):
        self.cnxn = pyodbc.connect(connectionString)
        self.cursor = self.cnxn.cursor()
        self.cursor.fast_executemany = True

        try:
            self.createTables()
        except:
            pass

        self.columns = self.getColumns()

    def getColumns(self):
        columnsDict = {"inserts":[], "events":[], "users": []}

        for key in columnsDict.keys():
            for row in self.cursor.execute("select c.name from sys.columns c where c.object_id = object_id('%s');" % key):
                columnsDict[key].append(row[0])

        return columnsDict

    def createTables(self):
        self.cursor.execute(open("createTable.sql").read())
        self.cnxn.commit()
    
    def addColumn(self, columnName, columnType, tableName):
        self.cursor.execute("ALTER TABLE %s ADD %s %s NULL" % (tableName, columnName, columnType))
        self.cnxn.commit()
    
    def uploadRow(self, row):

        values = json.dumps([row[key] for key in self.columns["inserts"]]).replace("false", "0").replace("true", "1").replace('"', "'")[1:-1]
        
        sqlStmt = "insert inserts([%s]) values (%s)" % ("], [".join(self.columns["inserts"]), values)
        print(sqlStmt)
        self.cursor.execute(sqlStmt)

    def unzip(self, data):
        zf = ZipFile(data)

        for file in zf.infolist():
            for row in gzip.GzipFile(fileobj=BytesIO(zf.open(file.filename).read())):
                self.uploadRow(json.loads(row))
                break
            break

downloader = downloadData("DSN=secfi;PWD=LT978885NUYZfJLa;UID=SA")
# downloader.createTables()
# downloader.addColumn("test", "varchar(64)", "events")
downloader.unzip(BytesIO(open("output.json","rb").read()))