import pyodbc

cnxn = pyodbc.connect("DSN=secfi;PWD=LT978885NUYZfJLa;UID=SA")
cursor = cnxn.cursor()
cursor.fast_executemany = True

cursor.execute("create table inserts (test varchar(32) primary key)")
cnxn.commit()
