import pandas as pd
import sqlalchemy
import pyodbc

pd.read_csv("~/programming/secfi/affiliations.csv").to_sql("users", con="mssql+pyodbc://SA:LT978885NUYZfJLa@secfi")