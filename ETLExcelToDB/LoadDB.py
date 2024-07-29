import pandas as pd
import psycopg2
from ExcelToDB import *




def connect():
    
    try:
        connection = psycopg2.connect(user="*****",
                                  # пароль, который указали при установке PostgreSQL
                                  password="****",
                                  host="******",
                                  port="5432",
                                  database = "core")
        
        print("connection completed")
        
        return connection
        
    except:
        
        return "connection refused"
    
        

def readTable (tableName, schemaName, pathFile, engine):
    
    try:
        df = pd.read_csv(pathFile)
    except:
        return "No such file or directory"
    
    try:
        df.to_sql(tableName, engine, schema = schemaName, if_exists='append', index=False)
    except:
        return "Error compieled"
    
    return f"Insert {pathFile} complete"


def insertProcess():
    
    arrayPathFiles = "/home/santalovdv/CVM_ALL_TO_DB"  #path 
    
    for files in arrayPathFiles:
        
        try:
            readTable(tableName, schemaName, pathFile, engine)
            
        except:
            print(f"Не получилось импортировать {files}")

    print("Operation completed")
    return "Completed"











