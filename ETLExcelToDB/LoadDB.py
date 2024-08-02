import pandas as pd
import psycopg2
from ExcelToDB import *


class LoadDB:
    
    
    def __init__(self, path):
        
        self.pathCSVFiles = path 
    
    def connect(self):
        
        try:
            connection = psycopg2.connect(user="*****",
                                    password="****",
                                    host="******",
                                    port="5432",
                                    database = "core")
            
            print("connection completed")
            
            return connection
            
        except:
            
            return "connection refused"
        
            

    def readTable (self, tableName, schemaName, pathFile, engine):
        
        try:
            df = pd.read_csv(pathFile)
        except:
            return "No such file or directory"
        
        try:
            df.to_sql(tableName, engine, schema = schemaName, if_exists='append', index=False)
        except:
            return "Error compieled"
        
        return f"Insert {pathFile} complete"


    def insertProcess(self):
        
        arrayPathFiles = self.pathCSVFiles  #path 
        
        for files in arrayPathFiles:
            
            try:
                self.readTable(tableName, schemaName, pathFile, engine) # Update feature
                
            except:
                print(f"Failed to import {files}")

        print("Operation completed")
        return "Completed"











