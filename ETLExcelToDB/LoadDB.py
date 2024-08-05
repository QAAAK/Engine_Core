import pandas as pd
import psycopg2
from ExcelToDB import *


class LoadDB:
    
    
    def __init__(self, path):
        """


        :param path: Путь к csv файлам
        """
        
        self.pathCSVFiles = path 
    
    def connect(self):

        """

        Подключение (пока что только к psycopg2 (gp))

        :return:
        """


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

        """
        Запись в базу данных

        :param tableName:
        :param schemaName:
        :param pathFile:
        :param engine:
        :return:
        """

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

        """
        Процесс загрузки в заданную таблицу

        :return:
        """
        
        arrayPathFiles = self.pathCSVFiles  #path 
        
        for files in arrayPathFiles:
            
            try:
                self.readTable(tableName, schemaName, pathFile, engine) # Update feature
                
            except:
                print(f"Failed to import {files}")

        print("Operation completed")
        return "Completed"











