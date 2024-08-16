import pandas as pd
import psycopg2
from ExcelToDB import *


class LoadDB:


    #Приватные переменные
    __tableName = ""
    __schemaName = ""
    __pathFile = ""
    __engine = psycopg2()



    
    
    def __init__(self, path):
        """
        
        :param path: Путь к csv файлам
        
        """
        
        self.pathCSVFiles = path 
    
    def connect(self, db):

        """

        Подключение (пока что только к psycopg2 (gp))

        :return:
        """

        if db == 'gp':

            try:

                connection = psycopg2.connect(user="*****",
                                             password="****",
                                             host="******",
                                             port="5432",
                                             database = "core")

                print("connection completed")

                __engine = connection

                return connection

            except:

                return "connection refused"

        
        return "no such connection"



    def setTableToWriting(self, tableName, schemaName, pathFile):

        self.__tableName = tableName
        self.__schemaName = schemaName
        self.__pathFile = pathFile



    def readTable (self):

        """
        Запись в базу данных

        :param tableName:
        :param schemaName:
        :param pathFile:
        :param engine:
        :return:
        """

        try:
            df = pd.read_csv(self.__pathFile)
        except:
            return "No such file or directory"
        
        try:
            df.to_sql(self.__tableName, self.__engine, schema = self.__schemaName, if_exists='append', index=False)
        except:
            return "Error compieled"
        
        return f"Insert {self.__pathFile} complete"


    def insertProcess(self):

        """
        Процесс загрузки в заданную таблицу

        :return:
        """
        
        arrayPathFiles = self.pathCSVFiles  #path 
        
        for files in arrayPathFiles:
            
            try:
                # self.readTable(tableName, schemaName, pathFile, engine) # Update feature
               pass
            except:
                print(f"Failed to import {files}")

        print("Operation completed")
        return "Completed"











