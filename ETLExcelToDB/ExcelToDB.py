import pandas as pd
from zipfile import ZipFile
from bs4 import BeautifulSoup
import os





class ExcelToDB:
    
    
    
    def __init__(self, pathFileSource, pathFileTarget):

        """

        :param pathFileSource: Путь к excel файлу (источнику)
        :param pathFileTarget: Путь к csv файлам
        """
        
        self.pathSource = pathFileSource
        self.pathTarget = pathFileTarget
        
    
    def excelToCSV (self, sheetName):

        """
        Функция, преобразующий лист в csv-файл

        :param sheetName: Имя листа
        :return:
        """

        result = "Passed"
        try:
            self.createCSVFile(sheetName)
            readFile = pd.read_excel (self.pathSource, sheet_name = sheetName)
            readFile.to_csv (f"{self.pathTarget}{sheetName}.csv", index = None, header=True)
            print(f"{sheetName}.csv created")
            
        except: 
            
            result = "Error"
            return result
        
        return result


    def quantitySheet(self,path):

        """
        Функция, возвращающая список наименований листов


        :param path: Путь к excel файлу (источнику)
        :return:
        """
        
        try:

            with ZipFile(path, 'r') as z:
                with z.open('xl/workbook.xml') as f:
                    soup = BeautifulSoup(f.read(), 'xml')
                    sheets = [sheet.get('name') for sheet in soup.find_all('sheet')]
                    
                    return sheets 
                    
        except:
            
            print("file not found")
            
            return 0
        
            
    def createCSVFile (self, name):
        """

        Функция создает csv файл

        :param name: Имя файла
        :return:
        """
        
        my_file = open(f"{self.pathTarget}{name}.csv", "w+")
        my_file.close()
        
        return f"{name}.csv File create"


    def saveDatainFiles(self):

        """

        Функция, вызывающая процесс репликации

        :return:
        """
        
        arraySheet = self.quantitySheet(self.pathSource)
        
        try:
            for sheet in arraySheet:
                self.excelToCSV(sheet)
                
            return "Passed"
        except:
            
            return "Error"
        


    def arrayFile(self, path):

        """

        Функция, возвращающая путь к файлам

        :param path: Путь к директории (источнику)
        :return:
        """
        
        directory = path
        
        try:
            files = os.listdir(directory)
            
            arrayFiles = []
            
            for file in files:
                
                if file == '.ipynb_checkpoints':
                    continue
                
                arrayFiles.append(directory + "/" + file)
            
            return arrayFiles
                
        except:
            
            return "no such directory"
    
        

# print(saveDatainFiles())


# excelToCsv()



