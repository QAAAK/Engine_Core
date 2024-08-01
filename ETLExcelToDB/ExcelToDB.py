import pandas as pd
from zipfile import ZipFile
from bs4 import BeautifulSoup
import os





class ExcelToDB:
    
    
    
    def __init__(self, pathFile):
        
        self.path = pathFile 
        
    
    def excelToCSV (self, sheetName):  

        result = "Выполнено"  
        try:
            self.createCSVFile(sheetName)
            readFile = pd.read_excel (self.path, sheet_name = sheetName)
            readFile.to_csv (f"/home/santalovdv/csvT/{sheetName}.csv", index = None, header=True) 
            print(f"{sheetName}.csv сохранен")
            
        except: 
            
            result = "Ошибка"
            return result
        
        return result


    def quantitySheet(self,path):
        
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
        
        my_file = open(f"/home/santalovdv/csvT/{name}.csv", "w+")
        my_file.close()
        
        return f"{name}.csv File create"


    def saveDatainFiles(self):
        
        arraySheet = self.quantitySheet(self.path)
        
        try:
            for sheet in arraySheet:
                self.excelToCSV(sheet)
                
            return "Passed"
        except:
            
            return "Error"
        


    def arrayFile(self, path):
        
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



