import pandas as pd
from zipfile import ZipFile
from bs4 import BeautifulSoup
import os






def excelToCSV (sheetName):  
    result = "Выполнено"  
    try:
        createCSVFile(sheetName)
        readFile = pd.read_excel ("/home/santalovdv/CVM_ALL/CVM_all.xlsx", sheet_name = sheetName)
        readFile.to_csv (f"/home/santalovdv/csvT/{sheetName}.csv", index = None, header=True) 
        print(f"{sheetName}.csv сохранен")
        
    except: 
        
        result = "Ошибка"
        return result
    
    return result


def quantitySheet(path):
    
    try:

        with ZipFile(path, 'r') as z:
            with z.open('xl/workbook.xml') as f:
                soup = BeautifulSoup(f.read(), 'xml')
                sheets = [sheet.get('name') for sheet in soup.find_all('sheet')]
                
                return sheets 
                
    except:
        
        print("file not found")
        
        return 0
    
        
def createCSVFile (name):
    
    my_file = open(f"/home/santalovdv/csvT/{name}.csv", "w+")
    my_file.close()
    
    return f"{name}.csv File create"


def saveDatainFiles():
    
    arraySheet = quantitySheet(r"/home/santalovdv/CVM_ALL/CVM_all.xlsx")
    
    try:
        for sheet in arraySheet:
            excelToCSV(sheet)
            
        return "Passed"
    except:
        
        return "Error"
    


def arrayFile(path):
    
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



