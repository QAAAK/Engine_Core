import pandas as pd
from zipfile import ZipFile
from bs4 import BeautifulSoup
import os






def excelToCsv (path, sheetName):  
    result = "Выполнено"  
    try:
        createCSVFile(sheetName)
        readFile = pd.read_excel (path, sheet_name = sheetName)
        readFile.to_csv (f"CVM_ALL_TO_DB/{sheetName}.csv", index = None, header=True) 
        print(f"{sheetName} создан и сохранен")
        
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
        
        print("Файл не найден.")
        
        return 0
    
        
def createCSVFile (name):
    
    my_file = open(f"CVM_ALL_TO_DB/{name}.csv", "w+")
    my_file.close()
    
    return "file create"


def saveDatainFiles():
    
    arraySheet = quantitySheet(r"CVM_ALL/CVM_all.xlsx")
    
    try:
        for sheet in arraySheet:
            excelToCsv(path, sheet)
        return "Passed"
    except:
        return "Error"
    





# excelToCsv()

path = "CVM_ALL/CVM_all.xlsx"

arraySheet = quantitySheet(r"CVM_ALL/CVM_all.xlsx")

for sheet in arraySheet:
    excelToCsv(path, sheet)
    
    
   