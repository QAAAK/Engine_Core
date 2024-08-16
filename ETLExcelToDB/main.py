from ExcelToDB import *
from LoadDB import *

#Source files
read = ExcelToDB("/home/santalovdv/CVM_ALL/", "home/santalovdv/csv")
read.saveDatainFiles()

writer = LoadDB("/home/santalovdv/CVM_ALL_TO_DB")
writer.connect()
writer.insertProcess()