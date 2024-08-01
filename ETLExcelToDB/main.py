from ExcelToDB import *
from LoadDB import *


read = ExcelToDB("/home/santalovdv/CVM_ALL/CVM_all.xlsx")
read.saveDatainFiles()

writer = LoadDB("/home/santalovdv/CVM_ALL_TO_DB")
writer.connect()
writer.insertProcess()