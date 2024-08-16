import os


class Config:
    
    
    pathDicrectory = []
    
    
    def createDir (self, path):
        
        """
            Функция, создающия папку
        
        """
        
        try:
            os.mkdir(path)
            
            print(f"directory created {path}")
        except:
            
            os.rmdir(path)
            os.mkdir(path)
            
            print(f"directory removed and created {path}")
    

    def aggrDirs(self, path):

        """

        Функция, создающая папки для каждого агрегированного файла.

        :param path: Путь к директории (источнику)
        :return:
        """

        directory = path

        try:
            files = os.listdir(directory)

            for file in files:

                if file == '.ipynb_checkpoints' or file == '.git':
                    continue
                    
                self.createDir(os.path.splitext(file)[0])

        except:
            
            return "no such directory"
                



            
            
a = Config()
a.aggrDirs("/home/santalovdv/CVM_P")