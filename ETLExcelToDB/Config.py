class Config:

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
