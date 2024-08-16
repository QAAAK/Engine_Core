class MapTable:


    __map = {}

    def __init__(self, project):

        self.project = project


    def getMapTable(self):

        if self.project == 'CVM_itog':

            tableToCSV = {'CVM_ALL': 'fgao_sb.cvm_abnt',
                         'CVM_SRV' : 'fgao_sb.cvm_crv',
                         'CVM_dokupka_abon' : 'fgao_sb.cvm_tb_abnt',
                         'CVM_TP_abon' : 'fgao_sb.cvm_tp_abnt'}

        __map = tableToCSV


        return tableToCSV

    def getKeyTableInMap (self, key):

        keyDict = self.__map.keys(key)

        return keyDict


    def getValueFileInMap(self, value):

        valueDict = self.__map.values(value)

        return valueDict



    