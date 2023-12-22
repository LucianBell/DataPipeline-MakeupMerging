import csv
import json


class Dados:
    def __init__(self, path, tipo_dados):
        self.__path = path
        self.__tipo_dado = tipo_dados
        self.__dados = self.leitura_dados()
        self.__nomes_colunas = self.get_columns()

    def __leitura_json(self):
        with open(self.__path, 'r') as file:
            dados_json = json.load(file)
        return dados_json
    
    def __leitura_csv(self):
        dados_csv = []
        with open(self.__path, 'r') as file:
            spamreader = csv.DictReader(file, delimiter=',') 
            for row in spamreader:
                dados_csv.append(row)
        return dados_csv
    
    def leitura_dados(self):
        dados = []
        if self.__tipo_dado == 'csv':
            dados = self.__leitura_csv()
        elif self.__tipo_dado == 'json':
            dados = self.__leitura_json()
        elif self.__tipo_dado == 'list':
            dados = self.__path
            self.__path = 'Lista em memória'

        return dados

    def get_columns(self):
        return list(self.__dados[-1].keys())   

    def rename_columns(self, key_mapping):
        novos_dados = []
    
        for old_dict in self.__dados:
            dict_temp = {}
            for old_key, value in old_dict.items():
                dict_temp[key_mapping[old_key]] = value
            novos_dados.append(dict_temp)
    
        self.__dados = novos_dados
        self.__nomes_colunas = self.get_columns()

    def join(dados1, dados2):
        dados_combinados = []

        dados_combinados.extend(dados1.__dados)
        dados_combinados.extend(dados2.__dados)

        return Dados(path=dados_combinados, tipo_dados='list')
    
    def __formata_campos_invalidos(self):
        combined_data_table = [self.__nomes_colunas]

        for row in self.__dados:
            linha = []
            for column in self.__nomes_colunas:
                linha.append(row.get(column, 'Indisponível'))
            combined_data_table.append(linha)

        return combined_data_table

    def salvando_dados(self, path):
        combined_data_table = self.__formata_campos_invalidos()

        try:
            with open(path, 'w') as file:
                writer = csv.writer(file)
                writer.writerows(combined_data_table)
            print("Dados salvos com sucesso")
        except:
            print("Dados não foram salvos")