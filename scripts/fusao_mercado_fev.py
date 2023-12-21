"""
Função do script: deixar claro para os novos devs como é feito o pipeline de dados
"""

import json
import csv
from processamento_dados import Dados

# Funções
def leitura_json(path_json):
    with open(path_json, 'r') as file:
        dados_json = json.load(file)
    return dados_json

def leitura_csv(path_csv):
    dados_csv = []
    with open(path_csv, 'r') as file:
        spamreader = csv.DictReader(file, delimiter=',') 
        for row in spamreader:
            dados_csv.append(row)
    return dados_csv

def leitura_dados(path, tipo_arquivo):
    dados = []
    if tipo_arquivo == 'csv':
        dados = leitura_csv(path_csv=path)
    else:
        dados = leitura_json(path_json=path)
    return dados

def get_columns(dados):
    return list(dados[-1].keys())    


def troca_keys(dados_antigos, key_mapping):
    novos_dados = []

    for old_dict in dados_antigos:
        dict_temp = {}
        for old_key, value in old_dict.items():
            dict_temp[key_mapping[old_key]] = value
        novos_dados.append(dict_temp)

    return novos_dados

def une_dado(dados1, dados2):
    dados_combinados = []
    
    dados_combinados.extend(dados2)
    dados_combinados.extend(dados1)

    return dados_combinados

def formata_campos_invalidos(dados, nome_colunas):
    combined_data_table = [nome_colunas]

    for row in dados:
        linha = []
        for column in nome_colunas:
            linha.append(row.get(column, 'Indisponível'))
        combined_data_table.append(linha)

    return combined_data_table

def salvando_dados(dados, path):
    try:
        with open(path, 'w') as file:
            writer = csv.writer(file)
            writer.writerows(dados)
        print("Dados salvos com sucesso")
    except:
        print("Dados não foram salvos")

# Paths usados
path_json_definido = "data_raw/dados_empresaA.json"
path_csv_definido = "data_raw/dados_empresaB.csv"
path_combined_data = "data_processed/combined_data.csv"


# Iniciando a leitura de dados
dados_json = leitura_dados(path=path_json_definido, tipo_arquivo='json')
nome_colunas_json = get_columns(dados_json)

dados_csv = leitura_dados(path=path_csv_definido, tipo_arquivo='csv')
nome_colunas_csv = get_columns(dados_csv)

print(dados_json[0])
print(nome_colunas_json[0])

print(dados_csv[0])
print(nome_colunas_csv[0])

# Transformação dos dados
key_mapping = {'Nome do Item': 'Nome do Produto',
                'Classificação do Produto': 'Categoria do Produto',
                'Valor em Reais (R$)': 'Preço do Produto (R$)',
                'Quantidade em Estoque': 'Quantidade em Estoque',
                'Nome da Loja': 'Filial',
                'Data da Venda': 'Data da Venda'}

novos_dados_csv = troca_keys(dados_antigos=dados_csv, key_mapping=key_mapping)

# Juntando os dados
dados_unidos = une_dado(dados1=dados_json, dados2=novos_dados_csv)

# Formatando os campos vazios
nome_colunas_unidas = get_columns(dados_unidos)

dados_prontos = formata_campos_invalidos(dados=dados_unidos, nome_colunas=nome_colunas_unidas)

salvando_dados(dados=dados_prontos, path=path_combined_data)

