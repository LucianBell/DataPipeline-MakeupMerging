"""
Função do script: deixar claro para os novos devs como é feito o pipeline de dados
"""

import json
import csv
from processamento_dados import Dados

# Paths usados
path_json_definido = "data_raw/dados_empresaA.json"
path_csv_definido = "data_raw/dados_empresaB.csv"
path_combined_data = "data_processed/combined_data.csv"

# Variaveis agora com lógica de negócio e não de scrip
dados_empresaA = Dados(path=path_json_definido, tipo_dados='json')
dados_empresaB = Dados(path=path_csv_definido, tipo_dados='csv')

# Extract
print(dados_empresaA.get_columns())
print(dados_empresaB.get_columns())

# Transform
            #  De -----------> Para
key_mapping = {'Nome do Item': 'Nome do Produto',
                'Classificação do Produto': 'Categoria do Produto',
                'Valor em Reais (R$)': 'Preço do Produto (R$)',
                'Quantidade em Estoque': 'Quantidade em Estoque',
                'Nome da Loja': 'Filial',
                'Data da Venda': 'Data da Venda'}
dados_empresaB.rename_columns(key_mapping=key_mapping)
print(f"Novas colunas: {dados_empresaB.get_columns()}")

dados_fusao = Dados.join(dados1=dados_empresaA, dados2=dados_empresaB)
print(f"Nome colunas fusão: {dados_fusao.get_columns}")

# Load
dados_fusao.salvando_dados(path=path_combined_data)
print(path_combined_data)