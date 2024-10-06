import pandas as pd
import json
import xml.etree.ElementTree as ET
import pyarrow.parquet as pq
import os

# Função para simular a conexão com a "nuvem"
def simular_conexao_armazenamento():
    storage_connection_string = "DefaultEndpointsProtocol=https;AccountName=fakeaccount;AccountKey=fakekey;EndpointSuffix=core.windows.net"
    print(f"Conectado à conta de armazenamento: {storage_connection_string}")

# Função para leitura de CSV
def ler_csv(filepath):
    try:
        df = pd.read_csv(filepath)
        print(f"\nAmostra de {os.path.basename(filepath)}:")
        print(df.head())  # Mostra uma amostra dos dados
        print(f"\nSchema de {os.path.basename(filepath)}:")
        print(df.dtypes)  # Exibe o schema (tipos de dados)
    except Exception as e:
        print(f"Erro ao ler o arquivo CSV {filepath}: {str(e)}")

# Função para leitura de JSON
def ler_json(filepath):
    try:
        with open(filepath, 'r') as file:
            data = json.load(file)
        df = pd.json_normalize(data)
        print(f"\nAmostra de {os.path.basename(filepath)}:")
        print(df.head())  # Mostra uma amostra dos dados
        print(f"\nSchema de {os.path.basename(filepath)}:")
        print(df.dtypes)  # Exibe o schema (tipos de dados)
    except Exception as e:
        print(f"Erro ao ler o arquivo JSON {filepath}: {str(e)}")

# Função para leitura de XML
def ler_xml(filepath):
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
        data = []
        for child in root:
            data.append(child.attrib)
        df = pd.DataFrame(data)
        print(f"\nAmostra de {os.path.basename(filepath)}:")
        print(df.head())  # Mostra uma amostra dos dados
        print(f"\nSchema de {os.path.basename(filepath)}:")
        print(df.dtypes)  # Exibe o schema (tipos de dados)
    except Exception as e:
        print(f"Erro ao ler o arquivo XML {filepath}: {str(e)}")

# Função para leitura de Parquet
def ler_parquet(filepath):
    try:
        table = pq.read_table(filepath)
        df = table.to_pandas()
        print(f"\nAmostra de {os.path.basename(filepath)}:")
        print(df.head())  # Mostra uma amostra dos dados
        print(f"\nSchema de {os.path.basename(filepath)}:")
        print(df.dtypes)  # Exibe o schema (tipos de dados)
    except Exception as e:
        print(f"Erro ao ler o arquivo Parquet {filepath}: {str(e)}")

# Função principal para ler os arquivos e simular o processo
def main():
    simular_conexao_armazenamento()

    # Leitura dos arquivos Olist
    print("\nLeitura dos arquivos Olist:")
    ler_csv('archive/olist_orders_dataset.csv')
    ler_csv('archive/olist_order_reviews_dataset.csv')
    ler_csv('archive/olist_customers_dataset.csv')

    # Leitura dos arquivos adicionais na pasta correta
    print("\nLeitura dos arquivos adicionais:")
    ler_xml('datalake/bronze/DADOS_ALUNOS.xml')
    ler_json('datalake/bronze/DADOS_ESTUDANTES/DADOS_ESTUDANTES.json')
    ler_csv('datalake/bronze/DADOS_EXAMES/DADOS_EXAMES.csv')
    ler_parquet('datalake/bronze/DADOS_VOOS/DADOS_VOOS.parquet')

# Executa o script
if __name__ == "__main__":
    main()
