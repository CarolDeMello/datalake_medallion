import requests
import zipfile
import os

URL = "https://dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios-024/202508/pda-024-icb-TO-2025_08.zip"
EXTRACT_DIR = "dados/landing_zone/"
FILE = "dados/landing_zone/pda-024-icb-TO-2025_08.zip"
BRONZE_DIR = "dados/bronze"

## Main da bronze
def process_bronze(spark):
    try:
        get_zip()
        path = extract_zip()
        if path:
            save_bronze(spark, path)

    except Exception as e:
        print(f"Erro durante a execução. {e}")        


def get_zip() -> None:
    # Cria a pasta onde será extraído
    os.makedirs(EXTRACT_DIR, exist_ok=True)

    try:
        #Faz um get na URL de origem com stream ativado (para caso o arquivo ser grande)
        response = requests.get(URL, stream=True)
        with open(FILE, 'wb') as file:
            file.write(response.content)
        print("Fonte de dados obtida com sucesso")

    except Exception as e:
        print(f"Erro na obtencao dos dados. {e}")
        exit()

def extract_zip() -> str:
    try:
        #Se der sucesso, descompacta o zip para pegar o csv
        with zipfile.ZipFile(FILE, 'r') as zip_ref:
            zip_ref.extract("pda-024-icb-TO-2025_08.csv", EXTRACT_DIR)    
        CSV_PATH = os.path.join(EXTRACT_DIR, "pda-024-icb-TO-2025_08.csv")
        print("Arquivo CSV extraído com sucesso")

        return CSV_PATH

    except Exception as e:
        print(f"Erro na obtencao dos dados. {e}")
        exit()

def save_bronze(spark, path) -> None:
    df_bronze = spark.read.format("csv") \
        .option("header", "true") \
        .option("sep", ";") \
        .load(path)

    # Salva a tabela Bronze como Parquet
    df_bronze.write \
        .mode("overwrite") \
        .parquet("dados/bronze")

    print("Camada bronze criada com sucesso!")