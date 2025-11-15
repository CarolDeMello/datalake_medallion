

BRONZE_DIR = "dados/bronze"
SILVER_DIR = "dados/silver"

## Main da silver
def process_silver(spark):
    try:
        cast_columns(spark)
        df_silver = filter_columns(spark)
        save_silver(df_silver)

    except Exception as e:
        print(f"Erro durante a execução. {e}")         


def cast_columns(spark) -> None:
    spark.read.csv(BRONZE_DIR, header=True, sep=";") \
        .createOrReplaceTempView("bronze_view")

    silver_sql = f"""
    CREATE OR REPLACE TEMPORARY VIEW silver_view AS
    SELECT 
        CAST(CD_OPERADORA AS STRING) AS CODIGO_OPERADORA,
        CAST(NM_MUNICIPIO AS STRING) AS NOME_MUNICIPIO,
        CAST(DE_FAIXA_ETARIA AS STRING) AS FAIXA_ETARIA,
        CAST(QT_BENEFICIARIO_ATIVO AS INT) AS BENEFICIARIOS_ATIVOS,
        CONCAT(REPEAT('X', 5), SUBSTR(CAST(CD_PLANO AS STRING), -4)) AS CODIGO_PLANO
    FROM
        bronze_view
    """

    spark.sql(silver_sql)
    spark.sql("SELECT * FROM silver_view").show(5, truncate=False)

def filter_columns(spark):
    COLUNAS = [
        "CODIGO_OPERADORA", "NOME_MUNICIPIO", "FAIXA_ETARIA", "CODIGO_PLANO", 
        "BENEFICIARIOS_ATIVOS"
    ]

    #Loop para adicionar todos os campos na condição de nulos e vazios
    condicao_limpeza_lista = []
    for coluna in COLUNAS:
        condicao = f"{coluna} IS NOT NULL AND {coluna} != ''"
        condicao_limpeza_lista.append(condicao)
    condicao_limpeza = " AND ".join(condicao_limpeza_lista)

    #No select faz tipagem dos dados e mascaramento do cd_plano no formato XXXXX1234
    silver_sql_filter = f"""
    SELECT *
    FROM
        silver_view
    WHERE
        BENEFICIARIOS_ATIVOS > 0
        AND {condicao_limpeza}
    """

    df_silver = spark.sql(silver_sql_filter)
    spark.sql("SELECT * FROM silver_view").show(5, truncate=False)
    return df_silver


def save_silver(df_silver) -> None:
    df_silver.write \
        .mode("overwrite") \
        .parquet(SILVER_DIR)

    print("Camada silver criada com sucesso!")