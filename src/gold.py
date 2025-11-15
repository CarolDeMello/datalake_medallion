SILVER_DIR = "dados/silver"
GOLD_DIR = "dados/gold"

## Main da gold
def process_gold(spark):
    try:
        df_gold = aggregate_beneficiarios(spark)
        save_gold(spark, df_gold)
        queries()

    except Exception as e:
        print(f"Erro durante a execução. {e}")

def aggregate_beneficiarios(spark):
    gold_sql = f"""
    SELECT
        CODIGO_OPERADORA,
        NOME_RAZAO_SOCIAL,
        NOME_MUNICIPIO,
        FAIXA_ETARIA,
        SUM(BENEFICIARIOS_ATIVOS) AS TotalBeneficiarios
    FROM
        parquet.`{SILVER_DIR}`
    GROUP BY CODIGO_OPERADORA, NOME_RAZAO_SOCIAL, NOME_MUNICIPIO, FAIXA_ETARIA
    """

    df_gold = spark.sql(gold_sql)
    return df_gold


def save_gold(spark, df_gold):
    #Gravacao com particionamento por operadora
    df_gold.write \
        .mode("overwrite") \
        .partitionBy("CODIGO_OPERADORA") \
        .parquet(GOLD_DIR)


def queries(spark):

    df_gold = spark.read.parquet(GOLD_DIR)
    df_gold.cache() 
    df_gold.createOrReplaceTempView("gold_view")
    
    consulta_a = """
    SELECT
        NOME_RAZAO_SOCIAL,
        SUM(TotalBeneficiarios) as BeneficiariosAtivos
    FROM
        gold_view
    GROUP BY CODIGO_OPERADORA, NOME_RAZAO_SOCIAL
    ORDER BY BeneficiariosAtivos desc
    LIMIT 5
    """
    spark.sql(consulta_a).show(truncate=False)

    consulta_b = """
    SELECT
        FAIXA_ETARIA,
        SUM(TotalBeneficiarios) as BeneficiariosAtivos
    FROM
        gold_view
    GROUP BY FAIXA_ETARIA
    ORDER BY BeneficiariosAtivos desc
    LIMIT 1
    """
    spark.sql(consulta_b).show(truncate=False)

    consulta_c = """
    SELECT
        NOME_MUNICIPIO,
        SUM(TotalBeneficiarios) AS BeneficiariosAtivos
    FROM
        gold_view
    GROUP BY NOME_MUNICIPIO
    ORDER BY BeneficiariosAtivos DESC
    """
    spark.sql(consulta_c).show(truncate=False)