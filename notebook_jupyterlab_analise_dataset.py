from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = ( 
 SparkSession
 .builder
    .master("local[*]")
 .appName('spark_dataframe_api')
 .getOrCreate()
)

df = (
    spark
    .read
    .option('delimiter', ';')
    .option('header', 'true')
    .option('inferSchema', 'true')
    .option('enconding', 'ISO-8859-1')
    .csv('./dados/precos-gasolina-etanol-10 (2).csv')
)

df.printSchema()
(
    df_precos
    .where(
        F.col('Valor de Compra').isNotNull()
    )
    .show()
)
df_precos = (
    df
    .select('Estado - Sigla', 'Produto', 'Valor de Venda', 'Unidade de Medida')
    .withColumn(
        "Valor de Venda",
        F.regexp_replace(F.col("Valor de Venda"), ",", ".")
        .cast("float")
    )
)
df_precos_analise = (
    df_precos
    .groupBy(
        F.col('Estado - Sigla'),
        F.col('Produto'),
        F.col('Unidade de Medida')
    )
    .agg(
        F.min(F.col("Valor de Venda")).alias('menor_valor'),
        F.max(F.col("Valor de Venda")).alias('maior_valor')
    )
    .withColumn(
        "diferenca",
        F.col('maior_valor') - F.col('menor_valor')
    )
    .orderBy('diferenca', ascending=False)
)
df_precos_analise.show(10)