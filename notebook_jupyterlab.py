# Importa os tipos de dados e estruturas para definir o esquema do DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType 
from pyspark.sql import SparkSession

# Cria uma sessão Spark
# .master("local[*]") indica que vai usar todos os núcleos do computador local
# .appName('test') define o nome da aplicação Spark
spark = ( 
    SparkSession
    .builder
    .master("local[*]")
    .appName('test')
    .getOrCreate()
)

# Lista de dados que será usada para criar o DataFrame
data = [ 
    ("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1) 
]

# Define o esquema (colunas e tipos) do DataFrame
schema = (
    StructType([
        StructField("firstname", StringType(), True),  # Nome próprio
        StructField("middlename", StringType(), True), # Nome do meio
        StructField("lastname", StringType(), True),   # Sobrenome
        StructField("id", StringType(), True),         # ID (como string)
        StructField("gender", StringType(), True),     # Gênero
        StructField("salary", IntegerType(), True)     # Salário
    ])
)

# Cria o DataFrame usando os dados e o esquema definidos
df = spark.createDataFrame(data=data, schema=schema)

# Exibe o esquema do DataFrame (colunas e tipos)
# Ctrl+Enter no JupyterLab executa a célula e mostra o resultado abaixo
df.printSchema() 

# Mostra os dados do DataFrame de forma tabular
# truncate=False evita que valores longos sejam cortados
df.show(truncate=False)