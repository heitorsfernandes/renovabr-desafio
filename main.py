from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, expr, split

spark = SparkSession.builder.appName("Dados Renovabr").getOrCreate()

df1 = spark.read.csv("./src/perfil_eleitorado_2020.csv", header=True, sep=";")
df2 = spark.read.csv("./src/SP_turno_1.csv", header=True, sep=";")

colunas_filtrar_1 = df1.columns
colunas_filtrar_2 = df2.columns


