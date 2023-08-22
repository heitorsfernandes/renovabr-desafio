from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, expr, split

spark = SparkSession.builder.appName("Dados Renovabr").getOrCreate()

df_perfil = spark.read.csv("./src/perfil_eleitorado_2020.csv", header=True, sep=";")
df_sp = spark.read.csv("./src/SP_turno_1.csv", header=True, sep=";")

colunas_filtrar_perfil = df_perfil.columns
colunas_filtrar_sp = df_sp.columns

condicoes = [
    ~(col(coluna).isin("#NULO", "#NE", -1, -3)) for coluna in colunas_filtrar_perfil
]

condicao_final = condicoes[0]
for condicao in condicoes[1:]:
    condicao_final &= condicao

df_perfil_filtrado = df_perfil.filter(condicao_final)

condicoes2 = [
    ~(col(coluna).isin("#NULO", "#NE", -1, -3))
    for coluna in colunas_filtrar_sp
]

condicao_final_2 = condicoes2[0]
for condicao in condicoes2[1:]:
    condicao_final_2 &= condicao

df_sp_filtrado = df_sp.filter(condicao_final_2)

df_perfil_filtrado.show()
df_sp_filtrado.show()

