from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1 Tratando os dados

spark = SparkSession.builder.appName("Dados Renovabr").getOrCreate()
df_perfil = spark.read.csv(
    "./src/perfil_eleitorado_2020.csv", header=True, sep=";")
df_sp = spark.read.csv("./src/SP_turno_1.csv", header=True, sep=";")

colunas_filtrar_perfil = df_perfil.columns
colunas_filtrar_sp = df_sp.columns

# Para excluir as linhas que contém os valores indesejados
condicoes = [
    ~(col(coluna).isin("#NULO", "#NE", -1, -3))
    for coluna in colunas_filtrar_perfil
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

# 2- Tabelas para cada base e filtradas
# df_perfil_filtrado.show()
# df_sp_filtrado.show()

# 3- Join das tabelas
# Encontra os valores iguais nas duas listas; converte resultado para lista.
valores_iguais = set(colunas_filtrar_perfil) & set(colunas_filtrar_sp)
valores_iguais_lista = list(valores_iguais)

# Realiza o join a partir dos valores iguais
df_joined = df_sp_filtrado.join(df_perfil_filtrado,
                                on=valores_iguais_lista,
                                how="inner")

# 3.1 - Encontra maior votação para Prefeito e Vereador
df_prefeito = df_joined.filter(col("DS_CARGO_PERGUNTA") == "Prefeito")
df_vereador = df_joined.filter(col("DS_CARGO_PERGUNTA") == "Vereador")

df_prefeito_votos = df_prefeito.groupBy(
    "NM_VOTAVEL").agg(sum("QT_VOTOS").alias("TOTAL_VOTOS"))

prefeito_max_votos = df_prefeito_votos.select(
    "NM_VOTAVEL", "TOTAL_VOTOS").orderBy(col("TOTAL_VOTOS").desc()).first()

df_vereador_votos = df_vereador.groupBy(
    "NM_VOTAVEL").agg(sum("QT_VOTOS").alias("TOTAL_VOTOS"))
vereador_max_votos = df_vereador_votos.select(
    "NM_VOTAVEL", "TOTAL_VOTOS").orderBy(col("TOTAL_VOTOS").desc()).first()

# Resultados
print("Candidato a Prefeito com mais votos:",
      prefeito_max_votos["NM_VOTAVEL"],
      "Total de votos:", prefeito_max_votos["TOTAL_VOTOS"])

print("Candidato a Vereador com mais votos:",
      vereador_max_votos["NM_VOTAVEL"],
      "Total de votos:", vereador_max_votos["TOTAL_VOTOS"])
