# renovabr-desafio

# Análise de Dados Eleitorais - README

Este é um projeto de análise de dados eleitorais para processar e analisar informações sobre eleições municipais.

## Configuração
```bash
python3 -m venv venv
source venv/bin/activate
```

3. Instale as dependências do projeto:
```bash
pip install -r requirements.txt
```

4. Execute o código
```bash
spark-submit --master local main.py
```

## Sobre o Projeto

O processo inclui os seguintes passos:

1. Carregar os dados dos arquivos CSV.
2. Filtrar os dados de acordo com critérios específicos, excluindo valores nulos.
3. Realizar joins entre os DataFrames.
4. Calcular informações sobre candidatos mais votados.

## Arquivos

- `main.py`: Script principal que contém o código para análise de dados eleitorais.
- `requirements.txt`: Lista de dependências do projeto.
- `dados/candidatos.csv`: Arquivo contendo informações sobre candidatos.
- `dados/perfil_eleitorado.csv`: Arquivo contendo informações sobre eleitores.
