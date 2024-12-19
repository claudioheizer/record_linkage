import pandas as pd
import numpy as np

# Exemplo de arquivos CSV a serem lidos
arquivos = [
    '2021_mmg_obitos - linkados_unicos.csv',
    '2022_mmg_obitos - linkados_unicos.csv',
    '2023_mmg_obitos - linkados_unicos.csv'
]

# Ler todos os DataFrames a partir da lista de arquivos
# Ajuste o "sep" conforme necessário
dfs = [pd.read_csv(arq, sep=';') for arq in arquivos]

# Agora temos uma lista de DataFrames: dfs[0], dfs[1], dfs[2], ...

# Determinar o DataFrame de referência (por exemplo, o primeiro)
ref_index = 0
df_ref = dfs[ref_index]

# Obter o conjunto de colunas de cada DataFrame
list_of_columns = [set(df.columns) for df in dfs]

# Colunas que aparecem em todos os DataFrames (interseção)
common_columns = set.intersection(*list_of_columns)

# Colunas que aparecem em pelo menos um DataFrame (união)
all_columns = set.union(*list_of_columns)

# Agora, identificar colunas exclusivas de cada DataFrame
columns_unique_to_each = {}
for i, cols in enumerate(list_of_columns):
    unique_cols = cols - common_columns
    columns_unique_to_each[f"df{i}"] = unique_cols

# Exibir relatório
print("=== Relatório de Colunas ===")
print(f"Colunas em todos os DataFrames: {common_columns}")
for key, val in columns_unique_to_each.items():
    print(f"Colunas exclusivas em {key}: {val}")

# Objeto para descarte de colunas 
drop_columns_dict = {i: [] for i in range(len(arquivos))}

# Indicação das colunas a serem dropadas em cada dataframe
drop_columns_dict[0] = ["CONTADOR"]
drop_columns_dict[1] = ["CONTADOR"]
drop_columns_dict[2] = ["contador"]

ref_index = 0

