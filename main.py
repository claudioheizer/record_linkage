import pandas as pd
import sqlite3
import psutil
import openpyxl
from tratamento_nomes import tratar_campo_nome, criar_novonome_primeiro_ultimo, criar_novonome_primeiros_tres
from tratamento_datas import converter_data_string, converter_data_ddmmyyyy, converter_data_ano
from tratamento_cnes import tratar_cnes
from tratamento_datatypes import converter_para_int, converter_para_str
from linkagem import linkar_bases

# Ajusta o ano para as operações abaixo 
ano = 2024

# Carregando as bases
df1 = pd.read_spss('base_nb.sav', convert_categoricals=False)
df2 = pd.read_csv(f'SINASC_{ano}.csv', sep=';', low_memory=False)

for col in df1.columns:
    print(col)

df1 = df1.iloc[:, 0:76]

# Função para verificar a existência de coluna no DataFrame
def verificar_coluna(df, coluna):
    if coluna not in df.columns:
        print(f"A coluna '{coluna}' não está presente no DataFrame.")
        return False
    return True

# Tratamento de datas
if verificar_coluna(df1, 'puerp_lu_1'):
    df1 = converter_data_ddmmyyyy(df1, 'puerp_lu_1', 'DT_NASC_NB')

if verificar_coluna(df1, 'puerp_bl2_q15'):
    df1 = converter_data_ddmmyyyy(df1, 'puerp_bl2_q15', 'DT_NASCMAE_NB')

if verificar_coluna(df1, 'puerp_lu_1'):
    df1 = converter_data_ano(df1, 'puerp_lu_1', 'ANO')

df2['DTNASC'] = pd.to_numeric(df2['DTNASC'], errors='coerce').astype('Int64')
df2['DTNASCMAE'] = pd.to_numeric(df2['DTNASCMAE'], errors='coerce').astype('Int64')

# Tratamento de cnes
if verificar_coluna(df1, 'Hosp'):
    df1 = tratar_cnes(df1, 'Hosp', 'CNES')

df2['CODESTAB'] = pd.to_numeric(df2['CODESTAB'], errors='coerce').astype('Int64')

# Filtragem de dados
df1_filtrado = df1[df1['ANO'] == ano]
df2_filtrado = df2[df2['CODESTAB'].isin(df1['CNES'])]

# Cria e conecta ao banco SQLite local
conn = sqlite3.connect('linkagem_data.db')

# Exporta os DataFrames para o banco de dados
df1_filtrado.to_sql('df1_filtrado', conn, index=False, if_exists='replace')
df2_filtrado.to_sql('df2_filtrado', conn, index=False, if_exists='replace')

# Realiza a linkagem no SQLite
query = """
    SELECT 
        df1_filtrado.*,
        df2_filtrado.*
    FROM 
        df1_filtrado
    INNER JOIN 
        df2_filtrado
    ON 
        df1_filtrado.DT_NASC_NB = df2_filtrado.DTNASC AND
        df1_filtrado.DT_NASCMAE_NB = df2_filtrado.DTNASCMAE AND
        df1_filtrado.CNES = df2_filtrado.CODESTAB
"""

df_linkado = pd.read_sql_query(query, conn)

# Cria um conjunto de IDs presentes em df_linkado
ids_linkados = set(df_linkado['puerp_record_id_2'].dropna())

# Identifica registros em df1_filtrado que não foram linkados
df_nao_linkado = df1_filtrado[~df1_filtrado['puerp_record_id_2'].isin(ids_linkados)]

# Copia e adiciona colunas com sufixo _v a fim para verificação
df_linkado['DT_NASC_NB_v'] = df_linkado['DT_NASC_NB']
df_linkado['DTNASC_v'] = df_linkado['DTNASC']
df_linkado['DT_NASCMAE_NB_v'] = df_linkado['DT_NASCMAE_NB']
df_linkado['DTNASCMAE_v'] = df_linkado['DTNASCMAE']
df_linkado['CNES_v'] = df_linkado['CNES']
df_linkado['CODESTAB_v'] = df_linkado['CODESTAB']

# Separa as duplicidades em df_linkado
duplicados = df_linkado.duplicated(subset=['puerp_record_id_2'], keep=False)
df_linkado_unicos = df_linkado[~duplicados]  # Registros sem duplicatas
df_linkado_duplicidades = df_linkado[duplicados]  # Registros com duplicatas

# Contar o número de elementos em cada planilha
num_filtrados = df1_filtrado.shape[0]
num_unicos = df_linkado_unicos.shape[0]
num_duplicidades = df_linkado_duplicidades.shape[0]
num_nao_linkados = df_nao_linkado.shape[0]

# Exibir os resultados
print(f"Número de mulheres na base NB no ano de {ano}: {num_filtrados}")
print(f"Número de registros com uma única correspondência: {num_unicos}")
print(f"Número de registros com duplicidades de correspondência: {num_duplicidades}")
print(f"Número de registros não linkados: {num_nao_linkados}")

# Salva resumo 
resumo = pd.DataFrame({
    'Categoria': [
        f'Mulheres no NB no de {ano}',
        'Registros com única correspondência', 
        'Registros com duplicidade de correspondência', 
        'Registros não linkados',     
    ],
    'Quantidade': [
        num_filtrados,
        num_unicos, 
        num_duplicidades, 
        num_nao_linkados
    ]
})

resumo.to_excel(f'{ano} - resumo.xlsx', index=False)

# Salva os resultados
df_linkado_unicos.to_csv(f'{ano} - linkados_unicos.csv', index=False, sep=';', encoding='utf-8')
df_linkado_duplicidades.to_csv(f'{ano} - linkados_duplicidades.csv', index=False, sep=';', encoding='utf-8')
df_nao_linkado.to_csv(f'{ano} - nao_linkados.csv', index=False, sep=';', encoding='utf-8')

# Exibe o uso de memória
memory = virtual_memory()
print(f"Total Memory: {memory.total / (1024**3):.2f} GB")
print(f"Available Memory: {memory.available / (1024**3):.2f} GB")

# Fechar conexão com o banco de dados
conn.close()

