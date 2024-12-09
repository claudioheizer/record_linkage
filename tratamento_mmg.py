import pandas as pd
import numpy as np

df = pd.read_csv('mmg_base.csv')

# 1) Renomea as variáveis para melhor compreensão e manipulação do dataframe
df.rename(columns={
    'pront_bl12_109': 'vaginal1',
    'pront_bl13_125': 'cesareo1',
    'pront_bl17_207': 'peso_bebe1',
    'pront_bl12_109g': 'vaginal2',
    'pront_bl13_125g': 'cesareo2', 
    'pront_bl17g_207g': 'peso_bebe2'
}, inplace=True)

# 2) Cria a coluna de data de nascimento do 1º bebê
df['dt_nasc_bebe1'] = df[['vaginal1', 'cesareo1']].apply(
    lambda x: (
        x['vaginal1'] 
        if pd.notnull(x['vaginal1']) 
        else (
            x['cesareo1'] 
            if pd.notnull(x['cesareo1']) 
            else np.nan
        )
    ), 
    axis=1
)

# 3) Cria a coluna de data de nascimento do 2º bebê
df['dt_nasc_bebe2'] = df[['vaginal2', 'cesareo2']].apply(
    lambda x: (
        x['vaginal2'] 
        if pd.notnull(x['vaginal2']) 
        else (
            x['cesareo2'] 
            if pd.notnull(x['cesareo2']) 
            else np.nan
        )
    ), 
    axis=1
)

# Separa os dataframes conforme a gemelaridade 
cols_to_keep = ['record_id', 'pront_hosp', 'pront_record_id', 'pront_dt_nasc']
df_gemelar = df[df['dt_nasc_bebe2'].notnull() & (df['dt_nasc_bebe2'] != "")]
df_nao_gemelar = df[~(df['dt_nasc_bebe2'].notnull() & (df['dt_nasc_bebe2'] != ""))]

# Cria DataFrame com as mulheres com gravidez gemelar (duplicando as linhas)
df_duplicadas = pd.concat([
    # Primeira linha (para o primeiro bebê)
    df_gemelar[cols_to_keep].assign(
        dt_nasc_bebe=df_gemelar['dt_nasc_bebe1'],
        peso_bebe=df_gemelar['peso_bebe1']
    ),
    # Segunda linha (para o segundo bebê)
    df_gemelar[cols_to_keep].assign(
        dt_nasc_bebe=df_gemelar['dt_nasc_bebe2'],
        peso_bebe=df_gemelar['peso_bebe2']
    )
], ignore_index=True)

# Ordena as duplicações para melhor verificação
df_duplicadas = df_duplicadas.sort_values(by=['record_id', 'dt_nasc_bebe']).reset_index(drop=True)

# Cria DataFrame para as mulheres sem gravidez gemelar
df_unica = df_nao_gemelar[cols_to_keep].assign(
    dt_nasc_bebe=df_nao_gemelar['dt_nasc_bebe1'],
    peso_bebe=df_nao_gemelar['peso_bebe1']
)

# Concatena os dois dataframes (Avaliar se é uma medida recomendável)
df_final = pd.concat([df_unica, df_duplicadas], ignore_index=True)

# Ordena o DataFrame final para garantir que a sequência de cada mulher fique correta
df_final = df_final.sort_values(by=['record_id', 'dt_nasc_bebe']).reset_index(drop=True)

