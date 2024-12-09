import pandas as pd
import numpy as np

def get_mmg(caminho_csv: str) -> pd.DataFrame:
    """
    Processa o DataFrame a partir do arquivo CSV fornecido.
    
    Parâmetros:
    - caminho_csv (str): Caminho para o arquivo CSV (ex: 'mmg_base.csv')
    
    Retorno:
    - df_final (pd.DataFrame): DataFrame processado conforme as regras de gemelaridade
    """
    # 1) Carregar o arquivo CSV
    df = pd.read_csv(caminho_csv)
    
    # 2) Renomear as variáveis para melhor compreensão e manipulação do dataframe
    df.rename(columns={
        'pront_hosp': 'hosp',
        'pront_record_id': 'cod_unico',
        'pront_dt_nasc': 'dt_nasc_mae',
        'pront_bl12_109': 'vaginal1',
        'pront_bl13_125': 'cesareo1',
        'pront_bl17_207': 'peso_bebe1',
        'pront_bl12_109g': 'vaginal2',
        'pront_bl13_125g': 'cesareo2', 
        'pront_bl17g_207g': 'peso_bebe2'
    }, inplace=True)
    
    # 3) Criar a coluna de data de nascimento do 1º bebê
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
    
    # 4) Criar a coluna de data de nascimento do 2º bebê
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
    
    # 5) Separar os dataframes conforme a gemelaridade 
    cols_to_keep = ['record_id', 'hosp', 'cod_unico', 'dt_nasc_mae']
    df_gemelar = df[df['dt_nasc_bebe2'].notnull() & (df['dt_nasc_bebe2'] != "")]
    df_nao_gemelar = df[~(df['dt_nasc_bebe2'].notnull() & (df['dt_nasc_bebe2'] != ""))]
    
    # 6) Criar DataFrame com as mulheres com gravidez gemelar (duplicando as linhas)
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
    
    # 7) Ordenar as duplicações para melhor verificação
    df_duplicadas = df_duplicadas.sort_values(by=['record_id', 'dt_nasc_bebe']).reset_index(drop=True)
    
    # 8) Criar DataFrame para as mulheres sem gravidez gemelar
    df_unica = df_nao_gemelar[cols_to_keep].assign(
        dt_nasc_bebe=df_nao_gemelar['dt_nasc_bebe1'],
        peso_bebe=df_nao_gemelar['peso_bebe1']
    )
    
    # 9) Concatenar os dois dataframes (mulheres únicas e duplicadas)
    df_final = pd.concat([df_unica, df_duplicadas], ignore_index=True)
    
    # 10) Ordenar o DataFrame final para garantir que a sequência de cada mulher fique correta
    df_final = df_final.sort_values(by=['record_id', 'dt_nasc_bebe']).reset_index(drop=True)
    
    return df_final