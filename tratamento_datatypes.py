import pandas as pd
import logging

# Configuração básica do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Função para tratar colunas de tipo de dado para inteiro
def converter_para_int(df, coluna):
    if coluna not in df.columns:
        logging.error(f"A coluna '{coluna}' não está presente no DataFrame.")
        return df
    
    df[coluna] = pd.to_numeric(df[coluna], errors='coerce').astype('Int64')
    
    # Log do resultado da conversão
    sucesso = df[coluna].notna().sum()
    falhas = df[coluna].isna().sum()
    logging.info(f"Conversão para inteiro concluída para coluna '{coluna}': {sucesso} sucessos, {falhas} falhas.")
    
    return df

# Função para tratar colunas de tipo de dado para string
def converter_para_str(df, coluna):
    if coluna not in df.columns:
        logging.error(f"A coluna '{coluna}' não está presente no DataFrame.")
        return df
    
    # Antes da conversão, registramos a quantidade de valores NaN
    df[coluna] = df[coluna].astype(str)
    
    # Log do resultado da conversão
    total = df.shape[0]
    logging.info(f"Conversão para string concluída para coluna '{coluna}': {total} valores convertidos.")
    
    return df

