import pandas as pd
import logging

# Configuração básica do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Função para converter data de string
def converter_data_string(df, coluna_data, coluna_resultado):
    if coluna_data not in df.columns:
        logging.error(f"A coluna '{coluna_data}' não está presente no DataFrame.")
        return df
    
    df[coluna_resultado] = df[coluna_data].str.replace('/', '', regex=False)
    
    def processar_data(data_str):
        if pd.isnull(data_str):
            return pd.NA
        if len(data_str) == 6:
            ano_base = int(data_str[4:])
            if ano_base < 24:
                return data_str[:4] + '20' + data_str[4:]
            else: 
                return data_str[:4] + '19' + data_str[4:]
        if len(data_str) == 8:
            return data_str
        return pd.NA
    
    df[coluna_resultado] = df[coluna_resultado].apply(processar_data)
    df[coluna_resultado] = pd.to_numeric(df[coluna_resultado], errors='coerce').astype('Int64')

    # Log do resultado da conversão
    sucesso = df[coluna_resultado].notna().sum()
    falhas = df[coluna_resultado].isna().sum()
    logging.info(f"Conversão de data concluída para '{coluna_data}' em '{coluna_resultado}': {sucesso} sucessos, {falhas} falhas.")

    return df

# Função para converter data para formato DDMMAAAA
def converter_data_ddmmyyyy(df, coluna_data, coluna_resultado):
    if coluna_data not in df.columns:
        logging.error(f"A coluna '{coluna_data}' não está presente no DataFrame.")
        return df
    
    df[coluna_resultado] = pd.to_datetime(df[coluna_data], format='%Y-%m-%d', errors='coerce')
    df[coluna_resultado] = df[coluna_resultado].dt.strftime('%d%m%Y')
    df[coluna_resultado] = pd.to_numeric(df[coluna_resultado], errors='coerce').astype('Int64')

    # Log do resultado da conversão
    sucesso = df[coluna_resultado].notna().sum()
    falhas = df[coluna_resultado].isna().sum()
    logging.info(f"Conversão de data para formato DDMMAAAA concluída para '{coluna_data}' em '{coluna_resultado}': {sucesso} sucessos, {falhas} falhas.")

    return df

# Função para converter data para formato DDMMAAAA
def converter_data_ano(df, coluna_data, coluna_resultado):
    if coluna_data not in df.columns:
        logging.error(f"A coluna '{coluna_data}' não está presente no DataFrame.")
        return df
    
    df[coluna_resultado] = pd.to_datetime(df[coluna_data], format='%Y-%m-%d', errors='coerce')
    df[coluna_resultado] = df[coluna_resultado].dt.year
    df[coluna_resultado] = pd.to_numeric(df[coluna_resultado], errors='coerce').astype('Int64')

    # Log do resultado da conversão
    sucesso = df[coluna_resultado].notna().sum()
    falhas = df[coluna_resultado].isna().sum()
    logging.info(f"Conversão de data para ano (AAAA) concluída para '{coluna_data}' em '{coluna_resultado}': {sucesso} sucessos, {falhas} falhas.")

    return df