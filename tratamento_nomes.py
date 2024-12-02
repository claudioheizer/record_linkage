import pandas as pd
import re
import logging

# Configuração básica do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Função para limpar nomes
def tratar_campo_nome(df, coluna_origem, coluna_destino):
    if coluna_origem not in df.columns:
        logging.error(f"A coluna '{coluna_origem}' não está presente no DataFrame.")
        return df
    
    df[coluna_destino] = df[coluna_origem]
    
    def processar_entrada(valor):
        if pd.isnull(valor):
            return pd.NA
        valor = valor.strip()  # Remove espaços nas extremidades
        
        if valor.isdigit():
            return pd.NA
        else:
            valor_sem_numeros = re.sub(r'^\d+', '', valor).strip()
            if valor_sem_numeros == '':
                return pd.NA
            return valor_sem_numeros
    
    df[coluna_destino] = df[coluna_destino].apply(processar_entrada)
    
    # Log do resultado da limpeza
    sucesso = df[coluna_destino].notna().sum()
    falhas = df[coluna_destino].isna().sum()
    logging.info(f"Tratamento de nomes concluído para coluna '{coluna_origem}' em '{coluna_destino}': {sucesso} sucessos, {falhas} falhas.")

    return df

# Função para criar novo nome com primeiro e último
def criar_novonome_primeiro_ultimo(df, coluna_nomes, coluna_resultado):
    if coluna_nomes not in df.columns:
        logging.error(f"A coluna '{coluna_nomes}' não está presente no DataFrame.")
        return df

    def processar_nome(nome_str):
        if pd.isnull(nome_str) or nome_str.strip() == '':
            return pd.NA
        termos = nome_str.strip().split()
        if len(termos) == 1:
            return termos[0]
        return f"{termos[0]} {termos[-1]}"
    
    df[coluna_resultado] = df[coluna_nomes].apply(processar_nome)
    
    # Log do resultado da criação do novo nome
    sucesso = df[coluna_resultado].notna().sum()
    falhas = df[coluna_resultado].isna().sum()
    logging.info(f"Criação de nome com primeiro e último termo concluída para coluna '{coluna_nomes}' em '{coluna_resultado}': {sucesso} sucessos, {falhas} falhas.")
    
    return df

# Função para criar novo nome com os primeiros até três nomes
def criar_novonome_primeiros_tres(df, coluna_nomes, coluna_resultado):
    if coluna_nomes not in df.columns:
        logging.error(f"A coluna '{coluna_nomes}' não está presente no DataFrame.")
        return df

    def processar_nome(nome_str):
        if pd.isnull(nome_str) or nome_str.strip() == '':
            return pd.NA
        termos = nome_str.strip().split()
        return ' '.join(termos[:3])
    
    df[coluna_resultado] = df[coluna_nomes].apply(processar_nome)
    
    # Log do resultado da criação de nome curto
    sucesso = df[coluna_resultado].notna().sum()
    falhas = df[coluna_resultado].isna().sum()
    logging.info(f"Criação de nome curto (primeiros três termos) concluída para coluna '{coluna_nomes}' em '{coluna_resultado}': {sucesso} sucessos, {falhas} falhas.")
    
    return df
