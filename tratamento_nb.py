import pandas as pd

def get_nb(caminho_sav: str) -> pd.DataFrame:
    """
    Processa o DataFrame a partir do arquivo SPSS fornecido.
    
    Parâmetros:
    - caminho_sav (str): Caminho para o arquivo SPSS (ex: 'base_nb.sav')
    
    Retorno:
    - df (pd.DataFrame): DataFrame processado com colunas filtradas e renomeadas
    """
    # 1) Carregar o arquivo SPSS
    df = pd.read_spss(caminho_sav, convert_categoricals=False)
    
    # 2) Lista das colunas desejadas
    variables = ["record_id", "Hosp", "Codigo_Unico", "puerp_bl2_q15", "puerp_lu_1", "pront_bl17_207"]
    
    # 3) Filtrar as colunas pela lista em variables, confirmando sua existência no dataframe
    existing_columns = [col for col in variables if col in df.columns]
    
    # 4) Identificar colunas ausentes
    missing_columns = [col for col in variables if col not in df.columns]
    
    # 5) Exibir aviso de colunas ausentes (se houver)
    if missing_columns:
        print(f"As seguintes colunas estão ausentes no DataFrame: {missing_columns}")
    
    # 6) Selecionar apenas as colunas existentes
    df = df[existing_columns]
    
    # 7) Renomear as colunas para melhor compreensão e manipulação do DataFrame
    df.rename(columns={
        'Hosp': 'hosp',
        "Codigo_Unico": 'cod_unico',
        'puerp_bl2_q15': 'dt_nasc_mae',
        'puerp_lu_1': 'dt_nasc_bebe',
        'pront_bl17_207': 'peso_bebe'   
    }, inplace=True)
    
    # 8) Conferência das operações acima (exibindo o nome de todas as colunas)
    for col in df.columns:
        print(f"Coluna presente: {col}")
    
    return df

dfz = get_nb('base_nb.sav')