import pandas as pd

# Função para linkagem de bases
def linkar_bases(df1, df2, chave_left, chave_right):
    """
    Realiza linkagem entre dois DataFrames com base em chaves diferentes para cada DataFrame.

    Args:
    df1 (pd.DataFrame): Primeiro DataFrame.
    df2 (pd.DataFrame): Segundo DataFrame.
    chave_left (list): Lista de colunas de df1 usadas na chave de linkagem.
    chave_right (list): Lista de colunas de df2 usadas na chave de linkagem.

    Returns:
    tuple: DataFrames contendo o resultado da linkagem (matches, não linkados de df1 e não linkados de df2).
    """
    
    # Verificação se as listas de chaves têm o mesmo tamanho
    if len(chave_left) != len(chave_right):
        raise ValueError(
            f"As listas de chaves devem ter o mesmo tamanho. "
            f"chave_left tem {len(chave_left)} elementos, enquanto chave_right tem {len(chave_right)}."
        )

    # Merge à esquerda (left join)
    merged_left = pd.merge(
        df1,
        df2,
        left_on=chave_left,
        right_on=chave_right,
        how='left',
        suffixes=('_df1', '_df2'),
        indicator=True
    )

    # Merge à direita (right join)
    merged_right = pd.merge(
        df1,
        df2,
        left_on=chave_left,
        right_on=chave_right,
        how='right',
        suffixes=('_df1', '_df2'),
        indicator=True
    )

    # Separar matches (presentes em ambos os DataFrames)
    matches = merged_left[merged_left['_merge'] == 'both'].copy()
    
    # Separar não-linkados do df1 (presentes apenas no df1)
    non_matches_left = merged_left[merged_left['_merge'] == 'left_only'].copy()
    
    # Separar não-linkados do df2 (presentes apenas no df2)
    non_matches_right = merged_right[merged_right['_merge'] == 'right_only'].copy()

    # Remover a coluna '_merge' dos resultados
    df_linkado= matches.drop(columns=['_merge'])
    df1_nao_linkado = non_matches_left.drop(columns=['_merge'])
    df2_nao_linkado = non_matches_right.drop(columns=['_merge'])

    return df_linkado, df1_nao_linkado, df2_nao_linkado

