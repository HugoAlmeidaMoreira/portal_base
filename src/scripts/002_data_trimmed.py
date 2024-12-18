import pandas as pd
from tqdm import tqdm

# Load the combined DataFrame from the pickle file
pkl_input_path = "F:/portal_base/data/processed/combined_data.pkl"

combined_df = pd.read_pickle(pkl_input_path)

# Drop columns
combined_df = combined_df.drop(columns=['Observacoes', 'numAcordoQuadro', 'DescrAcordoQuadro', 'concorrentes', 'regime', 'fundamentacao', 'nAnuncio', 'TipoAnuncio', 'idINCM', 'ProcedimentoCentralizado', 'dataFechoContrato', 'linkPecasProc', 'CritMateriais', 'tipoFimContrato', 'justifNReducEscrContrato', 'dataPublicacao'])

# Separar a coluna adjudicante usando a primeira ocorrência de " - "
combined_df['nif_adjudicante'] = combined_df['adjudicante'].str.split(' - ', n=1, expand=True)[0]
combined_df['nome_adjudicante'] = combined_df['adjudicante'].str.split(' - ', n=1, expand=True)[1]

# Remover a coluna original
combined_df.drop(columns=['adjudicante'], inplace=True)

# Separar a coluna cpv em código e descrição
combined_df[['cpv_código', 'cpv_descricao']] = combined_df['cpv'].str.split(' - ', n=1, expand=True)

# Criar a coluna cpv_division a partir dos dois primeiros números do cpv_código
combined_df['cpv_division'] = combined_df['cpv_código'].str[:2]

# Remover a coluna original
combined_df.drop(columns=['cpv'], inplace=True)

# Função para processar a geografia
def process_geografia(entry):
    if not isinstance(entry, str):
        return 'Desconhecido', 'Desconhecido'
    # Separar múltiplos locais pelo separador "|"
    locais = entry.split(' | ')
    # Extrair apenas NUTS I e NUTS II
    nuts_ii = set()
    for local in locais:
        niveis = local.split(', ')
        if len(niveis) >= 2:
            nuts_ii.add(niveis[1])  # Pegamos no NUTS II
        else:
            nuts_ii.add('Portugal')  # Apenas Portugal (NUTS I)
    
    # Decidir o âmbito_geo
    if len(nuts_ii) == 1:
        if 'Portugal' in nuts_ii:
            return 'Portugal', 'Nacional'
        else:
            return list(nuts_ii)[0], list(nuts_ii)[0]
    else:
        return 'Múltiplos', 'Múltiplos'

# Adicionar barra de progresso com tqdm
tqdm.pandas(desc="Processando geografia")

# Aplicar a função à coluna geografia com tqdm
combined_df[['NUTS II/NUTS I', 'ambito_geo']] = combined_df['localExecucao'].progress_apply(process_geografia).apply(pd.Series)

# Resultado final
print(combined_df.head())