import pandas as pd

sioe_df = pd.read_excel('/workspaces/portal_base/data/raw/ExportResultadosPesquisa20241219T113545438Z.xlsx')

# Drop columns: 'Contacto 4 -  Tipo', 'Contacto 4 -  Contacto' and 68 other columns
sioe_df = sioe_df.drop(columns=['Morada 1 -  Latitude', ' NISS',' Código de Certidão Permanente de Registo','Morada 1 -  Longitude', 'Morada 1 -  Altitude', 'Contacto 4 -  Tipo', 'Contacto 4 -  Contacto', 'Contacto 4 -  Utilização', 'Contacto 4 -  Principal', 'Contacto 5 -  Tipo', 'Contacto 5 -  Contacto', 'Contacto 5 -  Utilização', 'Contacto 5 -  Principal', 'Contacto 6 -  Tipo', 'Contacto 6 -  Contacto', 'Contacto 6 -  Utilização', 'Contacto 6 -  Principal', 'Contacto 7 -  Tipo', 'Contacto 7 -  Contacto', 'Contacto 7 -  Utilização', 'Contacto 7 -  Principal', 'Contacto - Link 2 -  URL', 'Contacto - Link 2 -  Descrição', 'Contacto - Link 2 -  Classificação', 'Contacto - Link 3 -  URL', 'Contacto - Link 3 -  Descrição', 'Contacto - Link 3 -  Classificação', 'Contacto - Link 4 -  URL', 'Contacto - Link 4 -  Descrição', 'Contacto - Link 4 -  Classificação', 'Contacto - Link 5 -  URL', 'Contacto - Link 5 -  Descrição', 'Contacto - Link 5 -  Classificação', 'Contacto - Link 6 -  URL', 'Contacto - Link 6 -  Descrição', 'Contacto - Link 6 -  Classificação', 'Contacto - Link 7 -  URL', 'Contacto - Link 7 -  Descrição', 'Contacto - Link 7 -  Classificação', 'Contacto - Link 8 -  URL', 'Contacto - Link 8 -  Descrição', 'Contacto - Link 8 -  Classificação', 'Contacto - Link 9 -  URL', 'Contacto - Link 9 -  Descrição', 'Contacto - Link 9 -  Classificação', 'Contacto - Link 10 -  URL', 'Contacto - Link 10 -  Descrição', 'Contacto - Link 10 -  Classificação', 'Contacto - Link 11 -  URL', 'Contacto - Link 11 -  Descrição', 'Contacto - Link 11 -  Classificação', 'Contacto - Link 12 -  URL', 'Contacto - Link 12 -  Descrição', 'Contacto - Link 12 -  Classificação', 'Contacto - Link 13 -  URL', 'Contacto - Link 13 -  Descrição', 'Contacto - Link 13 -  Classificação', 'Contacto - Link 14 -  URL', 'Contacto - Link 14 -  Descrição', 'Contacto - Link 14 -  Classificação', 'Contacto - Link 15 -  URL', 'Contacto - Link 15 -  Descrição', 'Contacto - Link 15 -  Classificação', 'Contacto - Link 16 -  URL', 'Contacto - Link 16 -  Descrição', 'Contacto - Link 16 -  Classificação', 'Contacto - Link 17 -  URL', 'Contacto - Link 17 -  Descrição', 'Contacto - Link 17 -  Classificação', 'Contacto - Link 18 -  URL', 'Contacto - Link 18 -  Descrição', 'Contacto - Link 18 -  Classificação','Contacto - Link 19 -  URL', 'Contacto - Link 19 -  Descrição', 'Contacto - Link 19 -  Classificação'])


# Separar a coluna adjudicante usando a primeira ocorrência de " - "
sioe_df['cae_codigo'] = sioe_df[' CAE'].str.split(' - ', n=1, expand=True)[0]
sioe_df['cae_atividade'] = sioe_df[' CAE'].str.split(' - ', n=1, expand=True)[1]
sioe_df.drop(columns=[' CAE'], inplace=True)

# Remover o sufixo '.0'
sioe_df[' NIPC'] = sioe_df[' NIPC'].astype(str)
sioe_df[' NIPC'] = sioe_df[' NIPC'].str.replace('.0', '', regex=False)

#salvar como pickle 
sioe_df.to_pickle("/workspaces/portal_base/data/processed/sioe_base.pkl")