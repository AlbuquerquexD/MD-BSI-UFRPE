import pandas as pd
from firebase_admin import firestore, credentials, initialize_app
cred = credentials.Certificate("key")
initialize_app(cred)
db = firestore.client()

df_cnpj = pd.read_csv("data//base_cnpj.csv")
df_pmfs = pd.read_csv("data\PMFS Amazônia Legal - pmfsAmazoniaLegal_LIMPA.csv", sep=",", encoding="utf-8")

print(f'Quantidade de registro da base de cnpjs: {df_cnpj.shape}')
print(f'Quantidade de registro da base da pmfs: {df_pmfs.shape}')

# Contar documentos na coleção "projetos"
projetos_docs = db.collection("projetos").stream()
projetos_count = sum(1 for _ in projetos_docs)

# Contar documentos na coleção "empresas"
empresas_docs = db.collection("empresas").stream()
empresas_count = sum(1 for _ in empresas_docs)

# Exibir resultados
print(f"Total de documentos na coleção 'projetos': {projetos_count}")
print(f"Total de documentos na coleção 'empresas': {empresas_count}")
