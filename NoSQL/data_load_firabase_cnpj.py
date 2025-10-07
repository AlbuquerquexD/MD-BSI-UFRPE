import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore

# Inicializar Firebase
cred = credentials.Certificate("key")  # Caminho do JSON da conta de serviço
firebase_admin.initialize_app(cred)
db = firestore.client()

# 2. Ler CSV
df = pd.read_csv("data/base_cnpj.csv", sep=",", encoding="utf-8")

# Intera sobre as linhas do df
n = 0
for _, row in df.iterrows():
    documento = {
        "nome_fantasia": row["NOME_FANTASIA"],
        "situacao_cadastral": row["SITUACAO_CADASTRAL"],
        "data_inicio_atividade": row["DATA_INICIO_ATIVIDADE"],
        "cnae_fiscal_principal": row["CNAE_FISCAL_PRINCIPAL"],
        "cnpj": str(row["CNPJ"])
    }

    n+=1
    print(n)
    #Salvar na coleção "empresas"
    db.collection("empresas").document(documento["cnpj"]).set(documento)