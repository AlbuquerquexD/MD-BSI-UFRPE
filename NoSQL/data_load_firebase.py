import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
import re

# --- 1. Inicialização ---
cred = credentials.Certificate("NoSQL/sua_key.json")

if not firebase_admin._apps:
    firebase_admin.initialize_app(cred)

db = firestore.client()

# --- 2. Carregamento dos Dados ---
try:
    df = pd.read_csv("data/PMFS Amazônia Legal - pmfsAmazoniaLegal_LIMPA.csv", sep=",", encoding="utf-8")
except FileNotFoundError as e:
    print(f"Erro: Arquivo não encontrado. Verifique o caminho. Detalhes: {e}")
    exit()

# --- 3. Envio para o Firestore ---
print("\nIniciando envio para o Firestore...")

for nro_registro, grupo in df.groupby("NRO_REGISTRO"):
    data = grupo.iloc[0]

    # --- Validação de CNPJ ---
    cpf_cnpj = str(data["CPF_CNPJ_DETENTOR"])
    cpf_cnpj_limpo = re.sub(r'\D', '', cpf_cnpj)  # remove pontos, traços, barras etc.

    if len(cpf_cnpj_limpo) != 14:
        print(f"⚠️ Pulando registro {nro_registro} (não é CNPJ válido: {cpf_cnpj})")
        continue

    documento = {
        "nro_registro": data["NRO_REGISTRO"],
        "nro_autorizacao": data["NRO_AUTORIZACAO"],
        "data_emissao": data["DATA_DE_EMISSAO"],
        "data_validade": data["DATA_DE_VALIDADE"],
        "municipio": data["MUNICIPIO"],
        "uf": data["UF"],
        "detentor": {
            "nome": data["NOME_DETENTOR"],
            "cpf_cnpj": cpf_cnpj_limpo
        },
        "imovel": {
            "nome": data["IMOVEL_RURAL_VINCULADO"],
            "car": data["NRO_CAR_IMOVEL_RURAL"],
            "nome_empreendimento": data["NOME_EMPREENDIMENTO_VINC"],
            "latitude": data["LATITUDE_EMPREENDIMENTO"],
            "longitude": data["LONGITUDE_EMPREENDIMENTO"]
        },
        "responsavel_tecnico": {
            "nome": data["NOME_DO_RT"],
            "nro_art": data["NRO_ART"],
            "atividade_rt": data["ATIVIDADE_RT"],
            "atividade": data["ATIVIDADE"]
        },
        "empreendimento_tipo": {
            "tipo": data["TIPO_DE_EMPREENDIMENTO"],
            "natureza_juridica": data["NATUREZA_JURIDICA"],
            "competencia_avaliacao": data["COMPETENCIA_AVALIACAO"],
            "orgao_ambiental": data["ORGAO_AMBIENTAL_RESP_ANALISE"]
        },
        "caracteristicas_ambientais": {
            "clima": data["CLIMA"],
            "solo": data["SOLO"],
            "bioma": data["BIOMA"],
            "fitofisionomia": data["FITOFISIONOMIA"]
        },
        "manejo": {
            "metodo_extracao": data["METODO_EXTRACAO"],
            "sistema_silvicultural": data["SISTEMA_SILVICULTURAL"],
            "ciclo_corte": data["CICLO_CORTE"],
            "area_total_propriedade": data["AREA_TOTAL_PROPRIEDADE"],
            "area_manejo_florestal": data["AREA_MANEJO_FLORESTAL"],
            "area_efetivo_manejo": data["AREA_EFETIVO_MANEJO"],
            "capacidade_produtiva": data["CAPACIDADE_PRODUTIVA"],
            "estimativa_produtiva_anual": data["ESTIMATIVA_PRODUTIVA_ANUAL"],
            "intensidade_corte": data["INTENSIDADE_CORTE"],
            "equacao_volume": data["EQUACAO_VOLUME"],
            "area_autorizada": data["AREA_AUTORIZADA"]
        },
        "situacao": {
            "status": data["SITUACAO"],
            "data_situacao": data["DATA_DA_SITUACAO"],
            "ultimo_tramite": data["ULTIMO_TRAMITE"],
            "data_tramite": data["DATA_DO_TRAMITE"],
            "ultima_atualizacao_relatorio": data["ULTIMA_ATUALIZACAO_RELATORIO"]
        },
        "modalidades_pmfs": grupo["MODALIDADE_PMFS"].unique().tolist()
    }

    db.collection("projetos_cnpj").document(str(nro_registro)).set(documento)
    print(f"✅ Registro {nro_registro} inserido com sucesso.")

print("\nProcesso finalizado.")
