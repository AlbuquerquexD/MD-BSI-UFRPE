import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd

# Substitua pelo caminho do seu arquivo de credenciais
cred = credentials.Certificate("sua_key.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

# Carregar CSV
df = pd.read_csv("data/PMFS Amazônia Legal - pmfsAmazoniaLegal_LIMPA.csv", sep=",", encoding="utf-8")

# Agrupar por projeto
for nro_registro, grupo in df.groupby("NRO_REGISTRO"):
    data = grupo.iloc[0]

    documento = {
        "nro_registro": data["NRO_REGISTRO"],
        "nro_autorizacao": data["NRO_AUTORIZACAO"],
        "data_emissao": data["DATA_DE_EMISSAO"],
        "data_validade": data["DATA_DE_VALIDADE"],
        "municipio": data["MUNICIPIO"],
        "uf": data["UF"],
        "detentor": {
            "nome": data["NOME_DETENTOR"],
            "cpf_cnpj": data["CPF_CNPJ_DETENTOR"]
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

    # Salvar no Firestore
    db.collection("projetos").document(str(nro_registro)).set(documento)
