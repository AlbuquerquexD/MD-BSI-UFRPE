import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
import numpy as np # Importamos a biblioteca NumPy

# --- 1. Inicialização ---
# Substitua pelo caminho do seu arquivo de credenciais
cred = credentials.Certificate("NoSQL/sua_key.json")

# Evita o erro de reinicialização se o script for rodado várias vezes
if not firebase_admin._apps:
    firebase_admin.initialize_app(cred)

db = firestore.client()

# --- 2. Carregamento dos Dados ---
try:
    df = pd.read_csv("data/PMFS Amazônia Legal - pmfsAmazoniaLegal_LIMPA.csv", sep=",", encoding="utf-8")
    df_cnpj = pd.read_csv("data/base_cnpj.csv", sep=",", encoding="utf-8")
except FileNotFoundError as e:
    print(f"Erro: Arquivo não encontrado. Verifique o caminho. Detalhes: {e}")
    exit()

# --- 3. Lógica para Preencher com CNPJs Cíclicos (Mock) ---

# Garante que a coluna de CNPJ não esteja vazia
if df_cnpj.empty or 'CNPJ' not in df_cnpj.columns:
    print("Erro: O arquivo base_cnpj.csv está vazio ou não contém a coluna 'CNPJ'.")
    exit()

# Extrai a lista de CNPJs para um objeto Series do Pandas
cnpjs_mock = df_cnpj["CNPJ"].astype(str)

# Obtém o número total de registros no DataFrame principal e o número de CNPJs disponíveis
num_registros_total = len(df)
num_cnpjs_disponiveis = len(cnpjs_mock)

# Cria uma sequência de índices que se repete. Ex: Se tiver 10 CNPJs, os índices serão 0,1,2..9,0,1,2..9,...
indices_repetidos = np.arange(num_registros_total) % num_cnpjs_disponiveis

# Usa os índices repetidos para selecionar os CNPJs da lista de mocks, criando uma nova série do tamanho exato do df
# O .reset_index(drop=True) garante que o índice da nova série seja compatível com o df principal
cnpjs_para_atribuir = cnpjs_mock.iloc[indices_repetidos].reset_index(drop=True)

# Atribui a nova série de CNPJs (que agora tem o tamanho correto e está ciclada) à coluna do DataFrame principal
df["CPF_CNPJ_DETENTOR"] = cnpjs_para_atribuir

print("CNPJs cíclicos atribuídos com sucesso.")
print("Exemplo do início da tabela:")
print(df[["NRO_REGISTRO", "CPF_CNPJ_DETENTOR"]].head())
print("\nExemplo do final da tabela (para mostrar a repetição):")
print(df[["NRO_REGISTRO", "CPF_CNPJ_DETENTOR"]].tail())


# --- 4. Envio para o Firestore ---
print("\nIniciando envio para o Firestore...")
# A lógica daqui para baixo permanece exatamente a mesma do seu código original
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

    db.collection("projetos").document(str(nro_registro)).set(documento)
print("\nProcesso finalizado.")