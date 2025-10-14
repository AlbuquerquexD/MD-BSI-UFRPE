# populacao_tabelas/orgao_resp_insert.py

import pandas as pd
from repositories.orgao_resp_repository import OrgaoRespRepository

class OrgaoRespService:
    def __init__(self, repository: OrgaoRespRepository):
        self.repository = repository

    def carregar_orgao(self, csv_path: str) -> int:
        try:
            # Lê apenas a coluna necessária para otimizar o uso de memória
            df = pd.read_csv(
                csv_path,
                sep=",",
                encoding="utf-8",
                usecols=["ORGAO_AMBIENTAL_RESP_ANALISE"]
            )
        except FileNotFoundError:
            print(f"🚨 Arquivo não encontrado: {csv_path}")
            return 0
        except ValueError:
            # Este erro ocorre se a coluna não for encontrada no CSV
            print(f"🚨 Coluna 'ORGAO_AMBIENTAL_RESP_ANALISE' não encontrada em {csv_path}")
            return 0

        # Remove linhas onde o nome do órgão é nulo ou vazio
        df.dropna(subset=["ORGAO_AMBIENTAL_RESP_ANALISE"], inplace=True)
        
        # Cria um conjunto de nomes únicos, limpando espaços em branco
        orgaos_unicos = {
            str(nome).strip() 
            for nome in df["ORGAO_AMBIENTAL_RESP_ANALISE"].unique() 
            if str(nome).strip()
        }

        # Itera apenas sobre a lista de nomes únicos e insere no banco
        for nome_orgao in orgaos_unicos:
            self.repository.insert_or_update(nome_orgao)

        # Confirma todas as inserções de uma vez, no final do processo
        self.repository.conn.commit()
        
        # O número de registros importados é o número de órgãos únicos
        return len(orgaos_unicos)