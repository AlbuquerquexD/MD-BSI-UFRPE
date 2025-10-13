import csv
from repositories.orgao_resp_repository import OrgaoRespRepository


class OrgaoRespService:
    """Importa dados do CSV para a tabela ORGAO_RESP_TECNICO."""

    def __init__(self, repository: OrgaoRespRepository):
        self.repository = repository

    def carregar_orgao(self, csv_path: str) -> int:
        registros_importados = 0

        with open(csv_path, "r", encoding="utf-8") as file:
            # Usa DictReader para identificar colunas pelo nome
            reader = csv.DictReader(file, delimiter=",")

            for linha in reader:
                # Pega o nome do órgão responsável — você pode ajustar o nome da coluna conforme o CSV real
                nome_orgao = linha["ORGAO_AMBIENTAL_RESP_ANALISE"].strip()

                if not nome_orgao:
                    continue  # ignora linhas vazias

                # Insere ou atualiza no banco
                self.repository.insert_or_update(nome_orgao)
                registros_importados += 1

        # Confirma transação
        self.repository.conn.commit()
        return registros_importados
