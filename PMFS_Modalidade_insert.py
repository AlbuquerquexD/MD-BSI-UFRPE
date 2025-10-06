import csv
from repositories.pmfs_modalidade_repository import PMFSModalidadeRepository


class PMFSModalidadeService:
    def __init__(self, repository: PMFSModalidadeRepository):
        self.repository = repository

    def carregar_modalidade(self, csv_path: str) -> int:
        registros_importados = 0
        with open(csv_path, "r", encoding="utf-8") as file:
            reader = csv.reader(file, delimiter=",")
            header = next(reader)  # lê cabeçalho
            if "MODALIDADE_PMFS" not in header:
                raise ValueError("Coluna 'MODALIDADE_PMFS' não encontrada no CSV")

            # identifica o índice da coluna MODALIDADE_PMFS
            idx = header.index("MODALIDADE_PMFS")

            for linha in reader:
                if len(linha) <= idx:
                    continue  # ignora linhas com menos colunas que o índice
                descricao = linha[idx].strip()
                self.repository.insert_or_update(descricao)
                registros_importados += 1

        self.repository.conn.commit()
        return registros_importados
