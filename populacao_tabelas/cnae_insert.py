import csv
from repositories.cnaes_repository import CnaeRepository

class CnaeService:
    """Importa dados CSV para CNAES."""

    def __init__(self, repository: CnaeRepository):
        self.repository = repository

    def carregar_cnae(self, csv_path: str) -> int:
        registros_importados = 0

        with open(csv_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter=';')
            next(reader)  # pula cabeçalho

            for linha in reader:
                if len(linha) < 2:
                    continue

                codigo = linha[0].strip()
                descricao = linha[1].strip()
                self.repository.insert_or_update(codigo, descricao)
                registros_importados += 1

        self.repository.conn.commit()
        return registros_importados
