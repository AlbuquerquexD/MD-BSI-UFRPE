import csv
from repositories.municipio_repository import MunicipioRepository


class MunicipioService:
    """Importa dados CSV para a tabela MUNICIPIO."""

    def __init__(self, repository: MunicipioRepository):
        self.repository = repository

    def carregar_municipio(self, csv_path: str) -> int:
        """
        Carrega os municípios do CSV para o banco de dados.

        Retorna:
            int: total de registros importados
        """
        registros_importados = 0

        with open(csv_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file, delimiter=",")
            for linha in reader:
                uf = linha.get("UF", "").strip()
                municipio = linha.get("MUNICIPIO", "").strip()

                if not uf or not municipio:
                    continue  # ignora linhas incompletas

                self.repository.insert_or_update(uf, municipio)
                registros_importados += 1

        self.repository.conn.commit()
        return registros_importados
