import csv
from repositories.empresa_repository import EmpresaRepository


class EmpresaService:
    """Importa dados EMPRESA para tabela EMPRESA."""

    def __init__(self, repository: EmpresaRepository):
        self.repository = repository

    def carregar_empresa(self, csv_path: str) -> int:
        registros_importados = 0

        with open(csv_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(
                file, delimiter=","
            )

            for linha in reader:
                cnpj = linha["CNPJ"].strip()
                nome = linha["NOME_FANTASIA"].strip()
                razao = ""  # se não tiver, coloca vazio
                situacao = linha["SITUACAO_CADASTRAL"].strip()
                data_inicio = linha.get("DATA_INICIO_ATIVIDADE", "").strip()
                cnae = linha["CNAE_FISCAL_PRINCIPAL"].strip()

                self.repository.insert_or_update(
                    cnpj, nome, razao, situacao, data_inicio, cnae
                )
                registros_importados += 1

        self.repository.conn.commit()
        return registros_importados
