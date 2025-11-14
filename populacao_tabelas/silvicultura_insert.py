import csv
from repositories.silvicultura_repository import SilviculturaRepository


class SilviculturaService:
    """Importa dados para a tabela SILVICULTURA."""

    def __init__(self, repository: SilviculturaRepository):
        self.repository = repository

    @staticmethod
    def parse_decimal(valor: str) -> float | None:
        if not valor or valor.strip() == "":
            return None
        valor = valor.strip()
        valor = valor.replace(".", "")
        valor = valor.replace(",", ".")
        try:
            return float(valor)
        except ValueError:
            return None

    @staticmethod
    def is_cnpj(valor: str) -> bool:
        """Retorna True se o valor for um CNPJ (>=14 dígitos numéricos)."""
        if not valor:
            return False
        digits = "".join(filter(str.isdigit, valor))
        return len(digits) >= 14

    def carregar_silvicultura(self, csv_path: str) -> int:
        """
        Lê CSV e insere ou atualiza registros na tabela SILVICULTURA.
        Espera as colunas:
        CICLO_CORTE, SISTEMA_SILVICULTURAL, AREA_MANEJO_FLORESTAL, AREA_EFETIVO_MANEJO,
        CAPACIDADE_PRODUTIVA, ESTIMATIVA_PRODUTIVA_ANUAL, INTENSIDADE_CORTE
        """
        registros_importados = 0

        with open(csv_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file, delimiter=",")

            for linha in reader:
                try:
                    ciclo_corte = self.parse_decimal(linha["CICLO_CORTE"])
                    sistema_silvicultural = linha["SISTEMA_SILVICULTURAL"]
                    metodo_extracao = linha["METODO_EXTRACAO"]
                    area_manejo = self.parse_decimal(linha["AREA_MANEJO_FLORESTAL"])
                    area_efetivo = self.parse_decimal(linha["AREA_EFETIVO_MANEJO"])
                    capacidade = self.parse_decimal(linha["CAPACIDADE_PRODUTIVA"])
                    estimativa = self.parse_decimal(linha["ESTIMATIVA_PRODUTIVA_ANUAL"])
                    intensidade = self.parse_decimal(linha["INTENSIDADE_CORTE"])
                    cpfcnpj_dataset = linha["CPF_CNPJ_DETENTOR"]

                    self.repository.insert_or_update(
                        ciclo_corte=ciclo_corte,
                        sistema_silvicultural=sistema_silvicultural,
                        metodo_extracao=metodo_extracao,
                        area_manejo_forestal=area_manejo,
                        area_efetivo_manejo=area_efetivo,
                        capacidade_produtiva=capacidade,
                        estimativa_produtiva_anual=estimativa,
                        intensidade_corte=intensidade,
                        cpfcnpj=cpfcnpj_dataset
                    )
                    registros_importados += 1
                except ValueError as e:
                    print(f"Aviso: valor inválido {e} (Linha: {linha})")

        self.repository.conn.commit()
        return registros_importados
