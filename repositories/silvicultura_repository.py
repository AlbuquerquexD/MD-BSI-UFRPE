from typing import Any


class SilviculturaRepository:
    """Repositório para tabela SILVICULTURA."""

    def __init__(self, connection: Any):
        self.conn = connection

    @staticmethod
    def is_cnpj(cpfcnpj: str) -> bool:
        """
        Verifica se o valor informado é um CNPJ válido (14 dígitos numéricos).
        Ignora formatações como pontos, barras, hífens ou asteriscos.
        Exemplo:
        - "657.***.***-**" → CPF → False
        - "02.023.852/0001-20" → CNPJ → True
        """
        if not cpfcnpj:
            return False

        # Remove tudo que não for número
        digits = "".join(filter(str.isdigit, cpfcnpj))

        # Retorna True somente se for um CNPJ (14 dígitos)
        return len(digits) == 14

    def insert_or_update(
        self,
        ciclo_corte: float | None,
        area_manejo_forestal: float | None,
        area_efetivo_manejo: float | None,
        capacidade_produtiva: float | None,
        estimativa_produtiva_anual: float | None,
        intensidade_corte: float | None,
        cpfcnpj: str,
    ) -> int | None:
        """
        Insere ou atualiza um registro na tabela SILVICULTURA **somente se for CNPJ**.
        Retorna o ID do registro inserido ou None se não inseriu.
        """

        # 🔒 Verifica se é CNPJ válido antes de inserir
        if not self.is_cnpj(cpfcnpj):
            return None

        query = """
        INSERT INTO SILVICULTURA (
            CICLO_CORTE, AREA_MANEJO_FLORESTAL, AREA_EFETIVO_MANEJO,
            CAPACIDADE_PRODUTIVA, ESTIMATIVA_PRODUTIVA_ANUAL, INTENSIDADE_CORTE
        )
        VALUES (%s, %s, %s, %s, %s, %s);
        """

        with self.conn.cursor() as cursor:
            cursor.execute(
                query,
                (
                    ciclo_corte,
                    area_manejo_forestal,
                    area_efetivo_manejo,
                    capacidade_produtiva,
                    estimativa_produtiva_anual,
                    intensidade_corte,
                ),
            )
            self.conn.commit()
            # Retorna o ID gerado (mantém sincronia com PROJETO)
            return cursor.lastrowid

    def count(self) -> int:
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM SILVICULTURA;")
            return cursor.fetchone()[0]
