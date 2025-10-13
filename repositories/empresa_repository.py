from typing import Any


class EmpresaRepository:
    """Repositório para tabela EMPRESA."""

    def __init__(self, connection: Any):
        self.conn = connection

    def insert_or_update(
        self,
        cnpj: str,
        nome: str,
        razao: str,
        situacao: str,
        data_inicio: str,
        cnae: str,
    ):
        query = """
        INSERT INTO EMPRESA (
            CNPJ,
            NOME_FANTASIA,
            RAZAO,
            SITUACAO_CADASTRAL,
            DATA_INICIO_ATIVIDADE,
            CODIGO_CNAE_FISCAL
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            NOME_FANTASIA = VALUES(NOME_FANTASIA),
            RAZAO = VALUES(RAZAO),
            SITUACAO_CADASTRAL = VALUES(SITUACAO_CADASTRAL),
            DATA_INICIO_ATIVIDADE = VALUES(DATA_INICIO_ATIVIDADE),
            CNAES_CODIGO_CNAE_FISCAL = VALUES(CODIGO_CNAE_FISCAL);
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, (cnpj, nome, razao, situacao, data_inicio, cnae))
            self.conn.commit()
        finally:
            cursor.close()

    def count(self) -> int:
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM EMPRESA;")
            result = cursor.fetchone()
            return result[0] if result else 0
        finally:
            cursor.close()
