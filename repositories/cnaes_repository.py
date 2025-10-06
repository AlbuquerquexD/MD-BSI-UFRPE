from typing import Any


class CnaeRepository:
    """Repositório para tabela CNAES."""

    def __init__(self, connection: Any):
        self.conn = connection

    def insert_or_update(self, codigo: str, descricao: str):
        query = """
        INSERT INTO CNAES (CODIGO_CNAE_FISCAL, DESCRICAO_CNAE_FISCAL)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE DESCRICAO_CNAE_FISCAL = VALUES(DESCRICAO_CNAE_FISCAL);
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query, (codigo, descricao))

    def count(self) -> int:
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM CNAES;")
            return cursor.fetchone()[0]
