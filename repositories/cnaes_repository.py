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
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, (codigo, descricao))
            self.conn.commit()
        finally:
            cursor.close()

    def count(self) -> int:
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM CNAES;")
            result = cursor.fetchone()
            return result[0] if result else 0
        finally:
            cursor.close()
