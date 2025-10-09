from typing import Any


class PMFSModalidadeRepository:
    """Repositório para tabela PMFS_MODALIDADE."""

    def __init__(self, connection: Any):
        self.conn = connection

    def insert_or_update(self, descricao: str):
        query = """
        INSERT INTO PMFS_MODALIDADE (MODALIDADE_PMFS)
        VALUES (%s)
        ON DUPLICATE KEY UPDATE MODALIDADE_PMFS = VALUES(MODALIDADE_PMFS);
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, (descricao,))
            self.conn.commit()
        finally:
            cursor.close()

    def count(self) -> int:
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM PMFS_MODALIDADE;")
            result = cursor.fetchone()
            return result[0] if result else 0
        finally:
            cursor.close()
