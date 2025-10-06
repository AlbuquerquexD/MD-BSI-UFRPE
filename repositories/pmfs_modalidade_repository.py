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
        with self.conn.cursor() as cursor:
            cursor.execute(query, (descricao,))

    def count(self) -> int:
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM PMFS_MODALIDADE;")
            return cursor.fetchone()[0]
