# /repositories/pmfs_modalidade_repository.py

from typing import Any

class PMFSModalidadeRepository:
    """Repositório para tabela PMFS_MODALIDADE."""

    def __init__(self, connection: Any):
        self.conn = connection

    def insert_or_update(self, descricao: str):
        # --- PRINT DE DEPURAÇÃO PARA CONFIRMAR EXECUÇÃO ---
        print(f"DEBUG: Repositório executando INSERT IGNORE para '{descricao}'")
        
        query = """
        INSERT IGNORE INTO PMFS_MODALIDADE (MODALIDADE_PMFS)
        VALUES (%s)
        ON DUPLICATE KEY UPDATE MODALIDADE_PMFS = VALUES(MODALIDADE_PMFS);
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, (descricao,))
            # O commit será feito no final pelo service, não precisa aqui.
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

    def get_all_as_map(self):
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT ID_PMFS_MODALIDADE, MODALIDADE_PMFS FROM PMFS_MODALIDADE"
        )
        modalidade_map = {nome: id_ for id_, nome in cursor.fetchall()}
        cursor.close()
        return modalidade_map