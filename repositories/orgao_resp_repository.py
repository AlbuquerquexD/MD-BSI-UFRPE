from typing import Any


class OrgaoRespRepository:
    """Repositório para tabela ORGAO_RESP_TECNICO."""

    def __init__(self, connection: Any):
        self.conn = connection

    def insert_or_update(self, nome_orgao: str):
        """
        Insere um órgão responsável ou atualiza o nome caso já exista (evita duplicados).
        """
        query = """
        INSERT IGNORE INTO ORGAO_RESP_TECNICO (NOME_ORGAO)
        VALUES (%s);
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, (nome_orgao,))
            self.conn.commit()
        finally:
            cursor.close()

    def count(self) -> int:
        """Retorna o total de registros na tabela ORGAO_RESP_TECNICO."""
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM ORGAO_RESP_TECNICO;")
            result = cursor.fetchone()
            return result[0] if result else 0
        finally:
            cursor.close()
