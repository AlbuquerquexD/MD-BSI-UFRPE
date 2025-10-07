from typing import Any


class OrgaoRespRepository:
    """Repositório para tabela ORGAO_RESP_TECNICO."""

    def __init__(self, connection: Any):
        self.conn = connection

    def insert_or_update(self, nome_orgao: str):
        """
        Insere um órgão responsável ou atualiza o nome caso já exista (evita duplicados)
        """
        query = """
        INSERT INTO ORGAO_RESP_TECNICO (NOME_ORGAO)
        VALUES (%s)
        ON DUPLICATE KEY UPDATE NOME_ORGAO = VALUES(NOME_ORGAO);
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query, (nome_orgao, ))

    def count(self) -> int:
        """Retorna o total de registros na tabela ORGAO_RESP_TECNICO"""
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM ORGAO_RESP_TECNICO;")
            return cursor.fetchone()[0]
