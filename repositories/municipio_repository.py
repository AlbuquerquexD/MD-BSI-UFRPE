from typing import Any


class MunicipioRepository:
    """Repositório para tabela MUNICIPIO."""

    def __init__(self, connection: Any):
        self.conn = connection

    def insert_or_update(self, uf: str, municipio: str):
        """
        Insere um município ou atualiza caso já exista (evita duplicados)
        """
        query = """
        INSERT INTO MUNICIPIO (UF, NOME_MUNICIPIO)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE NOME_MUNICIPIO = VALUES(NOME_MUNICIPIO);
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query, (uf, municipio))

    def count(self) -> int:
        """Retorna o total de registros na tabela MUNICIPIO"""
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM MUNICIPIO;")
            return cursor.fetchone()[0]
