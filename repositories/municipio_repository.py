from typing import Any


class MunicipioRepository:
    """Repositório para tabela MUNICIPIO."""

    def __init__(self, connection: Any):
        self.conn = connection

    def insert_or_update(self, uf: str, municipio: str):
        """
        Insere um município ou atualiza caso já exista (evita duplicados).
        """
        query = """
        INSERT INTO MUNICIPIO (UF, NOME_MUNICIPIO)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE NOME_MUNICIPIO = VALUES(NOME_MUNICIPIO);
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, (uf, municipio))
            self.conn.commit()
        finally:
            cursor.close()

    def count(self) -> int:
        """
        Retorna o total de registros na tabela MUNICIPIO.
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM MUNICIPIO;")
            result = cursor.fetchone()
            return result[0] if result else 0
        finally:
            cursor.close()
