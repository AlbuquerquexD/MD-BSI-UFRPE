from typing import Any

class OrgaoRespRepository:
    def __init__(self, connection: Any):
        self.conn = connection

    def insert_or_update(self, nome_orgao: str):
        query = """
        INSERT IGNORE INTO ORGAO_RESP_TECNICO (NOME_ORGAO)
        VALUES (%s);
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, (nome_orgao,))
        finally:
            cursor.close()

    def count(self) -> int:
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM ORGAO_RESP_TECNICO;")
            result = cursor.fetchone()
            return result[0] if result else 0
        finally:
            cursor.close()

    def get_all_as_map(self) -> dict[str, int]:
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT ID, NOME_ORGAO FROM ORGAO_RESP_TECNICO")
            return {nome_orgao: org_id for org_id, nome_orgao in cursor.fetchall()}
        except Exception as e:
            print(f"Erro ao criar mapa de órgãos: {e}")
            return {}
        finally:
            cursor.close()