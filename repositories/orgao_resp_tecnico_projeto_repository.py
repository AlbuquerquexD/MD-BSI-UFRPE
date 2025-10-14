from typing import Any

class OrgaoRespTecnicoProjetoRepository:
    def __init__(self, connection: Any):
        self.conn = connection

    def inserir_relacionamento(
        self,
        orgao_id: int,
        nro_registro: int,
        nro_art: int | None,
        atividade_rt: str | None,
        competencia_avaliacao: str | None,
    ):
        query = """
        INSERT IGNORE INTO ORGAO_RESP_TECNICO_PROJETO (
            ORGAO_RESP_TECNICO_ID, PROJETO_NRO_REGISTRO, NRO_ART,
            ATIVIDADE_RT, COMPETENCIA_DA_AVALIACAO
        ) VALUES (%s, %s, %s, %s, %s)
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                query,
                (orgao_id, nro_registro, nro_art, atividade_rt, competencia_avaliacao),
            )
        except Exception as e:
            print(f"Erro ao inserir relacionamento para ORGAO_ID {orgao_id} e NRO_REGISTRO {nro_registro}: {e}")
        finally:
            cursor.close()

    def count(self) -> int:
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM ORGAO_RESP_TECNICO_PROJETO")
            result = cursor.fetchone()
            return result[0] if result else 0
        finally:
            cursor.close()