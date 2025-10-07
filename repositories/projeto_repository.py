from typing import Any


class ProjetoRepository:
    """Repositório para tabela PROJETO."""

    def __init__(self, connection: Any):
        self.conn = connection

    def insert_or_update(
        self,
        nro_registro: int | None,
        nro_autorizacao: int | None,
        data_emissao: str | None,
        ano_autorizacao: int | None,
        descricao_autorizacao: str | None,
        data_validade: str | None,
        situacao: str | None,
        data_situacao: str | None,
        ultimo_tramite: str | None,
        data_tramite: str | None,
        tipo_empreendimento_rural: int | None,
        natureza_juridica_publica: int | None,
        silvicultura_id: int,
    ):
        query = """
        INSERT INTO PROJETO (
            NRO_REGISTRO, NRO_AUTORIZACAO, DATA_DE_EMISSAO, ANO_AUTORIZACAO,
            DESCRICAO_AUTORIZACAO, DATA_DE_VALIDADE, SITUACAO, DATA_SITUACAO,
            ULTIMO_TRAMITE, DATA_DO_TRAMITE, TIPO_DE_EMPREENDIMENTO_RURAL,
            NATUREZA_JURIDICA_PUBLICA, SILVICULTURA_ID_SILVICULTURA
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            NRO_AUTORIZACAO = VALUES(NRO_AUTORIZACAO),
            DATA_DE_EMISSAO = VALUES(DATA_DE_EMISSAO),
            ANO_AUTORIZACAO = VALUES(ANO_AUTORIZACAO),
            DESCRICAO_AUTORIZACAO = VALUES(DESCRICAO_AUTORIZACAO),
            DATA_DE_VALIDADE = VALUES(DATA_DE_VALIDADE),
            SITUACAO = VALUES(SITUACAO),
            DATA_SITUACAO = VALUES(DATA_SITUACAO),
            ULTIMO_TRAMITE = VALUES(ULTIMO_TRAMITE),
            DATA_DO_TRAMITE = VALUES(DATA_DO_TRAMITE),
            TIPO_DE_EMPREENDIMENTO_RURAL = VALUES(TIPO_DE_EMPREENDIMENTO_RURAL),
            NATUREZA_JURIDICA_PUBLICA = VALUES(NATUREZA_JURIDICA_PUBLICA),
            SILVICULTURA_ID_SILVICULTURA = VALUES(SILVICULTURA_ID_SILVICULTURA);
        """
        with self.conn.cursor() as cursor:
            cursor.execute(
                query,
                (
                    nro_registro,
                    nro_autorizacao,
                    data_emissao,
                    ano_autorizacao,
                    descricao_autorizacao,
                    data_validade,
                    situacao,
                    data_situacao,
                    ultimo_tramite,
                    data_tramite,
                    tipo_empreendimento_rural,
                    natureza_juridica_publica,
                    silvicultura_id,
                ),
            )

    def count(self) -> int:
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM PROJETO;")
            return cursor.fetchone()[0]
