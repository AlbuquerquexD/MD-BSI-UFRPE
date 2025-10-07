from typing import Any


class AreaLicitadaRepository:
    """Repositório para tabela AREA_LICITADA com lookup automático do município."""

    def __init__(self, connection: Any):
        self.conn = connection

    def get_municipio_id(self, nome_municipio: str, uf: str) -> int:
        """Busca o ID do município pelo nome e UF."""
        query = (
            "SELECT ID_MUNICIPIO FROM MUNICIPIO WHERE NOME_MUNICIPIO = %s AND UF = %s"
        )
        with self.conn.cursor() as cursor:
            cursor.execute(query, (nome_municipio.upper(), uf.upper()))
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                raise ValueError(f"Município '{nome_municipio}' ({uf}) não encontrado.")

    def insert_or_update(
        self,
        nro_car: str,
        imovel_vinculado: str,
        nome_empreendimento: str,
        latitude: float,
        id_municipio: int,
        longitude: float,
        area_total: float,
    ):
        """
        Insere ou atualiza um registro na tabela AREA_LICITADA.
        Recebe ID do município diretamente.
        """
        query = """
        INSERT INTO AREA_LICITADA
            (NRO_CAR_IMOVEL_RURAL, IMOVEL_RURAL_VINCULADO, NOME_EMPREENDIMENTO_VINC,
             LATITUDE_EMPREENDIMENTO, ID_MUNICIPIO, LONGITUDE_EMPREENDIMENTO, AREA_TOTAL_PROPRIEDADE)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            IMOVEL_RURAL_VINCULADO = VALUES(IMOVEL_RURAL_VINCULADO),
            NOME_EMPREENDIMENTO_VINC = VALUES(NOME_EMPREENDIMENTO_VINC),
            LATITUDE_EMPREENDIMENTO = VALUES(LATITUDE_EMPREENDIMENTO),
            LONGITUDE_EMPREENDIMENTO = VALUES(LONGITUDE_EMPREENDIMENTO),
            AREA_TOTAL_PROPRIEDADE = VALUES(AREA_TOTAL_PROPRIEDADE);
        """
        with self.conn.cursor() as cursor:
            cursor.execute(
                query,
                (
                    nro_car,
                    imovel_vinculado,
                    nome_empreendimento,
                    latitude,
                    id_municipio,
                    longitude,
                    area_total,
                ),
            )

    def count(self) -> int:
        """Retorna o total de registros na tabela AREA_LICITADA."""
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM AREA_LICITADA;")
            return cursor.fetchone()[0]
