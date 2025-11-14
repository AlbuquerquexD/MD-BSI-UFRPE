from typing import Any


class AreaLicitadaRepository:
    """Repositório para tabela AREA_LICITADA com lookup automático do município."""

    def __init__(self, connection: Any):
        self.conn = connection

    def get_all_as_map(self):
        """
        Busca todos os imóveis com CAR e retorna um dicionário (mapa)
        no formato {'NRO_CAR_IMOVEL_RURAL': ID_IMOVEL}.
        """
        cursor = self.conn.cursor()
        # Selecionamos apenas os que possuem CAR para criar um mapa limpo
        cursor.execute("""
            SELECT ID_EMPREENDIMENTO, NRO_CAR_IMOVEL_RURAL 
            FROM AREA_LICITADA 
            WHERE NRO_CAR_IMOVEL_RURAL IS NOT NULL
        """)
        # Cria o dicionário para busca rápida: {nro_car: id}
        imovel_map = {nro_car: id_ for id_, nro_car in cursor.fetchall()}
        cursor.close()
        return imovel_map

    def get_municipio_id(self, nome_municipio: str, uf: str) -> int:
        """Busca o ID do município pelo nome e UF."""
        query = """
        SELECT ID_MUNICIPIO
        FROM MUNICIPIO
        WHERE NOME_MUNICIPIO = %s AND UF = %s
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, (nome_municipio.upper(), uf.upper()))
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                raise ValueError(f"Município '{nome_municipio}' ({uf}) não encontrado.")
        finally:
            cursor.close()

    def insert_or_update(
        self,
        tipo_de_empreendimento: str,
        natureza_juridica: str,
        competencia_avaliacao: str,
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
        INSERT INTO AREA_LICITADA (
            TIPO_DE_EMPREENDIMENTO,
            NATUREZA_JURIDICA,
            COMPETENCIA_AVALIACAO,
            NRO_CAR_IMOVEL_RURAL,
            IMOVEL_RURAL_VINCULADO,
            NOME_EMPREENDIMENTO_VINC,
            LATITUDE_EMPREENDIMENTO,
            ID_MUNICIPIO,
            LONGITUDE_EMPREENDIMENTO,
            AREA_TOTAL_PROPRIEDADE
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            IMOVEL_RURAL_VINCULADO = VALUES(IMOVEL_RURAL_VINCULADO),
            NOME_EMPREENDIMENTO_VINC = VALUES(NOME_EMPREENDIMENTO_VINC),
            LATITUDE_EMPREENDIMENTO = VALUES(LATITUDE_EMPREENDIMENTO),
            LONGITUDE_EMPREENDIMENTO = VALUES(LONGITUDE_EMPREENDIMENTO),
            AREA_TOTAL_PROPRIEDADE = VALUES(AREA_TOTAL_PROPRIEDADE);
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                query,
                (
                    tipo_de_empreendimento,
                    natureza_juridica,
                    competencia_avaliacao,
                    nro_car,
                    imovel_vinculado,
                    nome_empreendimento,
                    latitude,
                    id_municipio,
                    longitude,
                    area_total,
                ),
            )
            self.conn.commit()
        finally:
            cursor.close()

    def count(self) -> int:
        """Retorna o total de registros na tabela AREA_LICITADA."""
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM AREA_LICITADA;")
            result = cursor.fetchone()
            return result[0] if result else 0
        finally:
            cursor.close()
