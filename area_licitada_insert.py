import csv
from repositories.area_licitada_repository import AreaLicitadaRepository


class AreaLicitadaService:
    """Importa dados CSV para AREA_LICITADA, usando lookup do ID do município."""

    def __init__(self, repository: AreaLicitadaRepository):
        self.repository = repository

    def parse_decimal_mysql(self, valor: str) -> float:
        """
        Converte string numérica do CSV para float,
        removendo pontos de milhar e usando ponto decimal.
        """
        if valor is None or valor.strip() == "":
            return 0.0  # permite NULL no banco
        # Remove todos os pontos e substitui vírgula decimal
        valor = valor.replace(".", "").replace(",", ".").strip()
        return float(valor)

    def carregar_area_licitada(self, csv_path: str) -> int:
        """
        Lê um CSV e insere ou atualiza registros na tabela AREA_LICITADA.
        Faz lookup automático do ID do município usando a tabela MUNICIPIO.
        """
        registros_importados = 0

        with open(csv_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)  # Usa cabeçalho do CSV
            for linha in reader:
                try:
                    nro_car = linha["NRO_CAR_IMOVEL_RURAL"].strip()
                    imovel_vinculado = linha["IMOVEL_RURAL_VINCULADO"].strip()
                    nome_empreendimento = linha["NOME_EMPREENDIMENTO_VINC"].strip()
                    area_total = self.parse_decimal_mysql(
                        linha["AREA_TOTAL_PROPRIEDADE"]
                    )
                    latitude = self.parse_decimal_mysql(
                        linha["LATITUDE_EMPREENDIMENTO"]
                    )
                    longitude = self.parse_decimal_mysql(
                        linha["LONGITUDE_EMPREENDIMENTO"]
                    )
                    nome_municipio = linha["MUNICIPIO"].strip()
                    uf = linha["UF"].strip()

                    # Obtém o ID do município
                    id_municipio = self.repository.get_municipio_id(nome_municipio, uf)

                    self.repository.insert_or_update(
                        nro_car=nro_car,
                        imovel_vinculado=imovel_vinculado,
                        nome_empreendimento=nome_empreendimento,
                        latitude=latitude,
                        id_municipio=id_municipio,
                        longitude=longitude,
                        area_total=area_total,
                    )
                    registros_importados += 1
                except KeyError as e:
                    print(f"Aviso: {e}")
                except ValueError as e:
                    print(f"Aviso: {e}")

        self.repository.conn.commit()
        return registros_importados
