import csv
from repositories.projeto_repository import ProjetoRepository


class ProjetoService:
    def __init__(self, repository: ProjetoRepository):
        self.repository = repository

    @staticmethod
    def is_cnpj(cpfcnpj: str) -> bool:
        if not cpfcnpj:
            return False
        digits = "".join(filter(str.isdigit, cpfcnpj))
        return len(digits) == 14

    def carregar_projeto(self, csv_path: str) -> int:
        registros_importados = 0
        silvicultura_id = 1

        with open(csv_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file, delimiter=",")

            for linha in reader:
                cpf_cnpj = linha.get("CPF_CNPJ_DETENTOR", "").strip()
                if not self.is_cnpj(cpf_cnpj):
                    continue

                try:
                    cnpj_limpo = "".join(filter(str.isdigit, cpf_cnpj))

                    nro_registro = (
                        int(float(linha["NRO_REGISTRO"]))
                        if linha["NRO_REGISTRO"]
                        else None
                    )
                    nro_autorizacao = (
                        int(float(linha["NRO_AUTORIZACAO"]))
                        if linha["NRO_AUTORIZACAO"]
                        else None
                    )
                    data_emissao = linha.get("DATA_DE_EMISSAO", None)
                    data_validade = linha.get("DATA_DE_VALIDADE", None)
                    situacao = linha.get("SITUACAO", None)
                    data_situacao = linha.get("DATA_DA_SITUACAO", None)
                    ultimo_tramite = linha.get("ULTIMO_TRAMITE", None)
                    data_tramite = linha.get("DATA_DO_TRAMITE", None)

                    tipo_empreendimento_rural = (
                        1 if linha.get("TIPO_DE_EMPREENDIMENTO") == "Rural" else 0
                    )
                    natureza_juridica_publica = (
                        1 if linha.get("NATUREZA_JURIDICA") == "Pública" else 0
                    )

                    ano_autorizacao = None
                    if data_emissao and len(data_emissao) >= 4:
                        ano_autorizacao = int(data_emissao[:4])

                    descricao_autorizacao = linha.get("ATIVIDADE", "")

                    self.repository.insert_or_update(
                        nro_registro=nro_registro,
                        nro_autorizacao=nro_autorizacao,
                        data_emissao=data_emissao,
                        ano_autorizacao=ano_autorizacao,
                        descricao_autorizacao=descricao_autorizacao,
                        data_validade=data_validade,
                        situacao=situacao,
                        data_situacao=data_situacao,
                        ultimo_tramite=ultimo_tramite,
                        data_tramite=data_tramite,
                        tipo_empreendimento_rural=tipo_empreendimento_rural,
                        natureza_juridica_publica=natureza_juridica_publica,
                        silvicultura_id=silvicultura_id,
                        empresa_cnpj=cnpj_limpo,
                    )

                    registros_importados += 1
                    silvicultura_id += 1

                except Exception as e:
                    print(f"Aviso: erro ao importar linha ({e}) → {linha}")

        self.repository.conn.commit()
        return registros_importados
