import pandas as pd
from repositories.orgao_resp_tecnico_projeto_repository import OrgaoRespTecnicoProjetoRepository
from repositories.orgao_resp_repository import OrgaoRespRepository
from repositories.projeto_repository import ProjetoRepository


class OrgaoRespTecnicoProjetoService:
    def __init__(
        self,
        relacionamento_repo: OrgaoRespTecnicoProjetoRepository,
        orgao_repo: OrgaoRespRepository,
        projeto_repo: ProjetoRepository,
    ):
        self.relacionamento_repo = relacionamento_repo
        self.orgao_repo = orgao_repo
        self.projeto_repo = projeto_repo

    def carregar_relacionamento(self, csv_path: str) -> int:
        try:
            df = pd.read_csv(csv_path, sep=",", encoding="utf-8")
            df.columns = df.columns.str.strip()
        except FileNotFoundError:
            print(f"🚨 Arquivo não encontrado: {csv_path}")
            return 0

        orgao_map = self.orgao_repo.get_all_as_map()
        projetos_validos = self.projeto_repo.get_all_nros_registro_as_set()
        registros_importados = 0

        colunas = {
            "registro": "NRO_REGISTRO",
            "orgao": "ORGAO_AMBIENTAL_RESP_ANALISE",
            "art": "NRO_ART",
            "atividade": "ATIVIDADE_RT",
            "competencia": "COMPETENCIA_AVALIACAO",
        }

        df[colunas["registro"]] = pd.to_numeric(df[colunas["registro"]], errors="coerce").astype("Int64")
        df[colunas["art"]] = pd.to_numeric(df[colunas["art"]], errors="coerce").astype("Int64")

        for index, row in df.iterrows():
            nro_registro = row.get(colunas["registro"])
            nome_orgao = row.get(colunas["orgao"])

            if pd.isna(nro_registro) or pd.isna(nome_orgao):
                continue

            nome_orgao = str(nome_orgao).strip()
            orgao_id = orgao_map.get(nome_orgao)
            projeto_existe = nro_registro in projetos_validos

            if orgao_id and projeto_existe:
                nro_art = row.get(colunas["art"])
                atividade_rt = row.get(colunas["atividade"])
                competencia = row.get(colunas["competencia"])

                self.relacionamento_repo.inserir_relacionamento(
                    orgao_id=orgao_id,
                    nro_registro=nro_registro,
                    nro_art=nro_art if pd.notna(nro_art) else None,
                    atividade_rt=atividade_rt if pd.notna(atividade_rt) else None,
                    competencia_avaliacao=competencia if pd.notna(competencia) else None,
                )
                registros_importados += 1
            else:
                if not orgao_id and nome_orgao:
                    print(f"⚠️ Aviso: Órgão '{nome_orgao}' não encontrado no banco. Relação com projeto '{nro_registro}' ignorada.")
                if not projeto_existe:
                    print(f"⚠️ Aviso: Projeto '{nro_registro}' não encontrado no banco. Relação com órgão '{nome_orgao}' ignorada.")

        self.relacionamento_repo.conn.commit()
        return registros_importados