from database import DatabaseConnection
from populacao_tabelas.empresa_insert import EmpresaService
from repositories.cnaes_repository import CnaeRepository
from populacao_tabelas.cnae_insert import CnaeService
from repositories.empresa_repository import EmpresaRepository
from populacao_tabelas.PMFS_Modalidade_insert import PMFSModalidadeService
from repositories.pmfs_modalidade_repository import PMFSModalidadeRepository
from repositories.municipio_repository import MunicipioRepository
from populacao_tabelas.municipio_insert import MunicipioService
from repositories.orgao_resp_repository import OrgaoRespRepository
from populacao_tabelas.orgao_resp_insert import OrgaoRespService
from repositories.area_licitada_repository import AreaLicitadaRepository
from populacao_tabelas.area_licitada_insert import AreaLicitadaService
from repositories.silvicultura_repository import SilviculturaRepository
from populacao_tabelas.silvicultura_insert import SilviculturaService
from repositories.projeto_repository import ProjetoRepository
from populacao_tabelas.projeto_insert import ProjetoService


CSV_PATH = "datasets/cnae.csv"
CSV_PATH_EMPRESA = "datasets/base_empresa.csv"
CSV_PATH_DATASET_PMFS = "datasets/base_pmfs_tratado.csv"


def main():
    print("🚀 Iniciando importação CNAE...")

    with DatabaseConnection() as conn:
        repo = CnaeRepository(conn)
        service = CnaeService(repo)

        total_importados = service.carregar_cnae(CSV_PATH)
        total_final = repo.count()

        print(f"✅ {total_importados} registros importados.")
        print(f"📊 Total na tabela CNAES: {total_final}")

    with DatabaseConnection() as conn:
        repo = EmpresaRepository(conn)
        service = EmpresaService(repo)

        total_importados = service.carregar_empresa(CSV_PATH_EMPRESA)
        total_final = repo.count()

        print(f"✅ {total_importados} registros importados.")
        print(f"📊 Total na tabela EMPRESA: {total_final}")

    with DatabaseConnection() as conn:
        repo = PMFSModalidadeRepository(conn)
        service = PMFSModalidadeService(repo)

        total_importados = service.carregar_modalidade(CSV_PATH_DATASET_PMFS)
        total_final = repo.count()

        print(f"✅ {total_importados} registros importados.")
        print(f"📊 Total na tabela PMFS: {total_final}")

    with DatabaseConnection() as conn:
        repo = MunicipioRepository(conn)
        service = MunicipioService(repo)

        total_importados = service.carregar_municipio(CSV_PATH_DATASET_PMFS)
        total_final = repo.count()

        print(f"✅ {total_importados} registros importados.")
        print(f"📊 Total na tabela Municipio: {total_final}")

    with DatabaseConnection() as conn:
        repo = OrgaoRespRepository(conn)
        service = OrgaoRespService(repo)

        total_importados = service.carregar_orgao(CSV_PATH_DATASET_PMFS)
        total_final = repo.count()

        print(f"✅ {total_importados} registros importados.")
        print(f"📊 Total na tabela OrgaoResponsavel: {total_final}")

    with DatabaseConnection() as conn:
        repo = AreaLicitadaRepository(conn)
        service = AreaLicitadaService(repo)

        total_importados = service.carregar_area_licitada(CSV_PATH_DATASET_PMFS)
        total_final = repo.count()

        print(f"✅ {total_importados} registros importados.")
        print(f"📊 Total na tabela AreaLicitada: {total_final}")

    with DatabaseConnection() as conn:
        repo = SilviculturaRepository(conn)
        service = SilviculturaService(repo)

        total_importados = service.carregar_silvicultura(CSV_PATH_DATASET_PMFS)
        total_final = repo.count()

        print(f"✅ {total_importados} registros importados.")
        print(f"📊 Total na tabela Silvicultura: {total_final}")

    with DatabaseConnection() as conn:
        repo = ProjetoRepository(conn)
        service = ProjetoService(repo)

        total_importados = service.carregar_projeto(CSV_PATH_DATASET_PMFS)
        total_final = repo.count()

        print(f"✅ {total_importados} registros importados.")
        print(f"📊 Total na tabela Projetos: {total_final}")


if __name__ == "__main__":
    main()
