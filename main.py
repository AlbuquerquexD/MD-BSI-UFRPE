from database import DatabaseConnection
from empresa_insert import EmpresaService
from repositories.cnaes_repository import CnaeRepository
from cnae_insert import CnaeService
from repositories.empresa_repository import EmpresaRepository
from PMFS_Modalidade_insert import PMFSModalidadeService
from repositories.pmfs_modalidade_repository import PMFSModalidadeRepository
from repositories.municipio_repository import MunicipioRepository
from municipio_insert import MunicipioService

CSV_PATH = "datasets/cnae.csv"
CSV_PATH_EMPRESA = "datasets/base_empresa.csv"
CSV_PATH_DATASET_PMFS = "datasets/base_pmfs.csv"


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


if __name__ == "__main__":
    main()
