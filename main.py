# main.py

from database import DatabaseConnection

# Imports para as tabelas principais e de domínio
from populacao_tabelas.orgao_resp_tecnico_projeto_insert import (
    OrgaoRespTecnicoProjetoService,
)
from repositories.cnaes_repository import CnaeRepository
from populacao_tabelas.cnae_insert import CnaeService
from repositories.empresa_repository import EmpresaRepository
from populacao_tabelas.empresa_insert import EmpresaService
from repositories.orgao_resp_tecnico_projeto_repository import (
    OrgaoRespTecnicoProjetoRepository,
)
from repositories.pmfs_modalidade_repository import PMFSModalidadeRepository
from populacao_tabelas.PMFS_Modalidade_insert import PMFSModalidadeService
from repositories.municipio_repository import MunicipioRepository
from populacao_tabelas.municipio_insert import MunicipioService
from repositories.orgao_resp_repository import OrgaoRespRepository
from populacao_tabelas.orgao_resp_insert import OrgaoRespService
from repositories.area_licitada_repository import AreaLicitadaRepository
from populacao_tabelas.area_licitada_insert import AreaLicitadaService
from repositories.projeto_empresa_repository import ProjetoEmpresaRepository
from repositories.silvicultura_repository import SilviculturaRepository
from populacao_tabelas.silvicultura_insert import SilviculturaService
from repositories.projeto_repository import ProjetoRepository
from populacao_tabelas.projeto_insert import ProjetoService
from repositories.projeto_imovel_repository import ProjetoImovelRepository
from populacao_tabelas.projeto_imovel_insert import ProjetoImovelService


# --- NOVOS IMPORTS para a tabela de relacionamento ---
from repositories.projeto_pmfs_modalidade_repository import (
    ProjetoPmfsModalidadeRepository,
)
from populacao_tabelas.projeto_pmfs_modalidade_insert import (
    ProjetoPmfsModalidadeService,
)


# --- Caminhos dos arquivos de dados ---
CSV_PATH_CNAE = "datasets/cnae.csv"
CSV_PATH_EMPRESA = "datasets/base_empresa.csv"
CSV_PATH_DATASET_PMFS = "datasets/base_pmfs_cpf_retirado.csv"


def main():
    """
    Função principal para executar a importação de dados
    para todas as tabelas do banco de dados.
    """

    # 1. Popula a tabela CNAES (tabela de domínio)
    print("🚀 Iniciando importação CNAE...")
    with DatabaseConnection() as conn:
        repo = CnaeRepository(conn)
        service = CnaeService(repo)
        total_importados = service.carregar_cnae(CSV_PATH_CNAE)
        total_final = repo.count()
        print(f"✅ {total_importados} registros importados.")
        print(f"📊 Total na tabela CNAES: {total_final}\n")

    # 2. Popula a tabela EMPRESA (depende de CNAES)
    print("🚀 Iniciando importação EMPRESA...")
    with DatabaseConnection() as conn:
        repo = EmpresaRepository(conn)
        service = EmpresaService(repo)
        total_importados = service.carregar_empresa(CSV_PATH_EMPRESA)
        total_final = repo.count()
        print(f"✅ {total_importados} registros importados.")
        print(f"📊 Total na tabela EMPRESA: {total_final}\n")

    # 3. Popula a tabela PMFS_MODALIDADE (tabela de domínio)
    print("🚀 Iniciando importação PMFS_MODALIDADE...")
    with DatabaseConnection() as conn:
        repo = PMFSModalidadeRepository(conn)
        service = PMFSModalidadeService(repo)
        total_importados = service.carregar_modalidade(CSV_PATH_DATASET_PMFS)
        total_final = repo.count()
        print(f"✅ {total_importados} registros importados.")
        print(f"📊 Total na tabela PMFS_MODALIDADE: {total_final}\n")

    # 4. Popula a tabela MUNICIPIO (tabela de domínio)
    print("🚀 Iniciando importação MUNICIPIO...")
    with DatabaseConnection() as conn:
        repo = MunicipioRepository(conn)
        service = MunicipioService(repo)
        total_importados = service.carregar_municipio(CSV_PATH_DATASET_PMFS)
        total_final = repo.count()
        print(f"✅ {total_importados} registros importados.")
        print(f"📊 Total na tabela MUNICIPIO: {total_final}\n")

    # 5. Popula a tabela ORGAO_RESPONSAVEL (tabela de domínio)
    print("🚀 Iniciando importação ORGAO_RESPONSAVEL...")
    with DatabaseConnection() as conn:
        repo = OrgaoRespRepository(conn)
        service = OrgaoRespService(repo)
        total_importados = service.carregar_orgao(CSV_PATH_DATASET_PMFS)
        total_final = repo.count()
        print(f"✅ {total_importados} registros importados.")
        print(f"📊 Total na tabela ORGAO_RESPONSAVEL: {total_final}\n")

    # 6. Popula a tabela AREA_LICITADA (tabela de domínio)
    print("🚀 Iniciando importação AREA_LICITADA...")
    with DatabaseConnection() as conn:
        repo = AreaLicitadaRepository(conn)
        service = AreaLicitadaService(repo)
        total_importados = service.carregar_area_licitada(CSV_PATH_DATASET_PMFS)
        total_final = repo.count()
        print(f"✅ {total_importados} registros importados.")
        print(f"📊 Total na tabela AREA_LICITADA: {total_final}\n")

    # 7. Popula a tabela SILVICULTURA (tabela de domínio)
    print("🚀 Iniciando importação SILVICULTURA...")
    with DatabaseConnection() as conn:
        repo = SilviculturaRepository(conn)
        service = SilviculturaService(repo)
        total_importados = service.carregar_silvicultura(CSV_PATH_DATASET_PMFS)
        total_final = repo.count()
        print(f"✅ {total_importados} registros importados.")
        print(f"📊 Total na tabela SILVICULTURA: {total_final}\n")

    # 8. Popula a tabela PROJETO (depende das tabelas de domínio acima)
    print("🚀 Iniciando importação PROJETO...")
    with DatabaseConnection() as conn:
        repo = ProjetoRepository(conn)
        service = ProjetoService(repo)
        total_importados = service.carregar_projeto(CSV_PATH_DATASET_PMFS)
        total_final = repo.count()
        print(f"✅ {total_importados} registros importados.")
        print(f"📊 Total na tabela PROJETO: {total_final}\n")

    # --- ETAPA FINAL ---
    # 9. Popula a tabela de relacionamento PROJETO_PMFS_MODALIDADE
    # (depende de PROJETO e PMFS_MODALIDADE estarem populadas)
    print("🚀 Iniciando importação do relacionamento Projeto x Modalidade...")
    with DatabaseConnection() as conn:
        repo_relacionamento = ProjetoPmfsModalidadeRepository(conn)
        repo_modalidade = PMFSModalidadeRepository(conn)  # Usado para consulta

        service = ProjetoPmfsModalidadeService(repo_relacionamento, repo_modalidade)

        total_importados = service.carregar_relacionamento(CSV_PATH_DATASET_PMFS)
        total_final = repo_relacionamento.count()

        print(f"✅ {total_importados} relacionamentos importados.")
        print(f"📊 Total na tabela PROJETO_PMFS_MODALIDADE: {total_final}\n")

    print("🚀 Iniciando importação do relacionamento Projeto x Modalidade...")
    with DatabaseConnection() as conn:
        # ... (código existente)
        print(f"✅ {total_importados} relacionamentos importados.")
        print(f"📊 Total na tabela PROJETO_PMFS_MODALIDADE: {total_final}\n")

    # 10. Popula a tabela de relacionamento PROJETO_IMOVEL
    print("🚀 Iniciando importação do relacionamento Projeto x Imóvel...")
    with DatabaseConnection() as conn:
        # Precisamos dos repositórios para inserir e para consultar
        repo_relacionamento_imovel = ProjetoImovelRepository(conn)
        repo_area_licitada = AreaLicitadaRepository(conn)  # Para o lookup

        service = ProjetoImovelService(repo_relacionamento_imovel, repo_area_licitada)

        total_importados = service.carregar_relacionamento(CSV_PATH_DATASET_PMFS)
        total_final = repo_relacionamento_imovel.count()

        print(f"✅ {total_importados} relacionamentos importados.")
        print(f"📊 Total na tabela PROJETO_IMOVEL: {total_final}\n")

    print("🚀 Iniciando importação do relacionamento Órgão Técnico x Projeto...")
    with DatabaseConnection() as conn:
        repo_relacionamento_orgao = OrgaoRespTecnicoProjetoRepository(conn)
        repo_orgao = OrgaoRespRepository(conn)
        repo_projeto = ProjetoRepository(conn)

        service = OrgaoRespTecnicoProjetoService(
            repo_relacionamento_orgao, repo_orgao, repo_projeto
        )

        total_importados = service.carregar_relacionamento(CSV_PATH_DATASET_PMFS)
        total_final = repo_relacionamento_orgao.count()

        print(f"✅ {total_importados} relacionamentos importados.")
        print(f"📊 Total na tabela ORGAO_RESP_TECNICO_PROJETO: {total_final}\n")

    print("🎉 Processo de importação finalizado com sucesso!")


if __name__ == "__main__":
    main()
