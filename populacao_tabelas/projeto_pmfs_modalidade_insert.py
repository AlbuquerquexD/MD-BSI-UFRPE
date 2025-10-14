import pandas as pd

class ProjetoPmfsModalidadeService:
    def __init__(self, relacionamento_repo, modalidade_repo):
        """
        O serviço precisa de dois repositórios:
        1. Para inserir na tabela de relacionamento.
        2. Para buscar o mapa de modalidades existentes.
        """
        self.relacionamento_repo = relacionamento_repo
        self.modalidade_repo = modalidade_repo

    def carregar_relacionamento(self, csv_path):
        """Lê o CSV, busca os IDs correspondentes e popula a tabela."""
        try:
            df = pd.read_csv(csv_path, sep=',', encoding='utf-8')
        except FileNotFoundError:
            print(f"🚨 Arquivo não encontrado: {csv_path}")
            return 0

        # Carrega o mapa de modalidades uma única vez para otimizar
        modalidade_map = self.modalidade_repo.get_all_as_map()
        
        registros_importados = 0
        
        # Supondo que as colunas no seu CSV se chamam 'NRO_REGISTRO' e 'MODALIDADE_PMFS'
        # **Ajuste os nomes das colunas se forem diferentes!**
        coluna_registro = 'NRO_REGISTRO'
        coluna_modalidade = 'MODALIDADE_PMFS'

        df[coluna_registro] = df[coluna_registro].astype('Int64')
        
        for index, row in df.iterrows():
            nro_registro = row[coluna_registro]
            nome_modalidade = row[coluna_modalidade]

            # Busca o ID da modalidade no mapa que carregamos
            id_modalidade = modalidade_map.get(nome_modalidade)

            if nro_registro and id_modalidade:
                self.relacionamento_repo.inserir_relacionamento(nro_registro, id_modalidade)
                registros_importados += 1
            else:
                if not id_modalidade:
                    print(f"⚠️ Aviso: Modalidade '{nome_modalidade}' não encontrada no banco de dados. Pulando linha {index}.")

        self.relacionamento_repo.conn.commit()
        return registros_importados