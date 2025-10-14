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
            # Dica: você pode definir o tipo da coluna já na leitura
            df = pd.read_csv(csv_path, sep=',', encoding='utf-8', 
                             dtype={'NRO_REGISTRO': 'Int64'})
        except FileNotFoundError:
            print(f"🚨 Arquivo não encontrado: {csv_path}")
            return 0

        # Carrega o mapa de modalidades uma única vez para otimizar
        modalidade_map = self.modalidade_repo.get_all_as_map()
        
        registros_importados = 0
        
        # Ajuste os nomes das colunas se forem diferentes no seu CSV
        coluna_registro = 'NRO_REGISTRO'
        coluna_modalidade = 'MODALIDADE_PMFS'

        # A conversão de tipo já foi feita no read_csv, mas deixo aqui se preferir
        # df[coluna_registro] = df[coluna_registro].astype('Int64')
        
        for index, row in df.iterrows():
            nro_registro = row[coluna_registro]
            nome_modalidade = row[coluna_modalidade]

            # Busca o ID da modalidade no mapa que carregamos
            id_modalidade = modalidade_map.get(nome_modalidade)

            # ▼▼▼ LINHA CORRIGIDA ▼▼▼
            # Verifica explicitamente se nro_registro não é nulo com pd.notna()
            if pd.notna(nro_registro) and id_modalidade:
                self.relacionamento_repo.inserir_relacionamento(nro_registro, id_modalidade)
                registros_importados += 1
            else:
                if not id_modalidade:
                    # Garante que o nome_modalidade não seja nulo antes de imprimir
                    if pd.notna(nome_modalidade):
                        print(f"⚠️ Aviso: Modalidade '{nome_modalidade}' não encontrada no banco de dados. Pulando linha {index}.")
                if pd.isna(nro_registro):
                     print(f"⚠️ Aviso: NRO_REGISTRO ausente na linha {index}. Pulando.")


        self.relacionamento_repo.conn.commit()
        return registros_importados