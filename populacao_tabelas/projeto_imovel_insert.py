# populacao_tabelas/projeto_imovel_insert.py

import pandas as pd

class ProjetoImovelService:
    def __init__(self, relacionamento_repo, area_licitada_repo):
        self.relacionamento_repo = relacionamento_repo
        self.area_licitada_repo = area_licitada_repo

    def carregar_relacionamento(self, csv_path):
        try:
            df = pd.read_csv(csv_path, sep=',', encoding='utf-8')
            df.columns = df.columns.str.strip()
        except FileNotFoundError:
            print(f"🚨 Arquivo não encontrado: {csv_path}")
            return 0

        # Carrega o mapa de imóveis (CAR -> ID) uma única vez
        imovel_map = self.area_licitada_repo.get_all_as_map()
        
        registros_importados = 0
        
        # IMPORTANTE: Verifique se esses são os nomes corretos das colunas no seu CSV!
        coluna_registro = 'NRO_REGISTRO'
        coluna_car = 'NRO_CAR_IMOVEL_RURAL'

        # Converte a coluna de registro para Int64 para aceitar nulos e ser inteiro
        df[coluna_registro] = df[coluna_registro].astype('Int64')

        for index, row in df.iterrows():
            nro_registro = row[coluna_registro]
            nro_car = row[coluna_car]

            # Pula a linha se o registro ou o CAR estiverem vazios
            if pd.isna(nro_registro) or pd.isna(nro_car):
                continue
            
            # Limpa a string do CAR para garantir correspondência
            nro_car = str(nro_car).strip()

            # Busca o ID do imóvel no mapa que carregamos
            id_imovel = imovel_map.get(nro_car)

            if nro_registro and id_imovel:
                self.relacionamento_repo.inserir_relacionamento(nro_registro, id_imovel)
                registros_importados += 1
            else:
                # O aviso agora só será exibido para CARs que existem no CSV mas não no banco
                if not id_imovel and nro_car:
                    print(f"⚠️ Aviso: Imóvel com CAR '{nro_car}' não encontrado no banco. Pulando linha {index}.")

        self.relacionamento_repo.conn.commit()
        return registros_importados