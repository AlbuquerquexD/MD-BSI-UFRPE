# populacao_tabelas/projeto_empresa_insert.py

import pandas as pd
import re  # Importado para ajudar na limpeza do campo de CNPJ

class ProjetoEmpresaService:
    def __init__(self, relacionamento_repo):
        """
        Inicializa o serviço com o repositório de relacionamento.
        """
        self.relacionamento_repo = relacionamento_repo

    def carregar_relacionamento(self, csv_path):
        """
        Lê um arquivo CSV, extrai os dados de projeto e empresa, e os insere no banco.
        """
        try:
            # Lê o CSV e remove espaços extras dos nomes das colunas
            df = pd.read_csv(csv_path, sep=',', encoding='utf-8')
            df.columns = df.columns.str.strip()
        except FileNotFoundError:
            print(f"🚨 Arquivo não encontrado: {csv_path}")
            return 0
        
        registros_importados = 0
        
        # Define os nomes das colunas que serão lidas do CSV
        coluna_registro = 'NRO_REGISTRO'
        coluna_cnpj = 'CPF_CNPJ_DETENTOR'

        # Verifica se as colunas necessárias existem no arquivo
        if coluna_registro not in df.columns or coluna_cnpj not in df.columns:
            print(f"🚨 Colunas '{coluna_registro}' e/ou '{coluna_cnpj}' não encontradas no CSV.")
            return 0

        # Converte a coluna de registro para Int64 para aceitar valores nulos
        df[coluna_registro] = pd.to_numeric(df[coluna_registro], errors='coerce').astype('Int64')

        # Remove linhas duplicadas baseadas no par (projeto, empresa) para evitar erros
        df.drop_duplicates(subset=[coluna_registro, coluna_cnpj], inplace=True)

        for index, row in df.iterrows():
            nro_registro = row[coluna_registro]
            cnpj_original = row[coluna_cnpj]

            # Pula a linha se alguma das informações essenciais estiver faltando
            if pd.isna(nro_registro) or pd.isna(cnpj_original):
                continue
            
            # Limpa a string do CNPJ/CPF, removendo pontos, traços e barras
            # Ex: '14.773.001/0001-83' se torna '14773001000183'
            cnpj_limpo = re.sub(r'\D', '', str(cnpj_original))

            # Pula se o CNPJ ficar vazio após a limpeza
            if not cnpj_limpo:
                continue

            # Usa o repositório para inserir o relacionamento no banco
            self.relacionamento_repo.inserir_relacionamento(nro_registro, cnpj_limpo)
            registros_importados += 1

        # Confirma todas as transações de uma vez no banco de dados
        self.relacionamento_repo.conn.commit()
        
        return registros_importados