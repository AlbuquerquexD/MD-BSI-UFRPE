# repositories/projeto_empresa_repository.py

class ProjetoEmpresaRepository:
    def __init__(self, conn):
        """
        Inicializa o repositório com a conexão de banco de dados fornecida.
        """
        self.conn = conn

    def inserir_relacionamento(self, nro_registro, cnpj):
        """
        Insere uma nova associação entre um projeto e uma empresa.
        A coluna ID_PROJETO_EMPRESA é auto-incrementada pelo banco.
        """
        cursor = self.conn.cursor()
        try:
            # A query insere o número de registro do projeto e o CNPJ da empresa associada.
            query = """
                INSERT INTO PROJETO_EMPRESA (PROJETO_NRO_REGISTRO, EMPRESA_CNPJ)
                VALUES (%s, %s)
            """
            cursor.execute(query, (nro_registro, cnpj))
        except Exception as e:
            # Imprime uma mensagem de erro detalhada caso a inserção falhe.
            print(f"Erro ao inserir relacionamento para NRO_REGISTRO {nro_registro} e CNPJ {cnpj}: {e}")
        finally:
            # Garante que o cursor seja fechado após a operação.
            cursor.close()

    def count(self):
        """
        Conta o total de registros na tabela de relacionamento PROJETO_EMPRESA.
        """
        cursor = self.conn.cursor()
        total = 0
        try:
            cursor.execute("SELECT COUNT(*) FROM PROJETO_EMPRESA")
            total = cursor.fetchone()[0]
        except Exception as e:
            print(f"Erro ao contar registros em PROJETO_EMPRESA: {e}")
        finally:
            cursor.close()
            return total