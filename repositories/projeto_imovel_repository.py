# repositories/projeto_imovel_repository.py

class ProjetoImovelRepository:
    def __init__(self, conn):
        self.conn = conn

    def inserir_relacionamento(self, nro_registro, id_imovel):
        """Insere uma nova associação entre um projeto e um imóvel."""
        cursor = self.conn.cursor()
        try:
            query = """
                INSERT INTO PROJETO_IMOVEL (NRO_REGISTRO, ID_IMOVEL)
                VALUES (%s, %s)
            """
            cursor.execute(query, (nro_registro, id_imovel))
        except Exception as e:
            print(f"Erro ao inserir relacionamento para NRO_REGISTRO {nro_registro} e ID_IMOVEL {id_imovel}: {e}")
        finally:
            cursor.close()

    def count(self):
        """Conta o total de registros na tabela de relacionamento."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM PROJETO_IMOVEL")
        total = cursor.fetchone()[0]
        cursor.close()
        return total