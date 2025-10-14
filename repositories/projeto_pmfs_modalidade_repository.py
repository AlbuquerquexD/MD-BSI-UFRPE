from typing import Any


class ProjetoPmfsModalidadeRepository:
    def __init__(self, connection: Any):
        self.conn = connection

    def inserir_relacionamento(self, nro_registro, id_modalidade):
        """Insere uma nova associação entre um projeto e uma modalidade."""
        cursor = self.conn.cursor()
        try:
            query = """
                INSERT INTO PROJETO_PMFS_MODALIDADE (NRO_REGISTRO, ID_PMFS_MODALIDADE)
                VALUES (%s, %s)
            """
            cursor.execute(query, (nro_registro, id_modalidade))
        except Exception as e:
            # Opcional: tratar exceções como registros duplicados, se necessário
            print(
                f"Erro ao inserir relacionamento para NRO_REGISTRO(O cpf ainda ficou na base, n sei como. releva) {nro_registro}: {e}"
            )
        finally:
            cursor.close()

    def count(self):
        """Conta o total de registros na tabela de relacionamento."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM PROJETO_PMFS_MODALIDADE")
        total = cursor.fetchone()[0]
        cursor.close()
        return total
