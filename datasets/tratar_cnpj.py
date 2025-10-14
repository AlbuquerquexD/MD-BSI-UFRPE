import csv
import re
from pathlib import Path

def limpar_cpf_cnpj(valor: str) -> str:
    """
    Remove todos os caracteres que não são dígitos numéricos.
    Retorna uma string vazia se a entrada não for uma string.
    """
    if not isinstance(valor, str):
        return ""
    return re.sub(r"\D", "", valor)


def is_cnpj_valido(valor: str) -> bool:
    """
    Verifica se o valor, após a limpeza, consiste em exatamente 14 dígitos.
    Isso lida com CNPJs formatados, CPFs, valores em branco ou qualquer outro texto.
    """
    numeros = limpar_cpf_cnpj(valor)
    return len(numeros) == 14


def filtrar_csv_por_cnpj(caminho_entrada: str, caminho_saida: str, coluna_cnpj: str) -> None:
    """
    Lê um arquivo CSV e cria uma nova versão contendo apenas as linhas
    em que a coluna especificada contém um CNPJ válido.
    """
    input_path = Path(caminho_entrada)
    output_path = Path(caminho_saida)

    try:
        with open(input_path, "r", encoding="utf-8") as infile, \
             open(output_path, "w", encoding="utf-8", newline="") as outfile:
            
            reader = csv.DictReader(infile)
            fieldnames = reader.fieldnames

            if not fieldnames:
                print("🚨 ERRO: O arquivo CSV de entrada está vazio ou não possui cabeçalho.")
                return

            if coluna_cnpj not in fieldnames:
                print(f"🚨 ERRO: A coluna '{coluna_cnpj}' não foi encontrada no arquivo de entrada.")
                return

            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            registros_mantidos = 0
            registros_removidos = 0

            for row in reader:
                valor_celula = row[coluna_cnpj]
                if is_cnpj_valido(valor_celula):
                    row[coluna_cnpj] = limpar_cpf_cnpj(valor_celula)
                    writer.writerow(row)
                    registros_mantidos += 1
                else:
                    registros_removidos += 1
            
        print(f"✅ Filtro concluído! Arquivo salvo em: {output_path}")
        print(f"📊 Linhas mantidas (com CNPJ válido): {registros_mantidos}")
        print(f"🗑️ Linhas removidas (sem CNPJ ou com valor inválido): {registros_removidos}")

    except FileNotFoundError:
        print(f"🚨 ERRO: O arquivo de entrada '{caminho_entrada}' não foi encontrado.")
    except Exception as e:
        print(f"🚨 Ocorreu um erro inesperado: {e}")


# --- Como usar o script ---
if __name__ == "__main__":
    filtrar_csv_por_cnpj(
        caminho_entrada="datasets/base_pmfs.csv",
        caminho_saida="datasets/base_pmfs_cpf_retirado.csv", 
        coluna_cnpj="CPF_CNPJ_DETENTOR"
    )