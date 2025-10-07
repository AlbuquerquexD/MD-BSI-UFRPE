import csv
import re
from pathlib import Path


def limpar_cpf_cnpj(valor: str) -> str:
    """
    Remove tudo que não for número.
    Retorna apenas os dígitos numéricos.
    """
    if not valor:
        return ""
    return re.sub(r"\D", "", valor)


def is_cnpj(valor: str) -> bool:
    """
    Verifica se o valor tem 14 dígitos numéricos (CNPJ válido em formato bruto).
    """
    numeros = limpar_cpf_cnpj(valor)
    return len(numeros) == 14


def tratar_csv(caminho_entrada: str, caminho_saida: str, coluna_cnpj: str) -> None:
    """
    Lê o CSV de entrada, limpa a coluna de CNPJ e salva o resultado em um novo CSV.
    Apenas mantém CNPJs válidos (14 dígitos).
    """

    input_path = Path(caminho_entrada)
    output_path = Path(caminho_saida)

    with open(input_path, "r", encoding="utf-8") as infile, open(output_path, "w", encoding="utf-8", newline="") as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        if not fieldnames:
            raise ValueError("CSV sem cabeçalho válido.")

        if coluna_cnpj not in fieldnames:
            raise ValueError(f"A coluna '{coluna_cnpj}' não foi encontrada no arquivo CSV.")

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            valor_original = row[coluna_cnpj]
            numeros = limpar_cpf_cnpj(valor_original)

            # Mantém apenas CNPJs válidos
            if is_cnpj(valor_original):
                row[coluna_cnpj] = numeros
            else:
                # 🔹 Opção 1: descartar linha (CPF)
                # continue

                # 🔹 Opção 2: manter a linha, mas limpar o valor CPF
                row[coluna_cnpj] = ""

            writer.writerow(row)

    print(f"✅ Arquivo tratado salvo em: {output_path}")


# Exemplo de uso:
if __name__ == "__main__":
    tratar_csv(
        caminho_entrada="datasets/base_pmfs.csv",
        caminho_saida="base_pmfs_tratado.csv",
        coluna_cnpj="CPF_CNPJ_DETENTOR"
    )
