"""
Script de Análise Exploratória da Qualidade da Base PMFS Amazônia Legal

Este script NÃO realiza limpeza. 
Ele gera métricas e relatórios sobre a base para identificar possíveis problemas.
"""

import pandas as pd

# === Carregar base original ===
df = pd.read_csv("data\pmfsAmazoniaLegal.csv", sep=";", encoding="utf-8")

# === 1. Resumo geral ===
print("===== RESUMO GERAL =====")
print(f"Linhas: {df.shape[0]}")
print(f"Colunas: {df.shape[1]}")
print("\nTipos de dados por coluna:")
print(df.dtypes.value_counts())

# === 2. Nulos ===
print("\n===== ANÁLISE DE NULOS =====")
nulos = df.isna().sum().sort_values(ascending=False)
print(nulos[nulos > 0])
print(f"\nTotal de colunas com nulos: {(nulos > 0).sum()}")
print(f"Colunas com mais de 50% de nulos: {(nulos > (len(df) * 0.5)).sum()}")

# Calcular quantas colunas restariam se removêssemos as com mais de 50% nulos
colunas_com_muitos_nulos = nulos > (len(df) * 0.5)
colunas_restantes = df.shape[1] - colunas_com_muitos_nulos.sum()
print(f"Colunas que restariam após remover as com >50% nulos: {colunas_restantes}")
print(f"Percentual de colunas mantidas: {(colunas_restantes / df.shape[1]) * 100:.1f}%")

# Mostrar quais colunas seriam removidas e quais seriam mantidas
colunas_para_remover = nulos[colunas_com_muitos_nulos].index.tolist()
colunas_para_manter = nulos[~colunas_com_muitos_nulos].index.tolist()

print(f"\nColunas que SERIAM REMOVIDAS ({len(colunas_para_remover)}):")
for col in colunas_para_remover:
    percentual_nulos = (nulos[col] / len(df)) * 100
    print(f"  - {col}: {nulos[col]} nulos ({percentual_nulos:.1f}%)")

print(f"\nColunas que SERIAM MANTIDAS ({len(colunas_para_manter)}):")
for col in colunas_para_manter:
    percentual_nulos = (nulos[col] / len(df)) * 100
    print(f"  - {col}: {nulos[col]} nulos ({percentual_nulos:.1f}%)")

# === 3. Duplicatas ===
print("\n===== ANÁLISE DE DUPLICATAS =====")
print(f"Duplicatas totais: {df.duplicated().sum()}")

if "NRO_REGISTRO" in df.columns:
    print(f"Duplicatas em NRO_REGISTRO: {df.duplicated(subset=['NRO_REGISTRO']).sum()}")
if set(["NRO_AUTORIZACAO", "UF", "MUNICIPIO"]).issubset(df.columns):
    print(f"Duplicatas em (NRO_AUTORIZACAO, UF, MUNICIPIO): {df.duplicated(subset=['NRO_AUTORIZACAO','UF','MUNICIPIO']).sum()}")

# === 4. Valores únicos ===
print("\n===== VALORES ÚNICOS POR COLUNA =====")
print(df.nunique().sort_values(ascending=False))


# === 6. Distribuição de categorias ===
print("\n===== DISTRIBUIÇÃO DE CATEGORIAS =====")
for col in ["MUNICIPIO", "ORGAO_AMBIENTAL_RESP_ANALISE", "ATIVIDADE", "TIPO_DE_EMPREENDIMENTO", "SITUACAO"]:
    if col in df.columns:
        print(f"\n{col} - {df[col].nunique()} categorias únicas")
        print(df[col].value_counts().head(10))

# === 5. Valores inconsistentes ===
print("\n===== VALORES INCONSISTENTES =====")
for col in df.columns:
    # Identificar valores inconsistentes
    valores_inconsistentes = ["NA", "N/A", "NULL", "NULO", "NONE", " ", ""]
    mask = df[col].astype(str).str.upper().isin([v.upper() for v in valores_inconsistentes])
    
    if mask.sum() > 0:
        print(f"\n{col}: {mask.sum()} valores possivelmente inválidos")
        # Mostrar os valores únicos inconsistentes encontrados
        valores_unicos = df.loc[mask, col].unique()
        print(f"Valores encontrados: {valores_unicos}")

print("\n📊 Análise exploratória concluída. Nenhum dado foi alterado.")
