"""
Script de Limpeza da Base PMFS Amazônia Legal

Critérios de limpeza definidos pelo grupo:
1. Remover colunas com mais de 70% de valores nulos
2. Manter CNPJs, mas tratar CPFs (colocar null nos CPFs)
3. Remover linhas onde os 4673 nulos se repetem (linhas com muitos nulos)
4. Gerar relatório de limpeza
5. Salvar base limpa
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime

def is_cpf(value):
    """Verifica se um valor é um CPF (11 dígitos) ou CNPJ (14 dígitos)"""
    if pd.isna(value):
        return None
    
    # Remove pontos, traços e espaços
    clean_value = str(value).replace('.', '').replace('-', '').replace('/', '').replace(' ', '')
    
    # Verifica se contém apenas dígitos
    if not clean_value.isdigit():
        return None
    
    # CPF tem 11 dígitos, CNPJ tem 14 dígitos
    if len(clean_value) == 11:
        return 'CPF'
    elif len(clean_value) == 14:
        return 'CNPJ'
    else:
        return None

def preencher_nulos(df):
    """Preenche valores nulos usando diferentes estratégias"""
    
    print("\n5. Preenchendo valores nulos...")
    
    # Analisar tipos de dados
    colunas_numericas = df.select_dtypes(include=[np.number]).columns.tolist()
    colunas_categoricas = df.select_dtypes(include=['object']).columns.tolist()
    
    print(f"   Colunas numéricas: {len(colunas_numericas)}")
    print(f"   Colunas categóricas: {len(colunas_categoricas)}")
    
    # Calcular percentual de nulos
    nulos_totais = df.isna().sum().sum()
    percentual_nulos = (nulos_totais / (df.shape[0] * df.shape[1])) * 100
    
    print(f"   Total de nulos: {nulos_totais} ({percentual_nulos:.2f}% da base)")
    
    if nulos_totais == 0:
        print("   ✅ Base já está sem nulos!")
        return df
    
    # Estratégias específicas por coluna
    print("   Aplicando estratégias específicas por coluna...")
    
    for col in df.columns:
        if df[col].isna().sum() > 0:
            nulos_antes = df[col].isna().sum()
            
            if col in colunas_numericas:
                # Para dados numéricos, usar mediana (mais robusta)
                df[col] = df[col].fillna(df[col].median())
                print(f"     {col}: {nulos_antes} nulos → mediana")
                
            else:
                # Para dados categóricos, usar moda ou "Desconhecido"
                moda = df[col].mode()
                if len(moda) > 0:
                    df[col] = df[col].fillna(moda[0])
                    print(f"     {col}: {nulos_antes} nulos → moda '{moda[0]}'")
                else:
                    df[col] = df[col].fillna("Desconhecido")
                    print(f"     {col}: {nulos_antes} nulos → 'Desconhecido'")
    
    # Verificar se ainda há nulos
    nulos_restantes = df.isna().sum().sum()
    if nulos_restantes > 0:
        print(f"   ⚠️  Ainda restam {nulos_restantes} nulos. Aplicando preenchimento final...")
        
        # Preenchimento final para qualquer nulo restante
        for col in df.columns:
            if df[col].isna().sum() > 0:
                if col in colunas_numericas:
                    df[col] = df[col].fillna(df[col].median())
                else:
                    df[col] = df[col].fillna("Desconhecido")
    
    nulos_finais = df.isna().sum().sum()
    print(f"   ✅ Preenchimento concluído. Nulos restantes: {nulos_finais}")
    
    return df

def limpar_base():
    """Função principal de limpeza da base"""
    
    print("=== INICIANDO LIMPEZA DA BASE PMFS AMAZÔNIA LEGAL ===\n")
    
    # Carregar base original
    print("1. Carregando base original...")
    df_original = pd.read_csv("data\pmfsAmazoniaLegal.csv", sep=";", encoding="utf-8")
    print(f"   Base original: {df_original.shape[0]} linhas x {df_original.shape[1]} colunas")
    
    # Criar cópia para limpeza
    df = df_original.copy()
    
    # === RELATÓRIO INICIAL ===
    relatorio = {
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'base_original': {
            'linhas': df.shape[0],
            'colunas': df.shape[1]
        },
        'etapas_limpeza': []
    }
    
    # === ETAPA 1: REMOVER COLUNAS COM MAIS DE 70% NULOS ===
    print("\n2. Removendo colunas com mais de 70% de valores nulos...")
    
    # Calcular percentual de nulos por coluna
    nulos_por_coluna = df.isna().sum()
    percentual_nulos = (nulos_por_coluna / len(df)) * 100
    
    # Identificar colunas para remover (>70% nulos)
    colunas_para_remover = percentual_nulos[percentual_nulos > 70].index.tolist()
    
    print(f"   Colunas identificadas para remoção ({len(colunas_para_remover)}):")
    for col in colunas_para_remover:
        print(f"   - {col}: {percentual_nulos[col]:.1f}% nulos")
    
    # Remover colunas
    df = df.drop(columns=colunas_para_remover)
    
    relatorio['etapas_limpeza'].append({
        'etapa': 'Remoção de colunas com >70% nulos',
        'colunas_removidas': len(colunas_para_remover),
        'colunas_restantes': df.shape[1],
        'detalhes': colunas_para_remover
    })
    
    print(f"   Após remoção: {df.shape[0]} linhas x {df.shape[1]} colunas")
    
    # === ETAPA 2: TRATAR CPF/CNPJ ===
    print("\n3. Tratando CPF/CNPJ na coluna CPF_CNPJ_DETENTOR...")
    
    if 'CPF_CNPJ_DETENTOR' in df.columns:
        # Analisar tipos de documentos
        tipos_documento = df['CPF_CNPJ_DETENTOR'].apply(is_cpf)
        contagem_tipos = tipos_documento.value_counts()
        
        print(f"   Análise dos documentos:")
        print(f"   - CPFs: {contagem_tipos.get('CPF', 0)}")
        print(f"   - CNPJs: {contagem_tipos.get('CNPJ', 0)}")
        print(f"   - Valores nulos/inválidos: {contagem_tipos.get(None, 0)}")
        
        # Substituir CPFs por null, manter CNPJs
        mask_cpf = tipos_documento == 'CPF'
        df.loc[mask_cpf, 'CPF_CNPJ_DETENTOR'] = np.nan
        
        print(f"   CPFs convertidos para null: {mask_cpf.sum()}")
        
        relatorio['etapas_limpeza'].append({
            'etapa': 'Tratamento CPF/CNPJ',
            'cpfs_convertidos_null': int(mask_cpf.sum()),
            'cnpjs_mantidos': int(contagem_tipos.get('CNPJ', 0))
        })
    
    # === ETAPA 3: REMOVER LINHAS COM MUITOS NULOS (4673 padrão) ===
    print("\n4. Removendo linhas com muitos valores nulos...")
    
    # Calcular número de nulos por linha
    nulos_por_linha = df.isna().sum(axis=1)
    
    # Identificar o padrão de 4673 nulos (ou similar)
    # Vamos remover linhas que têm mais de 50% de valores nulos
    threshold_nulos = len(df.columns) * 0.5
    linhas_com_muitos_nulos = nulos_por_linha > threshold_nulos
    
    print(f"   Linhas com mais de {threshold_nulos:.0f} nulos: {linhas_com_muitos_nulos.sum()}")
    print(f"   Percentual de linhas a remover: {(linhas_com_muitos_nulos.sum() / len(df)) * 100:.1f}%")
    
    # Remover linhas
    df_antes_remocao_linhas = df.shape[0]
    df = df[~linhas_com_muitos_nulos]
    
    relatorio['etapas_limpeza'].append({
        'etapa': 'Remoção de linhas com muitos nulos',
        'linhas_removidas': int(df_antes_remocao_linhas - df.shape[0]),
        'linhas_restantes': df.shape[0],
        'threshold_nulos': threshold_nulos
    })
    
    # Adicionar etapa de preenchimento de nulos
    nulos_antes_preenchimento = df.isna().sum().sum()
    relatorio['etapas_limpeza'].append({
        'etapa': 'Preenchimento de valores nulos',
        'nulos_antes': int(nulos_antes_preenchimento),
        'nulos_depois': 0,  # Será atualizado após o preenchimento
        'nulos_preenchidos': int(nulos_antes_preenchimento)
    })
    
    print(f"   Após remoção de linhas: {df.shape[0]} linhas x {df.shape[1]} colunas")
    
    # === ETAPA 4: PREENCHIMENTO DE NULOS ===
    df = preencher_nulos(df)
    
    # === ETAPA 5: LIMPEZA FINAL ===
    print("\n6. Limpeza final...")
    
    # Remover linhas completamente vazias (se houver)
    linhas_vazias = df.isna().all(axis=1)
    if linhas_vazias.sum() > 0:
        df = df[~linhas_vazias]
        print(f"   Linhas completamente vazias removidas: {linhas_vazias.sum()}")
    
    # === RELATÓRIO FINAL ===
    relatorio['base_final'] = {
        'linhas': df.shape[0],
        'colunas': df.shape[1]
    }
    
    relatorio['resumo'] = {
        'linhas_removidas': relatorio['base_original']['linhas'] - relatorio['base_final']['linhas'],
        'colunas_removidas': relatorio['base_original']['colunas'] - relatorio['base_final']['colunas'],
        'percentual_linhas_mantidas': (relatorio['base_final']['linhas'] / relatorio['base_original']['linhas']) * 100,
        'percentual_colunas_mantidas': (relatorio['base_final']['colunas'] / relatorio['base_original']['colunas']) * 100
    }
    
    print(f"\n=== RESUMO DA LIMPEZA ===")
    print(f"Base original: {relatorio['base_original']['linhas']} linhas x {relatorio['base_original']['colunas']} colunas")
    print(f"Base final: {relatorio['base_final']['linhas']} linhas x {relatorio['base_final']['colunas']} colunas")
    print(f"Linhas removidas: {relatorio['resumo']['linhas_removidas']} ({(100 - relatorio['resumo']['percentual_linhas_mantidas']):.1f}%)")
    print(f"Colunas removidas: {relatorio['resumo']['colunas_removidas']} ({(100 - relatorio['resumo']['percentual_colunas_mantidas']):.1f}%)")
    print(f"Nulos preenchidos: {df.isna().sum().sum()} (base final sem nulos!)")
    
    # === SALVAR BASE LIMPA ===
    print(f"\n7. Salvando base limpa...")
   
    pasta_destino = "data"  
    os.makedirs(pasta_destino, exist_ok=True)

    nome_arquivo_limpo = "pmfsAmazoniaLegal_LIMPA.csv"
    caminho_completo = os.path.join(pasta_destino, nome_arquivo_limpo)

    df.to_csv(caminho_completo, sep=";", encoding="utf-8", index=False)
    print(f"   Base limpa salva em: {caminho_completo}")
    
    # === GERAR RELATÓRIO DETALHADO ===
    print(f"\n8. Gerando relatório detalhado...")
    gerar_relatorio_detalhado(relatorio, df)
    
    return df, relatorio

def gerar_relatorio_detalhado(relatorio, df_limpo):
    """Gera relatório detalhado em arquivo de texto"""
    pasta_destino = "reports"
    os.makedirs(pasta_destino, exist_ok=True)
    
    nome_relatorio = f"relatorio_limpeza_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    caminho_completo = os.path.join(pasta_destino, nome_relatorio)
    
    with open(caminho_completo, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("RELATÓRIO DE LIMPEZA - BASE PMFS AMAZÔNIA LEGAL\n")
        f.write("=" * 80 + "\n")
        f.write(f"Data/Hora: {relatorio['timestamp']}\n\n")
        
        # Resumo geral
        f.write("RESUMO GERAL\n")
        f.write("-" * 40 + "\n")
        f.write(f"Base original: {relatorio['base_original']['linhas']} linhas x {relatorio['base_original']['colunas']} colunas\n")
        f.write(f"Base final: {relatorio['base_final']['linhas']} linhas x {relatorio['base_final']['colunas']} colunas\n")
        f.write(f"Linhas removidas: {relatorio['resumo']['linhas_removidas']} ({(100 - relatorio['resumo']['percentual_linhas_mantidas']):.1f}%)\n")
        f.write(f"Colunas removidas: {relatorio['resumo']['colunas_removidas']} ({(100 - relatorio['resumo']['percentual_colunas_mantidas']):.1f}%)\n\n")
        
        # Detalhes das etapas
        f.write("ETAPAS DE LIMPEZA\n")
        f.write("-" * 40 + "\n")
        for i, etapa in enumerate(relatorio['etapas_limpeza'], 1):
            f.write(f"{i}. {etapa['etapa']}\n")
            for key, value in etapa.items():
                if key != 'etapa':
                    f.write(f"   - {key}: {value}\n")
            f.write("\n")
        
        # Análise da base final
        f.write("ANÁLISE DA BASE FINAL\n")
        f.write("-" * 40 + "\n")
        f.write(f"Total de linhas: {df_limpo.shape[0]}\n")
        f.write(f"Total de colunas: {df_limpo.shape[1]}\n")
        
        # Nulos na base final
        nulos_finais = df_limpo.isna().sum()
        f.write(f"Colunas com nulos: {(nulos_finais > 0).sum()}\n")
        f.write(f"Colunas sem nulos: {(nulos_finais == 0).sum()}\n")
        f.write(f"Total de nulos na base final: {nulos_finais.sum()}\n\n")
        
        # Top 10 colunas com mais nulos
        f.write("TOP 10 COLUNAS COM MAIS NULOS (BASE FINAL)\n")
        f.write("-" * 40 + "\n")
        top_nulos = nulos_finais[nulos_finais > 0].sort_values(ascending=False).head(10)
        for col, nulos in top_nulos.items():
            percentual = (nulos / len(df_limpo)) * 100
            f.write(f"{col}: {nulos} nulos ({percentual:.1f}%)\n")
        
        f.write("\n" + "=" * 80 + "\n")
        f.write("RELATÓRIO GERADO AUTOMATICAMENTE\n")
        f.write("=" * 80 + "\n")
    
    print(f"   Relatório salvo como: {nome_relatorio}")

if __name__ == "__main__":
    df_limpo, relatorio = limpar_base()
    print(f"\n✅ Limpeza concluída com sucesso!")
    print(f"📊 Base limpa: {df_limpo.shape[0]} linhas x {df_limpo.shape[1]} colunas")
