import pandas as pd
import numpy as np
from datetime import datetime
import holidays
import mysql.connector
from mysql.connector import Error

# ========================================
# CONFIGURAÇÕES
# ========================================
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',  # preencha se necessário
}

DW_DATABASE = 'DW_PMFS_AMAZONAS'
BATCH_SIZE = 1000

# Arquivos de dados reais
BASE_PMFS_CSV = 'datasets/base_pmfs.csv'
BASE_EMPRESA_CSV = 'datasets/base_empresa.csv'


def get_season(month, day):
    if (month == 12 and day >= 21) or (month <= 2) or (month == 3 and day < 20):
        return 'Verão'
    elif (month == 3 and day >= 20) or (month <= 5) or (month == 6 and day < 21):
        return 'Outono'
    elif (month == 6 and day >= 21) or (month <= 8) or (month == 9 and day < 23):
        return 'Inverno'
    else:
        return 'Primavera'


def safe_date_convert(value):
    if pd.isnull(value):
        return None
    if hasattr(value, 'date'):
        return value.date()
    return value


def safe_datetime_convert(value):
    if pd.isnull(value):
        return None
    if hasattr(value, 'to_pydatetime'):
        return value.to_pydatetime()
    return value


def extract_dates_from_files():
    """
    Extrai datas dos CSVs e cria um mapa date -> campos específicos (data_inicio_atividade_empresa,
    data_tramite_status, ultima_atualizacao_relatorio_status, ciclo_corte).
    Retorna (sorted_dates, date_map)
    """
    date_map = {}

    # Ler base_pmfs.csv e agregar eventos por data (trâmites, últimas atualizações e ciclo_corte)
    try:
        pmfs = pd.read_csv(BASE_PMFS_CSV, dtype=str, low_memory=False)
    except FileNotFoundError:
        print(f"Arquivo não encontrado: {BASE_PMFS_CSV}")
        pmfs = pd.DataFrame()

    if not pmfs.empty:
        # Normalizar e parsear colunas de data/numéricas
        if 'DATA_DO_TRAMITE' in pmfs.columns:
            pmfs['__tramite_dt'] = pd.to_datetime(pmfs['DATA_DO_TRAMITE'], errors='coerce')
            pmfs['__tramite_date'] = pmfs['__tramite_dt'].dt.date
        else:
            pmfs['__tramite_dt'] = pd.NaT
            pmfs['__tramite_date'] = None

        if 'ULTIMA_ATUALIZACAO_RELATORIO' in pmfs.columns:
            pmfs['__ultima_dt'] = pd.to_datetime(pmfs['ULTIMA_ATUALIZACAO_RELATORIO'], errors='coerce')
            pmfs['__ultima_date'] = pmfs['__ultima_dt'].dt.date
        else:
            pmfs['__ultima_dt'] = pd.NaT
            pmfs['__ultima_date'] = None

        if 'DATA_DE_EMISSAO' in pmfs.columns:
            pmfs['__emissao_dt'] = pd.to_datetime(pmfs['DATA_DE_EMISSAO'], errors='coerce')
            pmfs['__emissao_date'] = pmfs['__emissao_dt'].dt.date
        else:
            pmfs['__emissao_dt'] = pd.NaT
            pmfs['__emissao_date'] = None

        # ciclo_corte numérico (substituir vírgula por ponto)
        if 'CICLO_CORTE' in pmfs.columns:
            pmfs['__ciclo_num'] = pd.to_numeric(pmfs['CICLO_CORTE'].astype(str).str.replace(',', '.', regex=False), errors='coerce')
        else:
            pmfs['__ciclo_num'] = np.nan

        # Coletar todas as datas únicas de todos os eventos PMFS
        all_pmfs_dates = set()
        if '__tramite_date' in pmfs.columns:
            all_pmfs_dates.update(pmfs['__tramite_date'].dropna().unique())
        if '__ultima_date' in pmfs.columns:
            all_pmfs_dates.update(pmfs['__ultima_date'].dropna().unique())
        if '__emissao_date' in pmfs.columns:
            all_pmfs_dates.update(pmfs['__emissao_date'].dropna().unique())

        # Para cada data, pegar o primeiro registro válido e extrair todos os campos
        for date_val in all_pmfs_dates:
            if pd.isna(date_val):
                continue
            
            entry = date_map.setdefault(date_val, {})
            
            # Buscar linha com DATA_DO_TRAMITE = date_val
            tramite_rows = pmfs[pmfs['__tramite_date'] == date_val]
            if not tramite_rows.empty:
                first_row = tramite_rows.iloc[0]
                entry['data_tramite_status'] = date_val
                # Pegar ciclo_corte da primeira linha válida (média se houver múltiplas)
                ciclo_vals = tramite_rows['__ciclo_num'].dropna()
                if not ciclo_vals.empty:
                    entry['ciclo_corte'] = float(ciclo_vals.mean())
            
            # Buscar linha com ULTIMA_ATUALIZACAO_RELATORIO = date_val
            ultima_rows = pmfs[pmfs['__ultima_date'] == date_val]
            if not ultima_rows.empty:
                entry['ultima_atualizacao_relatorio_status'] = date_val

    # Ler base_empresa.csv e extrair DATA_INICIO_ATIVIDADE
    try:
        empresa = pd.read_csv(BASE_EMPRESA_CSV, dtype=str, low_memory=False)
    except FileNotFoundError:
        print(f"Arquivo não encontrado: {BASE_EMPRESA_CSV}")
        empresa = pd.DataFrame()

    if not empresa.empty and 'DATA_INICIO_ATIVIDADE' in empresa.columns:
        # tentar formato YYYYMMDD primeiro
        parsed = pd.to_datetime(empresa['DATA_INICIO_ATIVIDADE'], errors='coerce', format='%Y%m%d')
        parsed2 = pd.to_datetime(empresa['DATA_INICIO_ATIVIDADE'], errors='coerce')
        final = parsed.fillna(parsed2)
        for d in final.dropna().dt.date.unique():
            entry = date_map.setdefault(d, {})
            entry.setdefault('data_inicio_atividade_empresa', d)

    # construir lista ordenada de datas únicas
    dates = sorted(date_map.keys())
    
    # PASSO 1: Forward-fill (propagação para frente - do passado para o futuro)
    last_inicio = None
    last_tramite = None
    last_ultima = None
    last_ciclo = None
    
    for d in dates:
        mapped = date_map[d]
        
        # Atualizar últimos valores válidos
        if mapped.get('data_inicio_atividade_empresa') is not None:
            last_inicio = mapped['data_inicio_atividade_empresa']
        if mapped.get('data_tramite_status') is not None:
            last_tramite = mapped['data_tramite_status']
        if mapped.get('ultima_atualizacao_relatorio_status') is not None:
            last_ultima = mapped['ultima_atualizacao_relatorio_status']
        if mapped.get('ciclo_corte') is not None:
            last_ciclo = mapped['ciclo_corte']
        
        # Preencher campos NULL com último valor válido
        if mapped.get('data_inicio_atividade_empresa') is None and last_inicio is not None:
            mapped['data_inicio_atividade_empresa'] = last_inicio
        if mapped.get('data_tramite_status') is None and last_tramite is not None:
            mapped['data_tramite_status'] = last_tramite
        if mapped.get('ultima_atualizacao_relatorio_status') is None and last_ultima is not None:
            mapped['ultima_atualizacao_relatorio_status'] = last_ultima
        if mapped.get('ciclo_corte') is None and last_ciclo is not None:
            mapped['ciclo_corte'] = last_ciclo
    
    # PASSO 2: Backward-fill (propagação para trás - do futuro para o passado)
    # Isso preenche as datas iniciais que ficaram NULL no forward-fill
    next_inicio = None
    next_tramite = None
    next_ultima = None
    next_ciclo = None
    
    for d in reversed(dates):
        mapped = date_map[d]
        
        # Atualizar próximos valores válidos
        if mapped.get('data_inicio_atividade_empresa') is not None:
            next_inicio = mapped['data_inicio_atividade_empresa']
        if mapped.get('data_tramite_status') is not None:
            next_tramite = mapped['data_tramite_status']
        if mapped.get('ultima_atualizacao_relatorio_status') is not None:
            next_ultima = mapped['ultima_atualizacao_relatorio_status']
        if mapped.get('ciclo_corte') is not None:
            next_ciclo = mapped['ciclo_corte']
        
        # Preencher campos NULL com próximo valor válido
        if mapped.get('data_inicio_atividade_empresa') is None and next_inicio is not None:
            mapped['data_inicio_atividade_empresa'] = next_inicio
        if mapped.get('data_tramite_status') is None and next_tramite is not None:
            mapped['data_tramite_status'] = next_tramite
        if mapped.get('ultima_atualizacao_relatorio_status') is None and next_ultima is not None:
            mapped['ultima_atualizacao_relatorio_status'] = next_ultima
        if mapped.get('ciclo_corte') is None and next_ciclo is not None:
            mapped['ciclo_corte'] = next_ciclo
    
    return dates, date_map


def build_rows_from_dates(dates, starting_sk, date_map=None):
    rows = []
    sk = starting_sk
    br_holidays = holidays.Brazil(years=range(1900, datetime.now().year + 2))

    for d in dates:
        # d is a datetime.date
        dia = d.day
        mes = d.month
        ano = d.year
        quartil = ( (mes - 1) // 3 ) + 1
        semestre = 1 if mes <= 6 else 2
        semana_do_ano = pd.Timestamp(d).isocalendar().week
        dia_da_semana = pd.Timestamp(d).dayofweek + 1
        eh_final_de_semana = 1 if dia_da_semana in (6, 7) else 0
        estacao_ano = get_season(mes, dia)
        eh_feriado = 1 if pd.Timestamp(d) in br_holidays else 0
        eh_dia_util = 1 if (eh_final_de_semana == 0 and eh_feriado == 0) else 0
        eh_ano_eleitoral, tipo_eleicao = (0, 0)  # não inferimos por arquivo aqui
        eh_periodo_pandemico = 1 if (pd.Timestamp('2020-03-11').date() <= d <= pd.Timestamp('2023-05-05').date()) else 0

        date_from = datetime.now()
        date_to = datetime(2099, 12, 31, 23, 59, 59)

        # pegar valores mapeados (se existirem) para esta data
        mapped = (date_map.get(d) if date_map else None) or {}

        # garantir tipos compatíveis para o driver MySQL
        inicio_val = safe_date_convert(mapped.get('data_inicio_atividade_empresa'))
        tramite_val = safe_date_convert(mapped.get('data_tramite_status'))
        ultima_val = safe_date_convert(mapped.get('ultima_atualizacao_relatorio_status'))
        ciclo_val = mapped.get('ciclo_corte')
        if ciclo_val is not None:
            try:
                ciclo_val = float(ciclo_val)
            except Exception:
                ciclo_val = None

        row = (
            int(sk),
            d,
            int(dia),
            int(mes),
            int(quartil),
            int(semestre),
            int(ano),
            int(semana_do_ano),
            int(dia_da_semana),
            int(eh_final_de_semana),
            int(eh_dia_util),
            int(eh_ano_eleitoral),
            int(tipo_eleicao),
            estacao_ano,
            int(eh_periodo_pandemico),
            inicio_val,  # data_inicio_atividade_empresa
            tramite_val,  # data_tramite_status
            ultima_val,  # ultima_atualizacao_relatorio_status
            date_from,
            date_to,
            1,     # version
            ciclo_val  # ciclo_corte
        )

        rows.append(row)
        sk += 1

    return rows


def main():
    print('Criando/populando dim_data com datas extraídas de CSVs reais...')

    dates, date_map = extract_dates_from_files()
    if not dates:
        print('Nenhuma data encontrada nos arquivos configurados. Abortando.')
        return

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Criar database se necessário e usar
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DW_DATABASE}")
        cursor.execute(f"USE {DW_DATABASE}")

        # Garantir tabela exist
        create_table_sql = """
CREATE TABLE IF NOT EXISTS dim_data (
    data_sk INT PRIMARY KEY,
    data DATE NOT NULL,
    dia INT NOT NULL,
    mes INT NOT NULL,
    quartil INT NOT NULL,
    semestre INT NOT NULL,
    ano INT NOT NULL,
    semana_do_ano INT NOT NULL,
    dia_da_semana INT NOT NULL,
    eh_final_de_semana TINYINT NOT NULL,
    eh_dia_util TINYINT NOT NULL,
    eh_ano_eleitoral TINYINT NOT NULL,
    tipo_eleicao TINYINT NOT NULL,
    estacao_ano VARCHAR(45) NOT NULL,
    eh_periodo_pandemico TINYINT NOT NULL,
    data_inicio_atividade_empresa DATE,
    data_tramite_status DATE,
    ultima_atualizacao_relatorio_status DATE,
    date_from DATETIME NOT NULL,
    date_to DATETIME NOT NULL,
    version INT NOT NULL,
    ciclo_corte DECIMAL(18,4)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""
        cursor.execute(create_table_sql)
        conn.commit()

        # Buscar datas já existentes para evitar duplicatas
        cursor.execute("SELECT data FROM dim_data")
        existing = {r[0] for r in cursor.fetchall()}

        # Filtrar apenas datas novas
        new_dates = [d for d in dates if d not in existing]

        # --- Atualizar linhas existentes com valores mapeados (bulk via tabela temporária + UPDATE ... JOIN) ---
        # Cria tabela temporária e insere os mapeamentos (um lote). Em seguida, atualiza dim_data
        # em massa juntando pela coluna `data`.
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_dim_map")
        cursor.execute("""
CREATE TEMPORARY TABLE temp_dim_map (
    data DATE PRIMARY KEY,
    data_inicio_atividade_empresa DATE,
    data_tramite_status DATE,
    ultima_atualizacao_relatorio_status DATE,
    ciclo_corte DECIMAL(18,4)
) ENGINE=Memory;
""")

        # Preparar linhas para inserir na tabela temporária (todos os mapeamentos disponíveis)
        temp_rows = []
        for d, mapped in date_map.items():
            inicio_val = safe_date_convert(mapped.get('data_inicio_atividade_empresa'))
            tramite_val = safe_date_convert(mapped.get('data_tramite_status'))
            ultima_val = safe_date_convert(mapped.get('ultima_atualizacao_relatorio_status'))
            ciclo_val = mapped.get('ciclo_corte')
            try:
                ciclo_val = float(ciclo_val) if ciclo_val is not None else None
            except Exception:
                ciclo_val = None
            temp_rows.append((d, inicio_val, tramite_val, ultima_val, ciclo_val))

        if temp_rows:
            insert_temp_sql = (
                "INSERT INTO temp_dim_map (data, data_inicio_atividade_empresa, data_tramite_status, "
                "ultima_atualizacao_relatorio_status, ciclo_corte) VALUES (%s, %s, %s, %s, %s)"
            )
            # inserir em um único lote (ou em batches se for muito grande)
            for i in range(0, len(temp_rows), BATCH_SIZE):
                batch = temp_rows[i:i + BATCH_SIZE]
                cursor.executemany(insert_temp_sql, batch)
            conn.commit()

            # Executar UPDATE ... JOIN para aplicar os valores mapeados nas linhas já existentes
            update_join_sql = """
UPDATE dim_data d
JOIN temp_dim_map t ON d.data = t.data
SET
    d.data_inicio_atividade_empresa = COALESCE(d.data_inicio_atividade_empresa, t.data_inicio_atividade_empresa),
    d.data_tramite_status = COALESCE(d.data_tramite_status, t.data_tramite_status),
    d.ultima_atualizacao_relatorio_status = COALESCE(d.ultima_atualizacao_relatorio_status, t.ultima_atualizacao_relatorio_status),
    d.ciclo_corte = COALESCE(d.ciclo_corte, t.ciclo_corte)
"""
            cursor.execute(update_join_sql)
            updated_count = cursor.rowcount
            conn.commit()
        else:
            updated_count = 0

        print(f"Atualizadas {updated_count} linhas existentes com valores mapeados (se aplicável)")
        if not new_dates:
            print('Nenhuma data nova para inserir. Nada a fazer.')
            return

        # Determinar próximo data_sk
        cursor.execute('SELECT MAX(data_sk) FROM dim_data')
        res = cursor.fetchone()
        max_sk = res[0] if res and res[0] is not None else 0
        starting_sk = max_sk + 1

        rows = build_rows_from_dates(new_dates, starting_sk, date_map=date_map)

        # --- DEBUG: sumarizar mapeamentos antes da inserção ---
        n_with_inicio = sum(1 for r in rows if r[15] is not None)
        n_with_tramite = sum(1 for r in rows if r[16] is not None)
        n_with_ultima = sum(1 for r in rows if r[17] is not None)
        n_with_ciclo = sum(1 for r in rows if r[21] is not None)

        print(f"Mapeamento: {len(rows)} linhas preparadas")
        print(f"  data_inicio_atividade_empresa preenchido em: {n_with_inicio} linhas")
        print(f"  data_tramite_status preenchido em: {n_with_tramite} linhas")
        print(f"  ultima_atualizacao_relatorio_status preenchido em: {n_with_ultima} linhas")
        print(f"  ciclo_corte preenchido em: {n_with_ciclo} linhas")

        print('\nAmostra das primeiras 10 linhas preparadas (data, inicio, tramite, ultima, ciclo):')
        for sample in rows[:10]:
            print(sample[1], sample[15], sample[16], sample[17], sample[21])

        insert_sql = """
INSERT INTO dim_data (
    data_sk, data, dia, mes, quartil, semestre, ano, semana_do_ano,
    dia_da_semana, eh_final_de_semana, eh_dia_util, eh_ano_eleitoral,
    tipo_eleicao, estacao_ano, eh_periodo_pandemico,
    data_inicio_atividade_empresa, data_tramite_status,
    ultima_atualizacao_relatorio_status, date_from, date_to, version, ciclo_corte
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

        # Inserir em lotes
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            cursor.executemany(insert_sql, batch)
            conn.commit()
            print(f'Inserido lote {i//BATCH_SIZE + 1}: {len(batch)} registros')

        print(f'Concluído: {len(rows)} novas datas inseridas em dim_data')

    except Error as e:
        print('Erro ao popular dim_data:', e)
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


if __name__ == '__main__':
    main()
