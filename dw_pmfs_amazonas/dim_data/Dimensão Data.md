# dim_data — Como rodar o loader `main_real.py`

Este documento descreve, de forma direta e simples, como executar o script `main_real.py` que popula a tabela `dim_data` do DW usando os CSVs reais em `datasets/`.

Pré-requisitos
- Python 3.8+ (recomendado).
- Um ambiente virtual (opcional, mas recomendado).
- MySQL rodando localmente ou acessível e com um usuário que tenha permissão para criar banco/tabelas/atualizar dados.

Arquivos importantes
- `dim_data/main_real.py` — script principal que cria/atualiza a tabela `dim_data` a partir de `datasets/base_pmfs.csv` e `datasets/base_empresa.csv`.
- `datasets/base_pmfs.csv` — fonte de eventos PMFS (tramites, emissões, ciclo_corte, etc.).
- `datasets/base_empresa.csv` — fonte com `DATA_INICIO_ATIVIDADE` das empresas.

Configurar conexão
1. Abra `dim_data/main_real.py` e edite o dicionário `DB_CONFIG` no topo do arquivo com os parâmetros corretos do seu MySQL (host, user, password). O script usará o banco `DW_PMFS_AMAZONAS` por padrão (criado se não existir).

Instalar dependências
1. (Opcional) Crie e ative um venv:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

2. Instale as dependências:

```powershell
pip install pandas numpy mysql-connector-python holidays
```

Executar o script
1. Confirme que os CSVs `datasets/base_pmfs.csv` e `datasets/base_empresa.csv` estão presentes na raiz do projeto.
2. Rode o script:

```powershell
python dim_data\main_real.py
```

O que o script faz (resumo rápido)
- Lê os CSVs e extrai datas relevantes.
- Agrega eventos PMFS por data (média de `CICLO_CORTE` e flags de presença para `DATA_DO_TRAMITE` e `ULTIMA_ATUALIZACAO_RELATORIO`).
- Cria/garante a existência da tabela `dim_data` no banco `DW_PMFS_AMAZONAS`.
- Atualiza em massa linhas já existentes (usando uma tabela temporária e `UPDATE ... JOIN`) para preencher campos mapeados quando estiverem nulos.
- Insere novas datas em lote, se houver.

Observações
- O script tenta converter datas de várias formas; linhas que não puderem ser convertidas serão ignoradas (valores ficarão `NULL`).
- Se precisar que eu rode o script aqui antes do push, diga e eu executo (mas não envio nada ao GitHub sem sua ordem).

Como subir no GitHub
1. Adicione/commit as mudanças no branch atual:

```powershell
git add dim_data\main_real.py dim_data\README.md
git commit -m "dim_data: bulk update + README para execução do loader"
```

2. Push para o branch remoto:

```powershell
git push origin carga-sql
```

Se a equipe encontrar problemas, peça para coletarem o output do script (terminal) e o `print` com o resumo de mapeamento — isso facilita o diagnóstico.

Fim.