import pandas as pd
import altair as alt

# 1. Recriar os dados da sua tabela resultante (imagem)
data = {
    'Modalidade_PMFS': [
        'PMFS em floresta de terra firme',
        'PMFS em floresta nacional, estadual ou municipal',
        'PMFS em floresta pública',
        'PMFS comunitário',
        'PMFS de baixa intensidade',
        'PMFS de floresta primária',
        'PMFS para produção em madeira',
        'PMFS empresarial',
        'PMFS para múltiplos produtos',
        'PMFS Pleno',
        'Área de Reserva Legal',
        'Área de Uso Alternativo do Solo',
        'PMFS de floresta secundária',
        'PMFS individual'
    ],
    'Quantidade_Projetos': [
        10, 7, 15, 16, 12, 122, 117, 82, 84, 329, 135, 54, 26, 87
    ],
    'Duracao_Media_Anos': [
        17.9, 14.3, 13.2, 8.1, 6.1, 4.4, 3.6, 3.1, 2.9, 2.7, 2.6, 1.3, 1.0, 1.0
    ]
}

df = pd.DataFrame(data)

# 2. Criar o gráfico de barras horizontais
base = alt.Chart(df).encode(
    # Eixo Y: Modalidade (Categórico)
    # 'sort='-x'' ordena as categorias pelo valor de x (Duração) em ordem decrescente
    y=alt.Y('Modalidade_PMFS:N', sort='-x', title='Modalidade PMFS'),
    
    # Eixo X: Duração Média (Quantitativo)
    x=alt.X('Duracao_Media_Anos:Q', title='Duração Média da Autorização (Anos)'),
    
    # Tooltip para interatividade (mostrar ao passar o mouse)
    tooltip=['Modalidade_PMFS', 'Duracao_Media_Anos', 'Quantidade_Projetos']
)

# 3. Criar as barras
bars = base.mark_bar()

# 4. Criar os rótulos de texto para o final das barras
text = base.mark_text(
    align='left',
    baseline='middle',
    dx=4  # Desloca o texto 4 pixels para a direita da barra
).encode(
    # Define o texto como o valor da 'Duracao_Media_Anos'
    text=alt.Text('Duracao_Media_Anos:Q', format='.1f'), # Formata para uma casa decimal
    color=alt.value('black') # Cor do texto
)

# 5. Combina as barras e os rótulos de texto
chart = (bars + text).properties(
    title='Duração Média da Autorização por Modalidade PMFS'
).interactive() # Permite zoom e pan

# 6. Salva o gráfico como um arquivo HTML
chart.save('duracao_media_por_modalidade.html')

print("Gráfico 'duracao_media_por_modalidade.json' foi salvo com sucesso.")