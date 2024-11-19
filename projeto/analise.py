import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# quantidade de acidentes em 2023
csv_2023 = pd.read_csv('./banco/ACGRBR23.csv')
ids_municipios = [431390, 431020, 431410]  # Panambi, Ijuí, Passo Fundo
dados_filtrados_2023 = csv_2023[csv_2023['ID_MUNICIP'].isin(ids_municipios)]
acidentes_2023 = dados_filtrados_2023.groupby('ID_MUNICIP').size()
acidentes_2023.index = ['Panambi', 'Ijuí', 'Passo Fundo']

# quantidade de acidentes em 2022
csv_2022 = pd.read_csv('./banco/ACGRBR22.csv')  
dados_filtrados_2022 = csv_2022[csv_2022['ID_MUNICIP'].isin(ids_municipios)]
acidentes_2022 = dados_filtrados_2022.groupby('ID_MUNICIP').size()
acidentes_2022.index = ['Panambi', 'Ijuí', 'Passo Fundo']

comparacao = pd.DataFrame({
    '2022': acidentes_2022,
    '2023': acidentes_2023
})

plt.figure(figsize=(10, 6))
comparacao.plot(kind='bar', color=['purple', 'pink'], alpha=0.8)
plt.title('Comparação de Acidentes de Trabalho (2022 vs 2023)')
plt.ylabel('Quantidade de Acidentes')
plt.xlabel('Municípios')
plt.xticks(rotation=0)
plt.legend(title='Ano')
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()

plt.show()

# Porcentagem de acidentes para cada cidade (2022 e 2023)
def calcular_porcentagem(acidentes):
    total_acidentes = acidentes.sum()
    return (acidentes / total_acidentes) * 100

porcentagem_2022 = calcular_porcentagem(acidentes_2022)
porcentagem_2023 = calcular_porcentagem(acidentes_2023)

plt.figure(figsize=(10, 6))

plt.plot(acidentes_2022.index, acidentes_2022, marker='o', color='purple', label='2022', linestyle='-', linewidth=2, markersize=8)

plt.plot(acidentes_2023.index, acidentes_2023, marker='o', color='hotpink', label='2023', linestyle='-', linewidth=2, markersize=8)

for cidade, porcentagem in porcentagem_2022.items():
    plt.text(cidade, acidentes_2022[cidade], f'{porcentagem:.1f}%', fontsize=12, ha='center', va='bottom', color='black')

for cidade, porcentagem in porcentagem_2023.items():
    plt.text(cidade, acidentes_2023[cidade], f'{porcentagem:.1f}%', fontsize=12, ha='center', va='bottom', color='black')

plt.title('Comparação de Acidentes de Trabalho nas Cidades (2022 vs 2023)', fontsize=14)
plt.xlabel('Cidades', fontsize=12)
plt.ylabel('Quantidade de Acidentes', fontsize=12)

plt.legend(title="Ano", loc="upper left")

plt.grid(True, axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()

# calcular faixa etária da população afetada
def extrair_idade(codigo_idade):
    return int(str(codigo_idade)[-2:])

def faixa_etaria(idade):
    if idade < 18:
        return '<18'
    elif 18 <= idade <= 25:
        return '18-25'
    elif 26 <= idade <= 35:
        return '26-35'
    elif 36 <= idade <= 50:
        return '36-50'
    else:
        return '>50'

ids_municipios = {431390: 'Panambi', 431020: 'Ijuí', 431410: 'Passo Fundo'}

def processar_dados(arquivo_csv, ano):
    dados = pd.read_csv(arquivo_csv)
    dados['IDADE_CORRETA'] = dados['NU_IDADE_N'].apply(extrair_idade)
    dados['FAIXA_ETARIA'] = dados['IDADE_CORRETA'].apply(faixa_etaria)
    dados_filtrados = dados[dados['ID_MUNICIP'].isin(ids_municipios.keys())]
    dados_filtrados['CIDADE'] = dados_filtrados['ID_MUNICIP'].map(ids_municipios)
    return dados_filtrados.groupby(['CIDADE', 'FAIXA_ETARIA']).size().unstack(fill_value=0)

faixas_2022 = processar_dados('./banco/ACGRBR22.csv', 2022)
faixas_2023 = processar_dados('./banco/ACGRBR23.csv', 2023)

dados_comparados = {}
for cidade in ids_municipios.values():
    dados_comparados[cidade] = pd.DataFrame({
        '2022': faixas_2022.loc[cidade] if cidade in faixas_2022.index else 0,
        '2023': faixas_2023.loc[cidade] if cidade in faixas_2023.index else 0
    }).fillna(0)

fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(18, 6), sharey=True)
for ax, (cidade, data) in zip(axes, dados_comparados.items()):
    data.plot(kind='bar', ax=ax, color=['skyblue', 'orange'], alpha=0.8)
    ax.set_title(f'Faixas Etárias - {cidade}')
    ax.set_ylabel('Quantidade de Acidentes')
    ax.set_xlabel('Faixa Etária')
    ax.legend(title='Ano')
    ax.grid(axis='x', linestyle='--', alpha=0.7)

plt.suptitle('Faixas Etárias Afetadas por Acidentes de Trabalho (2022 vs 2023)', fontsize=16)
plt.tight_layout(rect=[0, 0, 1, 0.95])

plt.show()

# acidentes mais comuns em cada cidade, comparação com foco econômico
atividades_economicas = {
    'Panambi': 'Indústria',
    'Ijuí': 'Agronegócio',
    'Passo Fundo': 'Comércio e Serviços'
}

tipos_acidentes = {
    'Z209': 'Exp. Material Biológico',
    'Y96': 'Grave/Fatal/Crianças',
    'V01-V09': 'Pedestres (veículos)',
    'V10-V19': 'Ciclistas',
    'V20-V29': 'Motociclistas',
    'V30-V39': 'Ocupantes de carros',
    'V40-V49': 'Ocupantes de caminhões/vans',
    'V80-V89': 'Outros veículos',
    'W00-W19': 'Quedas',
    'W20-W49': 'Forças mecânicas',
    'W50-W64': 'Golpes (animais/pessoas)',
    'W85-W99': 'Eletricidade',
    'X00-X09': 'Fogo/Calor',
    'X10-X19': 'Substâncias corrosivas',
    'X30': 'Calor excessivo',
    'X31': 'Frio excessivo',
    'X33': 'Forças naturais',
    'X40-X44': 'Intoxicação (drogas/medicamentos)',
    'X45': 'Intoxicação por álcool',
    'X46': 'Intoxicação por solventes',
    'X60-X84': 'Lesões autoinfligidas',
    'X85-Y09': 'Agressões',
    'Y10-Y34': 'Intenção indeterminada',
    'Y35': 'Intervenções legais/guerra',
    'Y85-Y89': 'Sequelas de causas externas'
}

def classificar_acidente(cid):
    if pd.isna(cid):  
        return 'Outros'
    cid = str(cid)  
    for codigo, descricao in tipos_acidentes.items():
        if '-' in codigo:  
            inicio, fim = codigo.split('-')
            if inicio <= cid <= fim:
                return descricao
        elif cid == codigo: 
            return descricao
    return 'Outros'

ids_municipios = {431390: 'Panambi', 431020: 'Ijuí', 431410: 'Passo Fundo'}

def processar_dados_por_cidade(arquivo_csv, ano):
    dados = pd.read_csv(arquivo_csv)
    dados = dados[dados['ID_MUNICIP'].isin(ids_municipios.keys())]
    dados['CIDADE'] = dados['ID_MUNICIP'].map(ids_municipios)
    dados['TIPO_ACIDENTE'] = dados['CID_ACID'].apply(classificar_acidente)
    dados['ANO'] = ano
    return dados.groupby(['CIDADE', 'ANO', 'TIPO_ACIDENTE']).size().unstack(fill_value=0)

dados_2022 = processar_dados_por_cidade('./banco/ACGRBR22.csv', 2022)
dados_2023 = processar_dados_por_cidade('./banco/ACGRBR23.csv', 2023)

dados_combinados = pd.concat([dados_2022, dados_2023])

for cidade in ids_municipios.values():
    for ano in [2022, 2023]:
        if cidade in dados_combinados.index:
            dados_cidade_ano = dados_combinados.loc[cidade].loc[ano]
            dados_cidade_ano = dados_cidade_ano[dados_cidade_ano > 0]  
            
            plt.figure(figsize=(8, 6))
            wedges, texts, autotexts = plt.pie(
                dados_cidade_ano, 
                autopct='%1.1f%%', 
                startangle=90, 
                textprops=dict(color="w"), 
                colors=plt.cm.tab20.colors[:len(dados_cidade_ano)]  
            )
            
            plt.legend(
                wedges, 
                dados_cidade_ano.index, 
                title="Tipos de Acidentes", 
                loc="center left", 
                bbox_to_anchor=(1, 0.5), 
                fontsize=10
            )
            
            plt.title(f'Tipos de Acidentes em {cidade} ({ano})')
            plt.tight_layout()
            plt.savefig(f'graficos_acidentes_{cidade}_{ano}.png') 
            plt.close()  

dados_combinados_2022_2023 = pd.concat([dados_2022, dados_2023], keys=[2022, 2023])

top_acidentes_2022 = dados_combinados_2022_2023.loc[2022].apply(lambda x: x.nlargest(3), axis=1)
top_acidentes_2023 = dados_combinados_2022_2023.loc[2023].apply(lambda x: x.nlargest(3), axis=1)

fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(18, 8), sharey=True)

top_acidentes_2022.plot(kind='bar', ax=axes[0], color=['skyblue', 'orange', 'green', 'hotpink', 'purple'], alpha=0.8)
axes[0].set_title('Tipos de Acidentes Mais Comuns em 2022')
axes[0].set_ylabel('Número de Acidentes')
axes[0].set_xlabel('Cidades')
axes[0].set_xticklabels(top_acidentes_2022.index, rotation=0)
axes[0].grid(True, axis='y', linestyle='--', alpha=0.7)

for i, cidade in enumerate(top_acidentes_2022.index):
    atividade = atividades_economicas[cidade]
    axes[0].text(i, top_acidentes_2022.iloc[i].max() + 1, f'Atividade: {atividade}', ha='center', color='black')

top_acidentes_2023.plot(kind='bar', ax=axes[1], color=['skyblue', 'orange', 'green', 'hotpink', 'purple'], alpha=0.8)
axes[1].set_title('Tipos de Acidentes Mais Comuns em 2023')
axes[1].set_ylabel('Número de Acidentes')
axes[1].set_xlabel('Cidades')
axes[1].set_xticklabels(top_acidentes_2023.index, rotation=0)
axes[1].grid(True, axis='y', linestyle='--', alpha=0.7)

for i, cidade in enumerate(top_acidentes_2023.index):
    atividade = atividades_economicas[cidade]
    axes[1].text(i, top_acidentes_2023.iloc[i].max() + 1, f'Atividade: {atividade}', ha='center', color='black')

plt.suptitle('Tipos de Acidentes Mais Comuns por Cidade (2022 e 2023)', fontsize=16)
plt.tight_layout(rect=[0, 0, 1, 0.96])
plt.show()

# análise entre densidade demográfica, acidentes e foco econômico

dados_demograficos_economicos = {
    'Panambi': {'densidade': 68.5, 'pib': 100000000},
    'Ijuí': {'densidade': 48.3, 'pib': 120000000},
    'Passo Fundo': {'densidade': 98.7, 'pib': 200000000},
}

comparacao_acidentes_com_dados = {}
for cidade in ['Panambi', 'Ijuí', 'Passo Fundo']:
    cidade_acidentes_2022 = acidentes_2022[cidade] if cidade in acidentes_2022.index else 0
    cidade_acidentes_2023 = acidentes_2023[cidade] if cidade in acidentes_2023.index else 0
    
    densidade = dados_demograficos_economicos[cidade]['densidade']
    pib = dados_demograficos_economicos[cidade]['pib']
    
    comparacao_acidentes_com_dados[cidade] = {
        'acidentes_2022': cidade_acidentes_2022,
        'acidentes_2023': cidade_acidentes_2023,
        'densidade': densidade,
        'pib': pib
    }

df_comparacao = pd.DataFrame(comparacao_acidentes_com_dados).T

fig, ax1 = plt.subplots(figsize=(10, 6))

largura = 0.2  
posicoes = range(len(df_comparacao))

barras_acidentes_2022 = ax1.bar([p - largura for p in posicoes], df_comparacao['acidentes_2022'], largura, label='Acidentes 2022', color='purple')
barras_acidentes_2023 = ax1.bar([p for p in posicoes], df_comparacao['acidentes_2023'], largura, label='Acidentes 2023', color='pink')

ax2 = ax1.twinx()  
barras_densidade = ax2.bar([p + largura for p in posicoes], df_comparacao['densidade'], largura, label='Densidade Populacional', color='green', alpha=0.6)

ax3 = ax1.twinx()  
ax3.spines['right'].set_position(('outward', 60)) 
barras_pib = ax3.bar([p + 2*largura for p in posicoes], df_comparacao['pib'] / 1000000, largura, label='PIB (milhões)', color='orange', alpha=0.6)

ax1.set_ylabel('Quantidade de Acidentes')
ax1.set_xlabel('Cidade')
ax1.set_title('Comparação de Acidentes de Trabalho, Densidade Populacional e PIB (2022 vs 2023)')
ax1.set_xticks(posicoes)
ax1.set_xticklabels(df_comparacao.index)

ax2.set_ylabel('Densidade Populacional (hab/km²)')
ax3.set_ylabel('PIB (R$ milhões)')

handles, labels = ax1.get_legend_handles_labels()
handles.extend([barras_densidade, barras_pib])  
labels.extend(['Densidade Populacional', 'PIB (milhões)'])  

ax1.legend(handles=handles, labels=labels, loc='upper left')

fig.tight_layout()
plt.show()