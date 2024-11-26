import pandas as pd
import matplotlib.pyplot as plt
import duckdb


csv_path = "./database/OCUPANET.csv"
db_path = "./database/database_cd.db"

conn = duckdb.connect(db_path)


ids_municipios = [431390, 431020, 431410]  
municipios_nome = {431390: 'Panambi', 431020: 'Ijuí', 431410: 'Passo Fundo'}

try:
    cargos = pd.read_csv(csv_path, encoding='ISO-8859-1', on_bad_lines='skip', delimiter=',')  
    cargos.columns = cargos.columns.str.strip().str.replace('"', '')  
    cargos.set_index('ID_OCUPA_N', inplace=True)

except KeyError as e:
    print(f"Erro: {e}. Verifique se a coluna 'ID_OCUPA_N' existe no DataFrame.")
    exit(1)  
except Exception as e:
    print(f"Ocorreu um erro ao ler o arquivo: {e}")
    exit(1)  

def carregar_dados_acidentes(ano):
    query = f"""
    SELECT ID_MUNICIP, ID_OCUPA_N
    FROM dadosacidentetrabalho
    WHERE ID_MUNICIP IN ({','.join(map(str, ids_municipios))})
    AND NU_ANO = {ano}
    """
    return pd.read_sql_query(query, conn)


dados_2022 = carregar_dados_acidentes(2022)
dados_2023 = carregar_dados_acidentes(2023)


for ano, dados in zip([2022, 2023], [dados_2022, dados_2023]):
    if 'ID_OCUPA_N' not in dados.columns:
        print(f"Erro: A coluna 'ID_OCUPA_N' não foi encontrada nos dados de {ano}.")
        exit(1)


dados_2022['Cargo'] = dados_2022['ID_OCUPA_N'].map(cargos['Descricao'])
dados_2023['Cargo'] = dados_2023['ID_OCUPA_N'].map(cargos['Descricao'])


dados_2022['Municipio'] = dados_2022['ID_MUNICIP'].map(municipios_nome)
dados_2023['Municipio'] = dados_2023['ID_MUNICIP'].map(municipios_nome)


def plotar_top_cargos_por_municipio(dados_2022, dados_2023):
    for municipio in municipios_nome.values():
        fig, ax = plt.subplots(figsize=(10, 6))


        dados_municipio_2022 = dados_2022[dados_2022['Municipio'] == municipio]['Cargo'].value_counts().head(5)
        dados_municipio_2023 = dados_2023[dados_2023['Municipio'] == municipio]['Cargo'].value_counts().head(5)


        df_plot = pd.DataFrame({
            '2022': dados_municipio_2022,
            '2023': dados_municipio_2023
        }).fillna(0)


        df_plot.plot(kind='bar', ax=ax, color=['purple', 'pink'], alpha=0.8)


        ax.set_title(f'Top 5 Cargos Mais Afetados em {municipio} (2022 vs 2023)', fontsize=14)
        ax.set_ylabel('Número de Acidentes', fontsize=12)
        ax.set_xlabel('Cargo', fontsize=2)
        ax.tick_params(axis='x', rotation=45)
        ax.legend(title='Ano', fontsize=5)

        plt.tight_layout()
        plt.show()

plotar_top_cargos_por_municipio(dados_2022, dados_2023)

conn.close()


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
        return ' 36-50'
    else:
        return '>50'

# FunÃ§Ã£o para processar dados de acidentes por faixa etÃ¡ria
def processar_dados(ano):
    query = f"""
    SELECT ID_MUNICIP, NU_IDADE_N
    FROM dadosacidentetrabalho
    WHERE NU_ANO = {ano} AND ID_MUNICIP IN ({','.join(map(str, ids_municipios))})
    """
    dados = pd.read_sql_query(query, conn)
    dados['IDADE_CORRETA'] = dados['NU_IDADE_N'].apply(extrair_idade)
    dados['FAIXA_ETARIA'] = dados['IDADE_CORRETA'].apply(faixa_etaria)
    dados['CIDADE'] = dados['ID_MUNICIP'].map(municipios_nome)
    return dados.groupby(['CIDADE', 'FAIXA_ETARIA']).size().unstack(fill_value=0)

# Processar dados de faixas etÃ¡rias para 2022 e 2023
faixas_2022 = processar_dados(2022)
faixas_2023 = processar_dados(2023)

# GrÃ¡ficos de faixas etÃ¡rias
fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(18, 6), sharey=True)
for ax, (cidade, nome) in zip(axes, municipios_nome.items()):
    data = pd.DataFrame({
        '2022': faixas_2022.loc[nome] if nome in faixas_2022.index else 0,
        '2023': faixas_2023.loc[nome] if nome in faixas_2023.index else 0
    }).fillna(0)
    data.plot(kind='bar', ax=ax, color=['skyblue', 'orange'], alpha=0.8)
    ax.set_title(f'Faixas EtÃ¡rias - {nome}')
    ax.set_ylabel('Quantidade de Acidentes')
    ax.set_xlabel('Faixa EtÃ¡ria')
    ax.legend(title='Ano')
    ax.grid(axis='x', linestyle='--', alpha=0.7)

plt.suptitle('Faixas EtÃ¡rias Afetadas por Acidentes de Trabalho (2022 vs 2023)', fontsize=16)
plt.tight_layout(rect=[0, 0, 1, 0.95])
plt.show()